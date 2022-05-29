package main

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	"github.com/montanaflynn/stats"
	"github.com/op/go-logging"
	"github.com/piax/go-ayame/ayame"
	"github.com/thoas/go-funk"
)

var alpha *int
var kValue *int
var dValue *int
var numberOfNodes *int
var numberOfTrials *int
var convergeTimes *int
var failureType *string
var failureRatio *float64
var expType *string
var seed *int64
var verbose *bool

var AdversaryList = []*KADNode{}
var JoinedAdversaryList = []*KADNode{}
var NormalList = []*KADNode{}

func ConstructOverlay(numberOfNodes int, convergeTimes int, alpha int, k int, d int) []*KADNode {
	nodes := make([]*KADNode, 0, numberOfNodes)
	for i := 0; i < numberOfNodes; i++ {
		var n *KADNode
		switch FailureType {
		case F_NONE:
			n = NewKADNode(i, k, false)
			NormalList = append(NormalList, n)
		case F_STOP:
			fallthrough
		case F_COLLAB:
			fallthrough
		case F_COLLAB_AFTER:
			f := rand.Float64() < *failureRatio
			n = NewKADNode(i, k, f)
			if f {
				AdversaryList = append(AdversaryList, n)
			} else {
				NormalList = append(NormalList, n)
			}
		}
		nodes = append(nodes, n)
	}
	// join time none
	//****
	bak := false
	if FailureType == F_COLLAB_AFTER {
		FailureType = F_NONE
		bak = true
	}
	err := FastJoinAllDisjoint(convergeTimes, nodes, alpha, k, d)
	if err != nil {
		fmt.Printf("join failed:%s\n", err)
	}
	// restore the failure type for search time
	if bak {
		FailureType = F_COLLAB_AFTER
	}
	return nodes
}

func meanOfInt(lst []int) float64 {
	v, _ := stats.Mean(funk.Map(lst, func(x int) float64 { return float64(x) }).([]float64))
	return v
}

func FastJoinAll(convergeTimes int, nodes []*KADNode, alpha int, k int, d int) error {
	bak := FailureType
	FailureType = F_NONE
	sumMsgs := 0
	for i := 0; i < convergeTimes; i++ {
		index := 0 // introducer index
		for _, n := range nodes {
			if n.isFailure && FailureType == F_COLLAB {
				AdversaryList = append(AdversaryList, n) // only used when F_COLLAB
			}
			n.routingTable.Add(nodes[index])
			_, _, msgs, _, _ := FastNodeLookup(n.routingTable.dhtId, n, alpha, k)
			sumMsgs += msgs
		}
	}
	ayame.Log.Infof("avg-join-msgs: %s %f\n", paramsString, float64(sumMsgs)/float64(len(nodes)))
	count := 0
	fcount := 0
	for _, n := range NormalList {
		c, f := n.routingTable.Count()
		count += c
		fcount += f
	}
	ayame.Log.Infof("polluted-entry-ratio: %s %f\n", paramsString, float64(fcount)/float64(count))

	FailureType = bak
	return nil
}

func min(a int, b int) int {
	if a <= b {
		return a
	}

	return b
}

func FastRefreshCpl(node *KADNode, cpl uint, alpha int, k int, d int) int {
	// gen a key for the query to refresh the cpl
	keyGenFnc := func(cpl uint) (string, error) {
		p, err := node.routingTable.table.GenRandPeerID(cpl)
		return string(p), err
	}

	key, err := keyGenFnc(cpl)
	if err != nil {
		fmt.Printf("failed to generated query key for cpl=%d, err=%s", cpl, err)
		return 0
	}

	targetKadID := kbucket.ConvertPeerID(peer.ID(key)) //kbucket.ConvertKey(key)
	//fmt.Printf("query: %x\n", key)
	_, _, msgs, _, _ := FastNodeLookupDisjoint(targetKadID, node, alpha, k, d)

	return msgs
}

func FastRefreshCplIfEligible(node *KADNode, cpl uint, lastRefreshedAt time.Time, alpha int, k int, d int) int {
	// gen a key for the query to refresh the cpl
	if time.Since(lastRefreshedAt) <= 1000 {
		return 0
	}
	return FastRefreshCpl(node, cpl, alpha, k, d)
}

func FastDoRefresh(node *KADNode, alpha, k, d int) int {
	refreshCpls := node.routingTable.table.GetTrackedCplsForRefresh()
	msgs := 0
	//fmt.Printf("refresh cpls=%d\n", len(refreshCpls))
	for c := range refreshCpls {
		cpl := uint(c)

		msgs += FastRefreshCplIfEligible(node, cpl, refreshCpls[cpl], alpha, k, d)
		// If we see a gap at a Cpl in the Routing table, we ONLY refresh up until the maximum cpl we
		// have in the Routing Table OR (2 * (Cpl+ 1) with the gap), whichever is smaller.
		// This is to prevent refreshes for Cpls that have no peers in the network but happen to be before a very high max Cpl
		// for which we do have peers in the network.
		// The number of 2 * (Cpl + 1) can be proved and a proof would have been written here if the programmer
		// had paid more attention in the Math classes at university.
		// So, please be patient and a doc explaining it will be published soon.
		if node.routingTable.table.NPeersForCpl(cpl) == 0 {
			lastCpl := min(2*(c+1), len(refreshCpls)-1)
			for i := c + 1; i < lastCpl+1; i++ {
				//fmt.Println("fill gap refresh")
				msgs += FastRefreshCplIfEligible(node, uint(i), refreshCpls[cpl], alpha, k, d)
			}
		}
	}
	return msgs
}

func FastJoinAllDisjoint(convergeTimes int, nodes []*KADNode, alpha int, k int, d int) error {
	//	bak := FailureType
	//	FailureType = F_NONE
	sumMsgs := 0
	index := 0 // introducer index
	for _, n := range nodes {
		if n.isFailure {
			JoinedAdversaryList = append(JoinedAdversaryList, n)
		}
		if !bytes.Equal(n.routingTable.dhtId, nodes[index].routingTable.dhtId) {
			n.routingTable.Add(nodes[index])
		}
		// query for self
		_, _, msgs, _, _ := FastNodeLookupDisjoint(n.routingTable.dhtId, n, alpha, k, d)
		sumMsgs += msgs
	}
	for _, n := range nodes {
		sumMsgs += FastDoRefresh(n, alpha, k, d)
	}
	/*
		for i := 0; i < convergeTimes; i++ { // search a randomly picked normal node
			for _, n := range nodes {
				dst := rand.Intn(len(NormalList))
				_, _, msgs, _, _ := FastNodeLookupDisjoint(nodes[dst].routingTable.dhtId, n, alpha, k, d)
				sumMsgs += msgs
			}
		}*/
	ayame.Log.Infof("avg-join-msgs: %s %f\n", paramsString, float64(sumMsgs)/float64(len(nodes)))

	count := 0
	fcount := 0
	for _, n := range NormalList {
		c, f := n.routingTable.Count()
		count += c
		fcount += f
	}
	ayame.Log.Infof("polluted-entry-ratio: %s %f\n", paramsString, float64(fcount)/float64(count))

	//	FailureType = bak
	return nil
}

const (
	EACH_UNICAST_TRIALS = 100
	EACH_UNICAST_TIMES  = 500
)

func expIterative(trials int) {
	path_lengths := []float64{}
	nums_msgs := []int{}
	match_lengths := []float64{}
	failures := 0

	for i := 1; i <= trials; i++ {
		src := rand.Intn(len(NormalList))
		dst := rand.Intn(len(NormalList))
		//founds, hops, msgs, hops_to_match, failure := FastNodeLookup(nodes[dst].routingTable.dhtId, nodes[src], *alpha)
		founds, hops, msgs, hops_to_match, failure := FastNodeLookupDisjoint(NormalList[dst].routingTable.dhtId, NormalList[src], *alpha, *kValue, *dValue)
		path_lengths = append(path_lengths, hops)
		nums_msgs = append(nums_msgs, msgs)
		if !failure {
			match_lengths = append(match_lengths, hops_to_match)
		}
		if failure {
			failures++
		}
		ayame.Log.Debugf("%d->%d: avg. results: %d, hops: %f, msgs: %d, hops_to_match: %f, fails: %d\n", src, dst, len(founds), hops, msgs, hops_to_match, failures)
	}
	pmean, _ := stats.Mean(path_lengths)
	ayame.Log.Infof("avg-paths-length: %s %f\n", paramsString, pmean)
	hmean, _ := stats.Mean(match_lengths)
	ayame.Log.Infof("avg-match-hops: %s %f\n", paramsString, hmean)
	ayame.Log.Infof("avg-msgs: %s %f\n", paramsString, meanOfInt(nums_msgs))
	ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-float64(failures)/float64(trials))
}

func expEachIterative() {
	match_lengths := []float64{}
	failures := 0
	count := 0
	sum_max_hops := 0.0
	sum_max_msgs := 0
	nums_msgs := []int{}
	for i := 1; i <= EACH_UNICAST_TRIALS; i++ {
		src := rand.Intn(len(NormalList))
		max_hops := 0.0
		max_msgs := 0
		for j := 1; j <= EACH_UNICAST_TIMES; j++ {
			count++
			dst := rand.Intn(len(NormalList))
			founds, _, msgs, hops_to_match, failure := FastNodeLookupDisjoint(NormalList[dst].routingTable.dhtId, NormalList[src], *alpha, *kValue, *dValue)
			if max_hops < hops_to_match {
				max_hops = hops_to_match
			}
			if max_msgs < msgs {
				max_msgs = msgs
			}
			nums_msgs = append(nums_msgs, msgs)
			if !failure {
				match_lengths = append(match_lengths, float64(hops_to_match))
			} else {
				failures++
				ayame.Log.Infof("%s->%s: FAILURE!!! %s\n", src, dst, ayame.SliceString(founds))
			}
			ayame.Log.Debugf("%d: nodes=%d, src=%s, dst=%s\n", int64(count*1000), len(NormalList), src, dst)
		}
		sum_max_hops += max_hops
		sum_max_msgs += max_msgs
	}
	ayame.Log.Infof("avg-max-hops: %s %f\n", paramsString, float64(sum_max_hops)/float64(EACH_UNICAST_TRIALS))
	ayame.Log.Infof("avg-max-msgs: %s %f\n", paramsString, float64(sum_max_msgs)/float64(EACH_UNICAST_TRIALS))
	hmean, _ := stats.Mean(match_lengths)
	ayame.Log.Infof("avg-match-hops: %s %f\n", paramsString, hmean)
	ayame.Log.Infof("avg-msgs: %s %f\n", paramsString, meanOfInt(nums_msgs))
	ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-float64(failures)/float64(EACH_UNICAST_TRIALS*EACH_UNICAST_TIMES))
}

var paramsString string

func main() {
	alpha = flag.Int("alpha", 3, "the parallelism parameter")
	kValue = flag.Int("k", 16, "the bucket size parameter")
	dValue = flag.Int("d", 1, "the disjoint path parameter")
	numberOfNodes = flag.Int("nodes", 1000, "number of nodes")
	numberOfTrials = flag.Int("trials", -1, "number of search trials (-1 means same as nodes)")
	convergeTimes = flag.Int("c", 3, "converge times")
	failureType = flag.String("type", "collab", "failure type {none|stop|collab|collab-after}")
	expType = flag.String("expType", "each", "each|uni")
	failureRatio = flag.Float64("f", 0.0, "failure ratio")
	seed = flag.Int64("seed", 1, "give a random seed")
	verbose = flag.Bool("v", false, "verbose output")

	flag.Parse()

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	if *seed > 0 {
		rand.Seed(*seed)
	}
	// At JOIN, normal behavior
	switch *failureType {
	case "none":
		FailureType = F_NONE
	case "stop":
		FailureType = F_STOP
	case "collab":
		FailureType = F_COLLAB
	case "collab-after":
		FailureType = F_COLLAB_AFTER
	default:
		panic(fmt.Errorf("no such type %s", *failureType))
	}
	trials := *numberOfNodes
	if *numberOfTrials > 0 {
		trials = *numberOfTrials
	}

	paramsString = fmt.Sprintf("%d %d %d %d %.2f %s %d", *numberOfNodes, *kValue, *dValue, *alpha, *failureRatio, *failureType, *convergeTimes)

	nodes := ConstructOverlay(*numberOfNodes, *convergeTimes, *alpha, *kValue, *dValue)
	//numberOfTrials := *numberOfNodes * 6

	switch *expType {
	case "each":
		expEachIterative()
	case "uni":
		expIterative(trials)
	default:
		panic(fmt.Errorf("no such experience type %s", *expType))
	}

	table_sizes := []int{}
	for _, node := range nodes {
		//ayame.Log.Infof("%v\n", node.Id())
		table_sizes = append(table_sizes, node.routingTable.table.Size())
	}

	ayame.Log.Infof("avg-table-size: %s %f\n", paramsString, meanOfInt(table_sizes))

}
