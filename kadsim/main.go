package main

import (
	"flag"
	"fmt"
	"math/rand"

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
var adversarialNet *bool
var seed *int64
var verbose *bool

var AdversaryList = []*KADNode{}
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
			f := rand.Float64() < *failureRatio
			n = NewKADNode(i, k, f)
			if f {
				if *adversarialNet {
					AdversaryList = append(AdversaryList, n) // only used when F_COLLAB
				}
			} else {
				NormalList = append(NormalList, n)
			}
		}
		nodes = append(nodes, n)
	}
	// join time none
	//****
	//bak := FailureType
	//FailureType = F_NONE
	//****

	//err := FastJoinAll(nodes, alpha)
	err := FastJoinAllDisjoint(convergeTimes, nodes, alpha, k, d)
	//err := FastJoinAll(convergeTimes, nodes, alpha, k, d)
	if err != nil {
		fmt.Printf("join failed:%s\n", err)
	}
	// restore for afterwards
	//****
	//FailureType = bak
	//****
	//aves, _ := stats.Mean(funk.Map(nodes, func(n *KADNode) float64 { return float64(n.routingTable.table.Size()) }).([]float64))
	//ayame.Log.Infof("avg. routing table size: %f\n", aves)
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
			if !*adversarialNet && n.isFailure {
				AdversaryList = append(AdversaryList, n) // only used when F_COLLAB
			}
			n.routingTable.Add(nodes[index])
			_, _, msgs, _, _ := FastNodeLookup(n.routingTable.dhtId, n, alpha, k)
			sumMsgs += msgs
		}
	}
	ayame.Log.Infof("avg-join-msgs: %d %f %f\n", *numberOfNodes, *failureRatio, float64(sumMsgs)/float64(len(nodes)))
	count := 0
	fcount := 0
	for _, n := range NormalList {
		c, f := n.routingTable.Count()
		count += c
		fcount += f
	}
	ayame.Log.Infof("polluted-entry-ratio: %d %f %f\n", *numberOfNodes, *failureRatio, float64(fcount)/float64(count))

	FailureType = bak
	return nil
}

func FastJoinAllDisjoint(convergeTimes int, nodes []*KADNode, alpha int, k int, d int) error {
	//	bak := FailureType
	//	FailureType = F_NONE
	sumMsgs := 0
	index := 0 // introducer index
	for _, n := range nodes {
		if !*adversarialNet && n.isFailure {
			AdversaryList = append(AdversaryList, n) // only used when F_COLLAB
		}
		n.routingTable.Add(nodes[index])
		_, _, msgs, _, _ := FastNodeLookupDisjoint(n.routingTable.dhtId, n, alpha, k, d)
		sumMsgs += msgs
	}
	for i := 0; i < convergeTimes; i++ { // search a randomly picked normal node
		for _, n := range nodes {
			dst := rand.Intn(len(NormalList))
			_, _, msgs, _, _ := FastNodeLookupDisjoint(nodes[dst].routingTable.dhtId, n, alpha, k, d)
			sumMsgs += msgs
		}
	}
	ayame.Log.Infof("avg-join-msgs: %d %f %f\n", *numberOfNodes, *failureRatio, float64(sumMsgs)/float64(len(nodes)))

	count := 0
	fcount := 0
	for _, n := range NormalList {
		c, f := n.routingTable.Count()
		count += c
		fcount += f
	}
	ayame.Log.Infof("polluted-entry-ratio: %d %f %f\n", *numberOfNodes, *failureRatio, float64(fcount)/float64(count))

	//	FailureType = bak
	return nil
}

func main() {
	alpha = flag.Int("alpha", 1, "the parallelism parameter")
	kValue = flag.Int("k", 8, "the bucket size parameter")
	dValue = flag.Int("d", 4, "the disjoint path parameter")
	numberOfNodes = flag.Int("nodes", 10000, "number of nodes")
	numberOfTrials = flag.Int("trials", -1, "number of trials (-1 means same as nodes)")
	convergeTimes = flag.Int("c", 3, "converge times")
	failureType = flag.String("type", "collab", "failure type {none|stop|collab}")
	failureRatio = flag.Float64("f", 0.05, "failure ratio")
	adversarialNet = flag.Bool("adv", true, "use an adversarial network for collaborative attack")
	seed = flag.Int64("seed", 1, "give a random seed")
	verbose = flag.Bool("v", false, "verbose output")

	flag.Parse()

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	rand.Seed(*seed)

	// At JOIN, normal behavior
	switch *failureType {
	case "none":
		FailureType = F_NONE
	case "stop":
		FailureType = F_STOP
	case "collab":
		FailureType = F_COLLAB
	}

	nodes := ConstructOverlay(*numberOfNodes, *convergeTimes, *alpha, *kValue, *dValue)
	//numberOfTrials := *numberOfNodes * 6

	path_lengths := []float64{}
	nums_msgs := []int{}
	match_lengths := []float64{}
	failures := 0

	trials := *numberOfNodes
	if *numberOfTrials > 0 {
		trials = *numberOfTrials
	}

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
	ayame.Log.Infof("avg-paths-length: %d %f\n", *numberOfNodes, pmean)
	hmean, _ := stats.Mean(match_lengths)
	ayame.Log.Infof("avg-match-hops: %d %f\n", *numberOfNodes, hmean)
	ayame.Log.Infof("avg-msgs: %d %f\n", *numberOfNodes, meanOfInt(nums_msgs))
	ayame.Log.Infof("success-ratio: %d %f %f\n", *numberOfNodes, *failureRatio, 1-float64(failures)/float64(trials))

	table_sizes := []int{}
	for _, node := range nodes {
		//ayame.Log.Infof("%v\n", node.Id())
		table_sizes = append(table_sizes, node.routingTable.table.Size())
	}

	ayame.Log.Infof("avg-table-size: %d %f\n", *numberOfNodes, meanOfInt(table_sizes))

}
