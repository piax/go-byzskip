package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/op/go-logging"
	"github.com/piax/go-ayame/ayame"
	bs "github.com/piax/go-ayame/byzskip"
	"github.com/thoas/go-funk"
)

var alpha *int
var kValue *int
var numberOfNodes *int
var numberOfTrials *int
var failureType *string
var failureRatio *float64
var joinType *string
var unicastType *string
var uniRoutingType *string
var experiment *string
var seed *int64
var verbose *bool

//var AdversaryList = []*BSNode{}
var JoinedAdversaryList = []*BSNode{}
var NormalList = []*BSNode{}

const (
	J_CHEAT int = iota
	J_RECUR
	J_ITER
	J_ITER_P
)

const (
	U_RECUR int = iota
	U_ITER
)

var JoinType int
var UnicastType int
var PiggybackJoinRequest bool

const (
	CPU_PROFILE = false
)

/*func containedNumber(ts []int, rs []int) int {
	count := 0
	for _, t := range ts {
		found := false
		for _, r := range rs {
			if t == r {
				found = true
				break
			}
		}
		if found {
			count++
		}
	}
	return count
}

//var DummyNodes []*MIRONode
/*
func correctEntryRatio(nodes []*BSNode) float64 {
	sumContained := 0
	sum := 0
	for i := 0; i < *numberOfNodes; i++ {
		keys := DummyNodes[i].AllKeys()
		looked := nodes[i].routingTable.AllKeys()
		contained := containedNumber(looked, keys)
		ayame.Log.Debugf("%d: %f %s should be %s", i, float64(contained)/float64(len(keys)), IntSliceString(looked), IntSliceString(keys))
		sumContained += contained
		sum += len(keys)
	}
	return float64(sumContained) / float64(sum)
}*/

func ComputeProbabilityMonteCarlo(msg *BSUnicastEvent, failureRatio float64, count int) float64 {
	src := msg.Sender().(*BSNode)
	var dst *BSNode
	for _, dstNode := range msg.root.destinations {
		if dstNode.key == msg.targetKey {
			dst = dstNode
			break
		}
	}
	if dst == nil {
		ayame.Log.Errorf("dst node not found")
	} //else {
	//ayame.Log.Infof("**** dst=%d\n", dst.key)
	//}

	graph := make(Graph)
	for _, pes := range msg.root.paths {
		//fmt.Printf("path: %s\n", ayame.SliceString(pes))
		var prev *BSNode = nil
		for _, pe := range pes {
			this := pe.node.(*BSNode)
			graph.Register(this)
			if prev != nil && prev != this {
				graph.AddChild(prev, this)
			}
			prev = this
		}
	}
	failures := 0
	for i := 0; i < count; i++ {
		graphCopy := make(Graph)
		for key, value := range graph {
			if key != src.key.String() && key != dst.key.String() && rand.Float64() < failureRatio {
				graphCopy[key] = nil // failure
			} else {
				graphCopy[key] = value
			}
		}
		path := make(Array, 0, 50)
		_, exists := graphCopy.PathExists(src, dst, path)

		if !exists {
			failures++
		}
	}
	return float64(failures) / float64(count)
}

func ConstructOverlay(numberOfNodes int) []*BSNode {
	nodes := make([]*BSNode, 0, numberOfNodes)
	for i := 0; i < numberOfNodes; i++ {
		var n *BSNode
		mv := ayame.NewMembershipVector(bs.ALPHA)
		switch FailureType {
		case F_CALC:
			fallthrough
		case F_NONE:
			n = NewBSNode(ayame.IntKey(i), mv, NewBSRoutingTable, false)
			NormalList = append(NormalList, n)
		case F_STOP:
			f := rand.Float64() < *failureRatio
			if i < bs.K { // first k nodes should not be a fault node
				f = false
			}
			if f {
				n = NewBSNode(ayame.IntKey(i), mv, NewStopRoutingTable, f)
			} else {
				n = NewBSNode(ayame.IntKey(i), mv, NewBSRoutingTable, f)
				NormalList = append(NormalList, n)
			}

		case F_COLLAB:
			fallthrough
		case F_COLLAB_AFTER:
			f := rand.Float64() < *failureRatio
			if i < bs.K { // first k nodes should not be a fault node
				f = false
			}
			if f {
				n = NewBSNode(ayame.IntKey(i), mv, NewAdversaryRoutingTable, f)
			} else {
				n = NewBSNode(ayame.IntKey(i), mv, NewBSRoutingTable, f)
				NormalList = append(NormalList, n)
			}
		}
		nodes = append(nodes, n)
	}

	bak := false
	if FailureType == F_COLLAB_AFTER {
		FailureType = F_NONE
		bak = true
	}
	switch JoinType {
	case J_CHEAT:
		err := FastJoinAllByCheat(nodes)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	case J_RECUR:
		// shuffle the join order
		// first K nodes is preserved.
		first := nodes[0:bs.K]
		last := nodes[bs.K:]
		rand.Shuffle(len(last), func(i, j int) { last[i], last[j] = last[j], last[i] })
		nodes := append(first, last...)
		err := FastJoinAllByRecursive(nodes)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	case J_ITER:
		fallthrough
	case J_ITER_P:
		first := nodes[0:bs.K]
		last := nodes[bs.K:]
		rand.Shuffle(len(last), func(i, j int) { last[i], last[j] = last[j], last[i] })
		nodes := append(first, last...)
		err := FastJoinAllByLookup(nodes)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	}
	if bak {
		FailureType = F_COLLAB_AFTER
	}
	return nodes
}

func meanOfInt(lst []int) float64 {
	v, _ := stats.Mean(funk.Map(lst, func(x int) float64 { return float64(x) }).([]float64))
	return v
}

func isFaultySet(nodes []*BSNode) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, n := range nodes {
		if !n.isFailure {
			return false
		}
	}
	// all faulty nodes!
	return true
}

func FastJoinAllByRecursive(nodes []*BSNode) error {
	index := 0 // introducer index
	sumMsgs := 0
	count := 0
	prev := 0
	allFaultyCount := 0
	for i, n := range nodes {
		if index != i {
			localn := n
			locali := i
			if localn.isFailure {
				JoinedAdversaryList = append(JoinedAdversaryList, localn)
				for _, p := range JoinedAdversaryList {
					if !p.Equals(localn) {
						ptable, pok := p.routingTable.(*AdversaryRoutingTable)
						if pok {
							ptable.AddAdversarial(localn)
							ntable, nok := localn.routingTable.(*AdversaryRoutingTable)
							if nok {
								ntable.AddAdversarial(p)
							}
						}
					}
				}
			}
			msg := NewBSUnicastEvent(nodes[index], ayame.MembershipVectorSize, localn.key)
			ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {
				localn.SendEvent(msg)
				localn.Sched(ayame.NewSchedEventWithJob(func() {

					sumMsgs += len(msg.results) // number of reply messages

					//
					localn.routingTable.Add(nodes[index])

					umsgs, hijacked := FastUpdateNeighbors(localn, nodes[index], msg.results, []*BSNode{})
					sumMsgs += umsgs
					if hijacked {
						allFaultyCount++
					}
					count++
					percent := 100 * count / len(nodes)
					if percent/10 != prev {
						fmt.Printf("%s %d percent of %d nodes (%d hijacked)\n", time.Now(), percent, len(nodes), allFaultyCount)
					}
					prev = percent / 10
				}), int64(100)) // runs after 100ms time out
			}), int64(locali*1000))
		}
	}
	ayame.GlobalEventExecutor.Sim(int64(len(nodes)*1000+200), true)
	ayame.GlobalEventExecutor.AwaitFinish()
	//fmt.Printf("ev count %d\n", ayame.GlobalEventExecutor.EventCount)
	ayame.Log.Infof("avg-join-lookup-msgs: %s %f\n", paramsString, float64(ayame.GlobalEventExecutor.EventCount)/float64(count))
	ayame.Log.Infof("avg-join-msgs: %s %f\n", paramsString, float64(sumMsgs+ayame.GlobalEventExecutor.EventCount)/float64(count))

	ncount := 0
	fcount := 0
	for _, n := range NormalList {
		c, f := CountTableEntries(n)
		ncount += c
		fcount += f
	}
	ayame.Log.Infof("polluted-entry-ratio: %s %f\n", paramsString, float64(fcount)/float64(ncount))

	return nil
}

func CountTableEntries(node *BSNode) (int, int) {
	lst := []*BSNode{}
	table := node.routingTable
	fcount := 0
	for _, levelTable := range table.GetNeighborLists() {
		for _, node := range levelTable.Neighbors[bs.LEFT] {
			exists := false
			n := node.(*BSNode)
			if lst, exists = appendIfMissingWithCheck(lst, n); !exists {
				if n.isFailure {
					fcount++
				}
			}
		}
		for _, node := range levelTable.Neighbors[bs.RIGHT] {
			exists := false
			n := node.(*BSNode)
			if lst, exists = appendIfMissingWithCheck(lst, n); !exists {
				if n.isFailure {
					fcount++
				}
			}
		}
	}
	return len(lst), fcount
}

func FastJoinAllByLookup(nodes []*BSNode) error {
	index := 0 // introducer index
	sumMsgs := 0
	sumLMsgs := 0
	count := 0
	prev := 0
	faultyCount := 0
	for i, n := range nodes {
		if n.isFailure {
			JoinedAdversaryList = append(JoinedAdversaryList, n)
			for _, p := range JoinedAdversaryList {
				if !p.Equals(n) {
					ptable, pok := p.routingTable.(*AdversaryRoutingTable)
					if pok {
						ptable.AddAdversarial(n)
						ntable, nok := n.routingTable.(*AdversaryRoutingTable)
						if nok {
							ntable.AddAdversarial(p)
						}
					}
				}
			}
		}
		if index != i {
			ayame.Log.Debugf("%d: introducer= %s, search=%s\n", n.Key(), nodes[index].String(), n.String())
			rets, _, msgs, _, lmsgs, faulty := FastNodeLookup(n, nodes[index])
			if faulty {
				faultyCount++
			}
			ayame.Log.Debugf("%d: rets %s\n", n.key, ayame.SliceString(rets))
			sumMsgs += msgs
			sumLMsgs += lmsgs
		}

		count++
		percent := 100 * count / len(nodes)
		if percent/10 != prev {
			fmt.Printf("%s %d percent of %d nodes (%d hijacked)\n", time.Now(), percent, len(nodes), faultyCount)
		}
		prev = percent / 10
	}
	ayame.Log.Infof("avg-join-lookup-msgs: %s %f\n", paramsString, float64(sumLMsgs)/float64(count))
	ayame.Log.Infof("avg-join-msgs: %s %f\n", paramsString, float64(sumMsgs)/float64(count))

	ncount := 0
	fcount := 0
	for _, n := range NormalList {
		c, f := CountTableEntries(n)
		ncount += c
		fcount += f
	}
	ayame.Log.Infof("polluted-entry-ratio: %s %f\n", paramsString, float64(fcount)/float64(ncount))

	return nil
}

func (pe PathEntry) String() string {
	return pe.node.Id()
}

func hops(lst []PathEntry, dstKey ayame.Key) (float64, bool) {
	ret := float64(0)
	prev := lst[0].node.(*BSNode)
	found := false
	for i, pe := range lst {
		if dstKey.Equals(pe.node.(*BSNode).Key()) {
			found = true
		}
		if i != 0 && !prev.Equals(pe.node.(*BSNode)) {
			ret++
		}
		prev = pe.node.(*BSNode)
	}
	///ayame.Log.Debugf("%s, %f\n", lst, ret)
	return ret, found
}

func minHops(lst [][]PathEntry, dstKey ayame.Key) (float64, bool) {
	for _, path := range lst {
		if ret, ok := hops(path, dstKey); ok {
			return ret, true
		}
	}
	return 0, false
}

func meanOfPathLength(lst [][]PathEntry) (float64, error) {
	return stats.Mean(funk.Map(lst, func(x []PathEntry) float64 { return float64(len(x)) }).([]float64))
}

func maxPathLength(lst [][]PathEntry) (float64, error) {
	return stats.Max(funk.Map(lst, func(x []PathEntry) float64 { return float64(len(x)) }).([]float64))
}

func minPathLength(lst [][]PathEntry) (float64, error) {
	return stats.Min(funk.Map(lst, func(x []PathEntry) float64 { return float64(len(x)) }).([]float64))
}

var paramsString string

// joinType cheat|recur|iter|iter-p
// unicastType i|r

func main() {
	alpha = flag.Int("alpha", 3, "the alphabet size of the membership vector")
	kValue = flag.Int("k", 4, "the redundancy parameter")
	numberOfNodes = flag.Int("nodes", 32, "number of nodes")
	numberOfTrials = flag.Int("trials", -1, "number of search trials (-1 means same as nodes)")
	failureType = flag.String("type", "collab", "failure type {none|stop|collab|collab-after|calc}")
	failureRatio = flag.Float64("f", 0.5, "failure ratio")
	joinType = flag.String("joinType", "cheat", "join type {cheat|recur|iter|iter-p|iter-pp}")
	unicastType = flag.String("unicastType", "recur", "unicast type {recur|iter}")
	uniRoutingType = flag.String("uniRoutingType", "single", "unicast routing type {single|prune|prune-opt1|prune-opt2}")
	experiment = flag.String("exp", "uni", "experiment type {uni|uni-each|join}")
	seed = flag.Int64("seed", 3, "give a random seed")
	verbose = flag.Bool("v", false, "verbose output")

	flag.Parse()

	bs.InitK(*kValue)

	bs.ALPHA = *alpha

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	rand.Seed(*seed)

	switch *failureType {
	case "none":
		FailureType = F_NONE
	case "stop":
		FailureType = F_STOP
	case "collab":
		FailureType = F_COLLAB
	case "collab-after":
		FailureType = F_COLLAB_AFTER
	case "calc":
		FailureType = F_CALC
	}

	switch *joinType {
	case "cheat":
		JoinType = J_CHEAT
	case "recur":
		JoinType = J_RECUR
	case "iter":
		JoinType = J_ITER
	case "iter-p":
		JoinType = J_ITER_P
	case "iter-pp":
		JoinType = J_ITER_P
		PiggybackJoinRequest = true
	}

	switch *unicastType {
	case "recur":
		UnicastType = U_RECUR
	case "iter":
		UnicastType = U_ITER
	}

	switch *uniRoutingType {
	case "single":
		RoutingType = SINGLE
	case "prune":
		RoutingType = PRUNE
	case "prune-opt1":
		RoutingType = PRUNE_OPT1
	case "prune-opt2":
		RoutingType = PRUNE_OPT2
	}

	trials := *numberOfNodes
	if *numberOfTrials > 0 {
		trials = *numberOfTrials
	}

	unicast := *unicastType
	if *unicastType == "recur" {
		unicast = *unicastType + ":" + *uniRoutingType
	}
	paramsString = fmt.Sprintf("%d %d %d %.2f %s %s %s", *numberOfNodes, *kValue, *alpha, *failureRatio, *failureType, *joinType, unicast)

	if CPU_PROFILE {
		f, _ := os.Create("cpu.pprof")

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("%v\n", err)
		}
		defer pprof.StopCPUProfile()
	}

	nodes := ConstructOverlay(*numberOfNodes)

	if *verbose {
		for _, n := range nodes {
			mark := ""
			if n.isFailure {
				mark += "*"
			}
			fmt.Printf("%skey=%d [%s]\n%s", mark, n.key, n.mv.String(), n.routingTableString())
		}
	}
	path_lengths := []float64{}
	nums_msgs := []int{}
	match_lengths := []float64{}
	failures := 0

	if UnicastType == U_RECUR {
		switch *experiment {
		case "uni":
			expUnicastRecursive(trials)
		case "uni-each":
			expUnicastEachRecursive()
		}
	} else {
		for i := 1; i <= trials; i++ {
			src := NormalList[rand.Intn(len(NormalList))]
			dst := NormalList[rand.Intn(len(NormalList))]
			//founds, hops, msgs, hops_to_match, failure := FastNodeLookup(nodes[dst].routingTable.dhtId, nodes[src], *alpha)
			founds, hops, msgs, hops_to_match, failure := FastLookup(dst.key, src)
			path_lengths = append(path_lengths, float64(hops))
			nums_msgs = append(nums_msgs, msgs)
			if !failure {
				match_lengths = append(match_lengths, float64(hops_to_match))
			}
			if failure {
				failures++
				ayame.Log.Debugf("%d->%d: FAILURE!!! %s\n", src, dst, ayame.SliceString(founds))
			}
			ayame.Log.Debugf("%d->%d: avg. results: %d, hops: %d, msgs: %d, hops_to_match: %d, fails: %d\n", src, dst, len(founds), hops, msgs, hops_to_match, failures)
		}
		pmean, _ := stats.Mean(path_lengths)
		ayame.Log.Infof("avg-paths-length: %s %f\n", paramsString, pmean)
		hmean, _ := stats.Mean(match_lengths)
		ayame.Log.Infof("avg-match-hops: %s %f\n", paramsString, hmean)
		ayame.Log.Infof("avg-msgs: %s %f\n", paramsString, meanOfInt(nums_msgs))
		ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-float64(failures)/float64(trials))
	}

	table_sizes := []int{}
	for _, node := range NormalList {
		//ayame.Log.Infof("%v\n", node.Id())
		table_sizes = append(table_sizes, node.routingTable.Size())
	}

	ayame.Log.Infof("avg-table-size: %s %f\n", paramsString, meanOfInt(table_sizes))
}
