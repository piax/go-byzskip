package main

import (
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/op/go-logging"
	"github.com/piax/go-ayame/ayame"
	"github.com/piax/go-ayame/byzskip"
	"github.com/thoas/go-funk"
)

var alpha *int
var kValue *int
var numberOfNodes *int
var numberOfTrials *int
var failureType *string
var failureRatio *float64
var useIterativeJoin *bool
var useCheatJoin *bool
var useRecursive *bool
var adversarialNet *bool
var seed *int64
var verbose *bool

var AdversaryList = []*BSNode{}
var NormalList = []*BSNode{}

var UseIterativeJoin bool // the global flag

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
			if key != src.key && key != dst.key && rand.Float64() < failureRatio {
				graphCopy[key] = nil // failure
			} else {
				graphCopy[key] = value
			}
		}
		//graphCopy.Dump()
		path := make(Array, 0, 50)
		reached, exists := graphCopy.PathExists(src, dst, path)
		//length := len(shortestPath)

		if !exists {
			failures++
			ayame.Log.Debugf("dst node not found\n")
		} else {
			ayame.Log.Debugf("reached path: %s\n", ayame.SliceString(reached))
		}
	}
	return float64(failures) / float64(count)
}

func ConstructOverlay(numberOfNodes int) []*BSNode {
	nodes := make([]*BSNode, 0, numberOfNodes)
	//DummyNodes = make([]*MIRONode, 0, numberOfNodes)
	for i := 0; i < numberOfNodes; i++ {
		var n *BSNode
		mv := ayame.NewMembershipVector(byzskip.ALPHA)
		//dummyNode := NewMIRONode(i, mv)
		switch FailureType {
		case F_CALC:
			fallthrough
		case F_NONE:
			n = NewBSNode(i, mv, false)
			NormalList = append(NormalList, n)
		case F_STOP:
			fallthrough
		case F_COLLAB:
			f := rand.Float64() < *failureRatio
			if i < byzskip.K { // first k nodes should not be a fault node
				f = false
			}
			//if 8 <= i && i <= 11 { // intentional
			//	f = true
			//}
			n = NewBSNode(i, mv, f)
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

	if *useCheatJoin {
		err := FastJoinAll(nodes)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	} else {
		err := FastJoinAllByLookup(nodes)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	}
	return nodes
}

func meanOfInt(lst []int) float64 {
	v, _ := stats.Mean(funk.Map(lst, func(x int) float64 { return float64(x) }).([]float64))
	return v
}

func FastJoinAllByLookup(nodes []*BSNode) error {
	index := 0 // introducer index
	sumMsgs := 0
	sumLMsgs := 0
	count := 0
	prev := 0
	for i, n := range nodes {
		if !*adversarialNet && n.isFailure {
			AdversaryList = append(AdversaryList, n) // only used when F_COLLAB
		}
		if index != i {
			rets, _, msgs, _, lmsgs, _ := FastNodeLookup(n, nodes[index])
			ayame.Log.Debugf("%d: rets %s\n", n.key, ayame.SliceString(rets))
			sumMsgs += msgs
			sumLMsgs += lmsgs
		}
		count++
		percent := 100 * count / len(nodes)
		if percent/10 != prev {
			fmt.Printf("%s %d percent of %d nodes\n", time.Now(), percent, len(nodes))
		}
		prev = percent / 10
	}
	ayame.Log.Infof("avg-join-lookup-msgs: %d %f %f\n", len(nodes), *failureRatio, float64(sumLMsgs)/float64(len(nodes)))
	if UseIterativeJoin {
		ayame.Log.Infof("avg-iterative-join-msgs: %d %f %f\n", len(nodes), *failureRatio, float64(sumMsgs)/float64(len(nodes)))
	} else {
		ayame.Log.Infof("avg-join-msgs: %d %f %f\n", len(nodes), *failureRatio, float64(sumMsgs)/float64(len(nodes)))
	}

	ncount := 0
	fcount := 0
	for _, n := range NormalList {
		c, f := n.routingTable.Count()
		ncount += c
		fcount += f
	}
	ayame.Log.Infof("polluted-entry-ratio: %d %f %f\n", *numberOfNodes, *failureRatio, float64(fcount)/float64(ncount))

	return nil
}

func meanOfPathLength(lst [][]PathEntry) (float64, error) {
	return stats.Mean(funk.Map(lst, func(x []PathEntry) float64 { return float64(len(x)) }).([]float64))
}

func main() {
	alpha = flag.Int("alpha", 2, "the alphabet size of the membership vector")
	kValue = flag.Int("k", 4, "the redundancy parameter")
	numberOfNodes = flag.Int("nodes", 1000, "number of nodes")
	numberOfTrials = flag.Int("trials", -1, "number of search trials (-1 means same as nodes)")
	failureType = flag.String("type", "collab", "failure type {none|stop|collab|calc}")
	failureRatio = flag.Float64("f", 0.2, "failure ratio")
	useCheatJoin = flag.Bool("cheatjoin", false, "use cheat join (cannot evaluate join)")
	useIterativeJoin = flag.Bool("i", false, "use iterative search with join (ignored when -cheatjoin)")
	useRecursive = flag.Bool("r", false, "use recursive routing in unicast (with -type calc)")
	adversarialNet = flag.Bool("adv", true, "use adversarial net (with -type collab)")
	seed = flag.Int64("seed", 2, "give a random seed")
	verbose = flag.Bool("v", false, "verbose output")

	flag.Parse()

	byzskip.InitK(*kValue)

	byzskip.ALPHA = *alpha

	UseIterativeJoin = *useIterativeJoin

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
	case "calc":
		FailureType = F_CALC
	}
	/* comment out if we do profiling
	f, _ := os.Create("cpu.pprof")

	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Printf("%v\n", err)
	}
	defer pprof.StopCPUProfile()
	*/
	nodes := ConstructOverlay(*numberOfNodes)
	//numberOfTrials := *numberOfNodes * 6
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

	trials := *numberOfNodes
	if *numberOfTrials > 0 {
		trials = *numberOfTrials
	}

	if *useRecursive {
		msgs := []*BSUnicastEvent{}
		for i := 1; i <= trials; i++ {
			src := rand.Intn(*numberOfNodes)
			dst := rand.Intn(*numberOfNodes)
			msg := NewBSUnicastEvent(nodes[src], ayame.MembershipVectorSize, dst) // starts with the max level.
			msgs = append(msgs, msg)
			ayame.Log.Debugf("id=%d,src=%d, dst=%d\n", msg.messageId, src, dst)
			ayame.GlobalEventExecutor.RegisterEvent(msg, int64(i*1000))
			//nodes[src].SendEvent(msg)
			// time out after 200ms
			ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {
				ayame.Log.Debugf("id=%d,src=%d, dst=%d timed out\n", msg.messageId, src, dst)
				msg.root.channel <- true
			}), int64(i*1000)+200)
		}
		success := 0
		for _, msg := range msgs {
			go func(msg *BSUnicastEvent) {
				<-msg.root.channel
				if *verbose {
					avg, _ := meanOfPathLength(msg.root.paths)
					ayame.Log.Debugf("%d: started %d, finished: %d, avg.hops %f\n", msg.messageId, msg.Time(), msg.finishTime, avg)
				}
				if ContainsKey(msg.targetKey, msg.root.destinations) {
					success++
				} else {
					ayame.Log.Infof("%s->%d: FAILURE!!! %s\n", msg.Sender().Id(), msg.targetKey, ayame.SliceString(msg.root.destinations))
				}
				close(msg.root.channel)
			}(msg)
		}
		ayame.GlobalEventExecutor.Reset()
		ayame.GlobalEventExecutor.Sim(int64(trials*1000*2), true)
		ayame.GlobalEventExecutor.AwaitFinish()

		ave, _ := stats.Mean(funk.Map(msgs, func(msg *BSUnicastEvent) float64 {
			avg, _ := meanOfPathLength(msg.destinationPaths)
			ayame.Log.Debugf("%s->%d: avg. path length: %f\n", msg.root.Sender().Id(), msg.targetKey, avg)
			return avg
		}).([]float64))
		counts := ayame.GlobalEventExecutor.EventCount
		if *useRecursive {
			ayame.Log.Infof("avg-rec-match-hops: %d %d %f %f\n", *numberOfNodes, *kValue, *failureRatio, ave)
			ayame.Log.Infof("avg-rec-msgs: %d %d %f %f\n", *numberOfNodes, *kValue, *failureRatio, float64(counts)/float64(trials))
		} else {
			ayame.Log.Infof("avg-match-hops: %d %d %f %f\n", *numberOfNodes, *kValue, *failureRatio, ave)
			ayame.Log.Infof("avg-msgs: %d %d %f %f\n", *numberOfNodes, *kValue, *failureRatio, float64(counts)/float64(trials))
		}
		if FailureType == F_CALC {
			// XXX
			probSum := 0.0
			count := 1000
			for _, msg := range msgs {
				probSum += ComputeProbabilityMonteCarlo(msg, *failureRatio, count)
			}
			ayame.Log.Infof("avg-success-ratio: %d %d %f %f\n", *numberOfNodes, *kValue, *failureRatio, 1-probSum/float64(len(msgs)))
		} else {
			ayame.Log.Infof("success-ratio: %d %d %f %f\n", *numberOfNodes, *kValue, *failureRatio, float64(success)/float64(trials))
		}
	} else {
		for i := 1; i <= trials; i++ {
			src := rand.Intn(len(NormalList))
			dst := rand.Intn(len(NormalList))
			//founds, hops, msgs, hops_to_match, failure := FastNodeLookup(nodes[dst].routingTable.dhtId, nodes[src], *alpha)
			founds, hops, msgs, hops_to_match, failure := FastLookup(NormalList[dst].key, NormalList[src])
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
		ayame.Log.Infof("avg-paths-length: %d %f %f\n", *numberOfNodes, *failureRatio, pmean)
		hmean, _ := stats.Mean(match_lengths)
		ayame.Log.Infof("avg-match-hops: %d %f %f\n", *numberOfNodes, *failureRatio, hmean)
		ayame.Log.Infof("avg-msgs: %d %f %f\n", *numberOfNodes, *failureRatio, meanOfInt(nums_msgs))
		ayame.Log.Infof("success-ratio: %d %f %f\n", *numberOfNodes, *failureRatio, 1-float64(failures)/float64(trials))
	}
	table_sizes := []int{}
	for _, node := range nodes {
		//ayame.Log.Infof("%v\n", node.Id())
		table_sizes = append(table_sizes, node.routingTable.Size())
	}

	ayame.Log.Infof("avg-table-size: %d %f\n", *numberOfNodes, meanOfInt(table_sizes))
}
