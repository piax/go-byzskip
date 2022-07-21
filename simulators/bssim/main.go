package main

import (
	"context"
	//"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/op/go-logging"
	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
	flag "github.com/spf13/pflag"
	"github.com/thoas/go-funk"
)

var alpha *int
var kValue *int
var numberOfNodes *int
var trialNumber *int
var underAttackType *string
var failureRatio *float64
var joinType *string
var routingOfUnicastType *string
var optimizeRouting *string
var experiment *string
var pollutePrevRatioCalc *bool
var seed *int64
var verbose *bool

//var AdversaryList = []*bs.BSNode{}
var JoinedAdversaryList = []*bs.BSNode{}
var NormalList = []*bs.BSNode{}

const (
	J_CHEAT int = iota
	J_RECUR
	J_ITER
	J_ITER_P
	J_ITER_EV
)

const (
	U_RECUR int = iota
	U_ITER
)

var JoinType int
var UnicastType int

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
func correctEntryRatio(nodes []*bs.BSNode) float64 {
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
/*
func newKeyIssuer(typeString string) key_issuer.KeyIssuer {
	split := strings.Split(typeString, "-")
	var param int = -1
	if len(split) == 2 {
		param, _ = strconv.Atoi(split[1])
	}
	ret := key_issuer.NewKeyIssuer(split[0], *seed, param)
	return ret
}
*/
func ComputeProbabilityMonteCarlo(msg *bs.BSUnicastEvent, failureRatio float64, count int) float64 {
	src := msg.Sender().(*bs.BSNode)
	var dst *bs.BSNode
	for _, dstNode := range msg.Root.Destinations {
		if dstNode.Key().Equals(msg.TargetKey) {
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
	for _, pes := range msg.Root.Paths {
		//fmt.Printf("path: %s\n", ayame.SliceString(pes))
		var prev *bs.BSNode = nil
		for _, pe := range pes {
			this := pe.Node.(*bs.BSNode)
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
			if key != src.Key().String() && key != dst.Key().String() && rand.Float64() < failureRatio {
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

func ConstructOverlay(numberOfNodes int) []*bs.BSNode {
	nodes := make([]*bs.BSNode, 0, numberOfNodes)
	for i := 0; i < numberOfNodes; i++ {
		var n *bs.BSNode
		mv := ayame.NewMembershipVector(bs.ALPHA)
		//var key ayame.Key
		/*if *issuerType != "none" {
			key = keyIssuer.GetKey(ayame.FloatKey(float64(i)))
		} else {
			key = ayame.IntKey(i)
		}*/
		key := ayame.IntKey(i)
		switch FailureType {
		case F_CALC:
			fallthrough
		case F_NONE:
			n = bs.NewWithParent(ayame.NewLocalNode(key, mv), bs.NewSkipRoutingTable, false)
			NormalList = append(NormalList, n)
		case F_STOP:
			f := rand.Float64() < *failureRatio
			if i < bs.K { // first k nodes should not be a fault node
				f = false
			}
			if f {
				n = bs.NewWithParent(ayame.NewLocalNode(key, mv), NewStopRoutingTable, f)
			} else {
				n = bs.NewWithParent(ayame.NewLocalNode(key, mv), bs.NewSkipRoutingTable, f)
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
				n = bs.NewWithParent(ayame.NewLocalNode(key, mv), NewAdversaryRoutingTable, f)
			} else {
				n = bs.NewWithParent(ayame.NewLocalNode(key, mv), bs.NewSkipRoutingTable, f)
				NormalList = append(NormalList, n)
			}
		}
		n.SetUnicastHandler(simUnicastHandler)
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
		if !*pollutePrevRatioCalc {
			rand.Shuffle(len(last), func(i, j int) { last[i], last[j] = last[j], last[i] })
		}
		nodes := append(first, last...)
		err := FastJoinAllByRecursive(nodes)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	case J_ITER:
		fallthrough
	case J_ITER_P:
		if !*pollutePrevRatioCalc {
			first := nodes[0:bs.K]
			last := nodes[bs.K:]
			rand.Shuffle(len(last), func(i, j int) { last[i], last[j] = last[j], last[i] })
			nodes = append(first, last...)
		}
		secondTime := false
		if secondTime && *pollutePrevRatioCalc { // second time.
			bak := FailureType
			FailureType = F_NONE
			FastJoinAllByCheat(nodes)
			FailureType = bak
		}
		err := FastJoinAllByIterative(nodes, !secondTime)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	case J_ITER_EV:
		first := nodes[0:bs.K]
		last := nodes[bs.K:]
		if !*pollutePrevRatioCalc {
			rand.Shuffle(len(last), func(i, j int) { last[i], last[j] = last[j], last[i] })
		}
		nodes := append(first, last...)
		//bs.USE_TABLE_INDEX = true
		err := JoinAllByIterative(nodes)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	}
	if bak {
		FailureType = F_COLLAB_AFTER
	}
	ncount := 0
	fcount := 0
	//for _, n := range NormalList {
	for i := 0; i < numberOfNodes; i++ {
		if !nodes[i].IsFailure {
			c, f := CountTableEntries(nodes[i])
			ncount += c
			fcount += f
		}
	}
	ayame.Log.Infof("faulty-entry-ratio: %s %d %d %f\n", paramsString, fcount, ncount, float64(fcount)/float64(ncount))

	return nodes
}

func meanOfInt(lst []int) float64 {
	v, _ := stats.Mean(funk.Map(lst, func(x int) float64 { return float64(x) }).([]float64))
	return v
}

func isFaultySet(nodes []*bs.BSNode) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, n := range nodes {
		if !n.IsFailure {
			return false
		}
	}
	// all faulty nodes!
	return true
}

func JoinAllByIterative(nodes []*bs.BSNode) error {
	index := 0 // introducer index
	count := 0
	prev := 0
	//ayame.SecureKeyMV = false // no authentication
	bs.ResponseCount = 0
	for i, n := range nodes {
		if index != i {
			localn := n
			locali := i

			ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {

				if localn.IsFailure {
					JoinedAdversaryList = append(JoinedAdversaryList, localn)
					for _, p := range JoinedAdversaryList {
						if !p.Equals(localn) {
							ptable, pok := p.RoutingTable.(*AdversaryRoutingTable)
							if pok {
								ptable.AddAdversarial(localn)
								ntable, nok := localn.RoutingTable.(*AdversaryRoutingTable)
								if nok {
									ntable.AddAdversarial(p)
								}
							}
						}
					}
				}

				localn.JoinAsync(context.TODO(), nodes[index])
				// need to wait until function end or channel waiting status
				count++
				percent := 100 * count / len(nodes)
				if percent/10 != prev {
					ayame.Log.Infof("%s %d percent of %d nodes\n", time.Now(), percent, len(nodes))
				}
				prev = percent / 10
			}), int64(locali*1000))
		}
	}
	ayame.GlobalEventExecutor.Sim(int64(len(nodes)*2000), true)
	ayame.GlobalEventExecutor.AwaitFinish()
	//fmt.Printf("ev count %d\n", ayame.GlobalEventExecutor.EventCount)
	ayame.Log.Infof("avg-sum-candidates: %s %f\n", paramsString, float64(bs.ResponseCount)/float64(count))
	ayame.Log.Infof("avg-join-msgs: %s %f\n", paramsString, float64(ayame.GlobalEventExecutor.EventCount)/float64(count))
	return nil
}

func FastJoinAllByRecursive(nodes []*bs.BSNode) error {
	index := 0 // introducer index
	sumMsgs := 0
	count := 0
	prev := 0
	allFaultyCount := 0
	for i, n := range nodes {
		if index != i {
			localn := n
			locali := i
			if localn.IsFailure {
				JoinedAdversaryList = append(JoinedAdversaryList, localn)
				for _, p := range JoinedAdversaryList {
					if !p.Equals(localn) {
						ptable, pok := p.RoutingTable.(*AdversaryRoutingTable)
						if pok {
							ptable.AddAdversarial(localn)
							ntable, nok := localn.RoutingTable.(*AdversaryRoutingTable)
							if nok {
								ntable.AddAdversarial(p)
							}
						}
					}
				}
			}
			mid := strconv.Itoa(index) + "." + NextId()
			msg := bs.NewBSUnicastEventNoAuthor(nodes[index], mid, ayame.MembershipVectorSize, localn.Key(), []byte("hello"))
			ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {
				localn.Send(context.TODO(), msg, true) // the second argument (sign) is ommited in simulation
				ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {

					sumMsgs += len(msg.Results) // number of reply messages

					//
					localn.RoutingTable.Add(nodes[index], true)

					// XXX candidate list length
					umsgs, hijacked, _ := FastUpdateNeighbors(localn, msg.Results, []*bs.BSNode{})
					sumMsgs += umsgs
					if hijacked {
						allFaultyCount++
					}
					count++
					percent := 100 * count / len(nodes)
					if percent/10 != prev {
						ayame.Log.Infof("%s %d percent of %d nodes (%d hijacked)\n", time.Now(), percent, len(nodes), allFaultyCount)
					}
					prev = percent / 10
				}), int64(100)) // runs after 100ms time out
			}), int64(locali*1000))
		}
	}
	ayame.GlobalEventExecutor.Sim(int64(len(nodes)*1000+200), true)
	ayame.GlobalEventExecutor.AwaitFinish()
	//fmt.Printf("ev count %d\n", ayame.GlobalEventExecutor.EventCount)
	if *joinType == "recur" {
		ayame.Log.Infof("avg-join-lookup-msgs: %s %f\n", paramsString, float64(ayame.GlobalEventExecutor.EventCount)/float64(count))
	}
	ayame.Log.Infof("avg-join-msgs: %s %f\n", paramsString, float64(sumMsgs+ayame.GlobalEventExecutor.EventCount)/float64(count))

	return nil
}

func appendIfMissingWithCheck(lst []*bs.BSNode, node *bs.BSNode) ([]*bs.BSNode, bool) {
	for _, ele := range lst {
		if ele.Equals(node) {
			return lst, true
		}
	}
	return append(lst, node), false
}

func CountTableEntries(node *bs.BSNode) (int, int) {
	lst := []*bs.BSNode{}
	table := node.RoutingTable
	fcount := 0
	for _, levelTable := range table.GetNeighborLists() {
		for _, node := range levelTable.Neighbors[bs.LEFT] {
			exists := false
			n := node.(*bs.BSNode)
			if lst, exists = appendIfMissingWithCheck(lst, n); !exists {
				if n.IsFailure {
					fcount++
				}
			}
		}
		for _, node := range levelTable.Neighbors[bs.RIGHT] {
			exists := false
			n := node.(*bs.BSNode)
			if lst, exists = appendIfMissingWithCheck(lst, n); !exists {
				if n.IsFailure {
					fcount++
				}
			}
		}
	}
	return len(lst), fcount
}

func findNode(key ayame.Key, from []*bs.BSNode) *bs.BSNode {
	for _, f := range from {
		if f.Key() == key {
			return f
		}
	}
	return nil
}

func FastJoinAllByIterative(nodes []*bs.BSNode, isFirstTime bool) error {
	index := 0 // introducer index
	sumMsgs := 0
	sumLMsgs := 0
	count := 0
	prev := 0
	faultyCount := 0
	sumCandidates := 0
	sumHops := 0
	sumReturns := 0
	sumCounts := 0
	sumMaxes := 0

	for i, n := range nodes {
		if n.IsFailure {
			ayame.Log.Debugf("ADV LENGTH: %d\n", len(JoinedAdversaryList))
			JoinedAdversaryList = append(JoinedAdversaryList, n)
			for _, p := range JoinedAdversaryList {
				if !p.Equals(n) {
					ptable, pok := p.RoutingTable.(*AdversaryRoutingTable)
					if pok {
						ptable.AddAdversarial(n)
						ntable, nok := n.RoutingTable.(*AdversaryRoutingTable)
						if nok {
							ntable.AddAdversarial(p)
						}
					}
				}
			}
		}
		if isFirstTime {
			if index != i {
				ayame.Log.Debugf("%d: introducer= %s, search=%s\n", n.Key(), nodes[index].String(), n.String())
				rets, _, msgs, _, lmsgs, faulty, candidateLen, hopSum, returnSum, co, maxHops := FastRefresh(n, []*bs.BSNode{nodes[index]}) //FastNodeLookup(n, nodes[index])
				if faulty {
					faultyCount++
				}
				ayame.Log.Debugf("%d: rets %s\n", n.Key(), ayame.SliceString(rets))
				sumMsgs += msgs
				sumLMsgs += lmsgs
				sumCandidates += candidateLen
				sumHops += hopSum
				sumReturns += returnSum
				sumMaxes += maxHops
				sumCounts += co
			}
		} else { // second time or later
			lvl0 := findNode(n.Key(), nodes).LevelZeroNodes()
			if n.IsFailure {
				// XXX should support stop failures
				n.RoutingTable = NewAdversaryRoutingTable(n)
			} else {
				n.RoutingTable = bs.NewSkipRoutingTable(n)
			}
			ayame.Log.Debugf("%s: start refresh with %s\n", n, lvl0)
			rets, _, msgs, _, lmsgs, faulty, candidateLen, hopSum, returnSum, co, maxHops := FastRefresh(n, lvl0) //FastNodeLookup(n, nodes[index])
			if faulty {
				faultyCount++
			}
			ayame.Log.Debugf("%d: rets %s\n", n.Key(), ayame.SliceString(rets))
			sumMsgs += msgs
			sumLMsgs += lmsgs
			sumCandidates += candidateLen
			sumHops += hopSum
			sumReturns += returnSum
			sumMaxes += maxHops
			sumCounts += co
		}

		count++
		percent := 100 * count / len(nodes)
		if percent/10 != prev {
			ayame.Log.Infof("%s %d percent of %d nodes (%d hijacked)\n", time.Now(), percent, len(nodes), faultyCount)
		}
		prev = percent / 10
	}
	ayame.Log.Infof("avg-join-lookup-msgs: %s %f\n", paramsString, float64(sumLMsgs)/float64(count))
	ayame.Log.Infof("avg-join-msgs: %s %f\n", paramsString, float64(sumMsgs)/float64(count))
	ayame.Log.Infof("avg-sum-candidates: %s %f\n", paramsString, float64(sumCandidates)/float64(count))
	ayame.Log.Infof("avg-hops: %s %f\n", paramsString, float64(sumHops)/float64(sumCounts))
	ayame.Log.Infof("avg-max-hops: %s %f\n", paramsString, float64(sumMaxes)/float64(count))
	ayame.Log.Infof("avg-returns: %s %f\n", paramsString, float64(sumReturns)/float64(sumCounts))

	return nil
}

func hops(lst []bs.PathEntry, dstKey ayame.Key) (float64, bool) {
	ret := float64(0)
	prev := lst[0].Node.(*bs.BSNode)
	found := false
	for i, pe := range lst {
		if dstKey.Equals(pe.Node.(*bs.BSNode).Key()) {
			found = true
		}
		if i != 0 && !prev.Equals(pe.Node.(*bs.BSNode)) {
			ret++
		}
		prev = pe.Node.(*bs.BSNode)
	}
	///ayame.Log.Debugf("%s, %f\n", lst, ret)
	return ret, found
}

func minHops(lst [][]bs.PathEntry, dstKey ayame.Key) (float64, bool) {
	for _, path := range lst {
		if ret, ok := hops(path, dstKey); ok {
			return ret, true
		}
	}
	return 0, false
}

func pureLen(lst []bs.PathEntry) int {
	ret := 0
	prev := lst[0].Node.(*bs.BSNode)
	for i, pe := range lst {
		if i != 0 && !prev.Equals(pe.Node.(*bs.BSNode)) {
			ret++
		}
		prev = pe.Node.(*bs.BSNode)
	}
	return ret
}

/*
func meanOfPathLength(lst [][]bs.PathEntry) (float64, error) {
	return stats.Mean(funk.Map(lst, func(x []bs.PathEntry) float64 { return float64(len(x)) }).([]float64))
}*/

func maxPathLength(lst [][]bs.PathEntry) (float64, error) {
	return stats.Max(funk.Map(lst, func(x []bs.PathEntry) float64 { return float64(pureLen(x)) }).([]float64))
}

func msgsByPath(lst [][]bs.PathEntry) (float64, error) {
	pathMap := make(map[string]bool)
	for _, pes := range lst {
		prev := pes[0].Node.(*bs.BSNode)
		for i, pe := range pes {
			if i != 0 && !prev.Equals(pe.Node.(*bs.BSNode)) {
				pathMap[fmt.Sprintf("%d:%d", prev.Key(), pe.Node.Key())] = true
			}
			prev = pe.Node.(*bs.BSNode)
		}
	}
	//fmt.Printf("purelen=%d\n", len(pathMap))
	return float64(len(pathMap)), nil
}

/*
func minPathLength(lst [][]bs.PathEntry) (float64, error) {
	return stats.Min(funk.Map(lst, func(x []bs.PathEntry) float64 { return float64(len(x)) }).([]float64))
}*/

var paramsString string

//var keyIssuer key_issuer.KeyIssuer

func main() {
	alpha = flag.IntP("alpha", "a", 2, "the alphabet size of the membership vector")
	experiment = flag.StringP("exp-type", "e", "uni", "experiment type {uni|uni-max}")
	failureRatio = flag.Float64P("failure-ratio", "f", 0.2, "failure ratio")
	//issuerType = flag.StringP("issuer-type", "i", "none", "issuer type {shuffle|random|asis|none}")
	joinType = flag.StringP("join-type", "j", "iter", "join type {recur|iter|iter-fast|cheat}")
	kValue = flag.IntP("k", "k", 4, "the redundancy parameter")
	numberOfNodes = flag.IntP("nodes", "n", 100, "number of nodes")
	optimizeRouting = flag.StringP("optimize-type", "o", "opt", "unicast routing type {normal|opt}")
	pollutePrevRatioCalc = flag.BoolP("enable-pollution-calc", "p", false, "calculate the pollution prevention ratio")
	routingOfUnicastType = flag.StringP("unicast-routing-type", "r", "recur", "unicast routing type {recur|iter}")
	seed = flag.Int64P("seed", "s", 3, "give a random seed")
	trialNumber = flag.IntP("trials", "t", -1, "number of search trials (-1 means same as nodes)")
	underAttackType = flag.StringP("attack-type", "u", "cea", "runs under attacks {none|ara|cea|calc|stop}")
	verbose = flag.BoolP("verbose", "v", false, "verbose output")

	flag.Parse()
	//ayame.SecureKeyMV = false // skip authentication
	bs.InitK(*kValue)

	bs.ALPHA = *alpha

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	rand.Seed(*seed)

	switch *underAttackType {
	case "none":
		FailureType = F_NONE
	case "stop":
		FailureType = F_STOP
	case "cea":
		FailureType = F_COLLAB
	case "ara":
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
		JoinType = J_ITER_EV
	case "iter-fast":
		//JoinType = J_ITER
		JoinType = J_ITER_P
	}

	switch *routingOfUnicastType {
	case "recur":
		UnicastType = U_RECUR
	case "iter":
		UnicastType = U_ITER
	}

	switch *optimizeRouting {
	case "normal":
		bs.RoutingType = bs.SINGLE
	case "opt":
		bs.RoutingType = bs.PRUNE_OPT2
		/*case "prune-opt1":
			bs.RoutingType = bs.PRUNE_OPT1
		case "prune-opt2":
			bs.RoutingType = bs.PRUNE_OPT2
		case "prune-opt3":
			bs.RoutingType = bs.PRUNE_OPT3*/
	}

	bs.MODIFY_ROUTING_TABLE_BY_RESPONSE = false //*modifyRoutingTableDirectly

	bs.SYMMETRIC_ROUTING_TABLE = true //*useSymmetricRoutingTable

	trials := *numberOfNodes
	if *trialNumber > 0 {
		trials = *trialNumber
	}

	unicast := *routingOfUnicastType
	if *routingOfUnicastType == "recur" {
		unicast = *routingOfUnicastType + ":" + *optimizeRouting
	}
	paramsString = fmt.Sprintf("%d %d %d %.2f %s %s %s", *numberOfNodes, *kValue, *alpha, *failureRatio, *underAttackType, *joinType, unicast)

	if CPU_PROFILE {
		f, _ := os.Create("cpu.pprof")

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("%v\n", err)
		}
		defer pprof.StopCPUProfile()
	}

	//keyIssuer = newKeyIssuer(*issuerType)
	nodes := ConstructOverlay(*numberOfNodes)

	if *verbose {
		for _, n := range nodes {
			mark := ""
			if n.IsFailure {
				mark += "*"
			}
			fmt.Printf("%skey=%d [%s]\n%s", mark, n.Key(), n.MV().String(), n.RoutingTable)
		}
	}

	if *pollutePrevRatioCalc {
		testPeers := make([]*bs.BSNode, *numberOfNodes)
		for i := 0; i < *numberOfNodes; i++ {
			testPeers[i] = bs.NewWithParent(ayame.NewLocalNode(nodes[i].Key(), nodes[i].MV()), bs.NewSkipRoutingTable, false)
		}
		FastJoinAllByCheat(testPeers)
		diffs := 0
		count := 0
		for i := 0; i < *numberOfNodes; i++ {
			//fmt.Printf("cheat key=%s\n%s\n", localPeers[i].Key(), localPeers[i].RoutingTable)
			if !nodes[i].IsFailure {
				diff := bs.RoutingTableDiffs(testPeers[i].RoutingTable, nodes[i].RoutingTable)
				//ayame.Log.Infof("diff: %s %d\n", nodes[i], diff)
				diffs += diff
				c := nodes[i].RoutingTable.Size()
				count += c
			}
		}
		ayame.Log.Infof("pollution-prevention-ratio: %s %d %d %f\n", paramsString, diffs, count, 1-float64(diffs)/float64(count))
		return
	}

	/*
		path_lengths := []float64{}
		nums_msgs := []int{}
		match_lengths := []float64{}
		failures := 0
	*/

	if UnicastType == U_RECUR {
		switch *experiment {
		case "uni":
			expUnicastRecursive(trials)
		case "uni-max":
			expUnicastEachRecursive()
		}
	} else {
		switch *experiment {
		case "uni":
			expIterative(trials)
		case "uni-max":
			expEachIterative()
		}
	}

	table_sizes := []int{}
	for _, node := range NormalList {
		//ayame.Log.Infof("%v\n", node.Id())
		table_sizes = append(table_sizes, node.RoutingTable.Size())
	}

	ayame.Log.Infof("avg-table-size: %s %f\n", paramsString, meanOfInt(table_sizes))
}
