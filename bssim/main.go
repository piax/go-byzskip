package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/op/go-logging"
	"github.com/piax/go-ayame/ayame"
	bs "github.com/piax/go-ayame/byzskip"
	"github.com/piax/go-ayame/key_issuer"
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
var keyIssuerType *string
var uniRoutingType *string
var experiment *string
var useTableIndex *bool
var modifyRoutingTableDirectly *bool
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

func newKeyIssuer(typeString string) key_issuer.KeyIssuer {
	split := strings.Split(typeString, "-")
	var param int = -1
	if len(split) == 2 {
		param, _ = strconv.Atoi(split[1])
	}
	ret := key_issuer.NewKeyIssuer(split[0], *seed, param)
	return ret
}

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
		var key ayame.Key
		if *keyIssuerType != "none" {
			key = keyIssuer.GetKey(ayame.FloatKey(float64(i)))
		} else {
			key = ayame.IntKey(i)
		}
		switch FailureType {
		case F_CALC:
			fallthrough
		case F_NONE:
			n = bs.NewBSNode(ayame.NewLocalNode(key, mv), bs.NewSkipRoutingTable, false)
			NormalList = append(NormalList, n)
		case F_STOP:
			f := rand.Float64() < *failureRatio
			if i < bs.K { // first k nodes should not be a fault node
				f = false
			}
			if f {
				n = bs.NewBSNode(ayame.NewLocalNode(key, mv), NewStopRoutingTable, f)
			} else {
				n = bs.NewBSNode(ayame.NewLocalNode(key, mv), bs.NewSkipRoutingTable, f)
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
				n = bs.NewBSNode(ayame.NewLocalNode(key, mv), NewAdversaryRoutingTable, f)
			} else {
				n = bs.NewBSNode(ayame.NewLocalNode(key, mv), bs.NewSkipRoutingTable, f)
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
		err := FastJoinAllByInteractive(nodes)
		if err != nil {
			fmt.Printf("join failed:%s\n", err)
		}
	case J_ITER_EV:
		first := nodes[0:bs.K]
		last := nodes[bs.K:]
		rand.Shuffle(len(last), func(i, j int) { last[i], last[j] = last[j], last[i] })
		nodes := append(first, last...)
		bs.USE_TABLE_INDEX = *useTableIndex
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
	for _, n := range NormalList {
		c, f := CountTableEntries(n)
		ncount += c
		fcount += f
	}
	ayame.Log.Infof("polluted-entry-ratio: %s %f\n", paramsString, float64(fcount)/float64(ncount))

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
	ayame.SecureKeyMV = false // no authentication
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
					fmt.Printf("%s %d percent of %d nodes\n", time.Now(), percent, len(nodes))
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

func FastJoinAllByInteractive(nodes []*bs.BSNode) error {
	index := 0 // introducer index
	sumMsgs := 0
	sumLMsgs := 0
	count := 0
	prev := 0
	faultyCount := 0
	sumCandidates := 0
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
		if index != i {
			ayame.Log.Debugf("%d: introducer= %s, search=%s\n", n.Key(), nodes[index].String(), n.String())
			rets, _, msgs, _, lmsgs, faulty, candidateLen := FastNodeLookup(n, nodes[index])
			if faulty {
				faultyCount++
			}
			ayame.Log.Debugf("%d: rets %s\n", n.Key(), ayame.SliceString(rets))
			sumMsgs += msgs
			sumLMsgs += lmsgs
			sumCandidates += candidateLen
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
	ayame.Log.Infof("avg-sum-candidates: %s %f\n", paramsString, float64(sumCandidates)/float64(count))

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

var keyIssuer key_issuer.KeyIssuer

// joinType cheat|recur|iter|iter-p
// unicastType i|r

func main() {
	alpha = flag.Int("alpha", 2, "the alphabet size of the membership vector")
	kValue = flag.Int("k", 4, "the redundancy parameter")
	numberOfNodes = flag.Int("nodes", 1000, "number of nodes")
	numberOfTrials = flag.Int("trials", -1, "number of search trials (-1 means same as nodes)")
	failureType = flag.String("type", "calc", "failure type {none|stop|collab|collab-after|calc}")
	failureRatio = flag.Float64("f", 0.0, "failure ratio")
	joinType = flag.String("joinType", "cheat", "join type {cheat|recur|iter|iter-p|iter-pp|iter-ev}")
	keyIssuerType = flag.String("issuerType", "none", "issuer type (type-param) type={shuffle|random|asis|none}")
	unicastType = flag.String("unicastType", "recur", "unicast type {recur|iter}")
	uniRoutingType = flag.String("uniRoutingType", "prune-opt2", "unicast routing type {single|prune|prune-opt1|prune-opt2|prune-opt3}")
	experiment = flag.String("exp", "uni-each", "experiment type {uni|uni-each|join}")
	useTableIndex = flag.Bool("index", true, "use table index to get candidates")
	modifyRoutingTableDirectly = flag.Bool("modRT", false, "modify routing table directly")
	seed = flag.Int64("seed", 2, "give a random seed")
	verbose = flag.Bool("v", false, "verbose output")

	flag.Parse()
	ayame.SecureKeyMV = false // skip authentication
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
	case "iter-ev":
		JoinType = J_ITER_EV
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
		bs.RoutingType = bs.SINGLE
	case "prune":
		bs.RoutingType = bs.PRUNE
	case "prune-opt1":
		bs.RoutingType = bs.PRUNE_OPT1
	case "prune-opt2":
		bs.RoutingType = bs.PRUNE_OPT2
	case "prune-opt3":
		bs.RoutingType = bs.PRUNE_OPT3
	}

	bs.MODIFY_ROUTING_TABLE_BY_RESPONSE = *modifyRoutingTableDirectly

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

	keyIssuer = newKeyIssuer(*keyIssuerType)
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
			founds, hops, msgs, hops_to_match, failure := FastLookup(dst.Key(), src)
			path_lengths = append(path_lengths, float64(hops))
			nums_msgs = append(nums_msgs, msgs)
			if !failure {
				match_lengths = append(match_lengths, float64(hops_to_match))
			}
			if failure {
				failures++
				ayame.Log.Infof("%s->%s: FAILURE!!! %s\n", src, dst, ayame.SliceString(founds))
			}
			ayame.Log.Debugf("%s->%s: avg. results: %d, hops: %d, msgs: %d, hops_to_match: %d, fails: %d\n", src, dst, len(founds), hops, msgs, hops_to_match, failures)
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
		table_sizes = append(table_sizes, node.RoutingTable.Size())
	}

	ayame.Log.Infof("avg-table-size: %s %f\n", paramsString, meanOfInt(table_sizes))
}
