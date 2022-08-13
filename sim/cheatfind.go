package sim

import (
	"github.com/piax/go-byzskip/ayame" // "go mod tidy" is needed on go 1.16

	bs "github.com/piax/go-byzskip/byzskip" // "go mod tidy" is needed on go 1.16
	//
)

// failure type
const (
	F_NONE int = iota
	F_STOP
	F_COLLAB
	F_COLLAB_AFTER
	F_CALC
)

func ksToNs(lst []bs.KeyMV) []*bs.BSNode {
	ret := []*bs.BSNode{}
	for _, ele := range lst {
		ret = append(ret, ele.(*bs.BSNode))
	}
	return ret
}

// called by remote
func FastFindKey(node *bs.BSNode, key ayame.Key) ([]*bs.BSNode, int) {
	nb, lv := node.RoutingTable.KClosestWithKey(key)
	return ksToNs(nb), lv
}

func FastFindNode(node *bs.BSNode, target *bs.BSNode) ([]*bs.BSNode, int, []*bs.BSNode) {
	nb, lv, can := node.GetNeighborsAndCandidates(target.Key(), target.MV())
	//ayame.Log.Debugf("%s: adding %s\n", node, target)
	node.RoutingTable.Add(target, true)
	//ayame.Log.Debugf("%s: %d's neighbors= %s (level %d)\n updated:\n %s\n", node, target.key, ayame.SliceString(nb), lv,
	//	node.routingTable.String())
	return nb, lv, can
}

func FastFindNodeWithRequest(node *bs.BSNode, target *bs.BSNode, req *bs.NeighborRequest) ([]*bs.BSNode, int, []*bs.BSNode) {
	can := node.RoutingTable.Neighbors(req)
	//nb, lv := node.GetClosestNodes(req.Key)
	nb, lv := node.RoutingTable.KClosest(req)
	//ayame.Log.Debugf("%s: adding %s\n", node, target)
	node.RoutingTable.Add(target, true)
	//ayame.Log.Debugf("%s: %d's neighbors= %s (level %d)\n updated:\n %s\n", node, target.key, ayame.SliceString(nb), lv,
	//	node.routingTable.String())
	return ksToNs(nb), lv, ksToNs(can)
}

const (
	CONFUSED_ROUTING_TABLE = true
)

func FastJoinRequest(node *bs.BSNode, target *bs.BSNode) []*bs.BSNode {
	//ret := node.GetCandidates()
	ret := node.RoutingTable.GetCommonNeighbors(target.MV())

	//	if !node.isFailure || FailureType == F_NONE {
	ayame.Log.Debugf("%s: adding %s for join request\n", node, target)
	node.RoutingTable.Add(target, true)
	//ayame.Log.Debugf("%s: %d \n updated:\n %s\n", node, target, node.routingTable.String())
	//for _, n := range piggyback {
	//	node.RoutingTable.Add(n, true)
	//}
	//	}
	return ksToNs(ret)
}

func FastJoinRequestWithIndex(node *bs.BSNode, target *bs.BSNode, req *bs.NeighborRequest) []*bs.BSNode {
	//ret := node.GetCandidates()
	ret := node.RoutingTable.Neighbors(req)

	//	if !node.isFailure || FailureType == F_NONE {
	ayame.Log.Debugf("%s: adding %s for join request\n", node, target)
	node.RoutingTable.Add(target, true)
	//ayame.Log.Debugf("%s: %d \n updated:\n %s\n", node, target, node.routingTable.String())
	//for _, n := range piggyback {
	//	node.RoutingTable.Add(n, true)
	//}
	//	}
	return ksToNs(ret)
}

func appendNodesIfMissing(lst []*bs.BSNode, nodes []*bs.BSNode) []*bs.BSNode {
	for _, ele := range nodes {
		lst = appendIfMissing(lst, ele)
	}
	return lst
}

func appendIfMissing(lst []*bs.BSNode, node *bs.BSNode) []*bs.BSNode {
	for _, ele := range lst {
		if ele.Equals(node) {
			return lst
		}
	}
	return append(lst, node)
}

var FailureType int

func FastLookup(key ayame.Key, source *bs.BSNode) ([]*bs.BSNode, int, int, int, bool) {
	hops := 0
	msgs := 0
	hops_to_match := -1
	failure := false

	hops++ // request
	msgs++
	neighbors, level := FastFindKey(source, key)
	hops++
	msgs++ // response

	rets := []*bs.BSNode{}
	if level == 0 {
		rets = append(rets, neighbors...)
		if hops_to_match < 0 {
			hops_to_match = hops
		}
	}
	queried := []*bs.BSNode{}

	for !ayame.IsSubsetOf(neighbors, queried) {
		// get uncontained neghbors
		nexts := ayame.Exclude(neighbors, queried)
		hops++ // request
		for _, next := range nexts {
			msgs++
			curNeighbors, curLevel := FastFindKey(next, key)
			//ayame.Log.Debugf("neighbors for %d = %s\n", key, ayame.SliceString(neighbors))
			msgs++
			queried = append(queried, next)
			if curLevel == 0 {
				rets = appendIfMissing(rets, next)
				if hops_to_match < 0 && bs.ContainsKey(key, rets) {
					hops_to_match = hops
				}
			}
			neighbors = appendNodesIfMissing(neighbors, curNeighbors)
			//ayame.Log.Debugf("hops=%d, queried=%d, neighbors=%s\n", hops, len(queried), ayame.SliceString(neighbors))
		}
		hops++ // response
	}
	if hops_to_match < 0 {
		failure = true
	}
	//return source.routingTable.getNearestNodes(id, K), hops, msgs, hops_to_match, failure
	return rets, hops, msgs, hops_to_match, failure
}

//length, _ := maxPathLength(msg.paths)
//hops = int(length)
func FastUpdateNeighbors(target *bs.BSNode, initialNodes []*bs.BSNode, queried []*bs.BSNode) (int, bool, int) {
	hijacked := false
	if !target.IsFailure && isFaultySet(initialNodes) {
		hijacked = true
		ayame.Log.Infof("initial nodes hijacked: %s\n", initialNodes)
	}

	msgs := 0
	sumCandidates := 0
	candidates := initialNodes
	ayame.Log.Debugf("%d: start neighbor collection from %s, queried=%s\n", target.Key(), ayame.SliceString(candidates), ayame.SliceString(queried))
	ayame.Log.Debugf("initial: %s\n", target.RoutingTable)
	for len(candidates) != 0 { //&& target.RoutingTable.PossiblyBeAdded(candidates[0]) { //len(candidates) != 0 {
		next := candidates[0]
		// XXX use message
		msgs++
		//piggyback := []*bs.BSNode{}
		//if PiggybackJoinRequest {
		//	piggyback = target.GetList(true, false)
		//}
		var newCandidates []*bs.BSNode
		if bs.USE_TABLE_INDEX {
			idxs := target.RoutingTable.GetTableIndex()
			for _, idx := range idxs {
				ayame.Log.Debugf("%s: index level=%d, min=%s, max=%s\n", target, idx.Level, idx.Min, idx.Max)
			}
			newCandidates = FastJoinRequestWithIndex(next, target, &bs.NeighborRequest{Key: target.Key(), MV: target.MV(), NeighborListIndex: idxs})
		} else {
			newCandidates = FastJoinRequest(next, target)
		}
		sumCandidates += len(newCandidates)
		ayame.Log.Debugf("%d: join request to %d, got %s \n", target.Key(), next.Key(), ayame.SliceString(newCandidates))
		msgs++
		queried = append(queried, next)

		//ayame.Log.Debugf("%s: adding %s\n", target, ayame.SliceString(newCandidates))
		//for _, c := range newCandidates {
		target.RoutingTable.Add(next, true)
		//}
		//ayame.Log.Debugf("%s: table is updated\ntable:%s\n", target, target.routingTable.String())

		//candidates = target.GetList(false, true)
		candidates = appendNodesIfMissing(candidates, newCandidates)
		candidates = ayame.Exclude(candidates, queried)
		ayame.Log.Debugf("%s: received candidates=%s, next candidates=%s\n", target.Key(), ayame.SliceString(newCandidates), ayame.SliceString(candidates))
		ayame.Log.Debugf("%d: next candidates %s\n", target.Key(), ayame.SliceString(candidates))
	}
	ayame.Log.Debugf("%d: update neighbors msgs %d\n", target.Key(), msgs)
	ayame.Log.Debugf("%d: finish\n%s", target.Key(), target.RoutingTable)
	return msgs, hijacked, sumCandidates
}

func FastNodeLookup(target *bs.BSNode, introducer *bs.BSNode) ([]*bs.BSNode, int, int, int, int, bool, int) {
	hops := 0
	msgs := 0
	hops_to_match := -1
	msgs_to_lookup := 0
	respCount := 0 // neighbor candidate node data size other than closer nodes data

	hops++ // request
	msgs++
	neighbors, level, candidates := FastFindNode(introducer, target)
	ayame.Log.Debugf("queried %d, neighbors for %d = %s @ level %d\n", introducer.Key(), target.Key(), ayame.SliceString(neighbors), level)
	hops++
	msgs++ // response

	respCount += len(candidates)

	ayame.Log.Debugf("%s: adding %s\n", target, introducer)
	target.RoutingTable.Add(introducer, true)

	rets := []*bs.BSNode{}
	queried := []*bs.BSNode{introducer}
	if level == 0 {
		rets = append(rets, neighbors...)
		if hops_to_match < 0 {
			hops_to_match = hops
			ayame.Log.Debugf("found %d's level 0: %s\n", target.Key(), ayame.SliceString(rets))
		}
	}
	if JoinType == J_ITER_P {
		//ayame.Log.Debugf("%s: adding iter-p %s\n", target, ayame.SliceString(candidates))
		for _, c := range candidates {
			target.RoutingTable.Add(c, true)
		}
	}

	for !ayame.IsSubsetOf(neighbors, queried) {
		ayame.Log.Debugf("neighbors: %s, queried: %s\n", ayame.SliceString(neighbors), ayame.SliceString(queried))
		// get uncontained neighbors
		nexts := ayame.Exclude(neighbors, queried)
		ayame.Log.Debugf("nexts: %s\n", ayame.SliceString(nexts))
		hops++ // request
		for _, next := range nexts {
			msgs++
			var curNeighbors, curCandidates []*bs.BSNode
			var curLevel int
			if bs.USE_TABLE_INDEX {
				idxs := target.RoutingTable.GetTableIndex()
				req := &bs.NeighborRequest{Key: target.Key(), MV: target.MV(), NeighborListIndex: idxs}
				curNeighbors, curLevel, curCandidates = FastFindNodeWithRequest(next, target, req)
			} else {
				curNeighbors, curLevel, curCandidates = FastFindNode(next, target)
			}
			respCount += len(curNeighbors)
			respCount += len(curCandidates)
			ayame.Log.Debugf("queried %d, neighbors for %d = %s @ level %d, candidates=%s\n", next.Key(), target.Key(), ayame.SliceString(curNeighbors), curLevel, ayame.SliceString(curCandidates))
			//ayame.Log.Debugf("queried %d, candidates for %d = %s\n", next.Key(), target.Key(), ayame.SliceString(curCandidates))
			msgs++
			queried = append(queried, next)
			if curLevel == 0 {
				for _, cur := range curNeighbors {
					rets = appendIfMissing(rets, cur)
				}
				if hops_to_match < 0 {
					hops_to_match = hops
					ayame.Log.Debugf("found %d's level 0: %s by %s\n", target.Key(), ayame.SliceString(curNeighbors), next)
				}
			}
			if JoinType == J_ITER_P {
				ayame.Log.Debugf("%s: adding %s\n", target, ayame.SliceString(curCandidates))
				for _, c := range curCandidates {
					target.RoutingTable.Add(c, true)
				}
				//ayame.Log.Debugf("%d: iter-p added andidates\n table=%s\n", target.Key(), target.routingTable.String())
			}
			neighbors = appendNodesIfMissing(neighbors, curNeighbors)
			ayame.Log.Debugf("hops=%d, queried=%d, neighbors=%s\n", hops, len(queried), ayame.SliceString(neighbors))
		}
		hops++ // response
	}

	msgs_to_lookup = msgs
	// the nodes in the current routing table, in order of closeness. (R->L->R->L in each level)
	if JoinType == J_ITER_P {
		candidates = ayame.Exclude(ksToNs(target.RoutingTable.AllNeighbors(false, true)), queried)
	} else {
		candidates = rets
		queried = []*bs.BSNode{}
	}
	ayame.Log.Debugf("%d: table=%s, candidates=%s, queried=%s\n", target.Key(), ayame.SliceString(target.RoutingTable.AllNeighbors(false, false)), ayame.SliceString(candidates), ayame.SliceString(queried))
	//processed := []*bs.BSNode{}
	umsgs, failure, sumCandidates := FastUpdateNeighbors(target, candidates, queried)
	respCount += sumCandidates
	msgs += umsgs
	ayame.Log.Debugf("%d: join-msgs %d\n", target.Key(), msgs)
	//return source.routingTable.getNearestNodes(id, K), hops, msgs, hops_to_match, failure
	return rets, hops, msgs, hops_to_match, msgs_to_lookup, failure, respCount
}

func FastGetNeighbors(target *bs.BSNode, req *bs.NeighborRequest, n *bs.BSNode, hops int,
	hopRecord map[ayame.Key]int, maxHopRecord map[ayame.Key]int,
	returnRecord map[ayame.Key]int) ([]*bs.BSNode, int, []*bs.BSNode) {
	var level int
	var candidates []*bs.BSNode
	var knn []*bs.BSNode

	if bs.USE_TABLE_INDEX {
		candidates = ksToNs(n.RoutingTable.Neighbors(req))
		clst, lvl := n.RoutingTable.KClosest(req) // GetClosestNodes(ue.req.Key)
		knn = ksToNs(clst)
		level = lvl
		candidates = ayame.Exclude(candidates, knn)

		kNeighbors, _, curCandidates := n.GetNeighborsAndCandidates(req.Key, req.MV)
		rets := kNeighbors
		rets = appendNodesIfMissing(rets, curCandidates)
		r := delNode(nsToKs(rets), n)

		ayame.Log.Debugf("%d: %s can return %s\n", target.Key(), n, ayame.SliceString(r))
		for _, n := range r {
			// first appearance
			if _, exist := hopRecord[n.Key()]; !exist {
				hopRecord[n.Key()] = hops
			}
			maxHopRecord[n.Key()] = hops
			if _, exist := returnRecord[n.Key()]; !exist {
				returnRecord[n.Key()] = 1
			} else {
				returnRecord[n.Key()] = returnRecord[n.Key()] + 1
			}
		}
	} else {
		knn, level, candidates = n.GetNeighborsAndCandidates(req.Key, req.MV)
	}
	n.RoutingTable.Add(target, true)
	return knn, level, candidates
}

func getIdx(v bs.KeyMV, lst []bs.KeyMV) int {
	for i, n := range lst {
		if v.Key().Equals(n.Key()) {
			return i
		}
	}
	return -1
}

func pickAlternately(v *bs.BSNode, left []bs.KeyMV, right []bs.KeyMV, queried []*bs.BSNode, tau int) []*bs.BSNode {
	leftUnqueried := ayame.Exclude(ksToNs(left), queried)
	rightUnqueried := ayame.Exclude(ksToNs(right), queried)
	ret := []*bs.BSNode{}

	for i := 0; i < tau; i++ {
		var cur *bs.BSNode = nil
		var pos = -1
		if len(leftUnqueried) != 0 {
			cur = leftUnqueried[0]
		}
		if len(rightUnqueried) != 0 {
			if cur == nil {
				cur = rightUnqueried[0]
			} else {
				// left exists
				pos = getIdx(cur, left)
				// rightward is closer
				if getIdx(rightUnqueried[0], right) < pos {
					cur = rightUnqueried[0]
				}
			}
		}
		if cur != nil {
			ret = append(ret, cur)
		}
	}
	return ret
}

func copyReverseSlice(a []bs.KeyMV) []bs.KeyMV {
	ret := make([]bs.KeyMV, len(a))
	copy(ret, a)
	for i, j := 0, len(ret)-1; i < j; i, j = i+1, j-1 {
		ret[i], ret[j] = ret[j], ret[i]
	}
	return ret
}

func FastRefresh(target *bs.BSNode, introducers []*bs.BSNode) ([]*bs.BSNode, int, int, int, int, bool, int, int, int, int, int) {
	hops := 0
	msgs := 0
	hijacked := false
	hops_to_match := -1
	msgs_to_lookup := 0
	respCount := 0 // neighbor candidate node data size other than closer nodes data
	hopRecord := make(map[ayame.Key]int)
	maxHopRecord := make(map[ayame.Key]int)
	returnRecord := make(map[ayame.Key]int)

	leftCandidates := nsToKs(introducers)

	queried := []*bs.BSNode{} // initially empty
	rets := []*bs.BSNode{}    // rets
	tau := 1

	nexts := pickAlternately(target, leftCandidates, copyReverseSlice(leftCandidates), queried, tau)

	//for len(nexts) != 0 && underTopmost(target, nexts[0]) {
	for len(nexts) != 0 {
		//for !bs.AllContained(ksToNs(leftCandidates), queried) {
		ayame.Log.Debugf("nexts: %s\n", ayame.SliceString(nexts))
		hops++ // request
		for _, next := range nexts {
			var kNeighbors, curCandidates []*bs.BSNode
			var curLevel int

			idxs := target.RoutingTable.GetTableIndex()
			req := &bs.NeighborRequest{Key: target.Key(), MV: target.MV(), NeighborListIndex: idxs}
			msgs++
			kNeighbors, curLevel, curCandidates = FastGetNeighbors(target, req, next, hops, hopRecord, maxHopRecord, returnRecord)
			msgs++
			respCount += len(kNeighbors)
			respCount += len(curCandidates)

			target.RoutingTable.Add(next, true)
			ayame.Log.Debugf("%d added %d by response.\n", target.Key(), next.Key())

			ayame.Log.Debugf("queried %d, k-neighbor nodes for %d = %s @ level %d, candidates=%s\n",
				next.Key(), target.Key(), ayame.SliceString(kNeighbors), curLevel, ayame.SliceString(curCandidates))
			queried = append(queried, next)
			if curLevel == 0 {
				for _, cur := range kNeighbors {
					rets = appendIfMissing(rets, cur)
				}
				if hops_to_match < 0 {
					hops_to_match = hops
					ayame.Log.Debugf("found %d's k-closest: %s by %s\n", target.Key(), ayame.SliceString(kNeighbors), next)
				}
			}
			for _, n := range kNeighbors {
				leftCandidates = bs.SortCircularAppend(target.Key(), leftCandidates, n)
			}
			for _, n := range curCandidates {
				leftCandidates = bs.SortCircularAppend(target.Key(), leftCandidates, n)
			}
			leftCandidates = bs.UniqueNodes(leftCandidates)
			if !target.IsFailure && isFaultySet(ksToNs(leftCandidates)) {
				hijacked = true
				ayame.Log.Infof("candidate nodes hijacked: %s\n", leftCandidates)
			}
			ayame.Log.Debugf("hops=%d, queried=%d, leftCandidates=%s\n", hops, len(queried), ayame.SliceString(leftCandidates))
		}
		hops++ // response
		nexts = pickAlternately(target, leftCandidates, copyReverseSlice(leftCandidates), queried, tau)
	}
	ayame.Log.Debugf("%d: join-msgs %d\n", target.Key(), msgs)

	hopsSum := 0
	maxHops := 0
	returnSum := 0
	count := 0
	for _, e := range target.RoutingTable.AllNeighbors(false, false) {
		hopsSum += (hopRecord[e.Key()] + 1) / 2
		max := (maxHopRecord[e.Key()] + 1) / 2
		ayame.Log.Debugf("%d: %d hops=%d maxHops=%d count=%d\n", target.Key(), e.Key(), (hopRecord[e.Key()]+1)/2, (maxHopRecord[e.Key()]+1)/2, returnRecord[e.Key()])
		if maxHops < max {
			maxHops = max
		}
		returnSum += returnRecord[e.Key()]
		count++
	}
	ayame.Log.Debugf("%d: ave. hops=%f maxHops=%f count=%f\n", target.Key(), float64(hopsSum)/float64(count),
		float64(maxHops), float64(returnSum)/float64(count))
	//return source.routingTable.getNearestNodes(id, K), hops, msgs, hops_to_match, failure
	return rets, hops, msgs, hops_to_match, msgs_to_lookup, hijacked, respCount, hopsSum, returnSum, count, maxHops
}
