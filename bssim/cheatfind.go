package main

import (
	"github.com/piax/go-ayame/ayame" // "go mod tidy" is needed on go 1.16

	bs "github.com/piax/go-ayame/byzskip" // "go mod tidy" is needed on go 1.16
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
	nb, lv := node.RoutingTable.GetNeighbors(key)
	return ksToNs(nb), lv
}

func FastFindNode(node *bs.BSNode, target *bs.BSNode) ([]*bs.BSNode, int, []*bs.BSNode) {
	nb, lv, can := node.GetNeighborsAndCandidates(target)
	//ayame.Log.Debugf("%s: adding %s\n", node, target)
	node.RoutingTable.Add(target)
	//ayame.Log.Debugf("%s: %d's neighbors= %s (level %d)\n updated:\n %s\n", node, target.key, ayame.SliceString(nb), lv,
	//	node.routingTable.String())
	return nb, lv, can
}

const (
	CONFUSED_ROUTING_TABLE = true
)

func FastJoinRequest(node *bs.BSNode, target *bs.BSNode, piggyback []*bs.BSNode) []*bs.BSNode {
	ret := node.GetCandidates()
	//	if !node.isFailure || FailureType == F_NONE {
	//ayame.Log.Debugf("%s: adding %s for join request\n", node, target)
	node.RoutingTable.Add(target)
	//ayame.Log.Debugf("%s: %d \n updated:\n %s\n", node, target, node.routingTable.String())
	for _, n := range piggyback {
		node.RoutingTable.Add(n)
	}
	//	}
	return ret
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

	for !bs.AllContained(neighbors, queried) {
		// get uncontained neghbors
		nexts := bs.UnincludedNodes(neighbors, queried)
		hops++ // request
		for _, next := range nexts {
			msgs++
			curNeighbors, curLevel := FastFindKey(next, key)
			ayame.Log.Debugf("neighbors for %d = %s\n", key, ayame.SliceString(neighbors))
			msgs++
			queried = append(queried, next)
			if curLevel == 0 {
				rets = appendIfMissing(rets, next)
				if hops_to_match < 0 && bs.ContainsKey(key, rets) {
					hops_to_match = hops
				}
			}
			neighbors = appendNodesIfMissing(neighbors, curNeighbors)
			ayame.Log.Debugf("hops=%d, queried=%d, neighbors=%s\n", hops, len(queried), ayame.SliceString(neighbors))
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
func FastUpdateNeighbors(target *bs.BSNode, source *bs.BSNode, initialNodes []*bs.BSNode, queried []*bs.BSNode) (int, bool) {
	hijacked := false
	if !target.IsFailure && isFaultySet(initialNodes) {
		hijacked = true
		ayame.Log.Infof("initial nodes hijacked: %s\n", initialNodes)
	}

	msgs := 0
	candidates := initialNodes
	ayame.Log.Debugf("queried %d neighbors for %d = %s\n", source.Key(), target.Key(), ayame.SliceString(candidates))

	ayame.Log.Debugf("%d: start neighbor collection from %s, queried=%s\n", target.Key(), ayame.SliceString(candidates), ayame.SliceString(queried))

	for len(candidates) != 0 {
		next := candidates[0]
		// XXX use message
		msgs++
		piggyback := []*bs.BSNode{}
		if PiggybackJoinRequest {
			piggyback = target.GetCandidates()
		}
		newCandidates := FastJoinRequest(next, target, piggyback)
		ayame.Log.Debugf("%d: join request to %d, got %s \n", target.Key(), next.Key(), ayame.SliceString(newCandidates))
		msgs++
		queried = append(queried, next)

		//ayame.Log.Debugf("%s: adding %s\n", target, ayame.SliceString(newCandidates))
		for _, c := range newCandidates {
			target.RoutingTable.Add(c)
		}
		//ayame.Log.Debugf("%s: table is updated\ntable:%s\n", target, target.routingTable.String())

		candidates = target.GetCloserCandidates()
		candidates = bs.UnincludedNodes(candidates, queried)
		ayame.Log.Debugf("%d: next candidates %s\n", target.Key(), ayame.SliceString(candidates))
	}
	ayame.Log.Debugf("%d: update neighbors msgs %d\n", target.Key(), msgs)
	return msgs, hijacked
}

func FastNodeLookup(target *bs.BSNode, source *bs.BSNode) ([]*bs.BSNode, int, int, int, int, bool) {
	hops := 0
	msgs := 0
	hops_to_match := -1
	msgs_to_lookup := 0

	hops++ // request
	msgs++
	neighbors, level, candidates := FastFindNode(source, target)
	ayame.Log.Debugf("queried %d, neighbors for %d = %s @ level %d\n", source.Key(), target.Key(), ayame.SliceString(neighbors), level)
	hops++
	msgs++ // response

	//ayame.Log.Debugf("%s: adding %s\n", target, source)
	target.RoutingTable.Add(source)

	rets := []*bs.BSNode{}
	queried := []*bs.BSNode{source}
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
			target.RoutingTable.Add(c)
		}
	}

	for !bs.AllContained(neighbors, queried) {
		ayame.Log.Debugf("neighbors: %s, queried: %s\n", ayame.SliceString(neighbors), ayame.SliceString(queried))
		// get uncontained neighbors
		nexts := bs.UnincludedNodes(neighbors, queried)
		ayame.Log.Debugf("nexts: %s\n", ayame.SliceString(nexts))
		hops++ // request
		for _, next := range nexts {
			msgs++
			curNeighbors, curLevel, curCandidates := FastFindNode(next, target)
			ayame.Log.Debugf("queried %d, neighbors for %d = %s @ level %d\n", next.Key(), target.Key(), ayame.SliceString(curNeighbors), curLevel)
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
				//ayame.Log.Debugf("%s: adding %s\n", target, ayame.SliceString(curCandidates))
				for _, c := range curCandidates {
					target.RoutingTable.Add(c)
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
		candidates = bs.UnincludedNodes(ksToNs(target.RoutingTable.GetCloserCandidates()), queried)
	} else {
		candidates = rets
		queried = []*bs.BSNode{}
	}
	ayame.Log.Debugf("%d: table=%s, candidates=%s, queried=%s\n", target.Key(), ayame.SliceString(target.RoutingTable.GetAll()), ayame.SliceString(candidates), ayame.SliceString(queried))
	//processed := []*bs.BSNode{}
	umsgs, failure := FastUpdateNeighbors(target, source, candidates, queried)
	msgs += umsgs
	ayame.Log.Debugf("%d: join-msgs %d\n", target.Key(), msgs)
	//return source.routingTable.getNearestNodes(id, K), hops, msgs, hops_to_match, failure
	return rets, hops, msgs, hops_to_match, msgs_to_lookup, failure
}
