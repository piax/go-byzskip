package main

import (
	"bytes"
	"math"
	"time"

	kbucket "github.com/libp2p/go-libp2p-kbucket"
	"github.com/piax/go-byzskip/ayame"
)

type DisjointQuery struct {
	index         int
	queryTable    *KADRoutingTable
	curKNodes     []*KADNode
	hops          int
	msgs          int
	hops_to_match int
	finished      bool
}

func NewDisjointQuery(id kbucket.ID, k int, index int) *DisjointQuery {
	return &DisjointQuery{index: index, queryTable: NewKADRoutingTableForQuery(id, k), curKNodes: []*KADNode{}, hops: 0, msgs: 0, hops_to_match: -1}
}

func (dq *DisjointQuery) Add(node *KADNode) {
	dq.queryTable.Add(node)
}

func (dq *DisjointQuery) DoNextQueries(id kbucket.ID, source *KADNode, alpha int, k int, queried []*KADNode) []*KADNode {
	if matchNode(id, dq.curKNodes) != nil {
		if dq.hops_to_match < 0 {
			log.Debugf("found!! matched hops=%d", dq.hops)
			dq.hops_to_match = dq.hops // actually, +1 is needed to reach the node.
		}
	}
	dq.hops++
	// select alpha nodes which weren't queried yet
	curNodes, _ := dq.queryTable.pickClosestUncontained(queried, alpha)
	for _, n := range curNodes {
		dq.msgs++ // FIND_NODE
		var founds []*KADNode
		if n.isFailure {
			switch FailureType {
			case F_STOP:
				founds = []*KADNode{}
			case F_COLLAB:
				fallthrough
			case F_COLLAB_AFTER:
				qt := NewKADRoutingTableForQuery(id, k)
				for _, q := range JoinedAdversaryList {
					qt.Add(q)
				}
				founds = qt.getNearestNodes(id, k)
			case F_NONE:
				founds = n.FastFindNode(id, source, k)
			}
		} else {
			founds = n.FastFindNode(id, source, k)
		}
		//founds := n.FastFindNode(id, source)
		dq.msgs++ // NEIGHBORS
		for _, found := range founds {
			dq.queryTable.Add(found)
			if !bytes.Equal(source.routingTable.dhtId, found.routingTable.dhtId) {
				source.routingTable.Add(found)
			}
		}
		queried = append(queried, n)
	}
	dq.hops++
	dq.curKNodes = dq.queryTable.getNearestNodes(id, k)
	dq.updateFinished(queried)

	log.Debugf("%d th: hops=%d, for %d, queried=%s\n", dq.index, dq.hops, len(curNodes), ayame.SliceString(curNodes))
	if matchNode(id, dq.curKNodes) != nil {
		if dq.hops_to_match < 0 {
			log.Debugf("found!! matched hops=%d", dq.hops)
			dq.hops_to_match = dq.hops // actually, +1 is needed to reach the node.
		}
	}
	return queried
}

func (dq *DisjointQuery) updateFinished(queried []*KADNode) bool {
	dq.finished = (numberOfUnincludedNodes(dq.curKNodes, queried) == 0)
	return dq.finished
}

func allFinished(contexts []*DisjointQuery, queried []*KADNode) bool {
	for _, dq := range contexts {
		if !dq.finished {
			return false
		}
	}
	return true
}

func FastNodeLookupDisjoint(id kbucket.ID, source *KADNode, alpha int, k int, d int, initSrc bool) ([]*KADNode, float64, int, float64, bool) {
	contexts := make([]*DisjointQuery, d)
	for i := 0; i < d; i++ {
		contexts[i] = NewDisjointQuery(id, k, i)
	}
	// for source table update
	//source.routingTable.Add(source)

	msgs := 0
	success := false

	imsgs := 0 // initial find_node msgs
	ihops := 0

	imsgs++
	ihops++
	initialKNodes := source.FastFindNode(id, source, k)
	imsgs++
	ihops++

	if initSrc && matchNode(id, initialKNodes) != nil {
		//log.Infof("src contains dst ret=%s\n", initialKNodes)
		return initialKNodes, 0, 0, 0, false
	}

	pos := 0
	// distribute to d query contexts.
	for _, n := range initialKNodes {
		contexts[pos%d].Add(n)
		pos++
	}

	queried := []*KADNode{}
	// if all nearest k nodes are queried, finish the FIND_NODE
	for !allFinished(contexts, queried) {
		for _, dq := range contexts {
			if !dq.finished {
				// XXX do the one query
				queried = dq.DoNextQueries(id, source, alpha, k, queried)
			}
		}
	}

	minHopsToMatch := math.MaxFloat64
	maxHops := float64(-1)

	for _, dq := range contexts {
		if matchNode(id, dq.curKNodes) != nil {
			success = true
		}
		if dq.hops_to_match >= 0 {
			minHopsToMatch = math.Min(float64(dq.hops_to_match), minHopsToMatch)
		}
		maxHops = math.Max(float64(dq.hops), float64(maxHops))
		msgs += dq.msgs
		log.Debugf("%d th: result=%s\n", dq.index, ayame.SliceString(dq.curKNodes))
	}
	qt := NewKADRoutingTableForQuery(id, k)
	for _, q := range queried {
		qt.Add(q)
	}
	//return source.routingTable.getNearestNodes(id, K), hops, msgs, hops_to_match, failure
	ret := qt.getNearestNodes(id, k)

	// XXX
	//if bytes.Equal(source.routingTable.dhtId, id) {
	//	ret = append(ret, source)
	//}
	log.Debugf("result=%s\n", ayame.SliceString(ret))

	// initial FIND_NODE
	if !initSrc {
		maxHops += float64(ihops)
		msgs += imsgs
	}

	source.routingTable.table.ResetCplRefreshedAtForID(id, time.Now())

	return ret, maxHops, msgs, minHopsToMatch, !success
}
