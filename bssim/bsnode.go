package main

import (
	"fmt"
	"github.com/piax/go-ayame/ayame" // "go mod tidy" is needed on go 1.16
	"github.com/thoas/go-funk"
	"strings"

	bs "github.com/piax/go-ayame/byzskip" // "go mod tidy" is needed on go 1.16
	"strconv"
	//
)

type BSNode struct {
	key int
	mv  *ayame.MembershipVector
	//bs.IntKeyMV

	routingTable    *BSRoutingTable
	isFailure       bool
	advRoutingTable *BSRoutingTable // used only collaborative adversarial node
	querySeen       map[int]int
	ayame.LocalNode
}

func (n *BSNode) Key() int {
	return n.key
}

func (n *BSNode) MV() *ayame.MembershipVector {
	return n.mv
}

func (n *BSNode) Equals(m bs.KeyMV) bool {
	return m.Key() == n.key
}

func (n *BSNode) String() string {
	ret := strconv.Itoa(n.Key())
	if n.isFailure {
		ret += "*"
	}
	return ret
}

type BSRoutingTable struct {
	//	nodes map[int]*BSNode // key=>node
	bs.RoutingTable
}

func NewBSRoutingTable(keyMV bs.KeyMV) *BSRoutingTable {
	t := bs.NewRoutingTable(keyMV)
	return &BSRoutingTable{RoutingTable: *t} //, nodes: make(map[int]*BSNode)}
}

func (table *BSRoutingTable) Count() (int, int) {
	lst := []*BSNode{}
	fcount := 0
	for _, levelTable := range table.NeighborLists {
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

func ksToNs(lst []bs.KeyMV) []*BSNode {
	ret := []*BSNode{}
	for _, ele := range lst {
		ret = append(ret, ele.(*BSNode))
	}
	return ret
}

// returns k-neighbors, the level found k-neighbors, neighbor candidates for s
func (rt *BSRoutingTable) GetNeighborsAndCandidates(s *BSNode) ([]*BSNode, int, []*BSNode) {
	ret, level := rt.GetNeighbors(s.Key())
	can := rt.GetCandidates()
	return ksToNs(ret), level, ksToNs(can)
}

func (rt *BSRoutingTable) FindCandidates(s *BSNode) []*BSNode {
	can := rt.GetCandidates()
	return ksToNs(can)
}

// failure type
const (
	F_NONE int = iota
	F_STOP
	F_COLLAB
	F_CALC
)

func NewBSNode(key int, mv *ayame.MembershipVector, isFailure bool) *BSNode {
	ret := &BSNode{key: key, mv: mv,
		//LocalNode: ayame.GetLocalNode(strconv.Itoa(key)),
		LocalNode: ayame.NewLocalNode(key),
		querySeen: make(map[int]int),
		isFailure: isFailure}
	ret.routingTable = NewBSRoutingTable(ret)
	//ret.routingTable.nodes[key] = ret
	return ret
}

// for local
func (node *BSNode) GetNeighbors(key int) ([]*BSNode, int) {
	var nb []bs.KeyMV
	var lv int
	if node.isFailure {
		switch FailureType {
		case F_STOP:
			nb = []bs.KeyMV{}
			lv = 0
		case F_COLLAB:
			if node.advRoutingTable == nil {
				qt := NewBSRoutingTable(node)
				for _, q := range AdversaryList {
					qt.Add(q)
				}
				// its a kind of cache
				node.advRoutingTable = qt
			}
			nb, lv = node.advRoutingTable.GetNeighbors(key)
		}
	} else {
		nb, lv = node.routingTable.GetNeighbors(key)
	}
	return ksToNs(nb), lv
}

func (node *BSNode) FastFindKey(key int) ([]*BSNode, int) {
	var nb []bs.KeyMV
	var lv int
	if node.isFailure {
		switch FailureType {
		case F_STOP:
			nb = []bs.KeyMV{}
			lv = 0
		case F_COLLAB:
			if node.advRoutingTable == nil {
				qt := NewBSRoutingTable(node)
				for _, q := range AdversaryList {
					qt.Add(q)
				}
				// its a kind of cache
				node.advRoutingTable = qt
			}
			nb, lv = node.advRoutingTable.GetNeighbors(key)
		}
	} else {
		nb, lv = node.routingTable.GetNeighbors(key)
	}
	return ksToNs(nb), lv
}

func (node *BSNode) FastFindNode(target *BSNode) ([]*BSNode, int, []*BSNode) {
	var nb []*BSNode
	var can []*BSNode
	var lv int
	if node.isFailure {
		switch FailureType {
		case F_STOP:
			nb = []*BSNode{}
			lv = 0
			can = []*BSNode{}
		case F_COLLAB:
			if node.advRoutingTable == nil {
				qt := NewBSRoutingTable(node)
				for _, q := range AdversaryList {
					qt.Add(q)
				}
				// its a kind of cache
				node.advRoutingTable = qt
			}
			nb, lv, can = node.advRoutingTable.GetNeighborsAndCandidates(target)
		}
	} else {
		nb, lv, can = node.routingTable.GetNeighborsAndCandidates(target)
		node.routingTable.Add(target) //
	}
	return nb, lv, can
}

func (node *BSNode) FastJoinRequest(target *BSNode) []*BSNode {
	ret := node.routingTable.FindCandidates(target)
	node.routingTable.Add(target) //
	return ret
}

func (node *BSNode) routingTableString() string {
	return node.routingTable.String()
}

func UnincludedNodes(nodes []*BSNode, queried []*BSNode) []*BSNode {
	ret := []*BSNode{}
	for _, n := range nodes {
		found := false
		for _, m := range queried {
			if n.Equals(m) {
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, n)
		}
	}
	//ayame.Log.Debugf("%d/%d queried\n", found, len(curKNodes))
	return ret
}

func allContained(curKNodes []*BSNode, queried []*BSNode) bool {
	found := 0
	for _, n := range curKNodes {
		contained := false
		for _, m := range queried {
			if n.Equals(m) {
				found++
				contained = true
				break
			}
		}
		if !contained {
			return false
		}
	}
	return true
}

func Contains(node *BSNode, nodes []*BSNode) bool {
	for _, n := range nodes {
		if n.Equals(node) {
			return true
		}
	}
	return false
}

func ContainsKey(key int, nodes []*BSNode) bool {
	for _, n := range nodes {
		if n.Key() == key {
			return true
		}
	}
	return false
}

func appendIfMissing(lst []*BSNode, node *BSNode) []*BSNode {
	for _, ele := range lst {
		if ele.Equals(node) {
			return lst
		}
	}
	return append(lst, node)
}

func appendIfMissingWithCheck(lst []*BSNode, node *BSNode) ([]*BSNode, bool) {
	for _, ele := range lst {
		if ele.Equals(node) {
			return lst, true
		}
	}
	return append(lst, node), false
}

func appendNodesIfMissing(lst []*BSNode, nodes []*BSNode) []*BSNode {
	for _, ele := range nodes {
		lst = appendIfMissing(lst, ele)
	}
	return lst
}

var FailureType int

func FastLookup(key int, source *BSNode) ([]*BSNode, int, int, int, bool) {
	hops := 0
	msgs := 0
	hops_to_match := -1
	failure := false

	hops++ // request
	msgs++
	neighbors, level := source.FastFindKey(key)
	hops++
	msgs++ // response

	rets := []*BSNode{}
	if level == 0 {
		rets = append(rets, neighbors...)
		if hops_to_match < 0 {
			hops_to_match = hops
		}
	}
	queried := []*BSNode{}

	for !allContained(neighbors, queried) {
		// get uncontained neghbors
		nexts := UnincludedNodes(neighbors, queried)
		hops++ // request
		for _, next := range nexts {
			msgs++
			curNeighbors, curLevel := next.FastFindKey(key)
			ayame.Log.Debugf("neighbors for %d = %s\n", key, ayame.SliceString(neighbors))
			msgs++
			queried = append(queried, next)
			if curLevel == 0 {
				rets = appendIfMissing(rets, next)
				if hops_to_match < 0 && ContainsKey(key, rets) {
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

func FastUpdateNeighbors(target *BSNode, source *BSNode, msg *BSUnicastEvent) int {
	//([]*BSNode, int, int, int, int, bool) {
	hops := 0
	msgs := 0

	length, _ := maxPathLength(msg.paths)
	neighbors := msg.results
	hops = int(length)
	ayame.Log.Debugf("queried %d/%d hops, neighbors for %d = %s\n", source.Key(), hops, target.Key(), ayame.SliceString(neighbors))
	target.routingTable.Add(source)

	candidates := neighbors
	queried := []*BSNode{}
	//processed := []*BSNode{}
	ayame.Log.Debugf("%d: start neighbor collection from %s, queried=%s\n", target.Key(), ayame.SliceString(candidates), ayame.SliceString(queried))

	for len(candidates) != 0 {
		next := candidates[0]

		// XXX use message
		msgs++
		newCandidates := next.FastJoinRequest(target)
		ayame.Log.Debugf("%d: join request to %d, got %s \n", target.Key(), next.Key(), ayame.SliceString(newCandidates))
		msgs++
		queried = append(queried, next)

		for _, c := range newCandidates {
			target.routingTable.Add(c)
			//processed = append(processed, c)
		}

		candidates = ksToNs(target.routingTable.GetCloserCandidates()) //append(candidates, newCandidates...)
		candidates = UnincludedNodes(candidates, queried)
		//candidates = UnincludedNodes(candidates, processed)
		ayame.Log.Debugf("%d: next candidates %s\n", target.Key(), ayame.SliceString(candidates))
	}
	ayame.Log.Debugf("%d: update neighbors msgs %d\n", target.Key(), msgs)
	return msgs
}

func FastNodeLookup(target *BSNode, source *BSNode) ([]*BSNode, int, int, int, int, bool) {
	hops := 0
	msgs := 0
	hops_to_match := -1
	failure := false
	msgs_to_lookup := 0

	hops++ // request
	msgs++
	//neighbors, level, candidates := //source.routingTable.GetNeighborsAndCandidates(target.keyMV)
	neighbors, level, candidates := source.FastFindNode(target)
	ayame.Log.Debugf("queried %d, neighbors for %d = %s\n", source.Key(), target.Key(), ayame.SliceString(neighbors))
	hops++
	msgs++ // response

	target.routingTable.Add(source)

	rets := []*BSNode{}
	if level == 0 {
		rets = append(rets, neighbors...)
		if hops_to_match < 0 {
			hops_to_match = hops
			ayame.Log.Debugf("found %d's level 0: %s\n", target.Key(), ayame.SliceString(rets))
		}
	}
	for _, c := range candidates {
		target.routingTable.Add(c)
	}

	queried := []*BSNode{}

	for !allContained(neighbors, queried) {
		// get uncontained neighbors
		nexts := UnincludedNodes(neighbors, queried)
		hops++ // request
		for _, next := range nexts {
			msgs++
			curNeighbors, curLevel, curCandidates := next.FastFindNode(target)
			ayame.Log.Debugf("queried %d, candidates for %d = %s\n", next.Key(), target.Key(), ayame.SliceString(curCandidates))
			msgs++
			queried = append(queried, next)
			if curLevel == 0 {
				rets = appendIfMissing(rets, next)
				if hops_to_match < 0 {
					hops_to_match = hops
					ayame.Log.Debugf("found %d's level 0: %s\n", target.Key(), ayame.SliceString(rets))
				}
			}
			for _, c := range curCandidates {
				target.routingTable.Add(c)
			}
			neighbors = appendNodesIfMissing(neighbors, curNeighbors)
			ayame.Log.Debugf("hops=%d, queried=%d, neighbors=%s\n", hops, len(queried), ayame.SliceString(neighbors))
		}
		hops++ // response
	}
	if hops_to_match < 0 {
		failure = true
	}

	msgs_to_lookup = msgs
	// the nodes in the current routing table, in order of closeness. (R->L->R->L in each level)
	if JoinType == J_ITER_P {
		candidates = UnincludedNodes(ksToNs(target.routingTable.GetCloserCandidates()), queried)
	} else {
		candidates = rets
		queried = []*BSNode{}
	}
	//processed := []*BSNode{}
	ayame.Log.Debugf("%d: start neighbor collection from %s, queried=%s\n", target.Key(), ayame.SliceString(candidates), ayame.SliceString(queried))

	for len(candidates) != 0 {
		next := candidates[0]

		// XXX use message
		msgs++
		newCandidates := next.FastJoinRequest(target)
		ayame.Log.Debugf("%d: join request to %d, got %s \n", target.Key(), next.Key(), ayame.SliceString(newCandidates))
		msgs++
		queried = append(queried, next)

		for _, c := range newCandidates {
			target.routingTable.Add(c)
			//processed = append(processed, c)
		}

		candidates = ksToNs(target.routingTable.GetCloserCandidates()) //append(candidates, newCandidates...)
		candidates = UnincludedNodes(candidates, queried)
		//candidates = UnincludedNodes(candidates, processed)
		ayame.Log.Debugf("%d: next candidates %s\n", target.Key(), ayame.SliceString(candidates))
	}
	ayame.Log.Debugf("%d: join-msgs %d\n", target.Key(), msgs)
	//return source.routingTable.getNearestNodes(id, K), hops, msgs, hops_to_match, failure
	return rets, hops, msgs, hops_to_match, msgs_to_lookup, failure
}

func (m *BSNode) handleUnicast(sev ayame.SchedEvent, sendToSelf bool) error {
	switch sev.(type) {
	case *BSUnicastEvent:
		return m.handleUnicastSingle(sev, sendToSelf)
	default:
		return nil
	}
}

func (m *BSNode) handleUnicastSingle(sev ayame.SchedEvent, sendToSelf bool) error {
	msg := sev.(*BSUnicastEvent)
	ayame.Log.Debugf("handling %s->%d on %s level %d\n", msg.root.Sender().Id(), msg.targetKey, msg.Receiver().Id(), msg.level)
	if !sendToSelf && msg.CheckAlreadySeen() {
		msg.root.numberOfDuplicatedMessages++
		msg.root.results = append(msg.root.results, m)
		msg.root.paths = append(msg.root.paths, msg.path)
		return nil
	}

	if msg.level == 0 { // level 0 means destination
		ayame.Log.Debugf("level=0 on %d msg=%s\n", m.key, msg)
		// reached to the destination.
		if Contains(m, msg.root.destinations) { // already arrived.
			ayame.Log.Debugf("redundant result: %s\n", msg)
		} else { // NEW!
			msg.root.destinations = append(msg.root.destinations, m)
			msg.root.destinationPaths = append(msg.root.destinationPaths, msg.path)

			if len(msg.root.destinations) == msg.root.expectedNumberOfResults {
				// XXX need to send UnicastReply
				ayame.Log.Debugf("dst=%d: completed %d\n", msg.targetKey, len(msg.root.destinations))
				//msg.root.channel <- true
				//msg.root.finishTime = sev.Time()
			} else {
				if len(msg.root.destinations) >= msg.root.expectedNumberOfResults {
					ayame.Log.Debugf("XXX should not be here. redundant results: %s\n", ayame.SliceString(msg.root.destinations))
				} else {
					ayame.Log.Debugf("wait for another result: currently %d\n", len(msg.root.destinations))
				}
			}
		}
		// add anyway to check redundancy & record number of messages
		msg.root.results = append(msg.root.results, m)
		msg.root.paths = append(msg.root.paths, msg.path)

	} else {
		nextMsgs := msg.findNextHops()
		//ayame.Log.Debugf("next msgs: %v\n", nextMsgs)
		for _, next := range nextMsgs {
			node := next.Receiver().(*BSNode)
			ayame.Log.Debugf("node: %d, m: %d\n", node.key, m.key)

			// already via the path.
			if node.Equals(m) {
				// myself
				ayame.Log.Debugf("I, %d, am one of the dest: %d, pass to myself\n", m.key, node.key)
				m.handleUnicast(next, true)
			} else {
				if Contains(node, funk.Map(msg.path, func(pe PathEntry) *BSNode { return pe.node.(*BSNode) }).([]*BSNode)) {
					// do nothing next but record to path
					ayame.Log.Debugf("I, %d, found %d is on the path %s, do nothing\n", m.key, node.key,
						strings.Join(funk.Map(msg.path, func(pe PathEntry) string {
							return fmt.Sprintf("%s@%d", pe.node.Id(), pe.level)
						}).([]string), ","))
					msg.root.results = append(msg.root.results, m)
					msg.root.paths = append(msg.root.paths, msg.path)
				} else {
					msg.SetAlreadySeen()
					ayame.Log.Debugf("I, %d, am not one of the dest: %d, forward\n", m.key, node.key)
					//ev := msg.createSubMessage(n)
					m.SendEvent(next)
				}
			}

		}
	}
	return nil
}
