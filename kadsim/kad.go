package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"log"
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	pstore "github.com/libp2p/go-libp2p-peerstore"
	mh "github.com/multiformats/go-multihash"
	"github.com/piax/go-ayame/ayame" // "go mod tidy" is needed on go 1.16
	"github.com/thoas/go-funk"
)

const (
//	K = 16 // Ethereum's default
)

var NoOpThreshold = 100 * time.Hour

type KADRoutingTable struct {
	table *kbucket.RoutingTable
	nodes map[peer.ID]*KADNode
	dhtId kbucket.ID
	idstr string
}

func NewKADRoutingTableForQuery(dhtId kbucket.ID, k int) *KADRoutingTable {
	m := pstore.NewMetrics()

	idstr := hex.EncodeToString(dhtId)

	t, _ := kbucket.NewRoutingTable(k, dhtId,
		time.Hour, m, NoOpThreshold, nil)
	return &KADRoutingTable{table: t, idstr: idstr, dhtId: dhtId, nodes: make(map[peer.ID]*KADNode)}
}

func NewKADRoutingTable(id peer.ID, k int) *KADRoutingTable {
	m := pstore.NewMetrics()

	dhtId := kbucket.ConvertPeerID(id)
	idstr := hex.EncodeToString(dhtId)

	t, _ := kbucket.NewRoutingTable(k, dhtId,
		time.Hour, m, NoOpThreshold, nil)
	return &KADRoutingTable{table: t, idstr: idstr, dhtId: dhtId, nodes: make(map[peer.ID]*KADNode)}
}

func (rt *KADRoutingTable) Add(n *KADNode) {
	rt.table.TryAddPeer(n.id, true, true)
	rt.nodes[n.id] = n
}

func (rt *KADRoutingTable) Count() (int, int) {
	fcount := 0
	for _, n := range rt.nodes {
		if n.isFailure {
			fcount++
		}
	}
	return len(rt.nodes), fcount
}

func (rt *KADRoutingTable) getNearestNodes(dhtId kbucket.ID, k int) []*KADNode {
	closer := rt.table.NearestPeers(dhtId, k)
	nearests := funk.Map(closer, func(i peer.ID) *KADNode {
		return rt.nodes[i]
	}).([]*KADNode)
	if len(nearests) < k {
		return nearests
	}
	return nearests[0:k]
}

func (rt *KADRoutingTable) PickClosestUncontained(target []*KADNode, alpha int, k int) ([]*KADNode, bool) {
	closer := rt.table.NearestPeers(rt.dhtId, k)
	/*sortedIds := funk.Filter(closer, func(i peer.ID) bool {
		for _, n := range target {
			if n.id == i {
				return false
			}
		}
		return true
	}).([]peer.ID)*/
	sortedIds := []peer.ID{}
	for _, n := range closer {
		contained := false
		for _, m := range target {
			if n == m.id {
				contained = true
				break
			}
		}
		if !contained {
			sortedIds = append(sortedIds, n)
		}
	}
	//fmt.Printf("sortedIds=%d/target=%d", len(sortedIds), len(target))

	var pickedSortedIds []peer.ID
	if len(sortedIds) < alpha {
		pickedSortedIds = sortedIds
	} else {
		pickedSortedIds = sortedIds[0:alpha]
	}
	nds := funk.Map(pickedSortedIds, func(i peer.ID) *KADNode {
		return rt.nodes[i]
	}).([]*KADNode)
	return nds, false
}

// target must be nodes in the routing table.
// returns nil if all contained.
func (rt *KADRoutingTable) pickClosestUncontained(target []*KADNode, alpha int) ([]*KADNode, bool) {
	ret := []peer.ID{}
	original := rt.table.ListPeers()
	for _, n := range original {
		contained := false
		for _, m := range target {
			if n == m.id {
				contained = true
				break
			}
		}
		if !contained {
			ret = append(ret, n)
		}
	}
	if len(ret) == 0 {
		return nil, true
	}
	sortedIds := kbucket.SortClosestPeers(ret, rt.dhtId)
	//fmt.Printf("sortedIds=%d/original=%d/target=%d\n", len(sortedIds), len(original), len(target))
	var pickedSortedIds []peer.ID
	if len(sortedIds) < alpha {
		pickedSortedIds = sortedIds
	} else {
		pickedSortedIds = sortedIds[0:alpha]
	}
	nds := funk.Map(pickedSortedIds, func(i peer.ID) *KADNode {
		return rt.nodes[i]
	}).([]*KADNode)
	return nds, false
	//	if len(sortedIds) >0 {
	//	}
	// should not reach here
	///	return nil, true
}

// failure type
const (
	F_NONE int = iota
	F_STOP
	F_COLLAB
)

type KADNode struct {
	number       int // just for debugging
	id           peer.ID
	routingTable *KADRoutingTable
	isFailure    bool
	ayame.LocalNode
}

func RandPeerID() (peer.ID, error) {
	buf := make([]byte, 16)
	rand.Read(buf)
	h, _ := mh.Sum(buf, mh.SHA2_256, -1)
	return peer.ID(h), nil
}

func NewKADNode(number int, k int, isFailure bool) *KADNode {
	id, err := RandPeerID()
	if err != nil {
		log.Fatalf("error: %s", err)
	}
	table := NewKADRoutingTable(id, k)
	return &KADNode{number: number, id: id, routingTable: table, LocalNode: ayame.GetLocalNode(table.idstr), isFailure: isFailure}
}

func (n *KADNode) equals(m *KADNode) bool {
	return n.id == m.id
}

//func PickNotQueried(from []*KadNode, queried []dht.ID) []*KadNode {
//	ret := []*KadNode{}
//	for _, v := range queried {

//	}
//	return true
//}

// lst is modified

func (node *KADNode) FastFindNode(id kbucket.ID, source *KADNode, k int) []*KADNode {
	node.routingTable.Add(source)
	return node.routingTable.getNearestNodes(id, k)
}

/* XXX not yet
func (node *KADNode) handleUnicast(sev ayame.SchedEvent) error {
	msg := sev.(*KADUnicastEvent)

	node.routingTable.Add(msg.Sender().(*KADNode))

	return nil
}*/

func numberOfUnincludedNodes(curKNodes []*KADNode, queried []*KADNode) int {
	found := 0
	for _, n := range curKNodes {
		for _, m := range queried {
			if n.equals(m) {
				found++
				break
			}
		}
	}
	//ayame.Log.Debugf("%d/%d queried\n", found, len(curKNodes))
	return len(curKNodes) - found
}

func AllContainedFast(curKNodes []*KADNode, queried []*KADNode) bool {
	found := 0
	for _, n := range curKNodes {
		contained := false
		for _, m := range queried {
			if n.equals(m) {
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

func matchNode(id kbucket.ID, nodes []*KADNode) *KADNode {
	for _, n := range nodes {
		if bytes.Equal(n.routingTable.dhtId, id) {
			return n
		}
	}
	return nil
}

func NodeSliceString(lst []*KADNode) string {
	ret := "["
	for i, l := range lst {
		ret += strconv.Itoa(l.number)
		if i != len(lst)-1 {
			ret += ", "
		}
	}
	ret += "]"
	return ret
}

var FailureType int

func FastNodeLookup(id kbucket.ID, source *KADNode, alpha int, k int) ([]*KADNode, int, int, int, bool) {
	switch FailureType {
	case F_STOP:
		return FastNodeLookupWithStopFailure(id, source, alpha, k)
	}
	return FastNodeLookupNoFailure(id, source, alpha, k)
}

func FastNodeLookupNoFailure(id kbucket.ID, source *KADNode, alpha int, k int) ([]*KADNode, int, int, int, bool) {
	queryTable := NewKADRoutingTableForQuery(id, k)
	queryTable.Add(source)
	source.routingTable.Add(source)

	hops := 0
	msgs := 0
	failure := false
	hops_to_match := -1
	curKNodes := queryTable.getNearestNodes(id, k)
	queried := []*KADNode{}
	nodes, _ := queryTable.pickClosestUncontained(queried, alpha)
	prevCurKNodes := append([]*KADNode{}, curKNodes...)
	// if all nearest k nodes are queried, finish the FIND_NODE
	for numberOfUnincludedNodes(curKNodes, queried) != 0 {
		if matchNode(id, curKNodes) != nil {
			if hops_to_match < 0 {
				ayame.Log.Debugf("matched hops=%d\n", hops)
				hops_to_match = hops + 1 // +1 hop to actually reach to the node
			}
		}
		hops++ // FIND_NODEs
		for _, n := range nodes {
			msgs++ // FIND_NODE
			founds := n.FastFindNode(id, source, k)
			msgs++ // NEIGHBORS
			for _, found := range founds {
				queryTable.Add(found)
				source.routingTable.Add(found)
			}
			queried = append(queried, n)
		}
		curKNodes = queryTable.getNearestNodes(id, k)
		nodes, _ = queryTable.pickClosestUncontained(queried, alpha)
		ayame.Log.Debugf("hops=%d, diff=%d, queried=%d, next=%d\n", hops_to_match, numberOfUnincludedNodes(curKNodes, prevCurKNodes), len(queried), len(nodes))
		prevCurKNodes = append([]*KADNode{}, curKNodes...)
		hops++ // NIGHBORSs
	}
	if hops_to_match < 0 {
		failure = true
	}
	ayame.Log.Debugf("END: hops=%d, diff=%d, queried=%d, next=%d\n", hops_to_match, numberOfUnincludedNodes(curKNodes, prevCurKNodes), len(queried), len(nodes))
	//return source.routingTable.getNearestNodes(id, K), hops, msgs, hops_to_match, failure
	return queryTable.getNearestNodes(id, k), hops, msgs, hops_to_match, failure
}

func FastNodeLookupWithStopFailure(id kbucket.ID, source *KADNode, alpha int, k int) ([]*KADNode, int, int, int, bool) {
	queryTable := NewKADRoutingTableForQuery(id, k)
	queryTable.Add(source)
	source.routingTable.Add(source)

	hops := 0
	msgs := 0
	failure := false
	hops_to_match := -1
	curKNodes := queryTable.getNearestNodes(id, k)
	queried := []*KADNode{}
	nodes, _ := queryTable.pickClosestUncontained(queried, alpha)
	prevCurKNodes := append([]*KADNode{}, curKNodes...)
	// if all nearest k nodes are queried, finish the FIND_NODE
	for numberOfUnincludedNodes(curKNodes, queried) != 0 {
		if matchNode(id, curKNodes) != nil {
			if hops_to_match < 0 {
				ayame.Log.Debugf("matched hops=%d\n", hops)
				hops_to_match = hops
			}
		}
		hops++ // FIND_NODEs
		for _, n := range nodes {
			msgs++ // FIND_NODE
			var founds []*KADNode
			if n.isFailure {
				founds = []*KADNode{}
			} else {
				founds = n.FastFindNode(id, source, k)
			}
			msgs++ // NEIGHBORS
			for _, found := range founds {
				queryTable.Add(found)
				source.routingTable.Add(found)
			}
			queried = append(queried, n)
		}
		curKNodes = queryTable.getNearestNodes(id, k)
		nodes, _ = queryTable.pickClosestUncontained(queried, alpha)
		ayame.Log.Debugf("hops=%d, diff=%d, queried=%d, next=%d\n", hops_to_match, numberOfUnincludedNodes(curKNodes, prevCurKNodes), len(queried), len(nodes))
		prevCurKNodes = append([]*KADNode{}, curKNodes...)
		hops++ // NIGHBORSs
	}
	if hops_to_match < 0 {
		failure = true
	}
	ayame.Log.Debugf("END: hops=%d, diff=%d, queried=%d, next=%d\n", hops_to_match, numberOfUnincludedNodes(curKNodes, prevCurKNodes), len(queried), len(nodes))
	//return source.routingTable.getNearestNodes(id, K), hops, msgs, hops_to_match, failure
	return queryTable.getNearestNodes(id, k), hops, msgs, hops_to_match, failure
}
