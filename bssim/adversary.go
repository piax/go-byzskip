package main

import (

	// "go mod tidy" is needed on go 1.16

	"github.com/piax/go-ayame/ayame"
	bs "github.com/piax/go-ayame/byzskip" // "go mod tidy" is needed on go 1.16
	//
)

type AdversaryRoutingTable struct {
	normal      bs.RoutingTable
	adversarial bs.RoutingTable
}

func NewAdversaryRoutingTable(keyMV bs.KeyMV) bs.RoutingTable {
	return &AdversaryRoutingTable{normal: bs.NewBSRoutingTable(keyMV), adversarial: bs.NewBSRoutingTable(keyMV)} //, nodes: make(map[int]*BSNode)}
}

// get k neighbors and its level
func (table *AdversaryRoutingTable) KClosest(k ayame.Key) ([]bs.KeyMV, int) {
	if FailureType == F_NONE { // Only in F_COLLAB_AFTER, join time.
		return table.normal.KClosest(k)
	} else {
		return table.adversarial.KClosest(k)
	}
}

// get all disjoint entries
func (table *AdversaryRoutingTable) AllNeighbors(includeSelf bool, sorted bool) []bs.KeyMV {
	// Use normal neighbors to advertise itself(adversarial) to the normal network.
	return table.normal.AllNeighbors(includeSelf, sorted)
}

// get all disjoint entries
func (table *AdversaryRoutingTable) GetCommonNeighbors(mv *ayame.MembershipVector) []bs.KeyMV {
	if FailureType == F_COLLAB_AFTER {
		return table.normal.GetCommonNeighbors(mv)
	} else {
		// stronger attacker
		return table.adversarial.GetCommonNeighbors(mv)
	}
}

// get neighbor candidates that belongs to the same ring and satisfies index
func (table *AdversaryRoutingTable) Neighbors(req *bs.NeighborRequest) []bs.KeyMV {
	if FailureType == F_NONE {
		return table.normal.Neighbors(req)
	} else {
		// stronger attacker
		return table.adversarial.Neighbors(req)
	}
}

func (table *AdversaryRoutingTable) GetTableIndex() []*bs.TableIndex {
	if FailureType == F_NONE {
		return table.normal.GetTableIndex()
	} else {
		return table.adversarial.GetTableIndex()
	}
}

// called as a normal behavior
func (table *AdversaryRoutingTable) Add(c bs.KeyMV) {
	table.normal.Add(c)
}

func (table *AdversaryRoutingTable) Delete(key ayame.Key) {
	table.normal.Delete(key)
}

// called by adversarial community
func (table *AdversaryRoutingTable) AddAdversarial(c bs.KeyMV) {
	table.adversarial.Add(c)
}

func (table *AdversaryRoutingTable) GetNeighborLists() []*bs.NeighborList {
	if FailureType == F_NONE {
		return table.normal.GetNeighborLists()
	} else {
		return table.adversarial.GetNeighborLists()
	}
}

func (table *AdversaryRoutingTable) AddNeighborList(s *bs.NeighborList) {
	// only for cheat
	table.normal.AddNeighborList(s)
}

func (table *AdversaryRoutingTable) String() string {
	return table.adversarial.String()
}

func (table *AdversaryRoutingTable) Size() int {
	return -100000000000 // not used
}
