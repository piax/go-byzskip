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
func (table *AdversaryRoutingTable) GetNeighbors(k ayame.Key) ([]bs.KeyMV, int) {
	if FailureType == F_NONE {
		return table.normal.GetNeighbors(k)
	} else {
		return table.adversarial.GetNeighbors(k)
	}
}

// get all disjoint entries
func (table *AdversaryRoutingTable) GetAll() []bs.KeyMV {
	if FailureType == F_NONE {
		return table.normal.GetAll()
	} else {
		return table.adversarial.GetAll()
	}
}

// get all disjoint entries
func (table *AdversaryRoutingTable) GetCommonNeighbors(mv *ayame.MembershipVector) []bs.KeyMV {
	if FailureType == F_NONE {
		return table.normal.GetCommonNeighbors(mv)
	} else {
		return table.adversarial.GetCommonNeighbors(mv)
	}
}

func (table *AdversaryRoutingTable) GetCloserCandidates() []bs.KeyMV {
	// should be confused?
	return table.normal.GetAll()
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
