package main

import (

	// "go mod tidy" is needed on go 1.16

	bs "github.com/piax/go-ayame/byzskip" // "go mod tidy" is needed on go 1.16
	//
)

type AdversaryRoutingTable struct {
	normal      bs.RoutingTable
	adversarial bs.RoutingTable
}

func NewAdversaryRoutingTable(keyMV bs.KeyMV) bs.RoutingTable {
	return &AdversaryRoutingTable{normal: NewBSRoutingTable(keyMV), adversarial: NewBSRoutingTable(keyMV)} //, nodes: make(map[int]*BSNode)}
}

// get k neighbors and its level
func (table *AdversaryRoutingTable) GetNeighbors(k int) ([]bs.KeyMV, int) {
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

func (table *AdversaryRoutingTable) GetCloserCandidates() []bs.KeyMV {
	// should be confused?
	return table.normal.GetAll()
}

// called as a normal behavior
func (table *AdversaryRoutingTable) Add(c bs.KeyMV) {
	table.normal.Add(c)
}

// called by adversarial community
func (table *AdversaryRoutingTable) AddAdversarial(c bs.KeyMV) {
	table.adversarial.Add(c)
}

func (table *AdversaryRoutingTable) GetNeighborLists() []*bs.NeighborList {
	return table.normal.GetNeighborLists()
}

func (table *AdversaryRoutingTable) AddNeighborList(s *bs.NeighborList) {
	table.normal.AddNeighborList(s)
}

func (table *AdversaryRoutingTable) String() string {
	return table.adversarial.String()
}

func (table *AdversaryRoutingTable) Size() int {
	return -100000000000 // not used
}
