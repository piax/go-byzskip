package main

import (

	// "go mod tidy" is needed on go 1.16

	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip" // "go mod tidy" is needed on go 1.16
	//
)

type StopRoutingTable struct {
}

func NewStopRoutingTable(keyMV bs.KeyMV) bs.RoutingTable {
	return &StopRoutingTable{} //, nodes: make(map[int]*BSNode)}
}

func (table *StopRoutingTable) KClosestWithKey(k ayame.Key) ([]bs.KeyMV, int) {
	return []bs.KeyMV{}, 0
}

func (table *StopRoutingTable) KClosest(req *bs.NeighborRequest) ([]bs.KeyMV, int) {
	return []bs.KeyMV{}, 0
}

func (table *StopRoutingTable) PossiblyBeAdded(km bs.KeyMV) bool {
	return false
}

// get all disjoint entries
func (table *StopRoutingTable) AllNeighbors(includeSelf bool, sorted bool) []bs.KeyMV {
	return []bs.KeyMV{}
}

func (table *StopRoutingTable) GetCommonNeighbors(mv *ayame.MembershipVector) []bs.KeyMV {
	return []bs.KeyMV{}
}

func (table *StopRoutingTable) HasSufficientNeighbors() bool {
	return true
}

func (table *StopRoutingTable) Add(c bs.KeyMV, truncate bool) {
	// do nothing.
}

func (table *StopRoutingTable) Delete(key ayame.Key) {
	// do nothing.
}

func (table *StopRoutingTable) Del(km bs.KeyMV) {
	// do nothing.
}

func (table *StopRoutingTable) GetNeighborLists() []*bs.NeighborList {
	return []*bs.NeighborList{}
}

func (table *StopRoutingTable) AddNeighborList(s *bs.NeighborList) {
	// do nothing.
}

func (table *StopRoutingTable) Neighbors(req *bs.NeighborRequest) []bs.KeyMV {
	return []bs.KeyMV{}
}

func (table *StopRoutingTable) GetTableIndex() []*bs.TableIndex {
	return []*bs.TableIndex{}
}

func (table *StopRoutingTable) GetClosestIndex() *bs.TableIndex {
	return &bs.TableIndex{}
}

func (table *StopRoutingTable) String() string {
	ret := ""
	for _, sl := range table.GetNeighborLists() {
		ret += sl.String() + "\n"
	}
	return ret
}

func (table *StopRoutingTable) Size() int {
	return 0
}

func (table *StopRoutingTable) PureSize() int {
	return 0
}
