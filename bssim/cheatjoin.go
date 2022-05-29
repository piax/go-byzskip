package main

import (
	"fmt"

	"github.com/piax/go-ayame/ayame"
	bs "github.com/piax/go-ayame/byzskip"
	"github.com/thoas/go-funk"
)

func nsToKs(lst []*bs.BSNode) []bs.KeyMV {
	ret := []bs.KeyMV{}
	for _, ele := range lst {
		ret = append(ret, ele)
	}
	return ret
}

func FastJoinAllByCheat(nodes []*bs.BSNode) error {
	fastJoinAllInit(nodes)
	er := fastJoinAllSub(bs.RIGHT, nsToKs(nodes))
	if er != nil {
		return er
	}
	er = fastJoinAllSub(bs.LEFT, nsToKs(nodes))
	if er != nil {
		return er
	}
	//if !bs.SYMMETRIC_ROUTING_TABLE {
	//for _, n := range nodes {
	//n.RoutingTable.(*bs.SkipRoutingTable).TrimRoutingTable()
	//}
	//}
	return nil
}

func extendRoutingTable(m *bs.BSNode, level int) {
	for len(m.RoutingTable.GetNeighborLists()) <= level {
		maxLevel := len(m.RoutingTable.GetNeighborLists()) - 1
		newLevel := maxLevel + 1
		s := bs.NewNeighborList(m, newLevel)
		m.RoutingTable.AddNeighborList(s)
		//m.routingTable.NeighborLists = append(m.routingTable.NeighborLists, s)
		// normal skip graph doesn't require this
		if maxLevel >= 0 {
			for _, n := range append(m.RoutingTable.GetNeighborLists()[maxLevel].Neighbors[bs.RIGHT],
				m.RoutingTable.GetNeighborLists()[maxLevel].Neighbors[bs.LEFT]...) {
				if n.MV().CommonPrefixLength(m.MV()) >= newLevel {
					s.Add(bs.RIGHT, n, true)
					s.Add(bs.LEFT, n, true)
				}
			}
		}
	}
}

func fastJoinAllInit(nodes []*bs.BSNode) {
	num := len(nodes)
	for _, n := range nodes {
		extendRoutingTable(n, 0)
	}
	for i, p := range nodes {
		q := nodes[(i+1)%num]
		p.RoutingTable.GetNeighborLists()[0].Neighbors[bs.RIGHT] = append(p.RoutingTable.GetNeighborLists()[0].Neighbors[bs.RIGHT], q)
		q.RoutingTable.GetNeighborLists()[0].Neighbors[bs.LEFT] = append(q.RoutingTable.GetNeighborLists()[0].Neighbors[bs.LEFT], p)
	}
}

func delNode(nodes []bs.KeyMV, node bs.KeyMV) []bs.KeyMV {
	for i := range nodes {
		if nodes[i] == node {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nil
}

func lenLessThanExists(buf [][]bs.KeyMV) bool {
	for _, n := range buf {
		if len(n) < bs.K-1 {
			return true
		}
	}
	return false
}

func mergeBuf(buf [][]bs.KeyMV) []bs.KeyMV {
	ret := []bs.KeyMV{}
	for _, l := range buf {
		ret = append(ret, l...)
	}
	return ret
}

func fastJoinAllSub(d int, nodes []bs.KeyMV) error {
	unfinished := append([]bs.KeyMV{}, nodes...)
	for level := 0; level < ayame.MembershipVectorSize; level++ {
		nodesAtCurrentLevel := append([]bs.KeyMV{}, unfinished...)
		for len(nodesAtCurrentLevel) > 0 {
			buf := make([][]bs.KeyMV, bs.ALPHA)
			p := nodesAtCurrentLevel[0].(*bs.BSNode)
			start := p
			q := p.RoutingTable.GetNeighborLists()[level].Neighbors[d][0].(*bs.BSNode)
			for {
				nodesAtCurrentLevel = delNode(nodesAtCurrentLevel, p)
				buf[p.MV().Val[level]] = delNode(buf[p.MV().Val[level]], p)
				for q != p && lenLessThanExists(buf) {
					dir := q.MV().Val[level]
					buf[dir] = append(buf[dir], q)
					q = q.RoutingTable.GetNeighborLists()[level].Neighbors[d][0].(*bs.BSNode)
				}
				merged := mergeBuf(buf)
				if !bs.SYMMETRIC_ROUTING_TABLE {
					if len(buf[p.MV().Val[level]]) > bs.K-2 {
						last := buf[p.MV().Val[level]][bs.K-2]
						if d == bs.RIGHT {
							merged = funk.Filter(merged, func(x bs.KeyMV) bool {
								return bs.IsOrdered(p.Key(), false, x.Key(), last.Key(), true)
							}).([]bs.KeyMV)
						} else { // LEFT
							merged = funk.Filter(merged, func(x bs.KeyMV) bool {
								return bs.IsOrdered(last.Key(), true, x.Key(), p.Key(), false)
							}).([]bs.KeyMV)
						}
					}
				}
				bs.SortC(p.Key(), merged)
				if d == bs.LEFT {
					ayame.ReverseSlice(merged)
				}
				p.RoutingTable.GetNeighborLists()[level].Neighbors[d] = merged
				upperBuf := buf[p.MV().Val[level]]
				if len(upperBuf) > 0 {
					extendRoutingTable(p, level+1)
					p.RoutingTable.GetNeighborLists()[level+1].Neighbors[d] = append([]bs.KeyMV{}, upperBuf...)
				} else {
					unfinished = delNode(unfinished, p)
				}
				p = p.RoutingTable.GetNeighborLists()[level].Neighbors[d][0].(*bs.BSNode)
				if p == start {
					break
				}
			}
		}
		if len(unfinished) == 0 {
			break
		}
	}
	if len(unfinished) != 0 {
		return fmt.Errorf("insuffichent membership vector digits")
	}
	return nil
}
