package main

import (
	"fmt"

	"github.com/piax/go-ayame/ayame"
	"github.com/piax/go-ayame/byzskip"
)

func nsToKs(lst []*BSNode) []byzskip.KeyMV {
	ret := []byzskip.KeyMV{}
	for _, ele := range lst {
		ret = append(ret, ele)
	}
	return ret
}

func FastJoinAllByCheat(nodes []*BSNode) error {
	fastJoinAllInit(nodes)
	er := fastJoinAllSub(byzskip.RIGHT, nsToKs(nodes))
	if er != nil {
		return er
	}
	return fastJoinAllSub(byzskip.LEFT, nsToKs(nodes))
}

func (m *BSNode) extendRoutingTable(level int) {
	for len(m.routingTable.NeighborLists) <= level {
		maxLevel := len(m.routingTable.NeighborLists) - 1
		newLevel := maxLevel + 1
		s := byzskip.NewNeighborList(m, newLevel)
		m.routingTable.NeighborLists = append(m.routingTable.NeighborLists, s)
		// normal skip graph doesn't require this
		if maxLevel >= 0 {
			for _, n := range append(m.routingTable.NeighborLists[maxLevel].Neighbors[byzskip.RIGHT],
				m.routingTable.NeighborLists[maxLevel].Neighbors[byzskip.LEFT]...) {
				if n.MV().CommonPrefixLength(m.mv) >= newLevel {
					s.Add(byzskip.RIGHT, n)
					s.Add(byzskip.LEFT, n)
				}
			}
		}
	}
}

func fastJoinAllInit(nodes []*BSNode) {
	num := len(nodes)
	for _, n := range nodes {
		n.extendRoutingTable(0)
	}
	for i, p := range nodes {
		q := nodes[(i+1)%num]
		p.routingTable.NeighborLists[0].Neighbors[byzskip.RIGHT] = append(p.routingTable.NeighborLists[0].Neighbors[byzskip.RIGHT], q)
		q.routingTable.NeighborLists[0].Neighbors[byzskip.LEFT] = append(q.routingTable.NeighborLists[0].Neighbors[byzskip.LEFT], p)
	}
}

func delNode(nodes []byzskip.KeyMV, node byzskip.KeyMV) []byzskip.KeyMV {
	for i := range nodes {
		if nodes[i] == node {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nil
}

func lenLessThanExists(buf [][]byzskip.KeyMV) bool {
	for _, n := range buf {
		if len(n) < byzskip.K-1 {
			return true
		}
	}
	return false
}

func mergeBuf(buf [][]byzskip.KeyMV) []byzskip.KeyMV {
	ret := []byzskip.KeyMV{}
	for _, l := range buf {
		ret = append(ret, l...)
	}
	return ret
}

func fastJoinAllSub(d int, nodes []byzskip.KeyMV) error {
	unfinished := append([]byzskip.KeyMV{}, nodes...)
	for level := 0; level < ayame.MembershipVectorSize; level++ {
		nodesAtCurrentLevel := append([]byzskip.KeyMV{}, unfinished...)
		for len(nodesAtCurrentLevel) > 0 {
			buf := make([][]byzskip.KeyMV, byzskip.ALPHA)
			p := nodesAtCurrentLevel[0].(*BSNode)
			start := p
			q := p.routingTable.NeighborLists[level].Neighbors[d][0].(*BSNode)
			for {
				nodesAtCurrentLevel = delNode(nodesAtCurrentLevel, p)
				buf[p.mv.Val[level]] = delNode(buf[p.mv.Val[level]], p)
				for q != p && lenLessThanExists(buf) {
					dir := q.MV().Val[level]
					buf[dir] = append(buf[dir], q)
					q = q.routingTable.NeighborLists[level].Neighbors[d][0].(*BSNode)
				}
				merged := mergeBuf(buf)
				byzskip.SortC(p.key, merged)
				if d == byzskip.LEFT {
					ayame.ReverseSlice(merged)
				}
				p.routingTable.NeighborLists[level].Neighbors[d] = merged
				upperBuf := buf[p.mv.Val[level]]
				if len(upperBuf) > 0 {
					p.extendRoutingTable(level + 1)
					p.routingTable.NeighborLists[level+1].Neighbors[d] = append([]byzskip.KeyMV{}, upperBuf...)
				} else {
					unfinished = delNode(unfinished, p)
				}
				p = p.routingTable.NeighborLists[level].Neighbors[d][0].(*BSNode)
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
