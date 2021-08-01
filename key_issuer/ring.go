package main

import (
	"sort"
)

type Ring []interface{}

func (r Ring) Len() int { return len(r) }

func (r *Ring) Push(x interface{}) {
	*r = append(*r, x)
}

func (r Ring) Find(key float64) (int, *Node) {
	for i, n := range r {
		node := n.(*Node)
		if node.netKey == key {
			return i, node
		}
	}
	return -1, nil
}

func (r Ring) Update() {
	sort.Slice(r, func(i, j int) bool { return r.Nth(i).netKey < r.Nth(j).netKey })
}

func (r Ring) Nth(index int) *Node {
	return r[index].(*Node)
}

func (r Ring) Prev(index int) (int, *Node) {
	if index == 0 {
		return r.Len() - 1, r[r.Len()-1].(*Node)
	}
	return index - 1, r[index-1].(*Node)
}

func (r Ring) Next(index int) (int, *Node) {
	if index == r.Len()-1 {
		return 0, r[0].(*Node)
	}
	return index + 1, r[index+1].(*Node)
}
