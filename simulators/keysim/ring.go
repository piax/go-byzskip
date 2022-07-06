package main

import (
	"sort"

	ki "github.com/piax/go-byzskip/key_issuer"
)

type Ring []interface{}

func (r Ring) Len() int { return len(r) }

func (r *Ring) Push(x interface{}) {
	*r = append(*r, x)
}

func (r Ring) Find(targetKey float64) (int, *ki.Node) {
	var cur *ki.Node
	var index int
	for i := 0; i < r.Len(); i++ {
		if i+1 == r.Len() && r.Nth(i).Key() <= targetKey { // the last one
			cur = r.Nth(0)
			index = 0
		} else if r.Nth(i).Key() <= targetKey && targetKey < r.Nth(i+1).Key() {
			cur = r.Nth(i)
			index = i
		} else if i == 0 && targetKey < r.Nth(i).Key() { // the first one
			cur = r.Nth(r.Len() - 1)
			index = r.Len() - 1
		}
	}
	return index, cur
}

func (r Ring) Update() {
	sort.Slice(r, func(i, j int) bool { return r.Nth(i).Key() < r.Nth(j).Key() })
}

func (r Ring) Nth(index int) *ki.Node {
	return r[index].(*ki.Node)
}

func (r Ring) Prev(index int) (int, *ki.Node) {
	if index == 0 {
		return r.Len() - 1, r[r.Len()-1].(*ki.Node)
	}
	return index - 1, r[index-1].(*ki.Node)
}

func (r Ring) Next(index int) (int, *ki.Node) {
	if index == r.Len()-1 {
		return 0, r[0].(*ki.Node)
	}
	return index + 1, r[index+1].(*ki.Node)
}
