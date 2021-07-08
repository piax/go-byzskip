package main

import (
	"fmt"

	"github.com/piax/go-ayame/ayame"
)

type Array []*BSNode
type Graph map[int][]*BSNode

func (arr Array) hasPropertyOf(node *BSNode) bool {
	for _, v := range arr {
		if node.Equals(v) {
			return true
		}
	}
	return false
}

func (graph Graph) Dump() {
	for k, v := range graph {
		fmt.Printf("%d: %s\n", k, ayame.SliceString(v))
	}
}

func (graph Graph) Register(node *BSNode) {
	if _, exist := graph[node.key]; !exist {
		graph[node.key] = Array{} //[]*MIRONode{node}
	}
}

func (graph Graph) AddChild(parent *BSNode, node *BSNode) {
	if array, exist := graph[parent.Key()]; exist {
		graph[parent.Key()] = appendIfMissing(array, node)
	} else {
		graph[parent.Key()] = Array{node} //[]*MIRONode{node}
	}
}

func (graph Graph) ShortestPath(start *BSNode, end *BSNode, path Array) Array {
	//fmt.Printf("start: %d\n", start.key)
	if _, exist := graph[start.Key()]; !exist {
		return path
	}
	path = append(path, start)
	if start == end {
		//fmt.Printf("** found: %d %s\n", start.key, NodeSliceString(path))
		return path
	}
	shortest := make(Array, 0)
	for _, node := range graph[start.Key()] {
		if !path.hasPropertyOf(node) {
			newPath := graph.ShortestPath(node, end, path)
			if len(newPath) > 0 {
				if len(shortest) == 0 || (len(newPath) < len(shortest)) {
					shortest = newPath
				}
			}
		}
	}
	return shortest
}

func (graph Graph) PathExists(start *BSNode, end *BSNode, path Array) (Array, bool) {
	//fmt.Printf("start: %d\n", start.key)
	if _, exist := graph[start.Key()]; !exist {
		return path, false
	}
	path = append(path, start)
	if start == end {
		//fmt.Printf("** found: %d %s\n", start.key, NodeSliceString(path))
		return path, true
	}
	shortest := make(Array, 0)
	for _, node := range graph[start.Key()] {
		if !path.hasPropertyOf(node) {
			newPath, found := graph.PathExists(node, end, path)
			if found {
				return newPath, true
			}
			if len(newPath) > 0 {
				if len(shortest) == 0 || (len(newPath) < len(shortest)) {
					shortest = newPath
				}
			}
		}
	}
	return nil, false
}
