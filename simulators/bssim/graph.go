package main

import (
	"fmt"
	"math/rand"

	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
)

type Array []*bs.BSNode
type Graph map[string][]*bs.BSNode

func (arr Array) hasPropertyOf(node *bs.BSNode) bool {
	for _, v := range arr {
		if node.Equals(v) {
			return true
		}
	}
	return false
}

func (graph Graph) Dump() {
	for k, v := range graph {
		fmt.Printf("%s: %s\n", k, ayame.SliceString(v))
	}
}

func (graph Graph) Register(node *bs.BSNode) {
	if _, exist := graph[node.Key().String()]; !exist {
		graph[node.Key().String()] = Array{} //[]*MIRONode{node}
	}
}

func (graph Graph) AddChild(parent *bs.BSNode, node *bs.BSNode) {
	if array, exist := graph[parent.Key().String()]; exist {
		graph[parent.Key().String()] = appendIfMissing(array, node)
	} else {
		graph[parent.Key().String()] = Array{node} //[]*MIRONode{node}
	}
}

func (graph Graph) ShortestPath(start *bs.BSNode, end *bs.BSNode, path Array) Array {
	//fmt.Printf("start: %d\n", start.key)
	if _, exist := graph[start.Key().String()]; !exist {
		return path
	}
	path = append(path, start)
	if start == end {
		//fmt.Printf("** found: %d %s\n", start.key, NodeSliceString(path))
		return path
	}
	shortest := make(Array, 0)
	for _, node := range graph[start.Key().String()] {
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

func (graph Graph) PathExists(start *bs.BSNode, end *bs.BSNode, path Array) (Array, bool) {
	//fmt.Printf("start: %d\n", start.key)
	if _, exist := graph[start.Key().String()]; !exist {
		return path, false
	}
	path = append(path, start)
	if start == end {
		//fmt.Printf("** found: %d %s\n", start.key, NodeSliceString(path))
		return path, true
	}
	shortest := make(Array, 0)
	for _, node := range graph[start.Key().String()] {
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

func CalcProbabilityMonteCarlo(paths [][]bs.PathEntry, src *bs.BSNode, dst *bs.BSNode, failureRatio float64, count int) float64 {

	graph := make(Graph)
	for _, pes := range paths {
		fmt.Printf("path: %s\n", ayame.SliceString(pes))
		var prev *bs.BSNode = nil
		for _, pe := range pes {
			this := pe.Node.(*bs.BSNode)
			graph.Register(this)
			if prev != nil && prev != this {
				graph.AddChild(prev, this)
			}
			prev = this
		}
	}
	failures := 0
	for i := 0; i < count; i++ {
		graphCopy := make(Graph)
		for key, value := range graph {
			if key != src.Key().String() && key != dst.Key().String() && rand.Float64() < failureRatio {
				graphCopy[key] = nil // failure
			} else {
				graphCopy[key] = value
			}
		}
		//graphCopy.Dump()
		path := make(Array, 0, 50)
		_, exists := graphCopy.PathExists(src, dst, path)
		//length := len(shortestPath)

		if !exists {
			failures++
		}
	}
	return float64(failures) / float64(count)
}
