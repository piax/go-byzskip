package miro

import (
	"fmt"
	"testing"
)

func TestGraph(t *testing.T) {
	//graph := make(map[int][]*MIRONode, 0)
	graph := make(map[string][]string)

	graph["A"] = []string{"B", "E"}
	graph["B"] = []string{"A", "C", "E"}
	graph["C"] = []string{"B", "D", "G"}
	graph["D"] = []string{"C", "E", "F"}
	graph["E"] = []string{"A", "B", "D"}
	graph["F"] = []string{"D"}

	path := make(Array, 0, 50)
	shortestPath := ShortestPath(graph, "A", "D", path)
	fmt.Printf("%s\n", shortestPath)

	path = make(Array, 0, 50)
	shortestPath = ShortestPath(graph, "A", "X", path)
	fmt.Printf("%s\n", shortestPath)
	//ast.Equal(t, len(lst), 2, "expected 2")
}
