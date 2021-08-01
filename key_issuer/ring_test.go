package main

import (
	"fmt"
	"testing"

	ast "github.com/stretchr/testify/assert"
)

func TestRing(t *testing.T) {
	ring := make(Ring, 0)
	for i := 10.0; i < 11.0; i += 0.1 {
		ring.Push(NewNode(i, i))
	}
	ring.Update()
	i, n := ring.Find(ring.Nth(5).netKey)
	if i < 0 {
		fmt.Printf("10.5 not found\n")
		return
	}
	p1, n1 := ring.Prev(i)
	_, n2 := ring.Prev(p1)
	p3, n4 := ring.Next(i)
	_, n5 := ring.Next(p3)
	fmt.Printf("%f, %f, %f, %f, %f\n", n2.netKey, n1.netKey, n.netKey, n4.netKey, n5.netKey)
	ast.Equal(t, n2.netKey, ring.Nth(3).netKey)

	i, n = ring.Find(ring.Nth(1).netKey)
	if i < 0 {
		fmt.Printf("10.5 not found\n")
		return
	}
	p1, n1 = ring.Prev(i)
	_, n2 = ring.Prev(p1)
	p3, n4 = ring.Next(i)
	_, n5 = ring.Next(p3)
	fmt.Printf("%f, %f, %f, %f, %f\n", n2.netKey, n1.netKey, n.netKey, n4.netKey, n5.netKey)
	ast.Equal(t, n2.netKey, ring.Nth(10).netKey)
}
