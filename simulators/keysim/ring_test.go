package main

import (
	"fmt"
	"testing"

	ki "github.com/piax/go-ayame/key_issuer"
	ast "github.com/stretchr/testify/assert"
)

func TestRing(t *testing.T) {
	ring := make(Ring, 0)
	for i := 10.0; i < 11.0; i += 0.1 {
		ring.Push(ki.NewNode(i, i))
	}
	ring.Update()
	i, n := ring.Find(ring.Nth(5).Key())
	if i < 0 {
		fmt.Printf("10.5 not found\n")
		return
	}
	p1, n1 := ring.Prev(i)
	_, n2 := ring.Prev(p1)
	p3, n4 := ring.Next(i)
	_, n5 := ring.Next(p3)
	fmt.Printf("%f, %f, %f, %f, %f\n", n2.Key(), n1.Key(), n.Key(), n4.Key(), n5.Key())
	ast.Equal(t, n2.Key(), ring.Nth(3).Key())

	i, n = ring.Find(ring.Nth(1).Key())
	if i < 0 {
		fmt.Printf("10.5 not found\n")
		return
	}
	p1, n1 = ring.Prev(i)
	_, n2 = ring.Prev(p1)
	p3, n4 = ring.Next(i)
	_, n5 = ring.Next(p3)
	fmt.Printf("%f, %f, %f, %f, %f\n", n2.Key(), n1.Key(), n.Key(), n4.Key(), n5.Key())
	ast.Equal(t, n2.Key(), ring.Nth(10).Key())
}
