package main

import (
	"fmt"
	"math/rand"

	"github.com/piax/go-ayame/ayame"
)

const (
	FLUCTUATION_RATIO = 0.5
)

type Node struct {
	logicalKey float64
	netKey     float64
	delegated  []*Node
	isFailure  bool
}

func NewNode(key float64, netKey float64) *Node {
	return &Node{logicalKey: key, netKey: netKey, isFailure: false}
}

func NewNodeWithFailure(key float64, netKey float64, failure bool) *Node {
	return &Node{logicalKey: key, netKey: netKey, isFailure: failure}
}

func (n *Node) ExtractKey() float64 {
	return n.netKey
}

func (n *Node) String() string {
	return fmt.Sprintf("{logicalKey:%f, netKey:%f, delegated: %d, faulty: %v}", n.logicalKey, n.netKey, len(n.delegated), n.isFailure)
}

type ShuffleKeyIssuer struct {
	count int
	nodes SkipList
	pool  []*Node
}

func NewShuffleKeyIssuer() *ShuffleKeyIssuer {
	ret := &ShuffleKeyIssuer{
		count: 0,
		nodes: NewSkipListWithSeed(*seed),
		pool:  []*Node{},
	}
	for ret.count < *poolSize {
		key := rand.Float64()
		requester := NewNode(key, key)
		ret.pool = append(ret.pool, requester)
		ret.count++
	}
	return ret
}

func (ki *ShuffleKeyIssuer) GetKey(key float64) float64 {
	return ki.getShuffled(key)
}

func excludeNodeWithKey(nodes []*Node, key float64) []*Node {
	ret := []*Node{}
	for _, n := range nodes {
		if n.logicalKey != key {
			ret = append(ret, n)
		}
	}
	return ret
}

func (ki *ShuffleKeyIssuer) getShuffled(key float64) float64 {
	var requester *Node

	// exclude the same key (in case a same key was requested)
	excluded := excludeNodeWithKey(ki.pool, key)
	// pick a key randomly
	index := rand.Intn(len(excluded))
	picked := excluded[index]
	//ayame.Log.Infof("picked %f->%f\n", key, picked.key)
	newKey := float64(0)

	next, found := ki.nodes.findExtended(picked.logicalKey, true)
	if !found { // I'm the greatest
		if last := ki.nodes.GetLargestNode(); last != nil {
			lastNode := last.GetValue().(*Node)
			lastKey := lastNode.netKey
			newKey = picked.logicalKey - (picked.logicalKey-lastKey)*FLUCTUATION_RATIO*(rand.Float64()-(1/2.0))
		} else { // bootstrap
			newKey = picked.logicalKey
		}
	} else {
		nextKey := next.GetValue().(*Node).netKey
		newKey = picked.logicalKey + (nextKey-picked.logicalKey)*FLUCTUATION_RATIO*(rand.Float64()-(1/2.0))
	}
	ayame.Log.Debugf("%f->%f\n", picked.logicalKey, newKey)
	requester = &Node{logicalKey: key, netKey: newKey}

	//		requester = &Node{key: key, netKey: picked.key}

	ki.pool = append(ki.pool, requester)
	ki.pool = excludeNodeWithKey(ki.pool, picked.logicalKey)
	ki.nodes.Insert(requester)
	ayame.Log.Debugf("%s inserted %dth\n", requester, ki.nodes.elementCount)
	ki.count++
	//fmt.Printf("unassigned=%v\n", a.unassigned)
	return requester.netKey
}
