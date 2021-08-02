package ayame

import (
	"fmt"
	"strconv"
)

type NodeMap map[string]LocalNode

var LocalNodes = make(NodeMap)

type Key interface {
	// Less reports whether the element is less than b
	Less(elem interface{}) bool
	// Equals reports whether the element equals to b
	Equals(elem interface{}) bool
	// Suger.
	LessOrEquals(elem interface{}) bool
	String() string
}

// Integer Key
type Int int

func (t Int) Less(elem interface{}) bool {
	if v, ok := elem.(Int); ok {
		return int(t) < int(v)
	}
	return false
}

func (t Int) Equals(elem interface{}) bool {
	if v, ok := elem.(Int); ok {
		return int(t) == int(v)
	}
	return false
}

func (t Int) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(Int); ok {
		return int(t) <= int(v)
	}
	return false
}

func (t Int) String() string {
	return strconv.Itoa(int(t))
}

type Node interface {
	Id() string
	Locator() (string, error) // Endpoint
}

type LocalNode struct {
	pid string
	Node
}

func (an *LocalNode) SendEvent(ev SchedEvent) {
	ev.SetSender(an)
	GlobalEventExecutor.RegisterEvent(ev, NETWORK_LATENCY)
}

func (an *LocalNode) Sched(ev SchedEvent, time int64) {
	ev.SetSender(an)
	GlobalEventExecutor.RegisterEvent(ev, time)
}

func NewLocalNode(key Key) LocalNode {
	return LocalNode{pid: key.String()}
}

func (n LocalNode) Id() string {
	return n.pid
}

func (n LocalNode) Locator() (string, error) {
	return "", fmt.Errorf("no locator in local node")
}

func GetLocalNode(id string) LocalNode {
	var n, isThere = LocalNodes[id]
	if !isThere {
		n = LocalNode{pid: id}
		LocalNodes[id] = n
	}
	return n
}
