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
type IntKey int

func (t IntKey) Less(elem interface{}) bool {
	if v, ok := elem.(IntKey); ok {
		return int(t) < int(v)
	}
	return false
}

func (t IntKey) Equals(elem interface{}) bool {
	if v, ok := elem.(IntKey); ok {
		return int(t) == int(v)
	}
	return false
}

func (t IntKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(IntKey); ok {
		return int(t) <= int(v)
	}
	return false
}

func (t IntKey) String() string {
	return strconv.Itoa(int(t))
}

type FloatKey float64

func (t FloatKey) Less(elem interface{}) bool {
	if v, ok := elem.(FloatKey); ok {
		return int(t) < int(v)
	}
	return false
}

func (t FloatKey) Equals(elem interface{}) bool {
	if v, ok := elem.(FloatKey); ok {
		return int(t) == int(v)
	}
	return false
}

func (t FloatKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(FloatKey); ok {
		return int(t) <= int(v)
	}
	return false
}

func (t FloatKey) String() string {
	return fmt.Sprintf("%f", float64(t))
}

type Node interface {
	Id() string
	Locator() (string, error) // Endpoint
}

type LocalNode struct {
	pid string
	Node
}

func (an *LocalNode) Send(ev SchedEvent) {
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
