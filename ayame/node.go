package ayame

import (
	"fmt"
	"strconv"
)

type NodeMap map[string]LocalNode

var LocalNodes = make(NodeMap)

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

func NewLocalNode(key int) LocalNode {
	return LocalNode{pid: strconv.Itoa(key)}
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
