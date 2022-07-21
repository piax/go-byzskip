package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/piax/go-byzskip/ayame"
	funk "github.com/thoas/go-funk"
)

type SGNode struct {
	key          int
	mv           *ayame.MembershipVector
	routingTable []*RoutingTableSingleLevel
	ayame.LocalNode
}

type UnicastEvent struct {
	///sourceNode *SGNode
	targetKey int
	messageId int
	path      []ayame.Node // node-ids
	level     int
	hop       int
	children  []*UnicastEvent
	root      *UnicastEvent
	// root only
	expectedNumberOfResults int
	results                 []*SGNode
	channel                 chan bool
	destinations            []*SGNode
	numberOfMessages        int
	finishTime              int64
	ayame.AbstractSchedEvent
}

var nextMessageId int = 0

func NewUnicastEvent(receiver ayame.Node, targetKey int) *UnicastEvent {
	nextMessageId++
	ev := &UnicastEvent{
		//		sourceNode:              receiver,
		targetKey:               targetKey,
		messageId:               nextMessageId,
		path:                    []ayame.Node{receiver},
		level:                   0,
		hop:                     0,
		children:                []*UnicastEvent{},
		expectedNumberOfResults: 1,
		results:                 []*SGNode{},
		channel:                 make(chan bool),
		destinations:            []*SGNode{},
		numberOfMessages:        0,
		finishTime:              0,
		AbstractSchedEvent:      *ayame.NewSchedEvent(nil, nil, nil)}
	ev.root = ev
	ev.SetReceiver(receiver)
	return ev
}

//func (ue *UnicastEvent) Receiver() ayame.Node {
//	return ue.sourceNode
//}

func (ue *UnicastEvent) createSubMessage(nextHop *SGNode, level int) *UnicastEvent {
	var sub UnicastEvent = *ue
	copy(sub.path, ue.path)
	sub.SetReceiver(nextHop)
	sub.SetSender(ue.Receiver())
	sub.path = append(sub.path, nextHop)
	sub.hop = ue.hop + 1
	sub.level = level
	sub.children = []*UnicastEvent{}
	return &sub
}

func (ue *UnicastEvent) Run(ctx context.Context, node ayame.Node) error {
	//	fmt.Print(node.Id())
	node.(*SGNode).handleUnicast(ue)
	return nil
}

type RoutingTableSingleLevel struct {
	owner *SGNode
	nodes [2]([]*SGNode)
	level int
}

const (
	RIGHT int = iota
	LEFT
)

func (sg *SGNode) newRoutingTableSingleLevel(level int) *RoutingTableSingleLevel {
	var ns [2][]*SGNode
	ns[0] = make([]*SGNode, 0, 10)
	ns[1] = make([]*SGNode, 0, 10)
	return &RoutingTableSingleLevel{owner: sg, nodes: ns, level: level}
}

func (sg *SGNode) allNeighbors() []*SGNode {
	ret := []*SGNode{}
	for _, lv := range sg.routingTable {
		ret = append(ret, lv.nodes[LEFT]...)
		ret = append(ret, lv.nodes[RIGHT]...)
	}
	return ret
}

func closestNode(target int, nodes []*SGNode) *SGNode {
	var sortedNodes = []*SGNode{}
	sortedNodes = append(sortedNodes, nodes...)
	sortCircular(target, sortedNodes)
	//return //sortedNodes[len(sortedNodes)-1]
	return sortedNodes[0]
}

func contains(s []ayame.Node, e *SGNode) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func sgContains(s []*SGNode, e *SGNode) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func (sg *SGNode) highestLevelInRoutingTable(node *SGNode) int {
	rt := append([]*RoutingTableSingleLevel{}, sg.routingTable...)
	ayame.ReverseSlice(rt)
	for i, t := range rt {
		if sgContains(t.nodes[RIGHT], node) || sgContains(t.nodes[LEFT], node) {
			return i
		}
	}
	return 0
}

func (sg *SGNode) findNextHops(msg *UnicastEvent) (*SGNode, int) {
	nodes := sg.allNeighbors()
	nodes = append(nodes, sg)
	nextNode := closestNode(msg.targetKey, nodes)
	level := sg.highestLevelInRoutingTable(nextNode)
	return nextNode, level
}

func (sg *SGNode) handleUnicast(sev ayame.SchedEvent) error {
	msg := sev.(*UnicastEvent)
	nextNode, level := sg.findNextHops(msg)
	if nextNode == sg {
		msg.root.results = append(msg.root.results, sg)
		msg.root.path = msg.path
		msg.root.hop = msg.hop
		if len(msg.root.results) == msg.root.expectedNumberOfResults {
			// XXX need to send UnicastReply
			msg.root.channel <- true
		}
		msg.root.destinations = append(msg.root.destinations, sg)
		return nil
	}
	if contains(msg.path, nextNode) {
		return nil
	}
	ev := msg.createSubMessage(nextNode, level)
	sg.Send(context.TODO(), ev, true) // the second argument is ommited
	return nil
}

func maxNode(nodes []*SGNode) *SGNode {
	var max *SGNode = nil
	for _, s := range nodes {
		if max == nil || max.key < s.key {
			max = s
		}
	}
	return max
}

/* reverse snipet
func reverseSlice(data interface{}) {
	value := reflect.ValueOf(data)
	valueLen := value.Len()
	for i := 0; i <= int((valueLen-1)/2); i++ {
		reverseIndex := valueLen - 1 - i
		tmp := value.Index(reverseIndex).Interface()
		value.Index(reverseIndex).Set(value.Index(i))
		value.Index(i).Set(reflect.ValueOf(tmp))
	}
}*/

// nodes is modified
func sortCircular(base int, nodes []*SGNode) {
	max := maxNode(nodes)
	sort.Slice(nodes, func(x, y int) bool {
		var xval, yval int
		xval = nodes[x].key
		if nodes[x].key <= base {
			xval += max.key + 1
		}
		yval = nodes[y].key
		if nodes[y].key <= base {
			yval += max.key + 1
		}
		return xval < yval
	})
}

func NewSGNode(key int, mv *ayame.MembershipVector) *SGNode {
	return &SGNode{key: key, mv: mv, LocalNode: *ayame.NewLocalNode(ayame.IntKey(key), mv)}
}

func (rts *RoutingTableSingleLevel) Add(d int, u *SGNode) {
	for _, a := range rts.nodes[d] {
		if a == u {
			return
		}
	}
	rts.nodes[d] = append(rts.nodes[d], u)
	sortCircular(rts.owner.key, rts.nodes[d])
	if d == LEFT {
		ayame.ReverseSlice(rts.nodes[d])
	}
}

func (sg *SGNode) extendRoutingTable(level int) {
	for len(sg.routingTable) <= level {
		maxLevel := len(sg.routingTable) - 1
		newLevel := maxLevel + 1
		s := sg.newRoutingTableSingleLevel(newLevel)
		sg.routingTable = append(sg.routingTable, s)
		// normal skip graph doesn't require this
		if maxLevel >= 0 {
			for _, n := range append(sg.routingTable[maxLevel].nodes[RIGHT], sg.routingTable[maxLevel].nodes[LEFT]...) {
				if n.mv.CommonPrefixLength(sg.mv) >= newLevel {
					s.Add(RIGHT, n)
					s.Add(LEFT, n)
				}
			}
		}
	}
}

func (rts RoutingTableSingleLevel) String() string {
	ret := ""
	ret += fmt.Sprintf("Level {%d}: ", rts.level)
	ret += "LEFT=["
	ret += strings.Join(funk.Map(rts.nodes[LEFT], func(n *SGNode) string {
		return strconv.Itoa(n.key)
	}).([]string), ",")
	ret += "], RIGHT=["
	ret += strings.Join(funk.Map(rts.nodes[RIGHT], func(n *SGNode) string {
		return strconv.Itoa(n.key)
	}).([]string), ",")
	ret += "]"
	return ret
}

func (sg *SGNode) routingTableString() string {
	ret := ""
	for _, sl := range sg.routingTable {
		ret += sl.String() + "\n"
	}
	return ret
}

func delNode(nodes []*SGNode, node *SGNode) []*SGNode {
	for i := range nodes {
		if nodes[i] == node {
			return append(nodes[:i], nodes[i+1:]...)
		}
	}
	return nil
}

func (sg *SGNode) routingTableHeight() int {
	return len(sg.routingTable)
}

func FastJoinAll(nodes []*SGNode) error {
	num := len(nodes)
	for _, n := range nodes {
		n.extendRoutingTable(0)
	}
	for i, p := range nodes {
		q := nodes[(i+1)%num]
		p.routingTable[0].nodes[RIGHT] = append(p.routingTable[0].nodes[RIGHT], q)
		q.routingTable[0].nodes[LEFT] = append(q.routingTable[0].nodes[LEFT], p)
	}
	unfinished := append([]*SGNode{}, nodes...)
	// for all membership digits
	for level := 0; level < ayame.MembershipVectorSize; level++ {
		nodesAtCurrentLevel := append([]*SGNode{}, unfinished...)
		for len(nodesAtCurrentLevel) > 0 {
			p := nodesAtCurrentLevel[0]
			start := p
			for {
				nodesAtCurrentLevel = delNode(nodesAtCurrentLevel, p)
				q := p.routingTable[level].nodes[RIGHT][0]
				// find next right
				for q != p && p.mv.CommonPrefixLength(q.mv) <= level {
					q = q.routingTable[level].nodes[RIGHT][0]
				}
				// q is the next right
				if q != p {
					p.extendRoutingTable(level + 1)
					p.routingTable[level+1].nodes[RIGHT] = []*SGNode{q}
					q.extendRoutingTable(level + 1)
					q.routingTable[level+1].nodes[LEFT] = []*SGNode{p}
				} else {
					unfinished = delNode(unfinished, p)
				}
				p = q
				if p == start {
					break
				}
			}
		}
		if len(unfinished) == 0 {
			break
		}
	}
	if len(unfinished) != 0 {
		return fmt.Errorf("insufficent membership vector digits")
	}
	return nil
}
