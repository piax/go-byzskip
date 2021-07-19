package main

import (
	"fmt"
	"strings"

	"github.com/piax/go-ayame/ayame"
	"github.com/piax/go-ayame/byzskip"
	"github.com/thoas/go-funk"
)

type BSUnicastEvent struct {
	///sourceNode *SGNode
	targetKey int
	messageId int
	path      []PathEntry // node-ids
	paths     [][]PathEntry
	level     int
	hop       int
	children  []*BSUnicastEvent
	root      *BSUnicastEvent
	// root only
	expectedNumberOfResults    int
	results                    []*BSNode
	destinations               []*BSNode // distinct results
	destinationPaths           [][]PathEntry
	channel                    chan bool
	numberOfMessages           int
	numberOfDuplicatedMessages int
	finishTime                 int64
	ayame.AbstractSchedEvent
}

type PathEntry struct {
	node  ayame.Node
	level int
}

var nextMessageId int = 0

func NewBSUnicastEvent(receiver *BSNode, level int, target int) *BSUnicastEvent {
	nextMessageId++
	ev := &BSUnicastEvent{
		targetKey:                  target,
		messageId:                  nextMessageId,
		path:                       []PathEntry{{node: receiver, level: ayame.MembershipVectorSize}},
		level:                      level,
		hop:                        0,
		children:                   []*BSUnicastEvent{},
		expectedNumberOfResults:    byzskip.K,
		results:                    []*BSNode{},
		paths:                      []([]PathEntry){},
		channel:                    make(chan bool),
		numberOfMessages:           0,
		numberOfDuplicatedMessages: 0,
		finishTime:                 0,
		AbstractSchedEvent:         *ayame.NewSchedEvent()}
	ev.root = ev
	ev.SetSender(receiver)
	ev.SetReceiver(receiver)
	return ev
}

func (ev *BSUnicastEvent) CheckAndSetAlreadySeen() bool {
	myNode := ev.Receiver().(*BSNode)
	msgLevel := ev.level
	seenLevel, exists := myNode.querySeen[ev.messageId]
	if exists && msgLevel <= seenLevel {
		ayame.Log.Debugf("already seen: %d at %d\n", ev.messageId, seenLevel)
		return true
	}
	myNode.querySeen[ev.messageId] = msgLevel
	return false
}

func (ev *BSUnicastEvent) CheckAlreadySeen() bool {
	myNode := ev.Receiver().(*BSNode)
	msgLevel := ev.level
	seenLevel, exists := myNode.querySeen[ev.messageId]
	if exists && msgLevel <= seenLevel {
		ayame.Log.Debugf("already seen: %d at %d on %d\n", ev.messageId, seenLevel, myNode.Key())
		return true
	}
	return false
}

func (ev *BSUnicastEvent) SetAlreadySeen() {
	myNode := ev.Receiver().(*BSNode)
	msgLevel := ev.level
	ayame.Log.Debugf("set seen: %d at %d on %d\n", ev.messageId, msgLevel, myNode.Key())
	myNode.querySeen[ev.messageId] = msgLevel
}

func (ue *BSUnicastEvent) createSubMessage(nextHop *BSNode, level int) *BSUnicastEvent {
	var sub BSUnicastEvent = *ue
	sub.path = append([]PathEntry{}, ue.path...)
	sub.SetReceiver(nextHop)
	sub.SetSender(ue.Receiver())
	sub.path = append(sub.path, PathEntry{node: nextHop, level: level})
	sub.hop = ue.hop + 1
	sub.level = level
	sub.children = []*BSUnicastEvent{}
	ue.children = append(ue.children, &sub)
	return &sub
}

func (ue *BSUnicastEvent) String() string {
	return ue.Receiver().Id() + "<" + strings.Join(funk.Map(ue.path, func(pe PathEntry) string {
		return fmt.Sprintf("%s@%d", pe.node.Id(), pe.level)
	}).([]string), ",") + ">"
}

func (ue *BSUnicastEvent) Run(node ayame.Node) {
	node.(*BSNode).handleUnicast(ue, false)
}

func (ev *BSUnicastEvent) nextMsg(n *BSNode, level int) *BSUnicastEvent {
	return ev.createSubMessage(n, level)
}

func (ev *BSUnicastEvent) findNextHops() []*BSUnicastEvent {
	myNode := ev.Receiver().(*BSNode)
	level := -1

	var kNodes []*BSNode
	nextMsgs := []*BSUnicastEvent{}

	// find the lowest level
	/*for i, singleLevel := range myNode.routingTable.NeighborLists {
		kn, exists := singleLevel.pickupKNodes(ev.targetKey)
		if exists {
			kNodes = kn
			level = i
			break
		}
	}*/

	//ks, lv := myNode.routingTable.GetNeighbors(ev.targetKey)
	//kNodes = ksToNs(ks)
	ks, lv := myNode.GetNeighbors(ev.targetKey)
	kNodes = ks
	level = lv
	ayame.Log.Debugf("%s: %d's neighbors= %s (level %d)\n%s\n", myNode, ev.targetKey, ayame.SliceString(kNodes), level, myNode.routingTable.String())
	for _, n := range kNodes {
		nextMsgs = append(nextMsgs, ev.nextMsg(n, level))
	}
	ayame.Log.Debugf("%s: next hops for target %d are %s (level %d)\n", myNode.Id(), ev.targetKey, ayame.SliceString(kNodes), level)
	return nextMsgs
}
