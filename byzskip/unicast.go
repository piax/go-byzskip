package byzskip

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/piax/go-ayame/ayame"
	"github.com/thoas/go-funk"

	p2p "github.com/piax/go-ayame/ayame/p2p"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

type BSUnicastEvent struct {
	///sourceNode *SGNode
	TargetKey ayame.Key
	MessageId string
	path      []PathEntry // node-ids
	Paths     [][]PathEntry
	level     int
	hop       int
	Children  []*BSUnicastEvent
	Root      *BSUnicastEvent
	// root only
	expectedNumberOfResults    int
	Results                    []*BSNode
	Destinations               []*BSNode // distinct results
	DestinationPaths           [][]PathEntry
	Channel                    chan bool
	numberOfMessages           int
	numberOfDuplicatedMessages int
	finishTime                 int64
	ayame.AbstractSchedEvent
}

type PathEntry struct {
	Node  ayame.Node
	level int
}

var nextMessageId int = 0

func NewBSUnicastEvent(receiver *BSNode, level int, target ayame.Key) *BSUnicastEvent {
	nextMessageId++
	ev := &BSUnicastEvent{
		TargetKey:                  target,
		MessageId:                  strconv.Itoa(nextMessageId),
		path:                       []PathEntry{{Node: receiver, level: ayame.MembershipVectorSize}},
		level:                      level,
		hop:                        0,
		Children:                   []*BSUnicastEvent{},
		expectedNumberOfResults:    K,
		Results:                    []*BSNode{},
		Paths:                      []([]PathEntry){},
		Channel:                    make(chan bool),
		numberOfMessages:           0,
		numberOfDuplicatedMessages: 0,
		finishTime:                 0,
		AbstractSchedEvent:         *ayame.NewSchedEvent()}
	ev.Root = ev
	ev.SetSender(receiver)
	ev.SetReceiver(receiver)
	return ev
}

func (ev *BSUnicastEvent) CheckAndSetAlreadySeen() bool {
	myNode := ev.Receiver().(*BSNode)
	msgLevel := ev.level
	seenLevel, exists := myNode.QuerySeen[ev.MessageId]
	if exists && msgLevel <= seenLevel {
		ayame.Log.Debugf("already seen: %d at %d\n", ev.MessageId, seenLevel)
		return true
	}
	myNode.QuerySeen[ev.MessageId] = msgLevel
	return false
}

func (ev *BSUnicastEvent) CheckAlreadySeen() bool {
	myNode := ev.Receiver().(*BSNode)
	msgLevel := ev.level
	seenLevel, exists := myNode.QuerySeen[ev.MessageId]
	if exists && msgLevel <= seenLevel {
		ayame.Log.Debugf("already seen: %d at %d on %d\n", ev.MessageId, seenLevel, myNode.Key())
		return true
	}
	return false
}

func (ev *BSUnicastEvent) SetAlreadySeen() {
	myNode := ev.Receiver().(*BSNode)
	msgLevel := ev.level
	ayame.Log.Debugf("set seen: %d at %d on %d\n", ev.MessageId, msgLevel, myNode.Key())
	myNode.QuerySeen[ev.MessageId] = msgLevel
}

func (ue *BSUnicastEvent) createSubMessage(nextHop *BSNode, level int) *BSUnicastEvent {
	var sub BSUnicastEvent = *ue
	sub.path = append([]PathEntry{}, ue.path...)
	sub.SetReceiver(nextHop)
	sub.SetSender(ue.Receiver())
	sub.path = append(sub.path, PathEntry{Node: nextHop, level: level})
	sub.hop = ue.hop + 1
	sub.level = level
	sub.Children = []*BSUnicastEvent{}
	ue.Children = append(ue.Children, &sub)
	return &sub
}

func (ue *BSUnicastEvent) String() string {
	return ue.Receiver().String() + "<" + strings.Join(funk.Map(ue.path, func(pe PathEntry) string {
		return fmt.Sprintf("%s@%d", pe.Node, pe.level)
	}).([]string), ",") + ">"
}

func (ue *BSUnicastEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).parent.(*p2p.P2PNode)
	ret := sender.NewMessage(fmt.Sprintf("%s-%s", sender.Key().String(), ue.MessageId),
		pb.MessageType_UNICAST, ue.TargetKey)
	return ret
}

func (ue *BSUnicastEvent) Run(node ayame.Node) {
	node.(*BSNode).handleUnicast(ue, false)
}

func (ev *BSUnicastEvent) nextMsg(n *BSNode, level int) *BSUnicastEvent {
	return ev.createSubMessage(n, level)
}

var RoutingType = PRUNE_OPT2

func (ev *BSUnicastEvent) findNextHops() []*BSUnicastEvent {
	var ret []*BSUnicastEvent
	switch RoutingType {
	case SINGLE:
		ret, _ = ev.findNextHopsSingle()
	case PRUNE:
		fallthrough
	case PRUNE_OPT1:
		fallthrough
	case PRUNE_OPT2:
		ret, _ = ev.findNextHopsPrune()
	}
	return ret
}

func (ev *BSUnicastEvent) findNextHopsSingle() ([]*BSUnicastEvent, error) {
	myNode := ev.Receiver().(*BSNode)
	level := -1

	var kNodes []*BSNode
	nextMsgs := []*BSUnicastEvent{}

	ks, lv := myNode.GetNeighbors(ev.TargetKey)
	kNodes = ks
	level = lv
	ayame.Log.Debugf("%s: %d's neighbors= %s (level %d)\n%s\n", myNode, ev.TargetKey, ayame.SliceString(kNodes), level, myNode.RoutingTable.String())
	for _, n := range kNodes {
		nextMsgs = append(nextMsgs, ev.nextMsg(n, level))
	}
	ayame.Log.Debugf("%s: next hops for target %d are %s (level %d)\n", myNode, ev.TargetKey, ayame.SliceString(kNodes), level)
	return nextMsgs, nil
}

const (
	SINGLE int = iota
	PRUNE
	PRUNE_OPT1
	PRUNE_OPT2
)

func (ev *BSUnicastEvent) findNextHopsPrune() ([]*BSUnicastEvent, error) {
	var err error = nil
	myNode := ev.Receiver().(*BSNode)
	// root node case(?)
	var kNodes []*BSNode = []*BSNode{}
	var destLevel int = ev.level - 1
	if ev == ev.Root {
		kNodes, destLevel = myNode.GetNeighbors(ev.TargetKey)
		ayame.Log.Debugf("%s->%d root destLevel=%d ****%s\n", myNode, ev.TargetKey, destLevel, ayame.SliceString(kNodes))
	} else {
		if ev.level == 0 {
			kNodes = []*BSNode{myNode}
		} else if RoutingType == PRUNE_OPT1 {
			ks, _ := myNode.RoutingTable.GetNeighborLists()[0].PickupKNodes(ev.TargetKey)
			if len(ks) > 0 {
				kNodes = funk.Filter(ksToNs(ks), func(n *BSNode) bool {
					return n.Equals(myNode) || myNode.MV().CommonPrefixLength(n.MV()) <= destLevel
				}).([]*BSNode)
				destLevel = 0
			}
		} else if RoutingType == PRUNE_OPT2 {
			for i := 0; i < destLevel; i++ {
				ks, _ := myNode.RoutingTable.GetNeighborLists()[i].PickupKNodes(ev.TargetKey)
				if len(ks) > 0 {
					kNodes = funk.Filter(ksToNs(ks), func(n *BSNode) bool {
						return n.Equals(myNode) || myNode.MV().CommonPrefixLength(n.MV()) <= destLevel
					}).([]*BSNode)
					destLevel = i
					break
				}
			}
			err = fmt.Errorf("implementation error")
		}
		if len(kNodes) == 0 {
			ks, _ := myNode.RoutingTable.GetNeighborLists()[destLevel].PickupKNodes(ev.TargetKey)
			kNodes = funk.Filter(ksToNs(ks), func(n *BSNode) bool {
				return n.Equals(myNode) || myNode.MV().CommonPrefixLength(n.MV()) <= destLevel
			}).([]*BSNode)
		}
	}
	nextMsgs := []*BSUnicastEvent{}

	level := destLevel
	ayame.Log.Debugf("%s: %d's neighbors= %s (level %d)\n%s\n", myNode, ev.TargetKey, ayame.SliceString(kNodes), level, myNode.RoutingTable.String())
	for _, n := range kNodes {
		nextMsgs = append(nextMsgs, ev.nextMsg(n, level))
	}
	ayame.Log.Debugf("%s: next hops for target %d are %s (level %d)\n", myNode, ev.TargetKey, ayame.SliceString(kNodes), level)
	return nextMsgs, err
}
