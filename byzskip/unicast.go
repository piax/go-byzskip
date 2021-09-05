package byzskip

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/piax/go-ayame/ayame"
	"github.com/thoas/go-funk"

	p2p "github.com/piax/go-ayame/ayame/p2p"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

type BSUnicastEvent struct {
	TargetKey ayame.Key
	MessageId string
	Path      []PathEntry // the path entry (level is only available in simulation)
	Payload   []byte      // the payload to send
	ayame.AbstractSchedEvent

	Paths                      [][]PathEntry
	level                      int
	hop                        int
	Children                   []*BSUnicastEvent
	Root                       *BSUnicastEvent
	ExpectedNumberOfResults    int
	Results                    []*BSNode
	Destinations               []*BSNode // distinct results
	DestinationPaths           [][]PathEntry
	Channel                    chan bool
	numberOfMessages           int
	NumberOfDuplicatedMessages int
	finishTime                 int64
}

type PathEntry struct {
	Node  ayame.Node
	Level int
}

func PathEntries(nodes []*BSNode) []PathEntry {
	ret := []PathEntry{}
	for _, n := range nodes {
		ret = append(ret, PathEntry{Node: n})
	}
	return ret
}

func NewBSUnicastEventNoAuthor(sender *BSNode, messageId string, level int, target ayame.Key, payload []byte) *BSUnicastEvent {
	ev := &BSUnicastEvent{
		TargetKey:                  target,
		MessageId:                  messageId,
		Path:                       []PathEntry{{Node: sender, Level: ayame.MembershipVectorSize}},
		Payload:                    payload,
		level:                      level,
		hop:                        0,
		Children:                   []*BSUnicastEvent{},
		ExpectedNumberOfResults:    K,
		Results:                    []*BSNode{},
		Paths:                      []([]PathEntry){},
		Channel:                    make(chan bool),
		numberOfMessages:           0,
		NumberOfDuplicatedMessages: 0,
		finishTime:                 0,
		AbstractSchedEvent:         *ayame.NewSchedEvent(nil, nil, nil)}
	ev.Root = ev
	ev.SetSender(sender)
	ev.SetReceiver(sender) // XXX weird
	return ev
}

func NewBSUnicastEvent(author *BSNode, authorSign []byte, authorPubKey []byte,
	messageId string, level int, target ayame.Key, payload []byte) *BSUnicastEvent {
	ev := &BSUnicastEvent{
		TargetKey:                  target,
		MessageId:                  messageId,
		Path:                       []PathEntry{{Node: author, Level: ayame.MembershipVectorSize}},
		Payload:                    payload,
		level:                      level,
		hop:                        0,
		Children:                   []*BSUnicastEvent{},
		ExpectedNumberOfResults:    K,
		Results:                    []*BSNode{},
		Paths:                      []([]PathEntry){},
		Channel:                    make(chan bool),
		numberOfMessages:           0,
		NumberOfDuplicatedMessages: 0,
		finishTime:                 0,
		AbstractSchedEvent:         *ayame.NewSchedEvent(author, authorSign, authorPubKey)}
	ev.Root = ev
	ev.SetSender(author)
	ev.SetReceiver(author) // XXX weird
	return ev
}

func (ev *BSUnicastEvent) CheckAndSetAlreadySeen(myNode *BSNode) bool {
	msgLevel := ev.level
	myNode.seenMutex.RLock()
	seenLevel, exists := myNode.QuerySeen[ev.MessageId]
	myNode.seenMutex.RUnlock()
	if exists && msgLevel <= seenLevel {
		ayame.Log.Debugf("already seen: %s at %d\n", ev.MessageId, seenLevel)
		return true
	}
	myNode.QuerySeen[ev.MessageId] = msgLevel
	return false
}

func (ev *BSUnicastEvent) CheckAlreadySeen(myNode *BSNode) bool {
	msgLevel := ev.level
	myNode.seenMutex.RLock()
	seenLevel, exists := myNode.QuerySeen[ev.MessageId]
	myNode.seenMutex.RUnlock()
	if exists && msgLevel <= seenLevel {
		ayame.Log.Debugf("already seen: %s at %d on %s\n", ev.MessageId, seenLevel, myNode.Key())
		return true
	}
	return false
}

func (ev *BSUnicastEvent) SetAlreadySeen(myNode *BSNode) {
	msgLevel := ev.level
	ayame.Log.Debugf("set seen: %s at %d on %d\n", ev.MessageId, msgLevel, myNode.Key())
	myNode.seenMutex.Lock()
	myNode.QuerySeen[ev.MessageId] = msgLevel
	myNode.seenMutex.Unlock()
}

func (ue *BSUnicastEvent) createSubMessage(nextHop *BSNode, level int) *BSUnicastEvent {
	var sub BSUnicastEvent = *ue // author, authorSign, authorPublicKey, payload are copied
	sub.Path = append([]PathEntry{}, ue.Path...)
	sub.SetReceiver(nextHop)
	sub.SetSender(ue.Receiver())
	sub.Path = append(sub.Path, PathEntry{Node: nextHop, Level: level})
	sub.hop = ue.hop + 1
	sub.level = level
	sub.Children = []*BSUnicastEvent{}
	ue.Children = append(ue.Children, &sub)
	return &sub
}

func (ue *BSUnicastEvent) String() string {
	return ue.Receiver().String() + "<" + strings.Join(funk.Map(ue.Path, func(pe PathEntry) string {
		return fmt.Sprintf("%s@%d", pe.Node, pe.Level)
	}).([]string), ",") + ">"
}

func (ue *BSUnicastEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.MessageId,
		pb.MessageType_UNICAST, ue.Author(), ue.AuthorSign(), ue.AuthorPubKey(),
		ue.TargetKey, nil)

	var peers []*pb.Peer
	for _, pe := range ue.Path {
		peers = append(peers, pe.Node.Encode())
	}
	ret.Data.Path = peers
	ret.Data.SenderAppData = strconv.Itoa(ue.level)
	ret.Data.Record = &pb.Record{Value: ue.Payload}
	return ret
}

func (ue *BSUnicastEvent) Run(ctx context.Context, node ayame.Node) {
	if err := node.(*BSNode).handleUnicast(ctx, ue, false); err != nil {
		panic(err)
	}
}

func (ev *BSUnicastEvent) nextMsg(n *BSNode, level int) *BSUnicastEvent {
	return ev.createSubMessage(n, level)
}

var RoutingType = PRUNE_OPT2

func (ev *BSUnicastEvent) findNextHops(myNode *BSNode) []*BSUnicastEvent {
	var ret []*BSUnicastEvent
	switch RoutingType {
	case SINGLE:
		ret, _ = ev.findNextHopsSingle(myNode)
	case PRUNE:
		fallthrough
	case PRUNE_OPT1:
		fallthrough
	case PRUNE_OPT2:
		ret, _ = ev.findNextHopsPrune(myNode)
	}
	return ret
}

func (ev *BSUnicastEvent) findNextHopsSingle(myNode *BSNode) ([]*BSUnicastEvent, error) {
	//	myNode := ev.Receiver().(*BSNode)
	level := -1

	var kNodes []*BSNode
	nextMsgs := []*BSUnicastEvent{}

	ks, lv := myNode.GetClosestNodes(ev.TargetKey)
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

func (ev *BSUnicastEvent) findNextHopsPrune(myNode *BSNode) ([]*BSUnicastEvent, error) {
	var err error = nil
	//	myNode := ev.Receiver().(*BSNode)
	// root node case(?)
	var kNodes []*BSNode = []*BSNode{}
	var destLevel int = ev.level - 1
	if ev == ev.Root {
		kNodes, destLevel = myNode.GetClosestNodes(ev.TargetKey)
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

func (ue *BSUnicastEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	panic("unicast does not support request")
}
