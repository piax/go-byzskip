package byzskip

import (
	"fmt"
	"strconv"

	"github.com/piax/go-ayame/ayame"

	p2p "github.com/piax/go-ayame/ayame/p2p"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

type BSFindNodeEvent struct {
	///sourceNode *SGNode
	isResponse bool
	TargetKey  ayame.Key
	MessageId  string
	level      int
	closers    []*BSNode
	candidates []*BSNode
	ayame.AbstractSchedEvent
}

func NewBSFindNodeReqEvent(sender *BSNode, target ayame.Key) *BSFindNodeEvent {
	nextMessageId++
	ev := &BSFindNodeEvent{
		isResponse:         false,
		TargetKey:          target,
		MessageId:          strconv.Itoa(nextMessageId),
		AbstractSchedEvent: *ayame.NewSchedEvent()}
	return ev
}

func NewBSFindNodeResEvent(request *BSFindNodeEvent, closers []*BSNode, level int, candidates []*BSNode) *BSFindNodeEvent {
	nextMessageId++
	ev := &BSFindNodeEvent{
		isResponse:         true,
		TargetKey:          request.Sender().Key(),
		MessageId:          request.MessageId,
		closers:            closers,
		candidates:         candidates,
		level:              level,
		AbstractSchedEvent: *ayame.NewSchedEvent()}
	return ev
}

func (ue *BSFindNodeEvent) String() string {
	return ue.Receiver().String()
}

func (ue *BSFindNodeEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).parent.(*p2p.P2PNode)
	ret := sender.NewMessage(fmt.Sprintf("%s-%s", sender.Key().String(), ue.MessageId),
		pb.MessageType_FIND_NODE, ue.TargetKey)
	ret.IsResponse = ue.isResponse
	var cpeers []*pb.Peer
	for _, n := range ue.candidates {
		cpeers = append(cpeers, n.parent.(*p2p.P2PNode).Encode())
	}
	ret.Data.CandidatePeers = cpeers
	var lpeers []*pb.Peer
	for _, n := range ue.closers {
		lpeers = append(lpeers, n.parent.(*p2p.P2PNode).Encode())
	}
	ret.Data.CloserPeers = lpeers
	ret.Data.SenderAppData = strconv.Itoa(ue.level)
	return ret
}

func (ue *BSFindNodeEvent) Run(node ayame.Node) {
	n := node.(*BSNode)
	n.handleFindNode(ue)

}
