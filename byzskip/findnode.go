package byzskip

import (
	"fmt"
	"strconv"

	"github.com/piax/go-ayame/ayame"

	p2p "github.com/piax/go-ayame/ayame/p2p"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

type BSFindNodeEvent struct {
	isResponse bool
	TargetKey  ayame.Key
	TargetMV   *ayame.MembershipVector
	MessageId  string
	level      int
	closers    []*BSNode
	candidates []*BSNode
	ayame.AbstractSchedEvent
}

func NewBSFindNodeReqEvent(sender *BSNode, requestId string, targetKey ayame.Key, targetMV *ayame.MembershipVector) *BSFindNodeEvent {

	ev := &BSFindNodeEvent{
		isResponse:         false,
		TargetKey:          targetKey,
		TargetMV:           targetMV,
		MessageId:          requestId,
		AbstractSchedEvent: *ayame.NewSchedEvent()}
	return ev
}

func NewBSFindNodeResEvent(request *BSFindNodeEvent, closers []*BSNode, level int, candidates []*BSNode) *BSFindNodeEvent {
	ev := &BSFindNodeEvent{
		isResponse:         true,
		TargetKey:          request.Sender().Key(), // no meaning, just fill
		TargetMV:           request.Sender().MV(),  // no meaning, just fill
		MessageId:          request.MessageId,
		closers:            closers,
		candidates:         candidates,
		level:              level,
		AbstractSchedEvent: *ayame.NewSchedEvent()}
	return ev
}

func (ue *BSFindNodeEvent) String() string {
	return fmt.Sprintf("findnode id=%s", ue.MessageId)
}

func (ue *BSFindNodeEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.MessageId, pb.MessageType_FIND_NODE, ue.TargetKey, ue.TargetMV)
	ret.IsResponse = ue.isResponse
	var cpeers []*pb.Peer
	for _, n := range ue.candidates {
		cpeers = append(cpeers, n.parent.Encode())
	}
	ret.Data.CandidatePeers = cpeers
	var lpeers []*pb.Peer
	for _, n := range ue.closers {
		lpeers = append(lpeers, n.parent.Encode())
	}
	ret.Data.CloserPeers = lpeers
	ret.Data.SenderAppData = strconv.Itoa(ue.level)
	return ret
}

func (ue *BSFindNodeEvent) Run(node ayame.Node) {
	n := node.(*BSNode)
	n.handleFindNode(ue)

}
