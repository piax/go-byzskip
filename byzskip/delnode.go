package byzskip

import (
	"context"

	"github.com/piax/go-ayame/ayame"
	p2p "github.com/piax/go-ayame/ayame/p2p"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

type BSDelNodeEvent struct {
	TargetKey ayame.Key
	MessageId string
	ayame.AbstractSchedEvent
}

func NewBSDelNodeEvent(sender *BSNode, requestId string, targetKey ayame.Key) *BSDelNodeEvent {
	ev := &BSDelNodeEvent{
		TargetKey:          targetKey,
		MessageId:          requestId,
		AbstractSchedEvent: *ayame.NewSchedEvent()}
	return ev
}

func (ue *BSDelNodeEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.MessageId, pb.MessageType_DEL_NODE, ue.TargetKey, nil, false)
	return ret
}

func (ue *BSDelNodeEvent) Run(ctx context.Context, node ayame.Node) {
	n := node.(*BSNode)
	n.handleDelNode(ue)
}

func (ue *BSDelNodeEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	panic("del_node does not support request")
}
