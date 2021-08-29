package byzskip

import (
	"context"
	"fmt"
	"strconv"

	"github.com/piax/go-ayame/ayame"

	p2p "github.com/piax/go-ayame/ayame/p2p"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

type FindNodeRequest struct {
	// requester's key
	Key ayame.Key
	// requester's Membership Vector. can be nil.
	MV                *ayame.MembershipVector
	ClosestIndex      *TableIndex
	NeighborListIndex []*TableIndex
}

func (req *FindNodeRequest) Encode() *pb.FindNodeRequest {
	var idxs []*pb.TableIndex
	for _, idx := range req.NeighborListIndex {
		idxs = append(idxs, idx.Encode())
	}
	if req.MV != nil {
		ret := &pb.FindNodeRequest{
			Key:               req.Key.Encode(),
			MV:                req.MV.Encode(),
			NeighborListIndex: idxs,
		}
		return ret
	} else { // lookup (find key)
		ret := &pb.FindNodeRequest{
			Key:               req.Key.Encode(),
			MV:                nil,
			NeighborListIndex: nil,
		}
		return ret
	}

}

type TableIndex struct {
	Level int
	Max   ayame.Key
	Min   ayame.Key
}

func (idx *TableIndex) Encode() *pb.TableIndex {
	return &pb.TableIndex{
		Min:   idx.Min.Encode(),
		Max:   idx.Max.Encode(),
		Level: int32(idx.Level),
	}
}

type BSFindNodeEvent struct {
	isResponse bool
	req        *FindNodeRequest
	//	TargetKey  ayame.Key
	//	TargetMV   *ayame.MembershipVector
	MessageId string
	// level in response
	level int
	// closest nodes in response
	closers []*BSNode
	// neighbor nodes in response
	candidates []*BSNode
	ayame.AbstractSchedEvent
}

func NewBSFindNodeReqEvent(sender *BSNode, requestId string, targetKey ayame.Key, targetMV *ayame.MembershipVector) *BSFindNodeEvent {
	ev := &BSFindNodeEvent{
		isResponse:         false,
		req:                &FindNodeRequest{Key: targetKey, MV: targetMV, NeighborListIndex: sender.RoutingTable.GetTableIndex()},
		MessageId:          requestId,
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	ev.SetRequest(true)
	return ev
}

func NewBSFindNodeResEvent(sender *BSNode, request *BSFindNodeEvent, closers []*BSNode, level int, candidates []*BSNode) *BSFindNodeEvent {
	ev := &BSFindNodeEvent{
		isResponse:         true,
		MessageId:          request.MessageId,
		closers:            closers,
		candidates:         candidates,
		level:              level,
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	return ev
}

func (ue *BSFindNodeEvent) String() string {
	return fmt.Sprintf("findnode id=%s", ue.MessageId)
}

func (ue *BSFindNodeEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.MessageId, pb.MessageType_FIND_NODE, nil, nil, nil, nil, nil)
	if ue.req != nil {
		ret.Data.Req = ue.req.Encode()
	}
	ret.IsResponse = ue.isResponse
	ret.IsRequest = ue.IsRequest()
	/*var idxs = []*pb.TableIndex
	for _, idx := range ue.req.NeighborListIndex {
		idxs = append (idxs, idx.Encode())
	}*/
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

func (ue *BSFindNodeEvent) Run(ctx context.Context, node ayame.Node) {
	n := node.(*BSNode)
	n.handleFindNode(ctx, ue)
}

func (ue *BSFindNodeEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	n := node.(*BSNode)
	return n.handleFindNodeRequest(ctx, ue)
}
