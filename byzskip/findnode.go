package byzskip

import (
	"context"
	"fmt"
	"strconv"

	"github.com/piax/go-byzskip/ayame"

	p2p "github.com/piax/go-byzskip/ayame/p2p"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
)

func (req *NeighborRequest) Encode() *pb.FindNodeRequest {
	var idxs []*pb.TableIndex
	for _, idx := range req.NeighborListIndex {
		idxs = append(idxs, idx.Encode())
	}
	if req.MV != nil {
		ret := &pb.FindNodeRequest{
			Key:               req.Key.Encode(),
			MV:                req.MV.Encode(),
			ClosestIndex:      req.ClosestIndex.Encode(),
			NeighborListIndex: idxs,
		}
		return ret
	} else { // lookup (find key)
		ret := &pb.FindNodeRequest{
			Key:               req.Key.Encode(),
			MV:                nil,
			ClosestIndex:      nil,
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
	var encodeMin *pb.Key = nil
	min := idx.Min
	if min != nil { // if not sufficient in LEFT, max becomes nil
		encodeMin = idx.Min.Encode()
	}
	var encodeMax *pb.Key = nil
	max := idx.Max
	if max != nil { // if not sufficient in RIGHT, max becomes nil
		encodeMax = idx.Max.Encode()
	}

	return &pb.TableIndex{
		Min:   encodeMin,
		Max:   encodeMax,
		Level: int32(idx.Level),
	}
}

type BSFindNodeMVEvent struct {
	isResponse bool
	mv         *ayame.MembershipVector
	src        ayame.Key
	messageId  string
	isTopmost  bool
	// closest nodes in response
	neighbors []*BSNode
	ayame.AbstractSchedEvent
}

func NewBSFindNodeMVReqEvent(sender *BSNode, requestId string, targetMV *ayame.MembershipVector, src ayame.Key) *BSFindNodeMVEvent {
	ev := &BSFindNodeMVEvent{
		isResponse:         false,
		mv:                 targetMV,
		src:                src,
		messageId:          requestId,
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	ev.SetRequest(true)
	return ev
}

func NewBSFindNodeMVResEvent(sender *BSNode, request *BSFindNodeMVEvent, neighbors []*BSNode, isTopmost bool) *BSFindNodeMVEvent {
	ev := &BSFindNodeMVEvent{
		isResponse:         true,
		mv:                 request.mv,
		src:                request.src,
		messageId:          request.messageId,
		neighbors:          neighbors,
		isTopmost:          isTopmost,
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	return ev
}

func (ue *BSFindNodeMVEvent) String() string {
	return fmt.Sprintf("findnode id=%s", ue.messageId)
}

func (ue *BSFindNodeMVEvent) MessageId() string {
	return ue.messageId
}

func (ue *BSFindNodeMVEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).Parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.messageId, pb.MessageType_FIND_MV, nil, nil, nil, ue.src, ue.mv)
	ret.IsResponse = ue.isResponse
	ret.IsRequest = ue.IsRequest()
	var lpeers []*pb.Peer
	for _, n := range ue.neighbors {
		lpeers = append(lpeers, n.Parent.Encode())
	}
	ret.Data.CloserPeers = lpeers
	if ue.isTopmost {
		ret.Data.SenderAppData = "t"
	} else {
		ret.Data.SenderAppData = "f"
	}
	return ret
}

func (ue *BSFindNodeMVEvent) Run(ctx context.Context, node ayame.Node) error {
	n := node.(*BSNode)
	return n.handleFindNodeMV(ctx, ue)
}

func (ue *BSFindNodeMVEvent) IsResponse() bool {
	return ue.isResponse
}

func (ue *BSFindNodeMVEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	n := node.(*BSNode)
	return n.handleFindNodeMVRequest(ctx, ue)
}

type BSFindNodeEvent struct {
	isResponse bool
	req        *NeighborRequest
	messageId  string
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
		isResponse: false,
		req: &NeighborRequest{Key: targetKey, MV: targetMV,
			ClosestIndex:      sender.RoutingTable.GetClosestIndex(),
			NeighborListIndex: sender.RoutingTable.GetTableIndex()},
		messageId:          requestId,
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	ev.SetRequest(true)
	return ev
}

func NewBSFindNodeResEvent(sender *BSNode, request *BSFindNodeEvent, closers []*BSNode, level int, candidates []*BSNode) *BSFindNodeEvent {
	ev := &BSFindNodeEvent{
		isResponse:         true,
		messageId:          request.messageId,
		closers:            closers,
		candidates:         candidates,
		level:              level,
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	return ev
}

func (ue *BSFindNodeEvent) String() string {
	return fmt.Sprintf("findnode id=%s", ue.messageId)
}

func (ue *BSFindNodeEvent) MessageId() string {
	return ue.messageId
}

func (ue *BSFindNodeEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).Parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.messageId, pb.MessageType_FIND_NODE, nil, nil, nil, nil, nil)
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
		cpeers = append(cpeers, n.Parent.Encode())
	}
	ret.Data.CandidatePeers = cpeers
	var lpeers []*pb.Peer
	for _, n := range ue.closers {
		lpeers = append(lpeers, n.Parent.Encode())
	}
	ret.Data.CloserPeers = lpeers
	ret.Data.SenderAppData = strconv.Itoa(ue.level)
	return ret
}

func (ue *BSFindNodeEvent) Run(ctx context.Context, node ayame.Node) error {
	n := node.(*BSNode)
	return n.handleFindNode(ctx, ue)
}

func (ue *BSFindNodeEvent) IsResponse() bool {
	return ue.isResponse
}

func (ue *BSFindNodeEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	n := node.(*BSNode)
	return n.handleFindNodeRequest(ctx, ue)
}

type BSFindRangeEvent struct {
	isResponse bool
	rng        ayame.Key
	messageId  string
	targets    []*BSNode
	ayame.AbstractSchedEvent
}

func NewBSFindRangeReqEvent(sender *BSNode, requestId string, rng ayame.Key) *BSFindRangeEvent {
	ev := &BSFindRangeEvent{
		isResponse:         false,
		rng:                rng,
		messageId:          requestId,
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	ev.SetRequest(true)
	return ev
}

func NewBSFindRangeResEvent(sender *BSNode, request *BSFindRangeEvent, targets []*BSNode) *BSFindRangeEvent {
	ev := &BSFindRangeEvent{
		isResponse:         true,
		rng:                request.rng,
		messageId:          request.messageId,
		targets:            targets,
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	return ev
}

func (ue *BSFindRangeEvent) String() string {
	return fmt.Sprintf("findrange id=%s", ue.messageId)
}

func (ue *BSFindRangeEvent) MessageId() string {
	return ue.messageId
}

func (ue *BSFindRangeEvent) Encode() *pb.Message {
	sender := ue.Sender().(*BSNode).Parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.messageId, pb.MessageType_FIND_RANGE, nil, nil, nil, ue.rng, nil)
	ret.IsResponse = ue.isResponse
	ret.IsRequest = ue.IsRequest()
	var lpeers []*pb.Peer
	for _, n := range ue.targets {
		lpeers = append(lpeers, n.Parent.Encode())
	}
	ret.Data.CloserPeers = lpeers
	return ret
}

func (ue *BSFindRangeEvent) Run(ctx context.Context, node ayame.Node) error {
	n := node.(*BSNode)
	return n.handleFindRange(ctx, ue)
}

func (ue *BSFindRangeEvent) IsResponse() bool {
	return ue.isResponse
}

func (ue *BSFindRangeEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	n := node.(*BSNode)
	return n.handleFindRangeRequest(ctx, ue)
}
