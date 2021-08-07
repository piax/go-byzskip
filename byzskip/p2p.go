package byzskip

import (
	"strconv"

	"github.com/piax/go-ayame/ayame"
	p2p "github.com/piax/go-ayame/ayame/p2p"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

// P2P related functions
/*
func ConvertEvent(ev ayame.SchedEvent, sender *BSNode) *p2p.Message {
	switch v := ev.(type) {
	case BSUnicastEvent:

	case BSFindNodeEvent:
	}
}*/

func ConvertPeers(sender *p2p.P2PNode, peers []*pb.Peer) []*BSNode {
	ret := []*BSNode{}
	for _, p := range peers {
		parent := p2p.NewRemoteNode(sender, p)
		ret = append(ret, NewBSNode(parent, NewBSRoutingTable, false))
	}
	return ret
}

func ConvertMessage(mes *pb.Message, self *p2p.P2PNode) ayame.SchedEvent {
	level, _ := strconv.Atoi(mes.Data.SenderAppData) // sender app data indicates the level
	var ev ayame.SchedEvent
	switch mes.Data.Type {
	case pb.MessageType_UNICAST:
		ev = &BSUnicastEvent{
			TargetKey:          p2p.NewKey(mes.Data.Key),
			MessageId:          mes.Data.Id,
			level:              level,
			Channel:            make(chan bool),
			AbstractSchedEvent: *ayame.NewSchedEvent()}
	case pb.MessageType_FIND_NODE:
		ev = &BSFindNodeEvent{
			isResponse:         mes.IsResponse,
			TargetKey:          p2p.NewKey(mes.Data.Key),
			MessageId:          mes.Data.Id,
			candidates:         ConvertPeers(self, mes.Data.CandidatePeers),
			closers:            ConvertPeers(self, mes.Data.CloserPeers),
			level:              level,
			AbstractSchedEvent: *ayame.NewSchedEvent()}
	}
	return ev
}
