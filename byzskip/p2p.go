package byzskip

import (
	"fmt"
	"strconv"

	"github.com/libp2p/go-libp2p/core/peer"
	peerstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
)

// P2P related functions
/*
func ConvertEvent(ev ayame.SchedEvent, sender *BSNode) *p2p.Message {
	switch v := ev.(type) {
	case BSUnicastEvent:

	case BSFindNodeEvent:
	}
}*/
func (n *BSNode) IntroducerNode(info peer.AddrInfo) (*BSNode, error) {
	// Turn the destination into a multiaddr.
	// Add the destination's peer multiaddress in the peerstore.
	self := n.Parent.(*p2p.P2PNode)
	self.Host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)

	ret := &BSNode{
		//LocalNode: ayame.GetLocalNode(strconv.Itoa(key)),
		Parent: p2p.NewIntroducerRemoteNode(self, info.ID, info.Addrs),
		// stats is not generated at this time
		IsFailure: false}
	ret.RoutingTable = NewSkipRoutingTable(ret)
	return ret, nil
}

/*
func NewNodeWithAuth(locator string, key ayame.Key, mv *ayame.MembershipVector, priv crypto.PrivKey,
	authorizer func(ayame.Node, peer.ID, ayame.Key, *ayame.MembershipVector) []byte,
	validator func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool) (*BSNode, error) {
	p2pNode, err := p2p.NewNode(context.TODO(), locator, key, mv, priv, ConvertMessage, validator, true)
	if err != nil {
		return nil, err
	}
	p2pNode.Cert = authorizer(p2pNode, p2pNode.Id(), key, mv)

	//p2pNode, err := p2p.NewNodeWithAuth(context.TODO(), locator, key, mv, ConvertMessage, authorizer)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	ret := NewWithParent(p2pNode, NewSkipRoutingTable, false)
	p2pNode.SetChild(ret)
	return ret, nil
}

/* deprecated
func NewNode(locator string, key ayame.Key, mv *ayame.MembershipVector, priv crypto.PrivKey) (*BSNode, error) {
	p2pNode, err := p2p.NewNode(context.TODO(), locator, key, mv, priv, ConvertMessage, nil, false)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	ret := NewWithParent(p2pNode, NewSkipRoutingTable, false)
	p2pNode.SetChild(ret)
	return ret, nil
}
*/

func ConvertPeers(self *p2p.P2PNode, peers []*pb.Peer) []*BSNode {
	ret := []*BSNode{}

	for _, p := range peers {
		if n, err := ConvertPeer(self, p); err == nil {
			ret = append(ret, n)
		} else {
			panic("pubkey verify failed")
		}
	}
	return ret
}

func ConvertPeer(self *p2p.P2PNode, p *pb.Peer) (*BSNode, error) {
	parent := p2p.NewRemoteNode(self, p)
	if self.VerifyIntegrity {
		if self.Validator != nil {
			ayame.Log.Debugf("id=%s, key=%s, mv=%s, cert=%v", parent.Id(), parent.Key(), parent.MV(), p.Cert)
			if self.Validator(parent.Id(), parent.Key(), parent.MV(), p.Cert) {
				return NewWithParent(parent, NewSkipRoutingTable, false), nil
			}
			ayame.Log.Debugf("validation failed")
		} else { // no validator case
			return NewWithParent(parent, NewSkipRoutingTable, false), nil
		}
		return nil, fmt.Errorf("invalid join certificate")
	} else {
		return NewWithParent(parent, NewSkipRoutingTable, false), nil
	}
}

func ConvertTableIndex(idx *pb.TableIndex) *TableIndex {
	var minKey ayame.Key = nil
	if idx.Min != nil {
		minKey = ayame.NewKey(idx.Min)
	}
	var maxKey ayame.Key = nil
	if idx.Max != nil {
		maxKey = ayame.NewKey(idx.Max)
	}
	return &TableIndex{
		Min:   minKey,
		Max:   maxKey,
		Level: int(idx.Level),
	}
}

func ConvertTableIndexList(idxs []*pb.TableIndex) []*TableIndex {
	ret := []*TableIndex{}
	for _, idx := range idxs {
		ret = append(ret, ConvertTableIndex(idx))
	}
	return ret
}

func ConvertFindNodeRequest(req *pb.FindNodeRequest) *NeighborRequest {
	if req.ClosestIndex == nil { // Lookup
		return &NeighborRequest{
			Key:               ayame.NewKey(req.Key),
			MV:                ayame.NewMembershipVectorFromBinary(req.MV),
			ClosestIndex:      nil,
			NeighborListIndex: nil,
		}
	}
	return &NeighborRequest{
		Key:               ayame.NewKey(req.Key),
		MV:                ayame.NewMembershipVectorFromBinary(req.MV),
		ClosestIndex:      ConvertTableIndex(req.ClosestIndex),
		NeighborListIndex: ConvertTableIndexList(req.NeighborListIndex),
	}
}

func ConvertMessage(mes *pb.Message, self *p2p.P2PNode, valid bool) ayame.SchedEvent {
	level, _ := strconv.Atoi(mes.Data.SenderAppData) // sender app data indicates the level
	var ev ayame.SchedEvent
	author, _ := ConvertPeer(self, mes.Data.Author)
	ayame.Log.Debugf("received msgid=%s,author=%s", mes.Data.Id, mes.Data.Author.Id)
	switch mes.Data.Type {
	case pb.MessageType_UNICAST:
		if mes.IsResponse {
			ev = NewBSUnicastResEvent(author, mes.Data.Id, mes.Data.Record[0].Value)
			ev.(*BSUnicastResEvent).Path = PathEntries(ConvertPeers(self, mes.Data.Path))
		} else {
			ev = NewBSUnicastEvent(author, mes.Data.AuthorSign, mes.Data.AuthorPubKey, mes.Data.Id, level, ayame.NewKey(mes.Data.Key), mes.Data.Record[0].Value)
			ev.(*BSUnicastEvent).Path = PathEntries(ConvertPeers(self, mes.Data.Path))
		}
		p, _ := ConvertPeer(self, mes.Sender)
		ev.SetSender(p)
		ev.SetVerified(!self.VerifyIntegrity || valid) // verification conscious
	case pb.MessageType_FIND_MV:
		tm := mes.Data.SenderAppData == "t"
		ev = &BSFindNodeMVEvent{
			isResponse:         mes.IsResponse,
			isTopmost:          tm,
			src:                ayame.NewKey(mes.Data.Key),
			mv:                 ayame.NewMembershipVectorFromBinary(mes.Data.Mv),
			messageId:          mes.Data.Id,
			neighbors:          ConvertPeers(self, mes.Data.CloserPeers),
			AbstractSchedEvent: *ayame.NewSchedEvent(nil, nil, nil)}
		p, err := ConvertPeer(self, mes.Sender)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetRequest(mes.IsRequest)
		ev.SetSender(p)
	case pb.MessageType_FIND_RANGE:
		ev = &BSFindRangeEvent{
			isResponse:         mes.IsResponse,
			rng:                ayame.NewKey(mes.Data.Key),
			messageId:          mes.Data.Id,
			targets:            ConvertPeers(self, mes.Data.CloserPeers),
			AbstractSchedEvent: *ayame.NewSchedEvent(nil, nil, nil)}
		p, err := ConvertPeer(self, mes.Sender)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetRequest(mes.IsRequest)
		ev.SetSender(p)
	case pb.MessageType_FIND_NODE:
		var req *NeighborRequest = nil
		if mes.Data.Req != nil {
			req = ConvertFindNodeRequest(mes.Data.Req)
		}
		ev = &BSFindNodeEvent{
			isResponse:         mes.IsResponse,
			req:                req, //ConvertFindNodeRequest(mes.Data.Req),
			messageId:          mes.Data.Id,
			candidates:         ConvertPeers(self, mes.Data.CandidatePeers),
			closers:            ConvertPeers(self, mes.Data.CloserPeers),
			level:              level,
			AbstractSchedEvent: *ayame.NewSchedEvent(nil, nil, nil)}
		p, err := ConvertPeer(self, mes.Sender)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetRequest(mes.IsRequest)
		ev.SetSender(p)
	case pb.MessageType_DEL_NODE:
		ev = &BSDelNodeEvent{
			MessageId: mes.Data.Id,
			TargetKey: ayame.NewKey(mes.Data.Key),
		}
		p, err := ConvertPeer(self, mes.Sender)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
	}
	return ev
}
