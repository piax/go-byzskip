package byzskip

import (
	"context"
	"fmt"
	"strconv"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	peerstore "github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/multiformats/go-multiaddr"
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
func (n *BSNode) IntroducerNode(locator string) (*BSNode, error) {
	// Turn the destination into a multiaddr.
	maddr, err := multiaddr.NewMultiaddr(locator)
	if err != nil {
		ayame.Log.Error(err)
		return nil, err
	}
	// Extract the peer ID from the multiaddr.
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		ayame.Log.Error(err)
		return nil, err
	}
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

func NewP2PNodeWithAuth(locator string, key ayame.Key, mv *ayame.MembershipVector, priv crypto.PrivKey,
	authorizer func(ayame.Node, peer.ID, ayame.Key, *ayame.MembershipVector) []byte,
	validator func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool) (*BSNode, error) {
	p2pNode, err := p2p.NewNode(context.TODO(), locator, key, mv, priv, ConvertMessage, validator)
	if err != nil {
		return nil, err
	}
	p2pNode.Cert = authorizer(p2pNode, p2pNode.Id(), key, mv)

	//p2pNode, err := p2p.NewNodeWithAuth(context.TODO(), locator, key, mv, ConvertMessage, authorizer)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	ret := NewBSNode(p2pNode, NewSkipRoutingTable, false)
	p2pNode.SetChild(ret)
	return ret, nil
}

func NewP2PNode(locator string, key ayame.Key, mv *ayame.MembershipVector, priv crypto.PrivKey) (*BSNode, error) {
	p2pNode, err := p2p.NewNode(context.TODO(), locator, key, mv, priv, ConvertMessage, nil)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	ret := NewBSNode(p2pNode, NewSkipRoutingTable, false)
	p2pNode.SetChild(ret)
	return ret, nil
}

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
	if ayame.SecureKeyMV {
		if self.Validator != nil {
			ayame.Log.Debugf("id=%s, key=%s, mv=%s, cert=%v", parent.Id(), parent.Key(), parent.MV(), p.Cert)
			if self.Validator(parent.Id(), parent.Key(), parent.MV(), p.Cert) {
				return NewBSNode(parent, NewSkipRoutingTable, false), nil
			}
			ayame.Log.Debugf("validation failed")
		} else { // no validator case
			return NewBSNode(parent, NewSkipRoutingTable, false), nil
		}
		return nil, fmt.Errorf("invalid join certificate")
	} else {
		return NewBSNode(parent, NewSkipRoutingTable, false), nil
	}
}

func ConvertTableIndex(idx *pb.TableIndex) *TableIndex {
	var minKey ayame.Key = nil
	if idx.Min != nil {
		minKey = p2p.NewKey(idx.Min)
	}
	var maxKey ayame.Key = nil
	if idx.Max != nil {
		maxKey = p2p.NewKey(idx.Max)
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
			Key:               p2p.NewKey(req.Key),
			MV:                ayame.NewMembershipVectorFromBinary(req.MV),
			ClosestIndex:      nil,
			NeighborListIndex: nil,
		}
	}
	return &NeighborRequest{
		Key:               p2p.NewKey(req.Key),
		MV:                ayame.NewMembershipVectorFromBinary(req.MV),
		ClosestIndex:      ConvertTableIndex(req.ClosestIndex),
		NeighborListIndex: ConvertTableIndexList(req.NeighborListIndex),
	}
}

func ConvertMessage(mes *pb.Message, self *p2p.P2PNode, valid bool) ayame.SchedEvent {
	level, _ := strconv.Atoi(mes.Data.SenderAppData) // sender app data indicates the level
	var ev ayame.SchedEvent
	author, _ := ConvertPeer(self, mes.Data.Author)
	ayame.Log.Infof("received msgid=%s,author=%s", mes.Data.Id, mes.Data.Author.Id)
	switch mes.Data.Type {
	case pb.MessageType_UNICAST:
		if mes.IsResponse {
			ev = NewBSUnicastResEvent(author, mes.Data.Id, mes.Data.Record.Value)
			ev.(*BSUnicastResEvent).Path = PathEntries(ConvertPeers(self, mes.Data.Path))
		} else {
			ev = NewBSUnicastEvent(author, mes.Data.AuthorSign, mes.Data.AuthorPubKey, mes.Data.Id, level, p2p.NewKey(mes.Data.Key), mes.Data.Record.Value)
			ev.(*BSUnicastEvent).Path = PathEntries(ConvertPeers(self, mes.Data.Path))
		}
		p, _ := ConvertPeer(self, mes.Sender)
		ev.SetSender(p)
		ev.SetVerified(valid) // verification conscious
	case pb.MessageType_FIND_NODE:
		var req *NeighborRequest = nil
		if mes.Data.Req != nil {
			req = ConvertFindNodeRequest(mes.Data.Req)
		}
		ev = &BSFindNodeEvent{
			isResponse:         mes.IsResponse,
			req:                req, //ConvertFindNodeRequest(mes.Data.Req),
			MessageId:          mes.Data.Id,
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
			TargetKey: p2p.NewKey(mes.Data.Key),
		}
		p, err := ConvertPeer(self, mes.Sender)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
	}
	return ev
}
