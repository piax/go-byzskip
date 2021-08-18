package byzskip

import (
	"context"
	"fmt"
	"strconv"

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
	self := n.parent.(*p2p.P2PNode)
	self.Host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)

	ret := &BSNode{
		//LocalNode: ayame.GetLocalNode(strconv.Itoa(key)),
		parent: p2p.NewIntroducerRemoteNode(self, info.ID, info.Addrs),
		// stats is not generated at this time
		IsFailure: false}
	ret.RoutingTable = NewBSRoutingTable(ret)
	return ret, nil
}

func NewP2PNodeWithAuth(locator string, key ayame.Key, mv *ayame.MembershipVector,
	authorizer func(peer.ID, ayame.Key, *ayame.MembershipVector) []byte,
	validator func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool) (*BSNode, error) {
	p2pNode, err := p2p.NewNode(context.TODO(), locator, key, mv, ConvertMessage, validator)
	p2pNode.Cert = authorizer(p2pNode.Id(), key, mv)

	//p2pNode, err := p2p.NewNodeWithAuth(context.TODO(), locator, key, mv, ConvertMessage, authorizer)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	ret := NewBSNode(p2pNode, NewBSRoutingTable, false)
	p2pNode.SetChild(ret)
	return ret, nil
}

func NewP2PNode(locator string, key ayame.Key, mv *ayame.MembershipVector) (*BSNode, error) {
	p2pNode, err := p2p.NewNode(context.TODO(), locator, key, mv, ConvertMessage, nil)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	ret := NewBSNode(p2pNode, NewBSRoutingTable, false)
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
		if self.Validator(parent.Id(), parent.Key(), parent.MV(), p.Cert) {
			return NewBSNode(parent, NewBSRoutingTable, false), nil
		}
		return nil, fmt.Errorf("invalid join certificate")
	} else {
		return NewBSNode(parent, NewBSRoutingTable, false), nil
	}
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
			Path:               PathEntries(ConvertPeers(self, mes.Data.CandidatePeers)),
			AbstractSchedEvent: *ayame.NewSchedEvent(),
			Payload:            mes.Data.Record.Value}
		p, _ := ConvertPeer(self, mes.Sender)
		ev.SetSender(p)
	case pb.MessageType_FIND_NODE:
		ev = &BSFindNodeEvent{
			isResponse:         mes.IsResponse,
			TargetKey:          p2p.NewKey(mes.Data.Key),
			TargetMV:           ayame.NewMembershipVectorFromBinary(mes.Data.Mv),
			MessageId:          mes.Data.Id,
			candidates:         ConvertPeers(self, mes.Data.CandidatePeers),
			closers:            ConvertPeers(self, mes.Data.CloserPeers),
			level:              level,
			AbstractSchedEvent: *ayame.NewSchedEvent()}
		p, err := ConvertPeer(self, mes.Sender)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
	}
	return ev
}
