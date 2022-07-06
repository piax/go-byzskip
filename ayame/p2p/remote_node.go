package ayame

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/piax/go-ayame/ayame"
	p2p "github.com/piax/go-ayame/ayame/p2p/pb"
)

type RemoteNode struct {
	self  *P2PNode
	key   ayame.Key
	mv    *ayame.MembershipVector
	addrs []ma.Multiaddr
	cert  []byte
	id    peer.ID
}

// ayame.Node interface
func (n *RemoteNode) Key() ayame.Key {
	return n.key
}

func (n *RemoteNode) SetKey(key ayame.Key) {
	n.key = key
}

func (n *RemoteNode) MV() *ayame.MembershipVector { // Endpoint
	return n.mv
}

func (n *RemoteNode) SetMV(mv *ayame.MembershipVector) { // Endpoint
	n.mv = mv
}

func (n *RemoteNode) String() string {
	return fmt.Sprintf("%s,%s", n.key, n.addrs)
}
func (n *RemoteNode) Id() peer.ID { // Endpoint
	return n.id
}

func EncodeAddrs(maddrs []ma.Multiaddr) [][]byte {
	addrs := make([][]byte, len(maddrs))
	for i, maddr := range maddrs {
		addrs[i] = maddr.Bytes() // Bytes, not String. Compressed.
	}
	return addrs
}

func (n *RemoteNode) Encode() *p2p.Peer {
	return &p2p.Peer{
		Id:         peer.Encode(n.id),
		Mv:         n.mv.Encode(),
		Key:        n.key.Encode(),
		Addrs:      EncodeAddrs(n.addrs),
		Cert:       IfNeededSign(n.cert),
		Connection: ConnectionType(n.self.Network().Connectedness(n.id)),
	}
}

func (n *RemoteNode) Close() error {
	// Nothing to do
	return nil
}

func (n *RemoteNode) Addrs() []ma.Multiaddr {
	return n.addrs
}

func Addresses(addrs [][]byte) []ma.Multiaddr {
	maddrs := make([]ma.Multiaddr, 0, len(addrs))
	for _, addr := range addrs {
		maddr, err := ma.NewMultiaddrBytes(addr)
		if err != nil {
			ayame.Log.Debugf("error decoding multiaddr for peer %s\n", err)
			continue
		}

		maddrs = append(maddrs, maddr)
	}
	return maddrs
}

func ConnectionType(c network.Connectedness) p2p.ConnectionType {
	switch c {
	default:
		return p2p.ConnectionType_NOT_CONNECTED
	case network.NotConnected:
		return p2p.ConnectionType_NOT_CONNECTED
	case network.Connected:
		return p2p.ConnectionType_CONNECTED
	case network.CanConnect:
		return p2p.ConnectionType_CAN_CONNECT
	case network.CannotConnect:
		return p2p.ConnectionType_CANNOT_CONNECT
	}
}

// XXX not used yet.
func Connectedness(c p2p.ConnectionType) network.Connectedness {
	switch c {
	default:
		return network.NotConnected
	case p2p.ConnectionType_NOT_CONNECTED:
		return network.NotConnected
	case p2p.ConnectionType_CONNECTED:
		return network.Connected
	case p2p.ConnectionType_CAN_CONNECT:
		return network.CanConnect
	case p2p.ConnectionType_CANNOT_CONNECT:
		return network.CannotConnect
	}
}

func NewRemoteNode(self *P2PNode, p *p2p.Peer) *RemoteNode {
	// store to peerstore on self.
	id, _ := peer.Decode(p.Id)
	self.Peerstore().AddAddrs(id, Addresses(p.Addrs), peerstore.ProviderAddrTTL)
	return &RemoteNode{
		self:  self,
		id:    id,
		key:   NewKey(p.Key),
		addrs: Addresses(p.Addrs),
		cert:  p.Cert,
		mv:    ayame.NewMembershipVectorFromBinary(p.Mv),
	}
}

func NewIntroducerRemoteNode(self *P2PNode, id peer.ID, addrs []ma.Multiaddr) *RemoteNode {
	// store to peerstore on self.
	self.Peerstore().AddAddrs(id, addrs, peerstore.ProviderAddrTTL)
	return &RemoteNode{
		self: self,
		id:   id,
	}
}

func (n *RemoteNode) Send(ctx context.Context, ev ayame.SchedEvent, sign bool) error {
	// XXX wrong behavior
	return nil
}
