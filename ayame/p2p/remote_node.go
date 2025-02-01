package ayame

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/piax/go-byzskip/ayame"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
)

type RemoteNode struct {
	self  *P2PNode
	key   ayame.Key
	mv    *ayame.MembershipVector
	addrs []ma.Multiaddr
	cert  []byte
	id    peer.ID
	app   interface{}
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

func (n *RemoteNode) Encode() *pb.Peer {
	return &pb.Peer{
		Id:         n.id.String(),
		Mv:         n.mv.Encode(),
		Key:        n.key.Encode(),
		Addrs:      EncodeAddrs(n.addrs),
		Cert:       IfNeededSign(n.self.VerifyIntegrity, n.cert),
		Connection: ConnectionType(n.self.Network().Connectedness(n.id)),
	}
}

func (n *RemoteNode) Close() error {
	// Nothing to do
	return nil
}

func (n *RemoteNode) SetApp(app interface{}) {
	n.app = app
}

func (n *RemoteNode) App() interface{} {
	return n.app
}

func (n *RemoteNode) Addrs() []ma.Multiaddr {
	return n.addrs
}

func (n *RemoteNode) MessageIdPrefix() string {
	return n.Id().String()
}

func (n *RemoteNode) Equals(o ayame.Node) bool {
	return n.Id() == o.Id()
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

func ConnectionType(c network.Connectedness) pb.ConnectionType {
	switch c {
	default:
		return pb.ConnectionType_NOT_CONNECTED
	case network.NotConnected:
		return pb.ConnectionType_NOT_CONNECTED
	case network.Connected:
		return pb.ConnectionType_CONNECTED
	case network.CanConnect:
		return pb.ConnectionType_CAN_CONNECT
	case network.CannotConnect:
		return pb.ConnectionType_CANNOT_CONNECT
	}
}

// XXX not used yet.
func Connectedness(c pb.ConnectionType) network.Connectedness {
	switch c {
	default:
		return network.NotConnected
	case pb.ConnectionType_NOT_CONNECTED:
		return network.NotConnected
	case pb.ConnectionType_CONNECTED:
		return network.Connected
	case pb.ConnectionType_CAN_CONNECT:
		return network.CanConnect
	case pb.ConnectionType_CANNOT_CONNECT:
		return network.CannotConnect
	}
}

func NewRemoteNode(self *P2PNode, p *pb.Peer) *RemoteNode {
	// store to peerstore on self.
	id, _ := peer.Decode(p.Id)
	self.Peerstore().AddAddrs(id, Addresses(p.Addrs), peerstore.AddressTTL)
	return &RemoteNode{
		self:  self,
		id:    id,
		key:   ayame.NewKey(p.Key),
		addrs: Addresses(p.Addrs),
		cert:  p.Cert,
		mv:    ayame.NewMembershipVectorFromBinary(p.Mv),
	}
}

func NewIntroducerRemoteNode(self *P2PNode, id peer.ID, addrs []ma.Multiaddr) *RemoteNode {
	// store to peerstore on self.
	self.Peerstore().AddAddrs(id, addrs, peerstore.AddressTTL)
	return &RemoteNode{
		self: self,
		id:   id,
	}
}

func (n *RemoteNode) Send(ctx context.Context, ev ayame.SchedEvent, sign bool) error {
	// XXX wrong behavior
	return nil
}
