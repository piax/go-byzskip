package ayame

import (
	"context"

	"github.com/libp2p/go-libp2p-core/peer"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

type Node interface {
	Key() Key
	MV() *MembershipVector
	String() string
	Id() peer.ID // ID as an Endpoint
	Send(ctx context.Context, ev SchedEvent, sign bool)
	Encode() *pb.Peer
	Close()
}

var SecureKeyMV bool = true

type LocalNode struct {
	key Key
	mv  *MembershipVector
}

func NewLocalNode(key Key, mv *MembershipVector) *LocalNode {
	return &LocalNode{key: key, mv: mv}
}

func (n *LocalNode) Key() Key {
	return n.key
}

func (n *LocalNode) MV() *MembershipVector {
	return n.mv
}

func (n *LocalNode) String() string {
	return n.key.String()
}

func (n *LocalNode) Id() peer.ID {
	return "" // empty identifier
}

func (n *LocalNode) Encode() *pb.Peer {
	return nil // empty result
}

func (n *LocalNode) Close() {
	// nothing to do
}

func (an *LocalNode) Send(ctx context.Context, ev SchedEvent, sign bool) {
	//ev.SetSender(an)
	GlobalEventExecutor.RegisterEvent(ev, NETWORK_LATENCY)
}

//type yieldCh chan struct{}
//var YieldCh yieldCh

func (an *LocalNode) Sched(ev SchedEvent, time int64) {
	ev.SetSender(an)
	GlobalEventExecutor.RegisterEvent(ev, time)
}
