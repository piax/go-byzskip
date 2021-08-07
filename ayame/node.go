package ayame

import (
	"github.com/libp2p/go-libp2p-core/peer"
)

type Node interface {
	Key() Key
	MV() *MembershipVector
	String() string
	Id() peer.ID // ID as an Endpoint
	Send(ev SchedEvent)
}

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

func (an *LocalNode) Send(ev SchedEvent) {
	ev.SetSender(an)
	GlobalEventExecutor.RegisterEvent(ev, NETWORK_LATENCY)
}

func (an *LocalNode) Sched(ev SchedEvent, time int64) {
	ev.SetSender(an)
	GlobalEventExecutor.RegisterEvent(ev, time)
}
