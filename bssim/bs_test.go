package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/piax/go-ayame/ayame"
	bs "github.com/piax/go-ayame/byzskip"
	ast "github.com/stretchr/testify/assert"
)

func TestSim(t *testing.T) {
	numberOfPeers := 32
	peers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		peers[i] = bs.NewSimNode(ayame.IntKey(i), ayame.NewMembershipVector(2))
	}

	err := JoinAllByIterative(peers)
	if err != nil {
		fmt.Printf("join failed:%s\n", err)
	}

	localPeers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		localPeers[i] = bs.NewBSNode(ayame.NewLocalNode(peers[i].Key(), peers[i].MV()), bs.NewSkipRoutingTable, false)
	}
	FastJoinAllByCheat(localPeers)
	for i := 1; i < numberOfPeers; i++ {
		//fmt.Printf("cheat key=%s\n%s\n", localPeers[i].Key(), localPeers[i].RoutingTable)
		ast.Equal(t, bs.RoutingTableEquals(peers[i].RoutingTable, localPeers[i].RoutingTable), true, fmt.Sprintf("routing table real=%s and cheat=%s equals", peers[i].RoutingTable, localPeers[i].RoutingTable))
	}

}

func TestP2P(t *testing.T) {
	numberOfPeers := 100
	peers := make([]*bs.BSNode, numberOfPeers)
	peers[0], _ = bs.NewP2PNode("/ip4/127.0.0.1/udp/9000/quic", ayame.IntKey(0), ayame.NewMembershipVector(2), nil)
	locator := fmt.Sprintf("/ip4/127.0.0.1/udp/9000/quic/p2p/%s", peers[0].Id())

	for i := 1; i < numberOfPeers/2; i++ {
		addr := fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)
		peers[i], _ = bs.NewP2PNode(addr, ayame.IntKey(i), ayame.NewMembershipVector(2), nil)
		go func(pos int) {
			peers[pos].Join(context.Background(), locator)
		}(i)
	}
	time.Sleep(time.Duration(10) * time.Second)
	for i := numberOfPeers / 2; i < numberOfPeers; i++ {
		addr := fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)
		peers[i], _ = bs.NewP2PNode(addr, ayame.IntKey(i), ayame.NewMembershipVector(2), nil)
		go func(pos int) {
			peers[pos].Join(context.Background(), locator)
		}(i)
	}
	time.Sleep(time.Duration(10) * time.Second)

	for i := 1; i < numberOfPeers; i++ {
		//fmt.Printf("key=%s\n%s\n", peers[i].Key(), peers[i].RoutingTable)
	}

	localPeers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		localPeers[i] = bs.NewBSNode(ayame.NewLocalNode(peers[i].Key(), peers[i].MV()), bs.NewSkipRoutingTable, false)
	}
	FastJoinAllByCheat(localPeers)
	for i := 1; i < numberOfPeers; i++ {
		fmt.Printf("cheat key=%s\n%s\n", localPeers[i].Key(), localPeers[i].RoutingTable)
		ast.Equal(t, bs.RoutingTableEquals(peers[i].RoutingTable, localPeers[i].RoutingTable), true, fmt.Sprintf("routing table real=%s and cheat=%s equals", peers[i].RoutingTable, localPeers[i].RoutingTable))
	}
}

func TestTableIndex(t *testing.T) {
	numberOfPeers := 100
	localPeers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		localPeers[i] = bs.NewBSNode(ayame.NewLocalNode(ayame.IntKey(i), ayame.NewMembershipVector(2)), bs.NewSkipRoutingTable, false)
	}
	FastJoinAllByCheat(localPeers)
	for i := 1; i < numberOfPeers; i++ {
		fmt.Printf("key=%s\n%s", localPeers[i].Key(), localPeers[i].RoutingTable)
		for _, idx := range localPeers[i].RoutingTable.(*bs.SkipRoutingTable).GetTableIndex() {
			fmt.Printf("index level=%d, min=%s, max=%s\n", idx.Level, idx.Min, idx.Max)
		}
	}
}
