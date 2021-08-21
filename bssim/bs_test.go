package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/piax/go-ayame/ayame"
	bs "github.com/piax/go-ayame/byzskip"
	ast "github.com/stretchr/testify/assert"
)

func TestP2P(t *testing.T) {
	numberOfPeers := 100
	peers := make([]*bs.BSNode, numberOfPeers)
	peers[0], _ = bs.NewP2PNode("/ip4/127.0.0.1/udp/9000/quic", ayame.IntKey(0), ayame.NewMembershipVector(2))
	locator := fmt.Sprintf("/ip4/127.0.0.1/udp/9000/quic/p2p/%s", peers[0].Id())

	for i := 1; i < numberOfPeers; i++ {
		addr := fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)
		peers[i], _ = bs.NewP2PNode(addr, ayame.IntKey(i), ayame.NewMembershipVector(2))
		peers[i].Join(context.Background(), locator)
	}

	for i := 1; i < numberOfPeers; i++ {
		fmt.Printf("key=%s\n%s\n", peers[i].Key(), peers[i].RoutingTable)
	}

	localPeers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		localPeers[i] = bs.NewBSNode(ayame.NewLocalNode(peers[i].Key(), peers[i].MV()), bs.NewBSRoutingTable, false)
	}
	FastJoinAllByCheat(localPeers)
	for i := 1; i < numberOfPeers; i++ {
		fmt.Printf("cheat key=%s\n%s\n", localPeers[i].Key(), localPeers[i].RoutingTable)
		ast.Equal(t, bs.RoutingTableEquals(peers[i].RoutingTable, localPeers[i].RoutingTable), true, "routing table equals")
	}
}

func TestTableIndex(t *testing.T) {
	numberOfPeers := 100
	localPeers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		localPeers[i] = bs.NewBSNode(ayame.NewLocalNode(ayame.IntKey(i), ayame.NewMembershipVector(2)), bs.NewBSRoutingTable, false)
	}
	FastJoinAllByCheat(localPeers)
	for i := 1; i < numberOfPeers; i++ {
		fmt.Printf("key=%s\n%s", localPeers[i].Key(), localPeers[i].RoutingTable)
		for _, idx := range localPeers[i].RoutingTable.(*bs.BSRoutingTable).GetTableIndex() {
			fmt.Printf("index level=%d, min=%s, max=%s\n", idx.Level, idx.Min, idx.Max)
		}
	}
}
