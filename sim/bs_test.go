package sim

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
	ast "github.com/stretchr/testify/assert"
)

func TestSim(t *testing.T) {
	numberOfPeers := 32
	peers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		peers[i] = bs.NewSimNode(ayame.IntKey(i), ayame.NewMembershipVector(2))
	}

	//err := JoinAllByIterative(peers)
	err := FastJoinAllByIterative(peers, true)

	if err != nil {
		fmt.Printf("join failed:%s\n", err)
	}
	/*

		use := true
		useTableIndex = &use
		for _, p := range peers {
			FastUpdateNeighbors(p, ksToNs(p.RoutingTable.AllNeighbors(false, true)), []*bs.BSNode{})
		}
	*/

	localPeers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		localPeers[i] = bs.NewWithParent(ayame.NewLocalNode(peers[i].Key(), "", peers[i].MV()), bs.NewSkipRoutingTable, nil, false)
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
	h, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/9000/quic"))
	peers[0], _ = bs.New(h, bs.Key(ayame.IntKey(0)))
	locator := fmt.Sprintf("/ip4/127.0.0.1/udp/9000/quic/p2p/%s", peers[0].Id())

	for i := 1; i < numberOfPeers/2; i++ {
		h, _ := libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)))
		peers[i], _ = bs.New(h, bs.Key(ayame.IntKey(i)), bs.Bootstrap(locator))
		go func(pos int) {
			peers[pos].Join(context.Background())
		}(i)
	}
	time.Sleep(time.Duration(10) * time.Second)
	for i := numberOfPeers / 2; i < numberOfPeers; i++ {
		h, _ := libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)))
		peers[i], _ = bs.New(h, bs.Key(ayame.IntKey(i)), bs.Bootstrap(locator))
		go func(pos int) {
			peers[pos].Join(context.Background())
		}(i)
	}
	time.Sleep(time.Duration(10) * time.Second)

	for i := 1; i < numberOfPeers; i++ {
		//fmt.Printf("key=%s\n%s\n", peers[i].Key(), peers[i].RoutingTable)
	}

	localPeers := make([]*bs.BSNode, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		localPeers[i] = bs.NewWithParent(ayame.NewLocalNode(peers[i].Key(), "", peers[i].MV()), bs.NewSkipRoutingTable, nil, false)
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
		localPeers[i] = bs.NewWithParent(ayame.NewLocalNode(ayame.IntKey(i), "", ayame.NewMembershipVector(2)), bs.NewSkipRoutingTable, nil, false)
	}
	FastJoinAllByCheat(localPeers)
	for i := 1; i < numberOfPeers; i++ {
		fmt.Printf("key=%s\n%s", localPeers[i].Key(), localPeers[i].RoutingTable)
		for _, idx := range localPeers[i].RoutingTable.(*bs.SkipRoutingTable).GetTableIndex() {
			fmt.Printf("index level=%d, min=%s, max=%s\n", idx.Level, idx.Min, idx.Max)
		}
	}
}

func TestPickAlternately(t *testing.T) {
	lst := []*bs.BSNode{}
	sorted := []bs.KeyMV{}
	queried := []bs.KeyMV{}
	keys := []int{8, 10, 11, 9, 5, 3, 4, 15, 6}
	self := bs.NewWithParent(ayame.NewLocalNode(ayame.IntKey(7), "", ayame.NewMembershipVector(2)), bs.NewSkipRoutingTable, nil, false)
	for _, n := range keys {
		lst = append(lst, bs.NewWithParent(ayame.NewLocalNode(ayame.IntKey(n), "", ayame.NewMembershipVector(2)), bs.NewSkipRoutingTable, nil, false))
	}
	for _, n := range lst {
		sorted = bs.SortCircularAppend(self.Key(), sorted, n)
	}

	fmt.Println(ayame.SliceString(sorted))

	p := pickAlternately(self, sorted, copyReverseSlice(sorted), ksToNs(queried), 1)
	for len(p) != 0 {
		fmt.Println(p)
		queried = append(queried, p[0])
		p = pickAlternately(self, sorted, copyReverseSlice(sorted), ksToNs(queried), 1)
	}

}
