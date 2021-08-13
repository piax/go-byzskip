package byzskip

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/piax/go-ayame/ayame"
	p2p "github.com/piax/go-ayame/ayame/p2p"
	ast "github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVector(2)})
	rt.ensureHeight(3)
	rt.ensureHeight(2)
	fmt.Println(rt.String())
	ast.Equal(t, rt.Height(), 4, "expected 4")
}

func TestLessCircular(t *testing.T) {
	fmt.Println(less(ayame.IntKey(5), ayame.IntKey(0), ayame.IntKey(10), ayame.IntKey(6), ayame.IntKey(3)))
	fmt.Println(less(ayame.IntKey(5), ayame.IntKey(0), ayame.IntKey(10), ayame.IntKey(3), ayame.IntKey(6)))
}

func TestSortCircular(t *testing.T) {
	lst := []KeyMV{}
	for i := 10; i > 0; i-- {
		lst = append(lst, &IntKeyMV{key: ayame.IntKey(i), Mvdata: ayame.NewMembershipVector(2)})
	}
	SortC(ayame.IntKey(7), lst)

	lst2 := []KeyMV{}
	base := ayame.IntKey(7)
	for i := 10; i > 0; i-- {
		lst2 = SortCircularAppend(base, lst2, &IntKeyMV{key: ayame.IntKey(i), Mvdata: ayame.NewMembershipVector(2)})
	}
	fmt.Println(ayame.SliceString(lst))
	fmt.Println(ayame.SliceString(lst2))
}

func TestSorted(t *testing.T) {
	InitK(2)
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt.Add(&IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0})})
	rt.Add(&IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0})})
	rt.Add(&IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0})})
	rt.Add(&IntKeyMV{key: 5, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0})})
	rt.Add(&IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0})})
	rt.Add(&IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})})
	rt.Add(&IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})})
	rslt := rt.GetCloserCandidates()
	fmt.Println(ayame.SliceString(rslt))
	rslt = rt.GetCommonNeighbors(ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0}))
	fmt.Println(ayame.SliceString(rslt))
}

func TestTicker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
	go func() {
		time.Sleep(time.Duration(300) * time.Millisecond)
		cancel()
	}()
	timer := time.NewTicker(time.Duration(100) * time.Millisecond)
L:
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("timed out %s\n", ctx.Err())
			timer.Stop() // stop the timer
			break L
		case <-timer.C:
			fmt.Println("timer event")
		}
	}

}

func TestP2P(t *testing.T) {
	numberOfPeers := 32
	peers := make([]*BSNode, numberOfPeers)
	peers[0], _ = NewP2PNode("/ip4/127.0.0.1/udp/9000/quic", ayame.IntKey(0), ayame.NewMembershipVector(2))
	fmt.Println(peers[0].parent.(*p2p.P2PNode).Host.ID().Pretty())
	locator := fmt.Sprintf("/ip4/127.0.0.1/udp/9000/quic/p2p/%s", peers[0].Id())

	for i := 1; i < numberOfPeers; i++ {
		addr := fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)
		peers[i], _ = NewP2PNode(addr, ayame.IntKey(i), ayame.NewMembershipVector(2))
		peers[i].Join(locator)
	}

	for i := 1; i < numberOfPeers; i++ {
		fmt.Printf("key=%s\n%s\n", peers[i].Key(), peers[i].RoutingTable)
	}
}
