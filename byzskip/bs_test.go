package byzskip

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/piax/go-ayame/authority"
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

func setupNodes(num int) []*BSNode {
	auth := authority.NewAuthorizer()
	InitK(4)
	numberOfPeers := num
	keys := []int{}
	for i := 0; i < numberOfPeers; i++ {
		keys = append(keys, i)
	}
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	authFunc := func(id peer.ID, key ayame.Key, mv *ayame.MembershipVector) []byte {
		bin := auth.Authorize(id, key, mv)
		return bin
	}
	validateFunc := func(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, cert []byte) bool {
		return authority.VerifyJoinCert(id, key, mv, cert, auth.PublicKey())
	}
	peers := make([]*BSNode, numberOfPeers)
	peers[0], _ = NewP2PNodeWithAuth("/ip4/127.0.0.1/udp/9000/quic", ayame.IntKey(keys[0]), ayame.NewMembershipVector(2), authFunc, validateFunc)
	locator := fmt.Sprintf("/ip4/127.0.0.1/udp/9000/quic/p2p/%s", peers[0].Id())

	for i := 1; i < numberOfPeers; i++ {
		addr := fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)
		peers[i], _ = NewP2PNodeWithAuth(addr, ayame.IntKey(keys[i]), ayame.NewMembershipVector(2), authFunc, validateFunc)
		peers[i].Join(context.Background(), locator)
	}
	sumCount := int64(0)
	sumTraffic := int64(0)
	for i := 0; i < numberOfPeers; i++ {
		sumCount += peers[i].parent.(*p2p.P2PNode).InCount
		sumTraffic += peers[i].parent.(*p2p.P2PNode).InBytes
		fmt.Printf("%s %d %d %f\n", peers[i].Key(), peers[i].parent.(*p2p.P2PNode).InBytes, peers[i].parent.(*p2p.P2PNode).InCount, float64(peers[i].parent.(*p2p.P2PNode).InBytes)/float64(peers[i].parent.(*p2p.P2PNode).InCount))
	}
	fmt.Printf("avg-join-num-msgs: %f\n", float64(sumCount)/float64(numberOfPeers))
	fmt.Printf("avg-join-traffic(bytes): %f\n", float64(sumTraffic)/float64(numberOfPeers))
	fmt.Printf("avg-msg-size(bytes): %f\n", float64(sumTraffic)/float64(sumCount))
	return peers
}

func TestLookup(t *testing.T) {
	numberOfPeers := 100
	peers := setupNodes(numberOfPeers)
	ayame.Log.Debugf("------- LOOKUP STARTS ---------")
	for i := 0; i < numberOfPeers; i++ { // RESET
		peers[i].parent.(*p2p.P2PNode).InCount = 0
		peers[i].parent.(*p2p.P2PNode).InBytes = 0
	}
	numberOfLookups := numberOfPeers
	for i := 0; i < numberOfLookups; i++ {
		src := rand.Intn(numberOfPeers)
		dst := rand.Intn(numberOfPeers)
		peers[src].Lookup(context.Background(), ayame.IntKey(dst))
	}
	sumCount := int64(0)
	sumTraffic := int64(0)
	for i := 0; i < numberOfPeers; i++ {
		sumCount += peers[i].parent.(*p2p.P2PNode).InCount
		sumTraffic += peers[i].parent.(*p2p.P2PNode).InBytes
		fmt.Printf("%s %d %d %f\n", peers[i].Key(), peers[i].parent.(*p2p.P2PNode).InBytes, peers[i].parent.(*p2p.P2PNode).InCount, float64(peers[i].parent.(*p2p.P2PNode).InBytes)/float64(peers[i].parent.(*p2p.P2PNode).InCount))
	}
	fmt.Printf("avg-lookup-num-msgs: %f\n", float64(sumCount)/float64(numberOfLookups))
	fmt.Printf("avg-lookup-traffic(bytes): %f\n", float64(sumTraffic)/float64(numberOfLookups))
}

func TestUnicast(t *testing.T) {
	numberOfPeers := 32
	peers := setupNodes(numberOfPeers)
	ayame.Log.Debugf("------- UNICAST STARTS ---------")

	lock := sync.Mutex{}
	results := make(map[string][]ayame.IntKey)

	for i := 0; i < numberOfPeers; i++ { // RESET
		peers[i].parent.(*p2p.P2PNode).InCount = 0
		peers[i].parent.(*p2p.P2PNode).InBytes = 0

		peers[i].SetUnicastHandler(func(node *BSNode, ev *BSUnicastEvent, alreadySeen bool, alreadyOnThePath bool) {
			if !alreadySeen && !alreadyOnThePath {
				lock.Lock()
				v, found := results[ev.MessageId]
				if !found {
					results[ev.MessageId] = []ayame.IntKey{node.Key().(ayame.IntKey)}
				} else {
					results[ev.MessageId] = append(v, node.Key().(ayame.IntKey))
				}
				lock.Unlock()
			}
			fmt.Printf("%s received message for mid=%s key=%s %v %v %s\n", node, ev.MessageId, ev.TargetKey, alreadySeen, alreadyOnThePath, PathString(ev.Path))
		})

	}
	numberOfLookups := numberOfPeers
	for i := 0; i < numberOfLookups; i++ {
		src := rand.Intn(numberOfPeers)
		dst := rand.Intn(numberOfPeers)
		for src == dst {
			dst = rand.Intn(numberOfPeers)
		}
		peers[src].Unicast(context.Background(), ayame.IntKey(dst), []byte("hello"))
	}
	time.Sleep(time.Duration(1) * time.Second)
	sumCount := int64(0)
	sumTraffic := int64(0)

	for i := 0; i < numberOfPeers; i++ {
		sumCount += peers[i].parent.(*p2p.P2PNode).InCount
		sumTraffic += peers[i].parent.(*p2p.P2PNode).InBytes
		fmt.Printf("%s %d %d %f\n", peers[i].Key(), peers[i].parent.(*p2p.P2PNode).InBytes, peers[i].parent.(*p2p.P2PNode).InCount, float64(peers[i].parent.(*p2p.P2PNode).InBytes)/float64(peers[i].parent.(*p2p.P2PNode).InCount))
	}

	fmt.Printf("sum-count: %d\n", sumCount)
	fmt.Printf("avg-lookup-num-msgs: %f\n", float64(sumCount)/float64(numberOfLookups))
	fmt.Printf("avg-lookup-traffic(bytes): %f\n", float64(sumTraffic)/float64(numberOfLookups))
	for _, lst := range results {
		fmt.Printf("%s\n", ayame.SliceString(lst))
	}
}
