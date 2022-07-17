package byzskip

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	ast "github.com/stretchr/testify/assert"
)

func TestTableDeletion(t *testing.T) {
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVector(2)})
	rt.Add(&IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVector(2)}, true)
	rt.Add(&IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVector(2)}, true)

	rt.Delete(ayame.IntKey(2))
	rt.Delete(ayame.IntKey(3))
	ast.Equal(t, rt.Size(), 0, "expected 0")
}

func TestTable(t *testing.T) {
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVector(2)})
	rt.(*SkipRoutingTable).ensureHeight(3)
	rt.(*SkipRoutingTable).ensureHeight(2)
	fmt.Println(rt.String())
	ast.Equal(t, rt.(*SkipRoutingTable).Height(), 4, "expected 4")
}

func TestLessCircular(t *testing.T) {
	fmt.Println(less(ayame.IntKey(5), ayame.IntKey(0), ayame.IntKey(10), ayame.IntKey(6), ayame.IntKey(3)))
	fmt.Println(less(ayame.IntKey(5), ayame.IntKey(0), ayame.IntKey(10), ayame.IntKey(3), ayame.IntKey(6)))
}

func TestSortCircular(t *testing.T) {
	lst := []KeyMV{}
	keys := []int{8, 10, 11, 9, 5, 3, 4, 15, 6}
	for _, n := range keys {
		lst = append(lst, &IntKeyMV{key: ayame.IntKey(n), Mvdata: ayame.NewMembershipVector(2)})
	}
	SortC(ayame.IntKey(7), lst)

	lst2 := []KeyMV{}
	base := ayame.IntKey(7)
	for _, n := range keys {
		lst2 = SortCircularAppend(base, lst2, &IntKeyMV{key: ayame.IntKey(n), Mvdata: ayame.NewMembershipVector(2)})
	}
	fmt.Println(ayame.SliceString(lst))
	fmt.Println(ayame.SliceString(lst2))
}

func TestSorted(t *testing.T) {
	InitK(2)
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt.Add(&IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0})}, true)
	rt.Add(&IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0})}, true)
	rt.Add(&IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0})}, true)
	rt.Add(&IntKeyMV{key: 5, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0})}, true)
	rt.Add(&IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0})}, true)
	rt.Add(&IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})}, true)
	rt.Add(&IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})}, true)
	rslt := rt.(*SkipRoutingTable).GetCloserCandidates()
	ast.Equal(t, ayame.SliceString(rslt), "[2,8,3,7,5]", "expected [2,8,3,7,5]")
	fmt.Println(ayame.SliceString(rslt))
	rslt = rt.GetCommonNeighbors(ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0}))
	fmt.Println(ayame.SliceString(rslt))
}

func TestSortByCloseness(t *testing.T) {
	rt := []*BSNode{}
	rt = append(rt, &BSNode{key: ayame.IntKey(1), mv: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt = append(rt, &BSNode{key: ayame.IntKey(3), mv: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt = append(rt, &BSNode{key: ayame.IntKey(11), mv: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt = append(rt, &BSNode{key: ayame.IntKey(2), mv: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt = append(rt, &BSNode{key: ayame.IntKey(9), mv: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt = append(rt, &BSNode{key: ayame.IntKey(7), mv: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt = append(rt, &BSNode{key: ayame.IntKey(4), mv: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})

	rslt := sortByCloseness(ayame.IntKey(5), rt)
	ast.Equal(t, ayame.SliceString(rslt), "[7,4,9,3,11,2]")
}

func TestNeighbors(t *testing.T) {
	InitK(3)
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt.Add(&IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0})}, true)
	rt.Add(&IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0})}, true)
	rt.Add(&IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0})}, true)
	rt.Add(&IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0})}, true)
	rt.Add(&IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0})}, true)
	rt.Add(&IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})}, true)
	rt.Add(&IntKeyMV{key: 9, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})}, true)
	rslt := rt.Neighbors(&NeighborRequest{Key: ayame.IntKey(5), MV: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 1})})
	fmt.Println(ayame.SliceString(rslt))
	kcls, lv := rt.KClosest(&NeighborRequest{Key: ayame.IntKey(4), MV: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 1})})
	fmt.Println(ayame.SliceString(kcls))
	fmt.Println(lv)
}

func TestSufficient(t *testing.T) {
	InitK(2)
	rt := NewSkipRoutingTable(&IntKeyMV{key: ayame.IntKey(1), Mvdata: ayame.NewMembershipVector(2)})
	kms := []*IntKeyMV{}
	for i := 2; i < 100; i++ {
		km := &IntKeyMV{key: ayame.IntKey(i), Mvdata: ayame.NewMembershipVector(2)}
		kms = append(kms, km)
		rt.Add(km, true)
	}
	fmt.Println(rt)
	ast.Equal(t, rt.HasSufficientNeighbors(), true, "expected to have sufficient neighbors")
	rt.Delete(ayame.IntKey(2))
	fmt.Println(rt)
	ast.Equal(t, rt.HasSufficientNeighbors(), false, "expected to have not sufficient neighbors")
	for _, a := range kms[1:] {
		rt.Add(a, true)
	}
	fmt.Println(rt)
	ast.Equal(t, rt.HasSufficientNeighbors(), true, "expected to have sufficient neighbors")
}

func TestSufficient2(t *testing.T) {
	InitK(2)
	rt := NewSkipRoutingTable(&IntKeyMV{key: ayame.IntKey(0), Mvdata: ayame.NewMembershipVector(2)})
	km := &IntKeyMV{key: ayame.IntKey(1), Mvdata: ayame.NewMembershipVector(2)}
	rt.Add(km, true)
	fmt.Println(rt)
	ast.Equal(t, rt.HasSufficientNeighbors(), true, "expected to have sufficient neighbors")
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

func addr(port int, quic bool) string {
	if quic {
		return fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", port)
	} else {
		return fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)
	}
}

func setupNodes(num int, shuffle bool, useQuic bool) []*BSNode {
	auth := authority.NewAuthorizer()
	InitK(4)
	numberOfPeers := num
	keys := []int{}
	for i := 0; i < numberOfPeers; i++ {
		keys = append(keys, i)
	}
	if shuffle {
		rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	}
	authFunc := func(id peer.ID, key ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error) {
		mv := ayame.NewMembershipVector(2)
		bin := auth.Authorize(id, key, mv)
		return key, mv, bin, nil
	}
	validateFunc := func(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, cert []byte) bool {
		return authority.VerifyJoinCert(id, key, mv, cert, auth.PublicKey())
	}
	peers := make([]*BSNode, numberOfPeers)
	var err error
	h, err := libp2p.New([]libp2p.Option{libp2p.ListenAddrStrings(addr(9000, useQuic))}...)
	if err != nil {
		panic(err)
	}
	peers[0], err = New(h, []Option{Key(ayame.IntKey(keys[0])), Authorizer(authFunc), AuthValidator(validateFunc)}...)
	if err != nil {
		panic(err)
	}
	peers[0].RunBootstrap(context.Background())
	locator := fmt.Sprintf("%s/p2p/%s", addr(9000, useQuic), peers[0].Id())

	for i := 1; i < numberOfPeers; i++ {
		h, err := libp2p.New([]libp2p.Option{libp2p.ListenAddrStrings(addr(9000+i, useQuic))}...)
		if err != nil {
			panic(err)
		}
		peers[i], err = New(h, []Option{Bootstrap(locator), Key(ayame.IntKey(keys[i])), Authorizer(authFunc), AuthValidator(validateFunc)}...)
		if err != nil {
			panic(err)
		}
		go func(pos int) {
			peers[pos].Join(context.Background())
		}(i)
	}
	time.Sleep(time.Duration(5) * time.Second)
	sumCount := int64(0)
	sumTraffic := int64(0)
	for i := 0; i < numberOfPeers; i++ {
		sumCount += peers[i].Parent.(*p2p.P2PNode).InCount
		sumTraffic += peers[i].Parent.(*p2p.P2PNode).InBytes
		fmt.Printf("%s %d %d %f\n", peers[i].Key(), peers[i].Parent.(*p2p.P2PNode).InBytes, peers[i].Parent.(*p2p.P2PNode).InCount, float64(peers[i].Parent.(*p2p.P2PNode).InBytes)/float64(peers[i].Parent.(*p2p.P2PNode).InCount))
	}
	fmt.Printf("avg-join-num-msgs: %f\n", float64(sumCount)/float64(numberOfPeers))
	fmt.Printf("avg-join-traffic(bytes): %f\n", float64(sumTraffic)/float64(numberOfPeers))
	fmt.Printf("avg-msg-size(bytes): %f\n", float64(sumTraffic)/float64(sumCount))
	return peers
}

func TestJoin(t *testing.T) {
	numberOfPeers := 32
	setupNodes(numberOfPeers, true, true)
}

func TestFix(t *testing.T) { // 30 sec long test
	numberOfPeers := 32
	periodicBootstrapInterval = 20 * time.Second
	peers := setupNodes(numberOfPeers, false, true)
	time.Sleep(10 * time.Second)
	peers[2].Close()
	peers[20].Close()
	time.Sleep(30 * time.Second)
	ast.Equal(t, false, ContainsKey(ayame.IntKey(2), ksToNs(peers[1].RoutingTable.AllNeighbors(false, true))), "should not contain deleted key 2")
}

func TestLookup(t *testing.T) {
	numberOfPeers := 32
	peers := setupNodes(numberOfPeers, true, true)
	ayame.Log.Debugf("------- LOOKUP STARTS ---------")
	for i := 0; i < numberOfPeers; i++ { // RESET
		peers[i].Parent.(*p2p.P2PNode).InCount = 0
		peers[i].Parent.(*p2p.P2PNode).InBytes = 0
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
		sumCount += peers[i].Parent.(*p2p.P2PNode).InCount
		sumTraffic += peers[i].Parent.(*p2p.P2PNode).InBytes
		fmt.Printf("%s %d %d %f\n", peers[i].Key(), peers[i].Parent.(*p2p.P2PNode).InBytes, peers[i].Parent.(*p2p.P2PNode).InCount, float64(peers[i].Parent.(*p2p.P2PNode).InBytes)/float64(peers[i].Parent.(*p2p.P2PNode).InCount))
	}
	fmt.Printf("avg-lookup-num-msgs: %f\n", float64(sumCount)/float64(numberOfLookups))
	fmt.Printf("avg-lookup-traffic(bytes): %f\n", float64(sumTraffic)/float64(numberOfLookups))
}

func TestUnicast(t *testing.T) {
	numberOfPeers := 100
	useQuic := true
	peers := setupNodes(numberOfPeers, true, useQuic)
	ayame.Log.Debugf("------- UNICAST STARTS (USE QUIC=%v) ---------", useQuic)
	lock := sync.Mutex{}
	results := make(map[string][]ayame.Key)

	for i := 0; i < numberOfPeers; i++ { // RESET
		peers[i].Parent.(*p2p.P2PNode).InCount = 0
		peers[i].Parent.(*p2p.P2PNode).InBytes = 0

		peers[i].SetMessageReceiver(func(node *BSNode, ev *BSUnicastEvent) {
			lock.Lock()
			v, found := results[ev.MessageId]
			if !found {
				results[ev.MessageId] = []ayame.Key{node.Key().(ayame.IntKey)}
			} else {
				results[ev.MessageId] = append(v, node.Key()) //ayame.AppendIfMissing(v, node.Key())
			}
			lock.Unlock()
			fmt.Printf("%s received message '%s'for mid=%s key=%s %s\n", node, string(ev.Payload), ev.MessageId, ev.TargetKey, PathString(ev.Path))
		})

	}
	numberOfUnicasts := numberOfPeers
	for i := 0; i < numberOfUnicasts; i++ {
		src := rand.Intn(numberOfPeers)
		dst := rand.Intn(numberOfPeers)
		for src == dst {
			dst = rand.Intn(numberOfPeers)
		}
		peers[src].Unicast(context.Background(), ayame.IntKey(dst), peers[src].NewMessageId(), []byte("hello from "+strconv.Itoa(src)))
	}
	time.Sleep(time.Duration(15) * time.Second)
	sumCount := int64(0)
	sumTraffic := int64(0)

	for i := 0; i < numberOfPeers; i++ {
		sumCount += peers[i].Parent.(*p2p.P2PNode).InCount
		sumTraffic += peers[i].Parent.(*p2p.P2PNode).InBytes
		fmt.Printf("%s %d %d %f\n", peers[i].Key(), peers[i].Parent.(*p2p.P2PNode).InBytes, peers[i].Parent.(*p2p.P2PNode).InCount, float64(peers[i].Parent.(*p2p.P2PNode).InBytes)/float64(peers[i].Parent.(*p2p.P2PNode).InCount))
	}
	for _, lst := range results {
		fmt.Printf("%s\n", ayame.SliceString(lst))
	}
	fmt.Printf("sum-count: %d\n", sumCount)
	fmt.Printf("avg-unicast-num-msgs: %f\n", float64(sumCount)/float64(numberOfUnicasts))
	fmt.Printf("avg-unicast-traffic(bytes): %f\n", float64(sumTraffic)/float64(numberOfUnicasts))
}

func TestClose(t *testing.T) {
	numberOfPeers := 16
	peers := setupNodes(numberOfPeers, true, true)
	ayame.Log.Debugf("------- Closing nodes ---------")
	for i := 0; i < numberOfPeers; i++ {
		peers[i].Close()
	}
	for i := 0; i < numberOfPeers; i++ {
		fmt.Printf("%d: %d\n", peers[i].Key(), peers[i].RoutingTable.Size())
	}
}

func Example() {
	numberOfPeers := 32
	peers := make([]*BSNode, numberOfPeers)

	h, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/9000/quic"))
	peers[0], _ = New(h)
	introducer := fmt.Sprintf("/ip4/127.0.0.1/udp/9000/quic/p2p/%s", peers[0].Id())
	peers[0].RunBootstrap(context.Background())
	for i := 1; i < numberOfPeers; i++ {
		h, _ := libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)))
		peers[i], _ = New(h, Bootstrap(introducer))
		peers[i].Join(context.Background())
		peers[i].SetMessageReceiver(func(node *BSNode, ev *BSUnicastEvent) {
			fmt.Printf("%s: received '%s'\n", node.Key(), string(ev.Payload))
		})
	}
	result, _ := peers[2].Lookup(context.Background(), ayame.NewStringIdKey("hello"))
	for _, r := range result {
		fmt.Printf("found %s\n", r.Key())
	}
	peers[1].Unicast(context.Background(), peers[1].Key(), peers[1].NewMessageId(), []byte("hello world"))
	time.Sleep(time.Duration(100) * time.Millisecond)
}

func TestTCP(t *testing.T) {
	numberOfPeers := 100
	setupNodes(numberOfPeers, true, false)
}
