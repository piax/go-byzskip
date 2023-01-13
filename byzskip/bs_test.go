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
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/op/go-logging"
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

func TestK(t *testing.T) {
	InitK(3)
	ast.Equal(t, LEFT_HALF_K, 2, "expected 2")
	ast.Equal(t, RIGHT_HALF_K, 1, "expected 1")
	InitK(1)
	ast.Equal(t, LEFT_HALF_K, 1, "expected 1")
	ast.Equal(t, RIGHT_HALF_K, 0, "expected 0")
}

func TestSortMV(t *testing.T) {
	InitK(4)
	lst := []KeyMV{}
	lst = append(lst, &IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 0})})
	lst = append(lst, &IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0, 0})})
	lst = append(lst, &IntKeyMV{key: 22, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0, 1})})
	lst = append(lst, &IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0, 0})})
	lst = append(lst, IntKeyMV{key: 14, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 1, 0})})
	lst = append(lst, IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0, 0})})
	lst = append(lst, IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0, 0})})
	target := ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0, 0})
	//fmt.Println(target.Less(lst[1].MV()))
	//fmt.Println(MVString(lst))
	SortCMV(target, lst)
	ast.Equal(t, ayame.SliceString(lst), "[6,14,22,1,2,3,4]", "expected [6,14,22,1,2,3,4]")
	//fmt.Println(MVString(lst))
	//fmt.Println(target.LessOrEquals(lst[1].MV()))
	//found := closestMV(target, lst)
	//fmt.Println(ayame.SliceString(found))
}

func TestSortKey(t *testing.T) {
	InitK(4)
	lst := []KeyMV{}
	lst = append(lst, IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 0})})
	lst = append(lst, IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0, 0})})
	lst = append(lst, IntKeyMV{key: 22, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0, 1})})
	lst = append(lst, IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0, 0})})
	lst = append(lst, IntKeyMV{key: 14, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 1, 0})})
	lst = append(lst, IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0, 0})})
	lst = append(lst, IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0, 0})})
	fmt.Println(ayame.SliceString(lst))
	SortC(ayame.IntKey(7), lst)
	fmt.Println(ayame.SliceString(lst))
	//found := closestMV(target, lst)
	//fmt.Println(ayame.SliceString(found))
}

func TestSorted(t *testing.T) {
	InitK(2)
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt.Add(IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0})}, true)
	rt.Add(IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0})}, true)
	rt.Add(IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0})}, true)
	rt.Add(IntKeyMV{key: 5, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0})}, true)
	rt.Add(IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0})}, true)
	rt.Add(IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})}, true)
	rt.Add(IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})}, true)
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

func TestLongest(t *testing.T) {
	K = 2
	lst := []KeyMV{
		&IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0})},
		&IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})},
		&IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0})},
		&IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0})},
		&IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0})},
		&IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})},
		&IntKeyMV{key: 9, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})},
	}
	lngst := longestMVMatches(lst, ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0}))
	fmt.Println(ayame.SliceString(lngst))
}

func TestKClosestMV(t *testing.T) {
	mv := ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0, 0, 0})
	lst := []KeyMV{
		IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0, 0, 0})},
		IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 1, 0, 0})},
		IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 0, 1})},
		IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0, 0, 1})},
		IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0, 0, 1})},
		IntKeyMV{key: 9, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 1, 0, 1})},
		IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0, 0, 0})},
	}
	SortCMV(mv, lst)
	fmt.Println(ayame.SliceString(lst))
	K = 4
	closest := kMVClosest(ayame.IntKey(4), lst, mv)
	fmt.Println(ayame.SliceString(closest))
	mv2 := ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 1, 1, 0})
	closest = kMVClosest(ayame.IntKey(4), lst, mv2)
	fmt.Println(ayame.SliceString(closest))

	lst = []KeyMV{
		IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 0, 0})},
		IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 1, 0, 0})},
		IntKeyMV{key: 9, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 0, 1})},
		IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 1, 0})},
		IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 1, 1, 0})},
		IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0, 0, 1})},
		IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0, 0, 0})},
	}
	closest = kMVClosest(ayame.IntKey(4), lst, mv2)
	fmt.Println(ayame.SliceString(closest))
}

func TestExtraRight(t *testing.T) {
	lst := []KeyMV{
		KeyMVData{key: ayame.NewUnifiedKeyFromString("a", ayame.RandomID()), Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0, 0, 0})},
		KeyMVData{key: ayame.NewUnifiedKeyFromString("a", ayame.RandomID()), Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0, 0, 0})},
		KeyMVData{key: ayame.NewUnifiedKeyFromString("a", ayame.RandomID()), Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 0, 0})},
		KeyMVData{key: ayame.NewUnifiedKeyFromString("a", ayame.RandomID()), Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0, 0, 0})},
		KeyMVData{key: ayame.NewUnifiedKeyFromString("a", ayame.RandomID()), Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0, 0, 0})},
	}
	base := ayame.NewUnifiedKeyFromString("a", ayame.ZeroID())
	SortC(base, lst)
	max := lst[2]
	ast.Equal(t, true, base.Less(max.Key()))
	ast.Equal(t, true, base.LessOrEquals(max.Key()))
	ast.NotEqual(t, nil, EndExtentIn(ayame.NewRangeKey(base, true, max.Key(), true), lst))
}

func TestPickExtra(t *testing.T) {
	InitK(4)
	lst := []KeyMV{
		IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 0, 0})},
		IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 1, 0, 0})},
		IntKeyMV{key: 9, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 0, 1})},
		IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0, 1, 0})},
		IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 1, 1, 0})},
		IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0, 0, 1})},
		IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0, 0, 0})},
	}
	base := IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0, 0, 1})}
	SortC(base.Key(), lst)
	l := pickRangeFrom(ayame.IntKey(2), ayame.NewRangeKey(ayame.IntKey(4), true, ayame.IntKey(5), true), lst)
	fmt.Println(ayame.SliceString(l))
	l2 := pickRangeFrom(ayame.IntKey(2), ayame.NewRangeKey(ayame.IntKey(4), true, ayame.IntKey(5), true), lst)
	fmt.Println(ayame.SliceString(l2))
}

func TestSufficient(t *testing.T) {
	InitK(2)
	rt := NewSkipRoutingTable(IntKeyMV{key: ayame.IntKey(1), Mvdata: ayame.NewMembershipVector(2)})
	kms := []IntKeyMV{}
	for i := 2; i < 100; i++ {
		km := IntKeyMV{key: ayame.IntKey(i), Mvdata: ayame.NewMembershipVector(2)}
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
	rt := NewSkipRoutingTable(IntKeyMV{key: ayame.IntKey(0), Mvdata: ayame.NewMembershipVector(2)})
	km := IntKeyMV{key: ayame.IntKey(1), Mvdata: ayame.NewMembershipVector(2)}
	rt.Add(km, true)
	fmt.Println(rt)
	ast.Equal(t, rt.HasSufficientNeighbors(), false, "expected to have sufficient neighbors")
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

func setupNodes(k int, num int, shuffle bool, useQuic bool) []*BSNode {
	auth := authority.NewAuthorizer()
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
	peers[0], err = New(h, []Option{Key(ayame.IntKey(keys[0])), RedundancyFactor(k), Authorizer(authFunc), AuthValidator(validateFunc)}...)
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

func setupNamedNodes(k int, num int, shuffle bool, useQuic bool) []*BSNode {
	auth := authority.NewAuthorizer()
	numberOfPeers := num
	keys := []int{}
	for i := 0; i < numberOfPeers; i++ {
		keys = append(keys, i)
	}
	if shuffle {
		rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	}
	authFunc := func(id peer.ID, key ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error) {
		//mv := ayame.NewMembershipVector(2)
		mv := ayame.NewMembershipVectorFromBinary([]byte(id))
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
	rkey := ayame.NewUnifiedKeyFromString(string([]byte{'r'}), h.ID())
	peers[0], err = New(h, []Option{Key(rkey), RedundancyFactor(k), Authorizer(authFunc), AuthValidator(validateFunc)}...)
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
		key := ayame.NewUnifiedKeyFromString(string([]byte{'a' + byte(i/10)}), h.ID())
		peers[i], err = New(h, []Option{Bootstrap(locator), Key(key), Authorizer(authFunc), AuthValidator(validateFunc)}...)
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

func TestMVFromPeerID(t *testing.T) {
	ayame.NewMembershipVectorFromBinary([]byte(ayame.RandomID()))
}

func TestJoin(t *testing.T) {
	numberOfPeers := 32
	setupNodes(4, numberOfPeers, true, true)
}

func TestFix(t *testing.T) { // 30 sec long test

	numberOfPeers := 32
	periodicBootstrapInterval = 20 * time.Second
	peers := setupNodes(4, numberOfPeers, false, true)
	time.Sleep(2 * time.Second)
	peers[2].Close()
	peers[20].Close()
	time.Sleep(20 * time.Second)
	ast.Equal(t, false, ContainsKey(ayame.IntKey(2), ksToNs(peers[1].RoutingTable.AllNeighbors(false, true))), "should not contain deleted key 2")
}

func TestLookup(t *testing.T) {
	numberOfPeers := 32
	peers := setupNodes(4, numberOfPeers, true, true)
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

func TestLookupMV(t *testing.T) {
	numberOfPeers := 32
	peers := setupNodes(4, numberOfPeers, false, true)
	ayame.Log.Debugf("------- LOOKUP MV STARTS ---------")
	for i := 0; i < numberOfPeers; i++ { // RESET
		peers[i].Parent.(*p2p.P2PNode).InCount = 0
		peers[i].Parent.(*p2p.P2PNode).InBytes = 0
		ayame.Log.Debugf("key=%s,mv=%s\n%s", peers[i].Key(), peers[i].MV(), peers[i].RoutingTable)
	}
	numberOfLookups := numberOfPeers
	for i := 0; i < numberOfLookups; i++ {
		src := rand.Intn(numberOfPeers)
		dst := rand.Intn(numberOfPeers)
		ayame.Log.Debugf("src=%d, dst=%s, search=%d", src, peers[dst].MV(), dst)
		nodes, _ := peers[src].LookupMV(context.Background(), peers[dst].MV(), ayame.IntKey(src))
		ayame.Log.Debugf("src=%d, dst=%s, searched=%s", src, peers[dst].MV(), ayame.SliceString(nodes))
		ast.Equal(t, true, Contains(peers[dst], nodes))
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

func TestLookupRange(t *testing.T) {
	ayame.InitLogger(logging.INFO)
	numberOfPeers := 32
	peers := setupNodes(4, numberOfPeers, false, true)
	ayame.Log.Debugf("------- LOOKUP RANGE STARTS ---------")
	for i := 0; i < numberOfPeers; i++ { // RESET
		peers[i].Parent.(*p2p.P2PNode).InCount = 0
		peers[i].Parent.(*p2p.P2PNode).InBytes = 0
		ayame.Log.Debugf("key=%s,mv=%s\n%s", peers[i].Key(), peers[i].MV(), peers[i].RoutingTable)
	}
	numberOfLookups := numberOfPeers
	for i := 0; i < numberOfLookups; i++ {
		src := rand.Intn(numberOfPeers)
		nodes, _ := peers[src].LookupRange(context.Background(), ayame.NewRangeKey(ayame.IntKey(5), true, ayame.IntKey(7), true))
		fmt.Printf("src=%d, searched=%s\n", src, ayame.SliceString(nodes))
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

func TestLookupName(t *testing.T) {
	ayame.InitLogger(logging.INFO)
	numberOfPeers := 32
	peers := setupNamedNodes(4, numberOfPeers, false, true)
	ayame.Log.Debugf("------- LOOKUP MV STARTS ---------")
	for i := 0; i < numberOfPeers; i++ { // RESET
		peers[i].Parent.(*p2p.P2PNode).InCount = 0
		peers[i].Parent.(*p2p.P2PNode).InBytes = 0
		ayame.Log.Debugf("key=%s,mv=%s\n%s", peers[i].Key(), peers[i].MV(), peers[i].RoutingTable)

	}
	numberOfLookups := numberOfPeers
	for i := 0; i < numberOfLookups; i++ {
		src := rand.Intn(numberOfPeers)
		nodes, _ := peers[src].LookupName(context.Background(), "a")
		fmt.Printf("src=%d, searched=%s\n, len=%d", src, ayame.SliceString(nodes), len(nodes))
		ast.Equal(t, 13, len(nodes), "expected 13")
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
	peers := setupNodes(4, numberOfPeers, true, useQuic)
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
	time.Sleep(time.Duration(5) * time.Second)
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
	peers := setupNodes(4, numberOfPeers, true, true)
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
	setupNodes(4, numberOfPeers, true, false)
}
