package dht

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/op/go-logging"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	"github.com/piax/go-byzskip/byzskip"
)

type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

func addr(port int, quic bool) string {
	if quic {
		return fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", port)
	} else {
		return fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", port)
	}
}

func setupDHTs(ctx context.Context, numberOfPeers int, useQuic bool) []*BSDHT {
	byzskip.InitK(4)
	auth := authority.NewAuthorizer()
	authFunc := func(id peer.ID, key ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error) {
		mv := ayame.NewMembershipVector(2)
		bin := auth.Authorize(id, key, mv)
		return key, mv, bin, nil
	}
	validateFunc := func(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, cert []byte) bool {
		return authority.VerifyJoinCert(id, key, mv, cert, auth.PublicKey())
	}

	dhtOpts := []Option{
		NamespacedValidator("v", blankValidator{}),
		Authorizer(authFunc),
		AuthValidator(validateFunc),
	}

	peers := make([]*BSDHT, numberOfPeers)

	var introducer string = ""
	for i := 0; i < numberOfPeers; i++ {
		locator := addr(9000+i, useQuic)
		p2pOpts := []libp2p.Option{libp2p.ListenAddrStrings(locator)}
		h, err := libp2p.New(p2pOpts...)
		if err != nil {
			panic(err)
		}
		if introducer != "" { // not a bootstrap node
			dhtOpts = append(dhtOpts, Bootstrap(introducer))
		}
		peer, err := New(h, dhtOpts...)
		if err != nil {
			panic(err)
		}
		peers[i] = peer
		if i == 0 { // bootstrap
			introducer = locator + "/p2p/" + peers[0].Node.Id().String()
			peers[i].RunAsBootstrap(ctx)
		} else {
			go func(pos int) {
				if err := peers[pos].Bootstrap(ctx); err != nil {
					panic(err)
				}
			}(i)
		}
	}
	time.Sleep(time.Duration(5) * time.Second)
	sumCount := int64(0)
	sumTraffic := int64(0)
	for i := 0; i < numberOfPeers; i++ {
		sumCount += peers[i].Node.Parent.(*p2p.P2PNode).InCount
		sumTraffic += peers[i].Node.Parent.(*p2p.P2PNode).InBytes
		fmt.Printf("%s %d %d %f\n", peers[i].Node.Key(), peers[i].Node.Parent.(*p2p.P2PNode).InBytes, peers[i].Node.Parent.(*p2p.P2PNode).InCount, float64(peers[i].Node.Parent.(*p2p.P2PNode).InBytes)/float64(peers[i].Node.Parent.(*p2p.P2PNode).InCount))
	}
	fmt.Printf("avg-join-num-msgs: %f\n", float64(sumCount)/float64(numberOfPeers))
	fmt.Printf("avg-join-traffic(bytes): %f\n", float64(sumTraffic)/float64(numberOfPeers))
	fmt.Printf("avg-msg-size(bytes): %f\n", float64(sumTraffic)/float64(sumCount))
	return peers
}

func TestValueGetSet(t *testing.T) {
	ayame.InitLogger(logging.INFO)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nDHTs := 32

	dhts := setupDHTs(ctx, nDHTs, true)
	defer func() {
		for i := 0; i < nDHTs; i++ {
			dhts[i].Close()
		}
	}()

	ctxT, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	err := dhts[0].PutValue(ctxT, "/v/hello", []byte("world"))
	if err != nil {
		t.Fatal(err)
	}

	ctxT, cancel = context.WithTimeout(ctx, time.Second*2*60)
	defer cancel()

	val, err := dhts[1].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}
	vala, err := dhts[2].GetValue(ctxT, "/v/hello", Quorum(0))
	if err != nil {
		t.Fatal(err)
	}

	if string(vala) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(vala))
	}

	val, err = dhts[nDHTs-1].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}

	val, err = dhts[nDHTs/2].GetValue(ctxT, "/v/hello")
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "world" {
		t.Fatalf("Expected 'world' got '%s'", string(val))
	}

}
