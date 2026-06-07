package byzskip

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
)

// Bootstrap nodes have no BootstrapAddrs; fixLowPeers must refresh from self/RT without panicking.
func TestFixLowPeersBootstrapWithoutBootstrapAddrs(t *testing.T) {
	InitK(3)
	auth := authority.NewAuthorizer()
	authFunc := func(id peer.ID) (ayame.Key, string, *ayame.MembershipVector, []byte, error) {
		mv := ayame.NewMembershipVector(2)
		key := ayame.IntKey(0)
		cert := auth.Authorize(id, key, "", mv, time.Now().Unix(), time.Now().Unix()+100)
		return key, "", mv, cert, nil
	}
	validateFunc := func(id peer.ID, key ayame.Key, name string, mv *ayame.MembershipVector, cert []byte) bool {
		return authority.VerifyJoinCert(id, key, name, mv, cert, auth.PublicKey())
	}

	h, err := libp2p.New(libp2p.ListenAddrStrings(addr(19400, true)))
	if err != nil {
		t.Fatal(err)
	}
	node, err := New(h, []Option{
		Key(ayame.IntKey(0)),
		RedundancyFactor(3),
		Authorizer(authFunc),
		AuthValidator(validateFunc),
		FixLowPeersInterval(24 * time.Hour),
	}...)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.RunBootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(node.BootstrapAddrs) != 0 {
		t.Fatalf("bootstrap node should have empty BootstrapAddrs, got %d", len(node.BootstrapAddrs))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	node.fixLowPeers(ctx)
}

// K=1 sparse mesh: LookupMV must not panic in pickupKNodesMV (PickupKNodes already guards K==1).
func TestLookupMVK1SparseMesh(t *testing.T) {
	InitK(1)
	peers := setupIntKeyNodes(1, 2, false, true, 24*time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := peers[0].LookupMV(ctx, peers[0].MV()); err != nil {
		t.Fatalf("LookupMV: %v", err)
	}
}
