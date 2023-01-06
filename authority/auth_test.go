package authority

import (
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
)

func TestAuthJoinInfo(t *testing.T) {
	key := ayame.IntKey(1)
	mv := ayame.NewMembershipVector(2)
	id := peer.ID("16Uiu2HAm23spX4oAbVpQKgrzPgUeHYHPhHiNME28qBYymmFFq4rm")
	auth := NewAuthorizer()
	sig := auth.Authorize(id, key, mv)
	fmt.Printf("%d\n", len(sig))
	if !VerifyJoinCert(id, key, mv, sig, auth.PublicKey()) {
		t.Fail()
	}
	id2 := peer.ID("16Uvu2HAm23spX4oAbVpQKgrzPgUeHYHPhHiNME28qBYymmFFq4rm") // modified
	if VerifyJoinCert(id2, key, mv, sig, auth.PublicKey()) {
		t.Fail()
	}
}
