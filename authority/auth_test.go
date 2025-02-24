package authority

import (
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
)

func TestAuthJoinInfo(t *testing.T) {
	key := ayame.IntKey(1)
	mv := ayame.NewMembershipVector(2)
	id := peer.ID("16Uiu2HAm23spX4oAbVpQKgrzPgUeHYHPhHiNME28qBYymmFFq4rm")
	auth := NewAuthorizer()
	d, _ := time.ParseDuration("8760h")
	sig := auth.Authorize(id, key, mv, time.Now().Unix(), time.Now().Add(d).Unix())
	fmt.Printf("%d\n", len(sig))
	if !VerifyJoinCert(id, key, mv, sig, auth.PublicKey()) {
		t.Fail()
	}
	id2 := peer.ID("16Uvu2HAm23spX4oAbVpQKgrzPgUeHYHPhHiNME28qBYymmFFq4rm") // modified
	if VerifyJoinCert(id2, key, mv, sig, auth.PublicKey()) {
		t.Fail()
	}
	id3 := peer.ID("16Uiu2HAmJWtZnYUiB7aMDVQs6itSUPc5bCWFytwdf3KQYRuMcjTv")
	sig2 := auth.Authorize(id3, key, mv, time.Now().Add(d).Unix(), time.Now().Add(d).Unix()) // not valid yet
	if VerifyJoinCert(id3, key, mv, sig2, auth.PublicKey()) {
		t.Fail()
	}

	id4 := peer.NewPeerRecord().PeerID
	sig3 := auth.Authorize(id4, key, mv, time.Now().Unix()-100, time.Now().Unix()-100) // not valid yet
	if VerifyJoinCert(id4, key, mv, sig3, auth.PublicKey()) {
		t.Fail()
	}
}
