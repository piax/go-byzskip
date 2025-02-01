package authority

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
)

// export AUTH_PUBKEY=BABBEIIDVUCM6VGALPTCTHLCIK2GA6FQNVCJFGQODVSO6MISXBZXQ2CQIXDA
const (
	CERT_FILE = "BS_CERT_FILE"
)

func authFile(filename string, id peer.ID, key ayame.Key) (*PCert, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("authority error: %w", err)
	}
	var c PCert
	err = json.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("participation certificate error: %s", err)
	}
	if id != c.id {
		return nil, fmt.Errorf("unauthorized id error: %s != %s", id.String(), c.id.String())
	}
	if !key.Equals(c.Key) {
		return nil, fmt.Errorf("unauthorized key error: %s", err)
	}
	return &c, nil
}

func FileAuthAuthorize(id peer.ID, key ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error) {
	c, err := authFile(os.Getenv(CERT_FILE), id, key)
	if err != nil {
		return nil, nil, nil, err
	}

	// since this function is called at bootstrap phase, initialize the public key.
	if AuthPubKey == nil {
		pk := os.Getenv(AUTH_PUBKEY)
		if pk == "" {
			panic("CERT_PUBKEY is empty")
		} else {
			p, err := UnmarshalStringToPubKey(pk)
			if err != nil {
				panic(err)
			}
			AuthPubKey = p
		}
	}
	return c.Key, c.Mv, c.Cert, nil
}

func FileAuthValidate(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, cert []byte) bool {
	return VerifyJoinCert(id, key, mv, cert, AuthPubKey)
}
