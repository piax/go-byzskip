package authority

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
)

const (
	WEBAUTH_URL    = "WEBAUTH_URL"
	WEBAUTH_PUBKEY = "WEBAUTH_PUBKEY"
)

var AuthPubKey crypto.PubKey = nil

func WebAuthAuthorize(id peer.ID, key ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error) {
	c, err := authWeb(os.Getenv(WEBAUTH_URL), id, key)
	if err != nil {
		return nil, nil, nil, err
	}

	// since this function is called at bootstrap phase, initialize the public key.
	if AuthPubKey == nil {
		pk := os.Getenv(WEBAUTH_PUBKEY)
		if pk == "" {
			panic("WEBAUTH_PUBKEY is empty")
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

func WebAuthValidate(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, cert []byte) bool {
	return VerifyJoinCert(id, key, mv, cert, AuthPubKey)
}

func authWeb(url string, id peer.ID, key ayame.Key) (*PCert, error) {
	var keyStr string
	if key == nil {
		keyStr = ""
	} else {
		keyStr = MarshalKeyToString(key)
	}
	resp, err := http.Get(url + fmt.Sprintf("/issue?key=%s&id=%s", keyStr, id.Pretty()))
	if err != nil {
		return nil, fmt.Errorf("authority error: %s", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var c PCert
	err = json.Unmarshal(body, &c)
	if err != nil {
		return nil, fmt.Errorf("participation certificate error: %s", err)
	}
	return &c, nil
}
