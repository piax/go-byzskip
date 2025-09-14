package authority

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
)

const (
	WEBAUTH_URL = "WEBAUTH_URL"
)

func WebGetCert(id peer.ID, key ayame.Key, name string) (*PCert, error) {
	c, err := authWeb(os.Getenv(WEBAUTH_URL), id, name)
	if err != nil {
		return nil, err
	}
	UpdateAuthPubKey()
	return c, nil
}

func AuthValidator(authPubkey string) func(id peer.ID, key ayame.Key, name string, mv *ayame.MembershipVector, cert []byte) bool {
	return func(id peer.ID, key ayame.Key, name string, mv *ayame.MembershipVector, cert []byte) bool {
		p, err := UnmarshalStringToPubKey(authPubkey)
		if err != nil {
			return false
		}
		return VerifyJoinCert(id, key, name, mv, cert, p)
	}
}

func AuthValidate(id peer.ID, key ayame.Key, name string, mv *ayame.MembershipVector, cert []byte) bool {
	UpdateAuthPubKey()
	return VerifyJoinCert(id, key, name, mv, cert, AuthPubKey)
}

func authWeb(authUrl string, id peer.ID, name string) (*PCert, error) {
	name = url.QueryEscape(name)
	resp, err := http.Get(authUrl + fmt.Sprintf("/issue?id=%s&name=%s", id.String(), name))
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
