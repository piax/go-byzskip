package dht

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
	"github.com/piax/go-byzskip/byzskip"
)

// Human-Readable Name System
// /hrns/<domain-name>:<peer-id>

func Normalize(key string) (string, error) {
	if strings.HasPrefix(key, "/ipns/") || strings.HasPrefix(key, "/ipfs/") || strings.HasPrefix(key, "/pk/") {
		return "", fmt.Errorf("not a hrns name %s", key)
	}
	if !strings.HasPrefix(key, "/hrns/") {
		if strings.HasPrefix(key, "/") {
			key = "/hrns" + key
		} else {
			key = "/hrns/" + key
		}
	}
	return key, nil
}

func FullName(name string, node *byzskip.BSNode) (string, error) {
	k, err := ParseName(name)
	if err != nil {
		return "", err
	}
	if len(k.ID()) == 0 {
		k.SetID(node.Id())
	}
	return "/hrns/" + name + ":" + node.Id().String(), nil
}

func ParseName(keystr string) (*ayame.UnifiedKey, error) {
	parts := strings.Split(keystr, "/")
	if parts[0] != "" || parts[1] != "hrns" {
		return nil, fmt.Errorf("only /hrns/... is allowed: %s", parts[0])
	} else if len(parts) < 3 {
		return nil, fmt.Errorf("/hrns/<name>/... is needed: %s", keystr)
	} else {
		var n string
		var id peer.ID
		var err error
		arr := strings.Split(parts[2], ":")
		if len(arr) == 2 {
			n, err = url.QueryUnescape(arr[0])
			if err != nil {
				return nil, err
			}
			id, err = peer.Decode(arr[1])
			if err != nil {
				return nil, err
			}
		} else if len(arr) == 1 {
			n, err = url.QueryUnescape(arr[0])
			if err != nil {
				return nil, err
			}
			id = "" // empty peer ID
		} else {
			return nil, fmt.Errorf("syntax error in %s", keystr)
		}
		ret := ayame.NewUnifiedKeyFromString(n, id).(*ayame.UnifiedKey)
		return ret, nil
	}
}

type HRNSValidator struct{}

func (HRNSValidator) Validate(_ string, _ []byte) error        { return nil }
func (HRNSValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }
