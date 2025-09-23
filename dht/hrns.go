package dht

import (
	"fmt"
	"time"

	"net/url"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
)

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

func FullName(name string, idStr string) string {
	return "/hrns/" + url.QueryEscape(name) + ":" + idStr
}

func ParseName(keystr string) (*ayame.UnifiedKey, error) {
	log.Infof("ParseName: %s", keystr)
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

type NamedValueValidator struct{}

func (NamedValueValidator) Validate(key string, value []byte) error {
	// check if the key is same as the key in pCeert in value.
	var namedValue pb.NamedValue
	if err := proto.Unmarshal(value, &namedValue); err != nil {
		return fmt.Errorf("failed to unmarshal named value: %v", err)
	}

	if namedValue.Pcert == nil {
		return fmt.Errorf("no pcert found in named value")
	}

	var pCert pb.PCert
	if err := proto.Unmarshal(namedValue.Pcert, &pCert); err != nil {
		return fmt.Errorf("failed to unmarshal pcert: %v", err)
	}

	// Validate the key format
	keyInCert := FullName(pCert.Name, pCert.Id)
	if key != keyInCert {
		return fmt.Errorf("key in pcert=%s is not the same as the key in the named value=%s", keyInCert, key)
	}

	// Validate the signature
	if namedValue.PubKey == nil || namedValue.Sign == nil {
		return fmt.Errorf("missing public key or signature")
	}

	// Validate the value is not empty
	if len(namedValue.Value) == 0 {
		return fmt.Errorf("empty value not allowed")
	}

	_, err := CheckHRNSRecord(value)
	return err
}

func (NamedValueValidator) Select(key string, values [][]byte) (int, error) {
	if len(values) == 0 {
		return 0, fmt.Errorf("no values to select from")
	}

	/*
		// For named values, we want to keep the most recent one
		// This is determined by the ValidBefore field in the Cert
		var bestValidBefore int64
		bestIndex := 0

		for i, value := range values {
			var namedValue pb.NamedValue
			if err := proto.Unmarshal(value, &namedValue); err != nil {
				continue
			}

			var pCert pb.PCert
			if err := proto.Unmarshal(namedValue.Pcert, &pCert); err != nil {
				continue
			}

			var cert pb.Cert
			if err := proto.Unmarshal(pCert.Cert, &cert); err != nil {
				continue
			}

			// Use the certificate's ValidBefore to determine which value is newer
			if cert.ValidBefore > bestValidBefore {
				bestValidBefore = cert.ValidBefore
				bestIndex = i
			}
		}

		return bestIndex, nil
	*/
	// TODO: Implement the logic to select the most recent value
	// For now, just return the first value
	return 0, nil
}

// The argument value is Record.Value
func CheckHRNSRecord(value []byte) (*pb.NamedValue, error) {
	var namedValue pb.NamedValue
	if err := proto.Unmarshal(value, &namedValue); err != nil {
		return nil, fmt.Errorf("failed to unmarshal named value: %v", err)
	}

	var pCert pb.PCert
	if err := proto.Unmarshal(namedValue.Pcert, &pCert); err != nil {
		return nil, fmt.Errorf("failed to unmarshal pcert: %v", err)
	}

	_, va, vb, err := authority.ExtractCert(pCert.Cert)

	if err != nil {
		return nil, fmt.Errorf("failed to extract cert: %v", err)
	}

	validAfter := time.Unix(va, 0)
	validBefore := time.Unix(vb, 0)

	// Check if record is valid
	now := time.Now()
	if now.Before(validBefore) && now.After(validAfter) {
		log.Infof("record is valid")
		return &namedValue, nil
	}
	log.Debugf("record is not valid %s, %s, %s", now, validBefore, validAfter)
	return nil, fmt.Errorf("record is not valid")
}
