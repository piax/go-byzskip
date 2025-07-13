package dht

import (
	"context"
	"fmt"
	"time"

	"net/url"
	"strings"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/kubo/repo"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
)

// Human-Readable Name System
// /hrns/<domain-name>:<peer-id>
const VALIDITY_PERIOD = 24 * time.Hour

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
	ayame.Log.Infof("ParseName: %s", keystr)
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

	return checkHRNSRecord(value)
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

// HRNSRepublisher runs a service to republish HRNS records at specified intervals
func HRNSRepublisher(repubPeriod time.Duration) func(lc fx.Lifecycle, dht *BSDHT, repo repo.Repo) error {
	return func(lc fx.Lifecycle, dht *BSDHT, repo repo.Repo) error {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go func() {
					ticker := time.NewTicker(repubPeriod)
					defer ticker.Stop()

					for {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
							err := RepublishHRNSRecords(ctx, repo.Datastore(), dht)
							if err != nil {
								ayame.Log.Errorf("failed to republish records: %v", err)
							}
						}
					}
				}()
				return nil
			},
		})
		return nil
	}
}

// The argument value is Record.Value
func checkHRNSRecord(value []byte) error {
	var namedValue pb.NamedValue
	if err := proto.Unmarshal(value, &namedValue); err != nil {
		return fmt.Errorf("failed to unmarshal named value: %v", err)
	}

	var pCert pb.PCert
	if err := proto.Unmarshal(namedValue.Pcert, &pCert); err != nil {
		return fmt.Errorf("failed to unmarshal pcert: %v", err)
	}

	_, va, vb, err := authority.ExtractCert(pCert.Cert)

	if err != nil {
		return fmt.Errorf("failed to extract cert: %v", err)
	}

	validAfter := time.Unix(va, 0)
	validBefore := time.Unix(vb, 0)

	// Check if record is valid
	now := time.Now()
	if now.Before(validBefore) && now.After(validAfter) {
		return nil
	}
	return fmt.Errorf("record is not valid")
}

func RepublishHRNSRecords(ctx context.Context, ds datastore.Datastore, dht *BSDHT) error {
	// Query all keys with prefix /hrns/
	q := query.Query{
		Prefix: "/hrns/",
	}

	results, err := ds.Query(ctx, q)
	if err != nil {
		return fmt.Errorf("failed to query datastore: %v", err)
	}
	defer results.Close()

	for r := range results.Next() {
		if r.Error != nil {
			ayame.Log.Errorf("error processing result: %v", r.Error)
			continue
		}

		var record pb.Record
		if err := proto.Unmarshal(r.Value, &record); err != nil {
			ayame.Log.Errorf("failed to unmarshal record: %v", err)
			continue
		}

		err := checkHRNSRecord(record.Value)
		if err == nil {
			ayame.Log.Infof("republishing record %s", r.Key)
			err := dht.PutNamedValue(ctx, r.Key, r.Value)
			if err != nil {
				ayame.Log.Errorf("failed to republish record %s: %v", r.Key, err)
			}
		} else {
			ayame.Log.Infof("record %s is not valid", r.Key)
			err := DeleteHRNSRecord(ctx, ds, r.Key)
			if err != nil {
				ayame.Log.Errorf("failed to delete record %s: %v", r.Key, err)
			}
		}
	}
	return nil
}

func DeleteHRNSRecord(ctx context.Context, ds datastore.Datastore, key string) error {
	if err := ds.Delete(ctx, datastore.NewKey(key)); err != nil {
		return fmt.Errorf("failed to delete record: %v", err)
	}
	return nil
}

// trust name
// byte-stream.abcde:id-1
// byzskip.org:id-2
// Suppose we have something like this.
// The prefix "by" is not unique. In this case, suppose we trusted "byte-stream.abcde:id-1".
// This means we trusted node id-1.
// If we force resolution with "by", it would return "byte-stream.abcde".
