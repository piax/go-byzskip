package dht

// This file contains all the default configuration options.

import (
	"fmt"
	"os"
	"os/user"
	"strings"
	"time"

	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/ipfs/boxo/ipns"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
)

func expandPath(path string) string {
	if strings.HasPrefix(path, "~") {
		currentUser, err := user.Current()
		if err != nil {
			return path
		}
		return strings.Replace(path, "~", currentUser.HomeDir, 1)
	}
	return path
}

var DefaultAuthorizer = func(cfg *Config) error {
	if filename := os.Getenv(authority.CERT_FILE); len(filename) != 0 { // if environment variable is set, use cert file.
		// file exists
		filename = expandPath(filename)
		if _, err := os.Stat(filename); err == nil {
			log.Infof("using %s as the cert file", filename)
			return cfg.Apply(Authorizer(authority.FileAuthAuthorizer(filename)))
		} else {
			if *cfg.VerifyIntegrity {
				panic(fmt.Errorf("failed to determine authorization method: %w", err))
			}
		}
	} else {
		if *cfg.VerifyIntegrity {
			panic(fmt.Sprintf("failed to determine authorization method: please check %s", authority.CERT_FILE))
		}
	}
	// default is to use the identity key and empty name.
	log.Infof("using empty authority")
	return cfg.Apply(Authorizer(func(pid peer.ID) (ayame.Key, string, *ayame.MembershipVector, []byte, error) {
		// given key is ignored.
		return ayame.IdKey(pid), "", ayame.NewMembershipVector(2), nil, nil // alpha=2
	}))
}

var DefaultAuthValidator = func(cfg *Config) error {
	if pk := os.Getenv(authority.AUTH_PUBKEY); len(pk) != 0 { // if environment variable is set, use the publickey
		log.Infof("using authority public key: %s", pk)
		return cfg.Apply(AuthValidator(authority.AuthValidator(pk)))
	}
	return cfg.Apply(AuthValidator(func(peer.ID, ayame.Key, string, *ayame.MembershipVector, []byte) bool {
		return true
	}))
}

var DefaultRedundancyFactor = func(cfg *Config) error {
	return cfg.Apply(RedundancyFactor(4))
}

type BlankValidator struct{}

func (BlankValidator) Validate(_ string, _ []byte) error        { return nil }
func (BlankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

// Complete list of default options and when to fallback on them.
//
// Please *DON'T* specify default options any other way. Putting this all here
// makes tracking defaults *much* easier.
var defaults = []struct {
	fallback func(cfg *Config) bool
	opt      Option
}{
	{
		fallback: func(cfg *Config) bool { return cfg.RedundancyFactor == nil },
		opt:      DefaultRedundancyFactor,
	},
	{
		fallback: func(cfg *Config) bool { return cfg.RoutingTableMaker == nil },
		opt:      RoutingTableMaker(bs.NewSkipRoutingTable),
	},
	{
		fallback: func(cfg *Config) bool { return cfg.VerifyIntegrity == nil },
		opt:      VerifyIntegrity(true),
	},
	{
		fallback: func(cfg *Config) bool { return cfg.Authorizer == nil },
		opt:      DefaultAuthorizer,
	},
	{
		fallback: func(cfg *Config) bool { return cfg.AuthValidator == nil },
		opt:      DefaultAuthValidator,
	},
	{
		fallback: func(cfg *Config) bool { return cfg.IdFinder == nil },
		opt:      IdFinder(MVIdFinder),
	},
	{
		fallback: func(cfg *Config) bool { return cfg.Datastore == nil },
		opt:      Datastore(dssync.MutexWrap(ds.NewMapDatastore())),
	},
	{
		fallback: func(cfg *Config) bool { return cfg.RecordValidator == nil },
		opt: NamespacedValidator([]NameValidator{
			{Name: "pk", Validator: record.PublicKeyValidator{}},
			{Name: "ipns", Validator: ipns.Validator{KeyBook: nil}},
			{Name: "hrns", Validator: NamedValueValidator{}},
			{Name: "v", Validator: BlankValidator{}},
		}),
	},
	{
		fallback: func(cfg *Config) bool { return cfg.MaxRecordAge == nil },
		opt:      MaxRecordAge(time.Hour * 36),
	},
	{
		fallback: func(cfg *Config) bool { return cfg.DetailedStatistics == nil },
		opt:      DetailedStatistics(false),
	},
	{
		fallback: func(cfg *Config) bool { return cfg.DisableFixLowPeers == nil },
		opt:      DisableFixLowPeers(false),
	},
	{
		fallback: func(cfg *Config) bool { return cfg.FixLowPeersInterval == 0 },
		opt:      FixLowPeersInterval(1 * time.Minute),
	},
}

// Defaults configures libp2p to use the default options. Can be combined with
// other options to *extend* the default options.
var Defaults Option = func(cfg *Config) error {
	for _, def := range defaults {
		if err := cfg.Apply(def.opt); err != nil {
			return err
		}
	}
	return nil
}

// FallbackDefaults applies default options to the libp2p node if and only if no
// other relevant options have been applied. will be appended to the options
// passed into New.
var FallbackDefaults Option = func(cfg *Config) error {
	for _, def := range defaults {
		if !def.fallback(cfg) {
			continue
		}
		if err := cfg.Apply(def.opt); err != nil {
			return err
		}
	}
	return nil
}
