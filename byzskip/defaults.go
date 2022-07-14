package byzskip

// This file contains all the default configuration options.

import (
	"github.com/piax/go-byzskip/ayame"

	"github.com/libp2p/go-libp2p-core/peer"
)

var DefaultAuthorizer = func(cfg *Config) error {
	return cfg.Apply(Authorizer(func(pid peer.ID, key ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error) {
		// given key is ignored.
		return ayame.IdKey(pid), ayame.NewMembershipVector(2), nil, nil // alpha=2
	}))
}

var DefaultAuthValidator = func(cfg *Config) error {
	return cfg.Apply(AuthValidator(func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool {
		return true
	}))
}

// Complete list of default options and when to fallback on them.
//
// Please *DON'T* specify default options any other way. Putting this all here
// makes tracking defaults *much* easier.
var defaults = []struct {
	fallback func(cfg *Config) bool
	opt      Option
}{
	{
		fallback: func(cfg *Config) bool { return cfg.RoutingTableMaker == nil },
		opt:      RoutingTableMaker(NewSkipRoutingTable),
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
		fallback: func(cfg *Config) bool { return cfg.VerifyIntegrity == nil },
		opt:      VerifyIntegrity(true),
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
