package dht

import (
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
)

// copied from byzskip to inherit all options.

func Authorizer(authorizer func(peer.ID, ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error)) Option {
	return func(c *Config) error {
		c.Authorizer = authorizer
		return nil
	}
}

func AuthValidator(validator func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool) Option {
	return func(c *Config) error {
		c.AuthValidator = validator
		return nil
	}
}

func RoutingTableMaker(rtMaker func(bs.KeyMV) bs.RoutingTable) Option {
	return func(c *Config) error {
		c.RoutingTableMaker = rtMaker
		return nil
	}
}

func SimulateFailure(failure bool) Option {
	return func(c *Config) error {
		*c.IsFailure = failure
		return nil
	}
}

func VerifyIntegrity(verify bool) Option {
	return func(c *Config) error {
		c.VerifyIntegrity = &verify
		return nil
	}
}
