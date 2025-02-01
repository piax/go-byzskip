package byzskip

import (
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/piax/go-byzskip/ayame"
)

func RedundancyFactor(k int) Option {
	return func(cfg *Config) error {
		cfg.RedundancyFactor = &k
		InitK(k)
		return nil
	}
}

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

func RoutingTableMaker(rtMaker func(KeyMV) RoutingTable) Option {
	return func(c *Config) error {
		c.RoutingTableMaker = rtMaker
		return nil
	}
}

func SimulateFailure(failure bool) Option {
	return func(c *Config) error {
		c.IsFailure = &failure
		return nil
	}
}

func VerifyIntegrity(verify bool) Option {
	return func(c *Config) error {
		c.VerifyIntegrity = &verify
		return nil
	}
}

func DetailedStatistics(enable bool) Option {
	return func(c *Config) error {
		c.DetailedStatistics = &enable
		return nil
	}
}

func DisableFixLowPeers(enable bool) Option {
	return func(c *Config) error {
		c.DisableFixLowPeers = &enable
		return nil
	}
}

// desired key
func Key(key ayame.Key) Option {
	return func(c *Config) error {
		c.Key = key
		return nil
	}
}

func Bootstrap(bns ...string) Option {
	return func(c *Config) error {
		infos := []peer.AddrInfo{}
		for _, bn := range bns {
			maddr, err := multiaddr.NewMultiaddr(bn)
			if err != nil {
				return err
			}
			// Extract the peer ID from the multiaddr.
			info, err := peer.AddrInfoFromP2pAddr(maddr)
			if err != nil {
				return err
			}
			infos = append(infos, *info)
		}
		c.BootstrapAddrs = infos
		return nil
	}
}

func BootstrapAddrs(infos ...peer.AddrInfo) Option {
	return func(c *Config) error {
		c.BootstrapAddrs = infos
		return nil
	}
}
