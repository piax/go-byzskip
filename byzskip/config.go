package byzskip

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
)

type Option func(*Config) error

type Config struct {
	Key                ayame.Key
	BootstrapAddrs     []peer.AddrInfo
	RedundancyFactor   *int // the parameter 'k'
	Authorizer         func(peer.ID, ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error)
	AuthValidator      func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool
	RoutingTableMaker  func(KeyMV) RoutingTable
	IsFailure          *bool
	VerifyIntegrity    *bool
	DetailedStatistics *bool
	DisableFixLowPeers *bool
}

// Apply applies the given options to this Option
func (c *Config) Apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return nil
}

func ChainOptions(opts ...Option) Option {
	return func(cfg *Config) error {
		for _, opt := range opts {
			if opt == nil {
				continue
			}
			if err := opt(cfg); err != nil {
				return err
			}
		}
		return nil
	}
}

const (
	PROTOCOL = "/byzskip/0.1.0"
)

func (c *Config) NewNode(h host.Host) (*BSNode, error) {
	var assignedKey ayame.Key
	var mv *ayame.MembershipVector
	var cert []byte
	var err error

	if c.Authorizer != nil {
		assignedKey, mv, cert, err = c.Authorizer(h.ID(), c.Key)
		if err != nil {
			return nil, err
		}
	} else {
		// skipping authorizations.
		assignedKey = c.Key
		mv = ayame.NewMembershipVector(2)
		cert = nil
		err = nil
	}

	parent := p2p.New(h, assignedKey, mv, cert, ConvertMessage, c.AuthValidator, *c.VerifyIntegrity,
		*c.DetailedStatistics, PROTOCOL)

	ret := &BSNode{key: assignedKey, mv: mv,
		BootstrapAddrs:     c.BootstrapAddrs,
		Parent:             parent,
		QuerySeen:          make(map[string]int),
		Procs:              make(map[string]*RequestProcess),
		DisableFixLowPeers: *c.DisableFixLowPeers,
	}
	ret.RoutingTable = c.RoutingTableMaker(ret)
	ret.EventForwarder = NodeEventForwarder
	parent.SetChild(ret)
	return ret, nil
}
