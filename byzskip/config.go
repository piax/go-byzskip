package byzskip

import (
	"fmt"
	"runtime"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/config"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
)

type Option func(*Config) error

type Config struct {
	config.Config
	Key               ayame.Key
	Authorizer        func(peer.ID, ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error)
	AuthValidator     func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool
	RoutingTableMaker func(KeyMV) RoutingTable
	IsFailure         *bool
	VerifyIntegrity   *bool
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

func traceError(err error, skip int) error {
	if err == nil {
		return nil
	}
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return err
	}
	return fmt.Errorf("%s:%d: %s", file, line, err)
}

func (c *Config) NewNode(h host.Host) (*BSNode, error) {
	assignedKey, mv, cert, err := c.Authorizer(h.ID(), c.Key)
	if err != nil {
		return nil, err
	}
	parent := p2p.New(h, assignedKey, mv, cert, ConvertMessage, c.AuthValidator, *c.VerifyIntegrity)

	ret := &BSNode{key: assignedKey, mv: mv,
		Parent:             parent,
		QuerySeen:          make(map[string]int),
		Procs:              make(map[string]*RequestProcess),
		DisableFixLowPeers: false,
	}
	ret.RoutingTable = c.RoutingTableMaker(ret)
	parent.SetChild(ret)
	return ret, nil
}
