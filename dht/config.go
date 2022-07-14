package dht

import (
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	bs "github.com/piax/go-byzskip/byzskip"
)

type Option func(*Config) error

type Config struct {
	bs.Config
	Datastore       ds.Batching
	RecordValidator record.Validator
	MaxRecordAge    *time.Duration
}

type GConfig interface {
	bs.Config | Config
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

func (c *Config) NewDHT(h host.Host) (*BSDHT, error) {
	key := ayame.NewIdKey(h.ID())
	assignedKey, mv, cert, err := c.Authorizer(h.ID(), key)
	if err != nil {
		return nil, err
	}
	parent := p2p.New(h, key, mv, cert, ConvertMessage, c.AuthValidator, *c.VerifyIntegrity)

	ret :=
		&BSDHT{
			node: &bs.BSNode{
				Parent:             parent,
				QuerySeen:          make(map[string]int),
				Procs:              make(map[string]*bs.RequestProcess),
				DisableFixLowPeers: false,
			},
			RecordValidator: c.RecordValidator,
			datastore:       c.Datastore,
		}
	ret.node.SetKey(assignedKey)
	ret.node.SetMV(mv)
	ret.node.SetApp(ret)
	ret.node.RoutingTable = c.RoutingTableMaker(ret.node)
	parent.SetChild(ret.node)
	return ret, nil
}

// options

func Datastore(v ds.Batching) Option {
	return func(c *Config) error {
		c.Datastore = v
		return nil
	}
}

func MaxRecordAge(v time.Duration) Option {
	return func(c *Config) error {
		c.MaxRecordAge = &v
		return nil
	}
}

func NamespacedValidator(ns string, v record.Validator) Option {
	return func(c *Config) error {
		c.RecordValidator = record.NamespacedValidator{}
		nsval, ok := c.RecordValidator.(record.NamespacedValidator)
		if !ok {
			return fmt.Errorf("can only add namespaced validators to a NamespacedValidator")
		}
		nsval[ns] = v
		return nil
	}
}
