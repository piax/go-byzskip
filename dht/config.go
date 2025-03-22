package dht

import (
	"context"
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/op/go-logging"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	bs "github.com/piax/go-byzskip/byzskip"
)

type Option func(*Config) error

type Config struct {
	bs.Config
	IdFinder        func(ctx context.Context, dht *BSDHT, id string) ([]*bs.BSNode, error)
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

const (
	PROTOCOL = "/ipfs/byzskip-dht/0.1.0"
)

func (c *Config) NewDHT(h host.Host) (*BSDHT, error) {
	ayame.InitLogger(logging.DEBUG) // set log level to error so that surpress messages.
	//key := ayame.NewIdKey(h.ID())
	assignedKey, name, mv, cert, err := c.Authorizer(h.ID())
	if err != nil {
		return nil, err
	}
	parent := p2p.New(h, assignedKey, name, mv, cert, ConvertMessage, c.AuthValidator, *c.VerifyIntegrity,
		*c.DetailedStatistics, PROTOCOL)

	// toplevel context
	ctx, cancel := context.WithCancel(context.Background())

	pm, err := providers.NewProviderManager(h.ID(), h.Peerstore(), c.Datastore)
	if err != nil {
		cancel()
		return nil, err
	}

	ret :=
		&BSDHT{
			ctx:      ctx,
			cancel:   cancel,
			IdFinder: c.IdFinder,
			Node: &bs.BSNode{
				BootstrapAddrs:     c.BootstrapAddrs,
				Parent:             parent,
				QuerySeen:          make(map[string]int),
				Procs:              make(map[string]*bs.RequestProcess),
				DisableFixLowPeers: false,
			},
			ProviderManager: pm,
			RecordValidator: c.RecordValidator,
			datastore:       c.Datastore,
		}
	ret.Node.SetKey(assignedKey)
	ret.Node.SetMV(mv)
	ret.Node.SetName(name)
	ayame.Log.Debugf("running key=%s, mv=%s, name=%s", assignedKey, mv, name)
	ret.Node.SetApp(ret)
	ret.Node.RoutingTable = c.RoutingTableMaker(ret.Node)
	parent.SetChild(ret.Node)
	ayame.Log.Infof("started BSDHT")
	return ret, nil
}

// options
func IdFinder(v func(ctx context.Context, dht *BSDHT, id string) ([]*bs.BSNode, error)) Option {
	return func(c *Config) error {
		c.IdFinder = v
		return nil
	}
}

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

type NameValidator struct {
	Name      string
	Validator record.Validator
}

func NamespacedValidator(vs []NameValidator) Option {
	return func(c *Config) error {
		c.RecordValidator = record.NamespacedValidator{}
		nsval, ok := c.RecordValidator.(record.NamespacedValidator)
		if !ok {
			return fmt.Errorf("can only add namespaced validators to a NamespacedValidator")
		}
		for _, v := range vs {
			nsval[v.Name] = v.Validator
		}
		return nil
	}
}

func RecordValidator(v record.Validator) Option {
	return func(c *Config) error {
		c.RecordValidator = v
		return nil
	}
}

// for IPFS
//func BSDHTOption(RoutingOptionArgs) (routing.Routing, error) {
//	return New(host, Datastore(dstore), RecordValidator(validator), BootstrapAddrs(BootstrapPeers...))
//}
