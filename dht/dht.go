package dht

import (
	"context"

	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
)

// assertion
var (
	_ routing.ContentRouting = (*BSDHT)(nil)
	_ routing.Routing        = (*BSDHT)(nil)
	_ routing.PeerRouting    = (*BSDHT)(nil)
	_ routing.PubKeyFetcher  = (*BSDHT)(nil)
	_ routing.ValueStore     = (*BSDHT)(nil)
)

type BSDHT struct {
	node *bs.BSNode

	Validator       record.Validator
	ProviderManager *providers.ProviderManager
	datastore       ds.Datastore
}

func New(ctx context.Context, node *bs.BSNode) (*BSDHT, error) {
	return &BSDHT{node: node}, nil
}

// PutValue
func (dht *BSDHT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	return routing.ErrNotSupported
}

// GetValue
func (dht *BSDHT) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	return nil, routing.ErrNotFound
}

// SearchValue
func (dht *BSDHT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	return nil, routing.ErrNotFound
}

// Provide
func (dht *BSDHT) Provide(ctx context.Context, key cid.Cid, brdcst bool) error {
	return routing.ErrNotSupported
}

// FindProvidersAsync
func (dht *BSDHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	keyMH := key.Hash()
	idKey := ayame.IdKey(keyMH)
	go func(key ayame.IdKey) {
		clst := dht.node.Lookup(ctx, key)
		for _, n := range clst {
			ch <- peer.AddrInfo{ID: n.Id(), Addrs: n.Addrs()}
		}
	}(idKey)
	return ch
}

func (dht *BSDHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}
	idkey := ayame.IdKey(id)
	clst := dht.node.Lookup(ctx, idkey)
	for _, n := range clst {
		if n.Key().Equals(idkey) {
			return peer.AddrInfo{ID: n.Id(), Addrs: n.Addrs()}, nil
		}
	}
	return peer.AddrInfo{}, routing.ErrNotFound
}

// don't know what it is
func (dht *BSDHT) Bootstrap(context.Context) error {
	return nil
}

func (dht *BSDHT) Close() error {
	return dht.node.Close()
}

func (dht *BSDHT) GetPublicKey(context.Context, peer.ID) (crypto.PubKey, error) {
	return nil, nil
}
