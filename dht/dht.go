package dht

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/piax/go-ayame/ayame"
	bs "github.com/piax/go-ayame/byzskip"
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
	strKey := ayame.StringKey(key.String()) // XXX ?
	go func(key ayame.StringKey) {
		clst := dht.node.Lookup(ctx, key)
		for _, n := range clst {
			ch <- peer.AddrInfo{ID: n.Id(), Addrs: n.Addrs()}
		}
	}(strKey)
	return ch
}

func (dht *BSDHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	strKey := ayame.StringKey(id)
	clst := dht.node.Lookup(ctx, strKey)
	for _, n := range clst {
		if n.Key().Equals(strKey) {
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
