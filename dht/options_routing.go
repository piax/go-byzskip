package dht

import (
	"github.com/libp2p/go-libp2p/core/routing"
)

type QuorumOptionKey struct{}

// Quorum is a DHT option that tells the DHT how many peers it needs to get
// values from before returning the best one. Zero means the DHT query
// should complete instead of returning early.
//
// Default: 0
func Quorum(n int) routing.Option {
	return func(opts *routing.Options) error {
		if opts.Other == nil {
			opts.Other = make(map[interface{}]interface{}, 1)
		}
		opts.Other[QuorumOptionKey{}] = n
		return nil
	}
}

const defaultQuorum = 0

func GetQuorum(opts *routing.Options) int {
	responsesNeeded, ok := opts.Other[QuorumOptionKey{}].(int)
	if !ok {
		responsesNeeded = defaultQuorum
	}
	return responsesNeeded
}
