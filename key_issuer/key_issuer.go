package key_issuer

import (
	"math/rand"

	"github.com/piax/go-byzskip/ayame"
)

type KeyIssuer interface {
	GetKey(logicalKey ayame.Key) ayame.Key
}

type RandomKeyIssuer struct{}

func NewRandomKeyIssuer() *RandomKeyIssuer {
	return &RandomKeyIssuer{}
}

func (ki *RandomKeyIssuer) GetKey(logicalKey ayame.Key) ayame.Key {
	return ayame.FloatKey(rand.Float64())
}

type AsIsKeyIssuer struct{}

func NewAsIsKeyIssuer() *AsIsKeyIssuer {
	return &AsIsKeyIssuer{}
}

func (ki *AsIsKeyIssuer) GetKey(logicalKey ayame.Key) ayame.Key {
	return logicalKey
}

func NewKeyIssuer(issuerType string, seed int64, poolSize int) KeyIssuer {
	switch issuerType {
	case "random":
		return NewRandomKeyIssuer()
	case "asis":
		return NewAsIsKeyIssuer()
	default:
		return NewShuffleKeyIssuer(seed, poolSize)
	}
}
