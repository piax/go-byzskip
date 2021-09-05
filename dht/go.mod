module github.com/piax/go-ayame/dht

go 1.16

replace github.com/piax/go-ayame/byzskip => ../byzskip

replace github.com/piax/go-ayame/ayame => ../ayame

replace github.com/piax/go-ayame/authority => ../authority

require (
	github.com/ipfs/go-cid v0.1.0
	github.com/libp2p/go-libp2p-core v0.9.0
	github.com/piax/go-ayame/ayame v0.0.0-20210804044908-86894c3be2c5
	github.com/piax/go-ayame/byzskip v0.0.0-00010101000000-000000000000
)
