module github.com/piax/go-ayame/byzskip

go 1.16

replace github.com/piax/go-ayame/ayame => ../ayame

replace github.com/piax/go-ayame/authority => ../authority

require (
	github.com/jbenet/goprocess v0.1.4 // indirect
	github.com/libp2p/go-libp2p v0.14.5-0.20210804000418-989fba5d3203 // indirect
	github.com/libp2p/go-libp2p-core v0.9.0
	github.com/libp2p/go-libp2p-noise v0.2.2 // indirect
	github.com/libp2p/go-libp2p-peerstore v0.2.8 // indirect
	github.com/libp2p/go-libp2p-quic-transport v0.11.2 // indirect
	github.com/multiformats/go-multiaddr v0.4.0
	github.com/piax/go-ayame/authority v0.0.0-00010101000000-000000000000
	github.com/piax/go-ayame/ayame v0.0.0-20210804044908-86894c3be2c5
	github.com/stretchr/testify v1.7.0
	github.com/thoas/go-funk v0.9.0
)
