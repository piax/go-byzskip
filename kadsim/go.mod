module github.com/piax/go-ayame/kadsim

go 1.16

replace github.com/piax/go-ayame/ayame => ../ayame

require (
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/libp2p/go-libp2p-core v0.8.5
	github.com/libp2p/go-libp2p-kbucket v0.4.7
	github.com/libp2p/go-libp2p-peerstore v0.2.7
	github.com/montanaflynn/stats v0.6.6
	github.com/multiformats/go-multihash v0.0.15
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/piax/go-ayame/ayame v0.0.0-20210708080844-81a48d3ce309
	github.com/thoas/go-funk v0.8.0
	golang.org/x/crypto v0.0.0-20210513164829-c07d793c2f9a // indirect
)
