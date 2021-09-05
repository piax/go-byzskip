module github.com/piax/go-ayame/bssim

go 1.16

replace github.com/piax/go-ayame/byzskip => ../byzskip

replace github.com/piax/go-ayame/ayame => ../ayame

replace github.com/piax/go-ayame/ayame/p2p => ../ayame/p2p

replace github.com/piax/go-ayame/key_issuer => ../key_issuer

replace github.com/piax/go-ayame/authority => ../authority

require (
	github.com/montanaflynn/stats v0.6.6
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/piax/go-ayame/ayame v0.0.0-20210804044908-86894c3be2c5
	github.com/piax/go-ayame/byzskip v0.0.0-00010101000000-000000000000
	github.com/piax/go-ayame/key_issuer v0.0.0-20210804044908-86894c3be2c5
	github.com/stretchr/testify v1.7.0
	github.com/thoas/go-funk v0.9.0
)
