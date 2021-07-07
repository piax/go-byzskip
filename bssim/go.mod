module github.com/piax/go-ayame/bssim

go 1.16

replace github.com/piax/go-ayame/ayame v0.0.0-local => ../ayame

replace github.com/piax/go-ayame/byzskip v0.0.0-local => ../byzskip

require (
	github.com/montanaflynn/stats v0.6.6
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/piax/go-ayame/ayame v0.0.0-local
	github.com/piax/go-ayame/byzskip v0.0.0-local
	github.com/thoas/go-funk v0.8.0
)
