module github.com/piax/go-ayame/bssim

go 1.16

replace github.com/piax/go-ayame/byzskip => ../byzskip
replace github.com/piax/go-ayame/ayame => ../ayame

require (
	github.com/montanaflynn/stats v0.6.6
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/piax/go-ayame/ayame v0.0.0-20210712013552-37e0882bd092
	github.com/piax/go-ayame/byzskip v0.0.0-20210711140148-311c2c956e93
	github.com/thoas/go-funk v0.9.0
)
