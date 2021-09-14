module github.com/piax/go-ayame/keysim

go 1.16

replace github.com/piax/go-ayame/ayame => ../ayame

replace github.com/piax/go-ayame/key_issuer => ../key_issuer

require (
	github.com/op/go-logging v0.0.0-20160315200505-970db520ece7
	github.com/piax/go-ayame/ayame v0.0.0-20210726025924-b388c039ea51
	github.com/piax/go-ayame/key_issuer v0.0.0-00010101000000-000000000000
	github.com/stretchr/testify v1.7.0
	golang.org/x/exp v0.0.0-20210722180016-6781d3edade3
	gonum.org/v1/gonum v0.9.3
)
