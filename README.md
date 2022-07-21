# go-byzskip
An implementation of ByzSkip by Go language.
This implementation uses [go-libp2p](https://github.com/libp2p/go-libp2p) for the P2P transport.

## Libraries

Our ByzSkip implementation codes are located in the byzskip directory.

* byzskip - ByzSkip modules
* ayame - real&sim abstract execution modules

## Command Line Tools

* [authsrv](cmd/authsrv) - an authority server that issues a participation certificate according to the request
* [bssrv](cmd/bssrv) - a ByzSkip node with API/web server

## Simulators

The simulators related to ByzSkip.

* [bssim](simulators/bssim) - a ByzSkip simulator
* [kadsim](simulators/bssim) - a S/Kademlia simulator
* [sgsim](simulators/sgsim) - a Skip Graph simulator

## Experimental

Some experimental codes.

* [dht](dht) - an experimental DHT implementation using ByzSkip
* [bssrv](cmd/dhtsrv) - an experimental DHT node with API/web server
* [mobile](mobile) - an experimental gomobile app for mobile platforms (iOS/Android)
* [keysim](simulators/keysim) - an experimental code to simulate shuffled keys

## Requirements

Go 1.18 or higher

## License

MIT License

## Acknowledgements

This work was partially supported by JSPS KAKENHI Grant Number JP20H04186.