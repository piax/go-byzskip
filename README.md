# go-byzskip
An implementation of ByzSkip by Go language.
This implementation uses [go-libp2p](https://github.com/libp2p/go-libp2p) for the P2P transport.

## Libraries

Our ByzSkip implementation codes are located in the byzskip directory.

* byzskip - ByzSkip modules
* ayame - real&sim abstract execution modules

## Command Line Tools

* [authsrv](cmd/authserv) - an authority server that issues a participation certificate according to the request
* [bssrv](cmd/bssrv) - a ByzSkip node with API/web server

## Simulators

The simulators related to ByzSkip.

* [bssim](simulators/bssim) - a ByzSkip simulator
* [kadsim](simulators/bssim) - a S/Kademlia simulator
* [sgsim](simulators/sgsim) - a Skip Graph simulator
* [keysim](simulators/keysim) - a key issuer simulator

##  Applications (Experimental)

* [dht](dht) - an experimental DHT implementation using ByzSkip
* [mobile](mobile) - an experimental gomobile app for mobile platforms (iOS/Android)
