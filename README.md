# go-byzskip: ByzSkip Implementation in Go

This implementation uses [go-libp2p](https://github.com/libp2p/go-libp2p) for the P2P transport.

## Libraries

Our ByzSkip implementation codes are located in the byzskip directory.

* [byzskip](byzskip) - a library for ByzSkip 
* [ayame](ayame) - a real & simulation abstraction library
* [sim](sim) - simulators functions
* [authority](authority) - a library for authority functions

## Command Line Tools

* [authsrv](cmd/authsrv) - an authority server that issues a participation certificate according to the request
* [bssrv](cmd/bssrv) - a ByzSkip node with API/web server

## Simulators

The simulators related to ByzSkip.

* [bssim](simulators/bssim) - a ByzSkip simulator
* [kadsim](simulators/kadsim) - a S/Kademlia simulator
* [sgsim](simulators/sgsim) - a Skip Graph simulator

## Experimental

Some experimental codes.

* [visskip](simulators/visskip) - an experimental visualizer of byzskip simulations.
* [dht](dht) - an experimental IPFS-compatible DHT implementation using ByzSkip
* [dhtsrv](cmd/dhtsrv) - an experimental DHT node with API/web server
* [mobile](mobile) - an experimental gomobile app for mobile platforms (iOS/Android)

## Run as a router in IPFS/Kubo

There are a few steps required to run ByzSkip/DHT on IPFS. Here's what you need to do:

1. Obtain kubo source code
```bash
git clone https://github.com/ipfs/kubo
```
2. Apply the patch
```bash
cd kubo
patch -p1 < bsdht.diff
```
3. Update dependencies
```bash
go mod tidy
```
4. Build kubo binary
```bash
cd cmd/kubo
go build
```

## License

AGPLv3

If you need alternative license without source code disclosure obligation, feel free to contact us to discuss alternative licensing options.

## Acknowledgements

This work was partially supported by JSPS KAKENHI Grant Number JP23K28081.
