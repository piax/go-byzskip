# go-byzskip: A ByzSkip Implementation in Go

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

## IPFS/Kubo

There's an experimental-version of kubo which applies ByzSkip-DHT.
See [https://github.com/teranisi/kubo/tree/bsdht](https://github.com/teranisi/kubo/tree/bsdht)
You need  us if you want to use this.


## License

AGPLv3

If you need alternative license without source code disclosure obligation, feel free to contact us to discuss alternative licensing options.

## Acknowledgements

This work was partially supported by JSPS KAKENHI Grant Number JP23K28081.

## Referring to the ByzSkip
If you have used ByzSkip in your research, please use the [INFOCOM paper](https://ieeexplore.ieee.org/document/11044766) as the reference.

```
@INPROCEEDINGS{11044766,
  author={Teranishi, Yuuichi and Akiyama, Toyokazu and Abe, Kota},
  booktitle={IEEE INFOCOM 2025 - IEEE Conference on Computer Communications}, 
  title={ByzSkip - A Byzantine-Resilient Skip Graph}, 
  year={2025},
  volume={},
  number={},
  pages={1-10},
  keywords={Fault tolerance;Analytical models;Overlay networks;Multicast algorithms;Upper bound;Pollution;Fault tolerant systems;Routing;Search problems;Resilience;P2P networks;key-order preserving structured overlay network;Byzantine fault tolerance},
  doi={10.1109/INFOCOM55648.2025.11044766}}
```
