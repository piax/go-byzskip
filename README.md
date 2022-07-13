# go-byzskip
A Go implementation of ByzSkip by Go language

## Libraries

Our ByzSkip implementation codes are located in the byzskip directory.
It uses ayame framework, which provides an abstract layer of real-p2p execution and discrete event simulation.

* byzskip - ByzSkip core modules
* ayame - a simulation / execution integration framework

## Command Line Tools

* authsrv - an authority server that issues the participation certificate according to the request
* bssrv - a ByzSkip node with API/web server

## Simulators

The simulators related to ByzSkip.

* bssim - a ByzSkip simulator
* kadsim - a S/Kademlia simulator
* sgsim - a Skip Graph simulator
* keysim - a key issuer simulator

##  Applications (Experimental)

* dht - an experimental DHT implementation using ByzSkip
* mobile - an experimental app for mobile platforms (iOS/Android)
