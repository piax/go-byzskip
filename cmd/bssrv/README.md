# bssrv
An experimental code to run a ByzSkip node.

It runs as a ByzSkip node with an integer key.
The aim of this implementation is to measure the performance of ByzSkip in real networks.

## Web API

It provides an Web API to operate ByzSkip nodes.
By default, the Web API server runs on http://localhost:8000

The provided interfaces are as follows:

### ``GET /stat``

The server shows the traffic statistics.

### ``GET /tab``

The server shows the routing table entries.

### ``GET /uni``

The server issues a unicast message. It equires ``key`` parameter.

For example, the following curl command runs a unicast to the k-closest nodes with key ``1``.

```sh
$ curl http://localhost:8000/uni?key=1
```

The response shows the latency and hops to receive the responses (k-closest nodes) for the specified key. The following shows an example of the result with ``k=4``:

```
13.010000 (ms) 1 (hops) 2 (=key)
81.996000 (ms) 1 (hops) 0 (=key)
119.388000 (ms) 1 (hops) 3 (=key)
158.534000 (ms) 1 (hops) 1 (=key)
```

## Usage

```
bssrv [OPTIONS]
  -apub string
    	the public key of the authority (default "BABBE...")
  -auth string
    	the authenticator web URL (default "http://localhost:7001")
  -b	runs as the bootstrap node
  -i string
    	introducer addr (ignored when bootstrap) (default "/ip4/127.0.0.1/udp/9000/quic/p2p/...")
  -k int
    	the parameter k (default 4)
  -key int
    	key (integer)
  -ks string
    	the path name of the keystore (default "keystore")
  -p int
    	the peer-to-peer port to listen to (default 9000)
  -s int
    	the web api server port to listen to (default 8000)
  -t	use TCP
  -v	verbose output
```
The bssrv server generates its key pair when it starts running.

The first bssrv server needs to be run as the 'bootstrap' node (with ``-b`` option). With ``-b`` option, the public key is stored to the keystore so that the bootstrap node can restart with the same identifier. If you want to refresh the key pair, remove the keystore file.

The second or later bssrv servers need to specify the address of the bootstrap node as an introducer address (``-i`` option). The address needs to be specified as a p2p multiaddr. The multiaddr (listen address) of the introducer is printed to the stdout when the introducer is started.

In all bssrv servers, the public key of authority server is required to be specified (``-apub`` option). The public key string (base32 encoded string) is printed to the stdout when the authsrv is started.

By default, the bssrv uses QUIC transport. If you specify ``-t`` option, it uses TCP transport.

## An example to run multiple bssrv servers on a machine

### 1. Run authsrv

First, build an authsrv binary.

```sh
$ cd cmd/authsrv 
$ go build
```

Then, the following runs an authsrv server.

```sh
$ ./authsrv
```

An example output:

```sh
authority publickey: XXXXX
```


where ``XXXXX`` is the authority public key string.

### 2. Run a bootstrap node

(The followigs need to be run on another terminal.)

To build a bssrv binary, run as follows:

```sh
$ cd ../bssrv 
$ go build
```

The command line to start a bootstrap node is as follows:

```sh
$ ./bssrv -b -apub XXXXX
```

An example output:

```sh
authority url: http://localhost:7001
authority pub: XXXXX
listen address: /ip4/ss.tt.uu.vv/udp/9000/quic/p2p/YYYYY
listen address: /ip4/127.0.0.1/udp/9000/quic/p2p/YYYYY
web port: 8000
```
The public key string ("XXXX" in above) is the string printed by authsrv.
By default, the key=0 is used.

### 3. Run nodes

(The followigs need to be run on another terminal.)

The second or later nodes can be started as follows:

```sh
$ ./bssrv -i /ip4/ss.tt.uu.vv/udp/9000/quic/p2p/YYYYY -apub XXXXX -p 9001 -s 8001 -key 1
```

The listen port for p2p protocol (``-p`` option), web api port (``-s`` option) needs to be changed to the number other than the ports used in the bootstrap node when multiple nodes are started on the same machine.

The key (``-key`` option) must be unique for each node.

An example output:

```sh
authority url: http://localhost:7001
authority pub: XXXXX
listen address: /ip4/ww.xx.yy.zz/udp/9001/quic/p2p/ZZZZZ
listen address: /ip4/127.0.0.1/udp/9001/quic/p2p/ZZZZZ
web port: 8001
```



