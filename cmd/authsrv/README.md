# authsrv
An experimental authority server.

It provides an Web API that issues perticipation certificates for ByzSkip nodes.

The only interface is ``GET /issue``, which accepts the following URL parameters:

* id: the base58-encoded string representation of the peer ID in libp2p host.
* key: the base32-encoded string representation of the serialized ayame.Key.

Note that this authority implementation is a Proof-of-Concept.
Currently it does not authenticate users which should be implemented to prevent Sybil attacks.

## Usage

```
authsrv [OPTIONS]
  -p int
    	the port to listen to (default 7001)
  -s string
    	the filepath for the keystore (default "keystore")
  -v	verbose output
```

The authsrv generates its key pair at first time (when the keystore exists).
The authsrv reads its key pair from its keystore after the second time.
If you need to refresh the key pair, remove the keystore file.

After successfully started, authsrv prints the following line.

```
authority publickey: BABBEIIDM7V3FR4RWNGVXYRSHOCL6SYWLNIJLP4ONDG...
````

This public key needs to be specified as the authority's publickey in ByzSkip nodes. You can copy and paste the displayed string.