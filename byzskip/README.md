# byzskip

A ByzSkip implementation on go-libp2p

## Install

```sh
$ go get github.com/piax/go-byzskip/byzskip
```

## Example

```go
numberOfPeers := 32
peers := make([]*BSNode, numberOfPeers)

h, _ := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/udp/9000/quic"))
peers[0], _ = New(h)
introducer := fmt.Sprintf("/ip4/127.0.0.1/udp/9000/quic/p2p/%s", peers[0].Id())
peers[0].RunBootstrap(context.Background())
for i := 1; i < numberOfPeers; i++ {
    h, _ := libp2p.New(libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/udp/%d/quic", 9000+i)))
    peers[i], _ = New(h, Bootstrap(introducer))
    peers[i].Join(context.Background())
    peers[i].SetMessageReceiver(func(node *BSNode, ev *BSUnicastEvent) {
        fmt.Printf("%s: received '%s'\n", node.Key(), string(ev.Payload))
    })
}
result, _ := peers[2].Lookup(context.Background(), ayame.NewStringIdKey("hello"))
for _, r := range result {
    fmt.Printf("found %s\n", r.Key())
}
peers[1].Unicast(context.Background(), peers[1].Key(), peers[1].NewMessageId(), []byte("hello world"))
time.Sleep(time.Duration(100) * time.Millisecond)
```

## License

MIT

