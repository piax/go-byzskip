package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/op/go-logging"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	bs "github.com/piax/go-byzskip/byzskip"
)

var verbose *bool
var bootstrap *bool
var key *int
var k *int
var iAddr *string

//var addr *string
var port *int
var srvPort *int
var keystore *string
var authURL *string
var pubKeyString *string

var node *bs.BSNode

var pubKey ci.PubKey
var tcp *bool
var resMap map[string]chan *bs.BSUnicastResEvent

var sem sync.Mutex

/*var joinElapsed int64
var joinInBytes int64
var joinOutBytes int64
var joinMsgs int64*/

var joinMsgs int64
var joinBytes int64

const (
	UNICAST_RESPONSE_TIMEOUT = 4000 // milliseconds
)

func nodeTable(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "%s\n", node.RoutingTable)
}

func nodeStat(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "--key: %d\n", *key)
	/*	fmt.Fprintf(w, "join-elapsed: %f\n", float64(joinElapsed)/1000.0)
		fmt.Fprintf(w, "join-out-bytes: %d\n", joinOutBytes)*/
	fmt.Fprintf(w, "join-msgs: %d\n", joinMsgs)
	fmt.Fprintf(w, "join-traffic: %d\n", joinBytes)
	fmt.Fprintf(w, "recv-msgs: %d\n", node.Parent.(*p2p.P2PNode).InCount)
	fmt.Fprintf(w, "recv-bytes: %d\n", node.Parent.(*p2p.P2PNode).InBytes)
	fmt.Fprintf(w, "out-bytes: %d\n", node.Parent.(*p2p.P2PNode).OutBytes)
	fmt.Fprintf(w, "conn-stats: %v\n", node.Parent.(*p2p.P2PNode).ConnManager().(*connmgr.BasicConnMgr).GetInfo())
	if p2p.RECORD_BYTES_PER_KEY {
		for k, v := range node.Parent.(*p2p.P2PNode).KeyInBytes {
			fmt.Fprintf(w, "key-bytes: %s %d\n", k, v)
		}
	}
}

func nodeUnicast(w http.ResponseWriter, req *http.Request) {
	path := req.URL.Path
	if strings.HasPrefix(path, "/uni") {
		vals := req.URL.Query()
		/*i, err := strconv.Atoi(vals["key"][0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		node.Unicast(context.Background(), ayame.IntKey(i), []byte("hello"))
		fmt.Fprintf(w, "unicasted to %s\n", vals["key"][0])*/
		sem.Lock() // only one search runs simultaneously
		//resCh = make(chan *byzskip.BSUnicastResEvent, 1) // XXX reset

		uniCtx, cancel := context.WithTimeout(context.Background(), time.Duration(UNICAST_RESPONSE_TIMEOUT)*time.Millisecond)
		defer cancel()
		//ayame.Log.Infof("unicasting: %s to %s", vals["body"][0], vals["key"][0])
		i, err := strconv.Atoi(vals["key"][0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		} else {
			start := time.Now().UnixMicro()
			mid := node.NewMessageId()
			// 128 bytes
			//node.Unicast(uniCtx, ayame.IntKey(i), mid, []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyyyyyy"))
			// 0 byte
			node.Unicast(uniCtx, ayame.IntKey(i), mid, []byte(""))
			resMap[mid] = make(chan *bs.BSUnicastResEvent)
			//elapsed := int64(0)
			results := make(map[string]int64)
			retStr := ""
		L:
			for len(results) < bs.K {
				//for i := 0; i < byzskip.K; i++ {
				select {
				case <-uniCtx.Done():
					ayame.Log.Infof("unicast timed out")
					break L
				case resp := <-resMap[mid]:
					if _, ok := results[resp.Sender().String()]; !ok {
						results[resp.Sender().String()] = time.Now().UnixMicro() - start // the first result
						retStr += fmt.Sprintf("%f (ms) %d (hops) %s (=key)\n", float64(results[resp.Sender().String()])/float64(1000), len(resp.Path), resp.Sender())
					}
					//results[resp.Sender().String()] = true
				}
			}
			//close(resMap[mid]) XXX when do we close?
			fmt.Fprintf(w, "%s", retStr)
		}
		sem.Unlock()
	}
}

func authorizeWeb(id peer.ID, key ayame.Key) (ayame.Key, *ayame.MembershipVector, []byte, error) {
	c, err := authWeb(*authURL, id, key)
	if err != nil {
		return nil, nil, nil, err
	}
	return c.Key, c.Mv, c.Cert, nil
}

func validateWeb(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, cert []byte) bool {
	return authority.VerifyJoinCert(id, key, mv, cert, pubKey)
}

func authWeb(url string, id peer.ID, key ayame.Key) (*authority.PCert, error) {
	resp, err := http.Get(url + fmt.Sprintf("/issue?key=%s&id=%s", authority.MarshalKeyToString(key), id.Pretty()))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var c authority.PCert
	err = json.Unmarshal(body, &c)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func receiveMessage(node *bs.BSNode, ev *bs.BSUnicastEvent) {
	ayame.Log.Infof("received message: %s", ev.Payload)
	sender := ev.Author().(*bs.BSNode)
	res := bs.NewBSUnicastResEvent(node, ev.MessageId, ev.Payload)
	res.Path = ev.Path       // copy the path info
	if sender.Equals(node) { //&& ev.TargetKey.Equals(node.Key()) {
		ayame.Log.Infof("Send to self: %s", node)
		go func() {
			receiveResponse(node, res)
		}()
	} else {
		ayame.Log.Infof("sending response to: %s from: %s", sender, node)
		node.SendEventAsync(context.Background(), sender, res, true)
		//if err != nil {
		//	ayame.Log.Infof("Sending response failed: %s", err)
		//}
	}
}

func receiveResponse(node *bs.BSNode, ev *bs.BSUnicastResEvent) {
	// ugly
	ayame.Log.Infof("received response response from: %s", ev.Sender())
	resMap[ev.MessageId] <- ev
}

func main() {
	verbose = flag.Bool("v", false, "verbose output")
	bootstrap = flag.Bool("b", false, "runs as the bootstrap node")
	key = flag.Int("key", 0, "key (integer)")
	k = flag.Int("k", 4, "the parameter k")
	tcp = flag.Bool("t", false, "use TCP")
	iAddr = flag.String("i", "/ip4/127.0.0.1/udp/9000/quic/p2p/16Uiu2HAkwNMzueRAnytwz3uR3cMVpNBCJ3DMbTgeStss2gNZuGxC", "introducer addr (ignored when bootstrap)")
	port = flag.Int("p", 9000, "the peer-to-peer port to listen to")
	//keystore = flag.String("s", filepath.Join(os.Getenv("HOME"), "keystore"), "the filepath for the keystore")
	keystore = flag.String("ks", "keystore", "the path name of the keystore")
	srvPort = flag.Int("s", 8000, "the web api server port to listen to")
	authURL = flag.String("auth", "http://localhost:7001", "the authenticator web URL")
	pubKeyString = flag.String("apub", "BABBEIIDM7V3FR4RWNGVXYRSHOCL6SYWLNIJLP4ONDGNB25HS7PKE6C56M2Q", "the public key of the authority")

	resMap = make(map[string]chan *bs.BSUnicastResEvent)

	flag.Parse()

	var selfAddr string

	if *tcp {
		//p2p.USE_QUIC = false
		selfAddr = fmt.Sprintf("/ip4/0.0.0.0/tcp/%d/", *port)
	} else {
		selfAddr = fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic/", *port)
	}

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	fmt.Printf("authority url: %s\nauthority pub: %s\n", *authURL, *pubKeyString)

	if len(*pubKeyString) == 0 {
		panic("need public key")
	}

	p, err := authority.UnmarshalStringToPubKey(*pubKeyString)
	if err != nil {
		panic(err)
	}

	pubKey = p

	ks, err := ayame.NewFSKeystore(*keystore)
	if err != nil {
		panic(err)
	}
	//var node *byzskip.BSNode
	var priv ci.PrivKey = nil
	if *bootstrap {
		if exist, err := ks.Has("introducer"); !exist || err != nil {
			priv, _, err = ci.GenerateKeyPair(ci.Secp256k1, 256)
			if err != nil {
				panic(err)
			}
			ks.Put("introducer", priv)
		} else {
			priv, err = ks.Get("introducer")
			if err != nil {
				panic(err)
			}
		}
	} else {
		priv, _, err = ci.GenerateKeyPair(ci.Secp256k1, 256)
		if err != nil {
			panic(err)
		}
	}

	p2pOpts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(selfAddr),
	}

	h, err := libp2p.New(p2pOpts...)
	if err != nil {
		panic(err)
	}

	for _, addr := range h.Addrs() {
		fmt.Printf("listen address: %s/p2p/%s\n", addr, h.ID())
	}
	fmt.Printf("web port: %d\n", *srvPort)

	bsOpts := []bs.Option{
		bs.Key(ayame.IntKey(*key)),
		bs.Bootstrap(*iAddr),
		bs.RedundancyFactor(*k),
		bs.Authorizer(authorizeWeb),
		bs.AuthValidator(validateWeb),
		bs.DetailedStatistics(true),
	}
	node, err = bs.New(h, bsOpts...)
	if err != nil {
		panic(err)
	}
	if *bootstrap {
		node.RunBootstrap(context.Background())
	} else {
		if err := node.Join(context.Background()); err != nil {
			panic(err)
		}
		joinBytes = node.Parent.(*p2p.P2PNode).InBytes + node.Parent.(*p2p.P2PNode).OutBytes
		joinMsgs = node.Parent.(*p2p.P2PNode).InCount
	}

	node.SetMessageReceiver(receiveMessage)
	node.SetResponseReceiver(receiveResponse)
	/*fmt.Printf("Node Addrs:\n")
	for _, addr := range node.Addrs() {
		fmt.Printf("%s\n", addr.String()+"/p2p/"+node.Id().Pretty())
	}*/
	/*
		reader := bufio.NewReader(os.Stdin)
		for {
			fmt.Print("bsrun> ")
			text, _ := reader.ReadString('\n')
			// convert CRLF to LF
			text = strings.Replace(text, "\n", "", -1)

			if strings.HasPrefix(text, "uni") {
				args := strings.Split(text, " ")
				ayame.Log.Infof("unicasting: " + strings.Join(args[1:], " "))
				node.Unicast(context.Background(), ayame.IntKey(1), []byte(strings.Join(args[1:], " ")))
			}

			if strings.HasPrefix(text, "tab") {
				fmt.Printf("key=%d, mv=[%s]\n%s", node.Key(), node.MV(), node.RoutingTable)
			}

			if strings.Compare("q", text) == 0 {
				node.Close()
				break
			}
		}
	*/
	http.HandleFunc("/tab", nodeTable)
	http.HandleFunc("/uni", nodeUnicast)
	http.HandleFunc("/stat", nodeStat)

	err2 := http.ListenAndServe(fmt.Sprintf(":%d", *srvPort), nil)
	if errors.Is(err2, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err2 != nil {
		fmt.Printf("error starting server: %s\n", err2)
		os.Exit(1)
	}
}
