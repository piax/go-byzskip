package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/julienschmidt/httprouter"
	"github.com/libp2p/go-libp2p"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/op/go-logging"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	"github.com/piax/go-byzskip/dht"
)

var verbose *bool
var bootstrap *bool
var key *int
var k *int
var iAddr *string

// var addr *string
var port *int
var srvPort *int
var keystore *string
var authURL *string
var pubKeyString *string

var bsdht *dht.BSDHT

var pubKey ci.PubKey
var tcp *bool
var insecure *bool

/*var joinElapsed int64
var joinInBytes int64
var joinOutBytes int64
var joinMsgs int64*/

var joinMsgs int64
var joinBytes int64

const (
	UNICAST_RESPONSE_TIMEOUT = 4000 // milliseconds
)

func nodeTable(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "%s\n", bsdht.Node.RoutingTable)
}

func nodeStat(w http.ResponseWriter, req *http.Request, _ httprouter.Params) {
	fmt.Fprintf(w, "--key: %s\n", bsdht.Node.Key())
	/*	fmt.Fprintf(w, "join-elapsed: %f\n", float64(joinElapsed)/1000.0)
		fmt.Fprintf(w, "join-out-bytes: %d\n", joinOutBytes)*/
	fmt.Fprintf(w, "join-msgs: %d\n", joinMsgs)
	fmt.Fprintf(w, "join-traffic: %d\n", joinBytes)
	fmt.Fprintf(w, "recv-msgs: %d\n", bsdht.Node.Parent.(*p2p.P2PNode).InCount)
	fmt.Fprintf(w, "recv-bytes: %d\n", bsdht.Node.Parent.(*p2p.P2PNode).InBytes)
	fmt.Fprintf(w, "out-bytes: %d\n", bsdht.Node.Parent.(*p2p.P2PNode).OutBytes)
	fmt.Fprintf(w, "conn-stats: %v\n", bsdht.Node.Parent.(*p2p.P2PNode).ConnManager().(*connmgr.BasicConnMgr).GetInfo())
	if p2p.RECORD_BYTES_PER_KEY {
		for k, v := range bsdht.Node.Parent.(*p2p.P2PNode).KeyInBytes {
			fmt.Fprintf(w, "key-bytes: %s %d\n", k, v)
		}
	}
}

func dhtPut(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	body := req.Body
	defer body.Close()
	buf := new(bytes.Buffer)
	io.Copy(buf, body)
	s := ps.ByName("name")
	bsdht.PutValue(context.Background(), s, buf.Bytes())
	w.WriteHeader(http.StatusCreated)
	fmt.Fprint(w, "put done\n")
}

func dhtGet(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	s := ps.ByName("name")
	out, err := bsdht.GetValue(context.Background(), s)
	if err == routing.ErrNotFound {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "%s not found\n", s)
		return
	} else if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "internal error (failed to get) %s\n", err)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write(out)
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
	resp, err := http.Get(url + fmt.Sprintf("/issue?key=%s&id=%s", authority.MarshalKeyToString(key), id.String()))
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

type blankValidator struct{} // do nothing

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

func main() {
	verbose = flag.Bool("v", false, "verbose output")
	bootstrap = flag.Bool("b", false, "runs as the bootstrap node")
	k = flag.Int("k", 4, "the parameter k")
	tcp = flag.Bool("t", false, "use TCP")
	iAddr = flag.String("i", "/ip4/127.0.0.1/udp/9000/quic/p2p/16Uiu2HAkwNMzueRAnytwz3uR3cMVpNBCJ3DMbTgeStss2gNZuGxC", "introducer addr (ignored when bootstrap)")
	port = flag.Int("p", 9000, "the peer-to-peer port to listen to")
	//keystore = flag.String("s", filepath.Join(os.Getenv("HOME"), "keystore"), "the filepath for the keystore")
	keystore = flag.String("ks", "keystore", "the path name of the keystore")
	srvPort = flag.Int("s", 8000, "the server port to listen to")
	authURL = flag.String("auth", "http://localhost:7001", "the authenticator web URL")
	insecure = flag.Bool("insecure", false, "run without authority")
	pubKeyString = flag.String("apub", "BABBEIIDM7V3FR4RWNGVXYRSHOCL6SYWLNIJLP4ONDGNB25HS7PKE6C56M2Q", "the public key of the authority")

	flag.Parse()

	//bs.InitK(*k)

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

	if err != nil {
		panic(err)
	}

	dhtOpts := []dht.Option{
		dht.Bootstrap(*iAddr),
		dht.RedundancyFactor(*k),
		dht.NamespacedValidator("v", blankValidator{}),
	}
	if !*insecure {
		dhtOpts = append(dhtOpts, dht.Authorizer(authorizeWeb),
			dht.AuthValidator(validateWeb))
	}

	bsdht, err = dht.New(h, dhtOpts...)

	if err != nil {
		panic(err)
	}

	//node, _ = byzskip.NewNodeWithAuth(selfAddr, ayame.IntKey(*key), nil, priv,
	//	authorizeWeb, validateWeb)
	if *bootstrap {
		bsdht.RunAsBootstrap(context.Background())
	} else {
		//node, _ = byzskip.NewNodeWithAuth(selfAddr, ayame.IntKey(*key), nil, nil,
		//			authorizeWeb, validateWeb)

		//start := time.Now().UnixMicro()
		if err := bsdht.Bootstrap(context.Background()); err != nil {
			panic(err)
		}
		joinBytes = bsdht.Node.Parent.(*p2p.P2PNode).InBytes + bsdht.Node.Parent.(*p2p.P2PNode).OutBytes
		joinMsgs = bsdht.Node.Parent.(*p2p.P2PNode).InCount
		/*joinElapsed = time.Now().UnixMicro() - start
		joinMsgs = node.Parent.(*p2p.P2PNode).InCount
		joinInBytes = node.Parent.(*p2p.P2PNode).InBytes
		joinOutBytes = node.Parent.(*p2p.P2PNode).OutBytes*/
	}

	router := httprouter.New()
	router.GET("/get/*name", dhtGet)
	router.PUT("/put/*name", dhtPut)
	router.GET("/tab", nodeTable)
	router.GET("/stat", nodeStat)

	err2 := http.ListenAndServe(fmt.Sprintf(":%d", *srvPort), router)
	if errors.Is(err2, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err2 != nil {
		fmt.Printf("error starting server: %s\n", err2)
		os.Exit(1)
	}
}
