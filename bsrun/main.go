package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/op/go-logging"
	"github.com/piax/go-ayame/ayame"
	"github.com/piax/go-ayame/byzskip"
)

var verbose *bool
var bootstrap *bool
var key *int
var k *int
var alpha *int
var iAddr *string
var addr *string
var port *int

func main() {
	verbose = flag.Bool("v", false, "verbose output")
	bootstrap = flag.Bool("b", false, "runs as the bootstrap node")
	key = flag.Int("key", 0, "key (integer)")
	k = flag.Int("k", 4, "the parameter k")
	alpha = flag.Int("alpha", 2, "the parameter alpha")
	iAddr = flag.String("i", "", "introducer addr (ignored when bootstrap)")
	port = flag.Int("p", 9000, "the port to listen to")

	byzskip.InitK(*k)

	flag.Parse()

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	ksp := filepath.Join(os.Getenv("HOME"), "keystore")
	ks, err := NewFSKeystore(ksp)
	if err != nil {
		panic(err)
	}
	var node *byzskip.BSNode

	selfAddr := fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic/", *port)

	if *bootstrap {
		var priv ci.PrivKey = nil
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
		node, _ = byzskip.NewP2PNode(selfAddr, ayame.IntKey(*key), ayame.NewMembershipVector(*alpha), priv)
		node.RunBootstrap(context.Background())
	} else {
		if len(*iAddr) == 0 {
			panic("need introducer address")
		}
		node, _ = byzskip.NewP2PNode(selfAddr, ayame.IntKey(*key), ayame.NewMembershipVector(*alpha), nil)
		if err := node.Join(context.Background(), *iAddr); err != nil {
			panic(err)
		}
	}
	fmt.Printf("Node Addrs:\n")
	for _, addr := range node.Addrs() {
		fmt.Printf("%s\n", addr.String()+"/p2p/"+node.Id().Pretty())
	}

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
}
