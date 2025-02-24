package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/op/go-logging"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
)

var verbose *bool
var port *int
var keystore *string

var auth *authority.Authorizer

const (
	SEQ_NO              = "_seq"
	KEY_PREFIX          = "key_"
	CERT_PREFIX         = "cert_"
	AUTH_VALID_DURATION = "8760h"
)

// honest integer key
func issueCert(w http.ResponseWriter, req *http.Request) {
	vals := req.URL.Query()

	// only the first param value is used.

	id, err := peer.Decode(vals["id"][0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	key, err := authority.UnmarshalStringToKey(vals["key"][0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	d, _ := time.ParseDuration(AUTH_VALID_DURATION)

	va := time.Now().Unix()
	vb := va + int64(d.Seconds())
	mv := ayame.NewMembershipVector(2)
	bin := auth.Authorize(id, key, mv, va, vb)
	p := authority.NewPCert(key, id, mv, bin, time.Unix(va, 0), time.Unix(vb, 0))

	pcert, _ := json.Marshal(p)
	//db.Put([]byte(KEY_PREFIX+vals["user"][0]), pcert, nil)

	ayame.Log.Infof("issueing: id=%s, key=%s", vals["id"][0], key)

	fmt.Fprintf(w, "%s", pcert)
}

func main() {
	verbose = flag.Bool("v", false, "verbose output")
	port = flag.Int("p", 7001, "the port to listen to")
	keystore = flag.String("s", "keystore", "the filepath for the keystore")

	flag.Parse()

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	ks, err := ayame.NewFSKeystore(*keystore)
	if err != nil {
		panic(err)
	}
	var priv ci.PrivKey
	if exist, err := ks.Has("authority"); !exist || err != nil {
		// does not exist
		if err != nil {
			panic(err)
		}
		priv, _, err = ci.GenerateKeyPair(ci.Secp256k1, 256)
		if err != nil {
			panic(err)
		}
		ks.Put("authority", priv)
	}
	priv, err = ks.Get("authority")
	if err != nil {
		panic(err)
	}
	auth = authority.NewAuthorizerWithKeyPair(priv, priv.GetPublic())
	ps, _ := authority.MarshalPubKeyToString(auth.PublicKey())
	fmt.Fprintf(os.Stderr, "authority publickey: %s\n", ps)

	http.HandleFunc("/issue", issueCert)

	err = http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		os.Exit(1)
	}

}
