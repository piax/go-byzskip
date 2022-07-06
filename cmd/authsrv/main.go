package main

import (
	"encoding/base32"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"

	ci "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/op/go-logging"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
)

var verbose *bool
var port *int
var keystore *string

var auth *authority.Authorizer

var codec = base32.StdEncoding.WithPadding(base32.NoPadding)

const (
	SEQ_NO      = "_seq"
	KEY_PREFIX  = "key_"
	CERT_PREFIX = "cert_"
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

	ikey, err := strconv.Atoi(vals["key"][0])
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	//db, err := leveldb.OpenFile("authority", nil)
	//if err != nil {
	//	panic(err)
	//}
	//defer db.Close()

	mv := ayame.NewMembershipVector(2)
	key := ayame.IntKey(ikey)
	bin := auth.Authorize(id, key, mv)

	p := authority.NewPCert(key, id, mv, bin)

	pcert, _ := json.Marshal(p)
	//db.Put([]byte(KEY_PREFIX+vals["user"][0]), pcert, nil)

	ayame.Log.Infof("issueing: id=%s, key=%d pcert=%s", vals["id"][0], ikey, pcert)

	fmt.Fprintf(w, "%s", pcert)
}

func pubKey2String(pub ci.PubKey) (string, error) {
	b, err := ci.MarshalPublicKey(pub)
	if err != nil {
		return "", err
	}
	return codec.EncodeToString(b), nil
}

func string2PubKey(pubstr string) (ci.PubKey, error) {
	b, err := codec.DecodeString(pubstr)
	if err != nil {
		return nil, err
	}
	return ci.UnmarshalPublicKey(b)
}

func main() {
	verbose = flag.Bool("v", true, "verbose output")
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
	ps, _ := pubKey2String(auth.PublicKey())
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
