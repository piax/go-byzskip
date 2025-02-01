package authority

import (
	"os"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
)

const (
	AUTH_PUBKEY = "AUTH_PUBKEY"
)

type Authorizer struct {
	pubKey  crypto.PubKey
	privKey crypto.PrivKey
}

// Global variable to store public key of authority.
var AuthPubKey crypto.PubKey = nil

func UpdateAuthPubKey() {
	// since this function is called at bootstrap phase, initialize the public key.
	if AuthPubKey == nil {
		pk := os.Getenv(AUTH_PUBKEY)
		if pk == "" {
			panic("AUTH_PUBKEY is empty")
		} else {
			p, err := UnmarshalStringToPubKey(pk)
			if err != nil {
				panic(err)
			}
			AuthPubKey = p
		}
	}
}

func NewAuthorizer() *Authorizer {
	priv, pub, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil
	}
	return &Authorizer{pubKey: pub, privKey: priv}
}

func NewAuthorizerWithKeyPair(priv crypto.PrivKey, pub crypto.PubKey) *Authorizer {
	return &Authorizer{pubKey: pub, privKey: priv}
}

func (auth *Authorizer) PublicKey() crypto.PubKey {
	return auth.pubKey
}

func (auth *Authorizer) Authorize(id peer.ID, key ayame.Key, mv *ayame.MembershipVector) []byte {
	return newJoinInfoCert(id, key, mv, auth.privKey)
}

func marshalJoinInfo(id peer.ID, key ayame.Key, mv *ayame.MembershipVector) []byte {
	ret, _ := id.MarshalBinary() // XXX discarded errors
	bin, _ := key.Encode().Marshal()
	ret = append(ret, bin...)
	ret = append(ret, mv.Encode()...)
	return ret
}

// Create a new node with its implemented protocols
func VerifyJoinCert(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, cert []byte, pubKey crypto.PubKey) bool {
	data := marshalJoinInfo(id, key, mv)
	ayame.Log.Debugf("verifying joincert id=%s, key=%s, mv=%s, data=%v, cert=%x", id, key, mv, data, cert)

	res, err := pubKey.Verify(data, cert)
	if err != nil {
		ayame.Log.Errorf("Verify error: %s\n", err)
		return false
	}
	return res
}

func newJoinInfoCert(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, privKey crypto.PrivKey) []byte {
	data := marshalJoinInfo(id, key, mv)
	//mHashBuf, _ := multihash.EncodeName(data, "sha2-256")
	ayame.Log.Debugf("joincert id=%s, key=%s, mv=%s, data=%v", id, key, mv, data)
	res, _ := privKey.Sign(data) // XXX discarded errors
	return res
}
