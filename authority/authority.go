package authority

import (
	"encoding/binary"
	"os"
	"time"

	//proto "github.com/gogo/protobuf/proto"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
	"google.golang.org/protobuf/proto"
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

func (auth *Authorizer) Authorize(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, va int64, vb int64) []byte {
	return newJoinInfoCert(id, key, mv, va, vb, auth.privKey)
}

func MarshalJoinInfo(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, va int64, vb int64) []byte {
	ret, _ := id.MarshalBinary() // XXX discarded errors
	//bin, _ := key.Encode().Marshal()
	bin, _ := proto.Marshal(key.Encode())
	ret = append(ret, bin...)
	ret = append(ret, mv.Encode()...)
	ret = append(ret, int64ToBytes(va)...)
	ret = append(ret, int64ToBytes(vb)...)
	return ret
}

func int64ToBytes(i int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(i))
	return b
}

func Sign(data []byte, va int64, vb int64, privKey crypto.PrivKey) ([]byte, error) {
	res, err := privKey.Sign(data)
	if err != nil {
		return nil, err
	}
	c := &pb.Cert{Sig: res, ValidAfter: va, ValidBefore: vb}
	ret, err := proto.Marshal(c)
	return ret, err
}

func ExtractCert(cert []byte) ([]byte, int64, int64, error) {
	c := &pb.Cert{}
	err := proto.Unmarshal(cert, c)
	if err != nil {
		return nil, 0, 0, err
	}
	return c.Sig, c.ValidAfter, c.ValidBefore, nil
}

func Verify(data []byte, cert []byte, pubKey crypto.PubKey) bool {
	c := &pb.Cert{}
	err := proto.Unmarshal(cert, c)
	if err != nil {
		ayame.Log.Errorf("Verify error: %s\n", err)
		return false
	}
	res, err := pubKey.Verify(data, c.Sig)
	if err != nil {
		ayame.Log.Errorf("Verify error: %s\n", err)
		return false
	}
	if !res {
		ayame.Log.Errorf("Verify error\n")
		return false
	}
	now := time.Now().Unix()
	if c.ValidAfter > now {
		ayame.Log.Errorf("Not valid yet: validAfter=%s\n", time.Unix(c.ValidAfter, 0).Format(time.RFC3339))
		return false
	}
	if c.ValidBefore < now {
		ayame.Log.Errorf("Certificate expired: validBefore=%s\n", time.Unix(c.ValidBefore, 0).Format(time.RFC3339))
		return false
	}
	return true
}

// Create a new node with its implemented protocols
func VerifyJoinCert(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, cert []byte, pubKey crypto.PubKey) bool {
	c := &pb.Cert{}
	if err := proto.Unmarshal(cert, c); err != nil {
		ayame.Log.Errorf("Verify error: %s\n", err)
		return false
	}
	data := MarshalJoinInfo(id, key, mv, c.ValidAfter, c.ValidBefore)
	ayame.Log.Debugf("verifying joincert id=%s, key=%s, mv=%s, data=%v, cert=%x", id, key, mv, data, cert)

	//res, err := pubKey.Verify(data, cert)
	res := Verify(data, cert, pubKey)
	//if err != nil {
	//	ayame.Log.Errorf("Verify error: %s\n", err)
	//	return false
	//}
	return res
}

func newJoinInfoCert(id peer.ID, key ayame.Key, mv *ayame.MembershipVector, va int64, vb int64, privKey crypto.PrivKey) []byte {
	data := MarshalJoinInfo(id, key, mv, va, vb)
	//mHashBuf, _ := multihash.EncodeName(data, "sha2-256")
	ayame.Log.Debugf("joincert id=%s, key=%s, mv=%s, data=%v", id, key, mv, data)
	//res, _ := privKey.Sign(data) // XXX discarded errors
	res, _ := Sign(data, va, vb, privKey) // XXX discarded errors

	return res
}
