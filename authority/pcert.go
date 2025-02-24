package authority

import (
	"encoding/json"
	"time"

	//proto "github.com/gogo/protobuf/proto"
	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-base32"
	"github.com/piax/go-byzskip/ayame"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
	"google.golang.org/protobuf/proto"
)

type PCert struct {
	Key         ayame.Key               `json:"-"`
	id          peer.ID                 `json:"-"`
	Mv          *ayame.MembershipVector `json:"-"`
	ValidAfter  time.Time               `json:"validAfter"`
	ValidBefore time.Time               `json:"validBefore"`
	Cert        []byte                  `json:"cert"`
}

var codec = base32.StdEncoding.WithPadding(base32.NoPadding)

func MarshalKeyToString(key ayame.Key) string {
	//bin, _ := key.Encode().Marshal()
	bin, _ := proto.Marshal(key.Encode())
	return codec.EncodeToString(bin)
}

func UnmarshalStringToKey(keyStr string) (ayame.Key, error) {
	if len(keyStr) == 0 {
		return nil, nil // empty key
	}
	key := &pb.Key{}
	buf, err := codec.DecodeString(keyStr)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(buf, key)
	// err = key.Unmarshal(buf)
	if err != nil {
		return nil, err
	}
	return ayame.NewKey(key), nil
}

func MarshalPubKeyToString(pub ci.PubKey) (string, error) {
	b, err := ci.MarshalPublicKey(pub)
	if err != nil {
		return "", err
	}
	return codec.EncodeToString(b), nil
}

func UnmarshalStringToPubKey(pubstr string) (ci.PubKey, error) {
	b, err := codec.DecodeString(pubstr)
	if err != nil {
		return nil, err
	}
	return ci.UnmarshalPublicKey(b)
}

func NewPCert(key ayame.Key, id peer.ID, mv *ayame.MembershipVector, cert []byte, va time.Time, vb time.Time) *PCert {
	return &PCert{Key: key, id: id, Mv: mv, Cert: cert, ValidBefore: vb, ValidAfter: va}
}

func (r *PCert) MarshalJSON() ([]byte, error) {
	type alias PCert
	kb := r.Key.Encode()
	idb := r.id.String()
	mvb := r.Mv.Encode()
	return json.Marshal(&struct {
		*alias
		AliasKey         *pb.Key   `json:"key"`
		AliasId          string    `json:"id"`
		AliasMv          []byte    `json:"mv"`
		AliasValidAfter  time.Time `json:"validAfter"`
		AliasValidBefore time.Time `json:"validBefore"`
	}{
		alias:            (*alias)(r),
		AliasKey:         kb,
		AliasId:          idb,
		AliasMv:          mvb,
		AliasValidAfter:  r.ValidAfter,
		AliasValidBefore: r.ValidBefore,
	})
}

func (r *PCert) UnmarshalJSON(b []byte) error {
	type alias PCert

	aux := &struct {
		*alias
		AliasKey         *pb.Key   `json:"key"`
		AliasId          string    `json:"id"`
		AliasMv          []byte    `json:"mv"`
		AliasValidBefore time.Time `json:"validBefore"`
		AliasValidAfter  time.Time `json:"validAfter"`
	}{
		alias: (*alias)(r),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	r.id, _ = peer.Decode(aux.AliasId)
	r.Key = ayame.NewKey(aux.AliasKey)
	r.Mv = ayame.NewMembershipVectorFromBinary(aux.AliasMv)
	r.ValidAfter = aux.ValidAfter
	r.ValidBefore = aux.ValidBefore
	return nil
}
