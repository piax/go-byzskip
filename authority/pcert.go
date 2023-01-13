package authority

import (
	"encoding/json"

	ci "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-base32"
	"github.com/piax/go-byzskip/ayame"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
)

type PCert struct {
	Key  ayame.Key               `json:"-"`
	id   peer.ID                 `json:"-"`
	Mv   *ayame.MembershipVector `json:"-"`
	Cert []byte                  `json:"cert"`
}

var codec = base32.StdEncoding.WithPadding(base32.NoPadding)

func MarshalKeyToString(key ayame.Key) string {
	bin, _ := key.Encode().Marshal()
	return codec.EncodeToString(bin)
}

func UnmarshalStringToKey(keyStr string) (ayame.Key, error) {
	key := &pb.Key{}
	buf, err := codec.DecodeString(keyStr)
	if err != nil {
		return nil, err
	}
	err = key.Unmarshal(buf)
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

func NewPCert(key ayame.Key, id peer.ID, mv *ayame.MembershipVector, cert []byte) *PCert {
	return &PCert{Key: key, id: id, Mv: mv, Cert: cert}
}

func (r *PCert) MarshalJSON() ([]byte, error) {
	type alias PCert
	kb := r.Key.Encode()
	idb := peer.Encode(r.id)
	mvb := r.Mv.Encode()

	return json.Marshal(&struct {
		*alias
		AliasKey *pb.Key `json:"key"`
		AliasId  string  `json:"id"`
		AliasMv  []byte  `json:"mv"`
	}{
		alias:    (*alias)(r),
		AliasKey: kb,
		AliasId:  idb,
		AliasMv:  mvb,
	})
}

func (r *PCert) UnmarshalJSON(b []byte) error {
	type alias PCert

	aux := &struct {
		*alias
		AliasKey *pb.Key `json:"key"`
		AliasId  string  `json:"id"`
		AliasMv  []byte  `json:"mv"`
	}{
		alias: (*alias)(r),
	}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}
	r.id, _ = peer.Decode(aux.AliasId)
	r.Key = ayame.NewKey(aux.AliasKey)
	r.Mv = ayame.NewMembershipVectorFromBinary(aux.AliasMv)
	return nil
}
