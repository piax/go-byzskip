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
	ID          peer.ID                 `json:"-"`
	Mv          *ayame.MembershipVector `json:"-"`
	Name        string                  `json:"name"`
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

func NewPCert(key ayame.Key, name string, id peer.ID, mv *ayame.MembershipVector, cert []byte, va time.Time, vb time.Time) *PCert {
	return &PCert{Key: key, Name: name, ID: id, Mv: mv, Cert: cert, ValidBefore: vb, ValidAfter: va}
}

func (r *PCert) MarshalJSON() ([]byte, error) {
	type alias PCert
	kb := r.Key.Encode()
	idb := r.ID.String()
	mvb := r.Mv.Encode()

	aux := struct {
		*alias
		AliasKey         *pb.Key   `json:"key"`
		AliasName        string    `json:"name,omitempty"`
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
	}

	if r.Name != "" {
		aux.AliasName = r.Name
	}

	return json.Marshal(aux)
}

func (r *PCert) UnmarshalJSON(b []byte) error {
	type alias PCert

	aux := &struct {
		*alias
		AliasKey         *pb.Key   `json:"key"`
		AliasName        string    `json:"name,omitempty"`
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
	r.ID, _ = peer.Decode(aux.AliasId)
	r.Key = ayame.NewKey(aux.AliasKey)
	r.Name = aux.AliasName // Will be empty string if field missing
	r.Mv = ayame.NewMembershipVectorFromBinary(aux.AliasMv)
	r.ValidAfter = aux.ValidAfter
	r.ValidBefore = aux.ValidBefore
	return nil
}

func PCertToBytes(cert *PCert) ([]byte, error) {
	p := &pb.PCert{
		Id:   cert.ID.String(),
		Mv:   cert.Mv.Encode(),
		Cert: cert.Cert,
		Key:  cert.Key.Encode(),
		Name: cert.Name,
	}
	return proto.Marshal(p)
}

func BytesToPCert(dat []byte) (*PCert, error) {
	p := &pb.PCert{}
	err := proto.Unmarshal(dat, p)
	if err != nil {
		return nil, err
	}
	id, err := peer.Decode(p.Id)
	if err != nil {
		return nil, err
	}
	mv := ayame.NewMembershipVectorFromBinary(p.Mv)
	_, va, vb, err := ExtractCert(p.Cert)
	if err != nil {
		return nil, err
	}
	cert := &PCert{
		Key:         ayame.NewKey(p.Key),
		ID:          id,
		Mv:          mv,
		Name:        p.Name,
		Cert:        p.Cert,
		ValidAfter:  time.Unix(va, 0),
		ValidBefore: time.Unix(vb, 0),
	}
	return cert, nil
}
