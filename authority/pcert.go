package authority

import (
	"encoding/json"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/piax/go-ayame/ayame"
	p2p "github.com/piax/go-ayame/ayame/p2p"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
)

type PCert struct {
	Key  ayame.Key               `json:"-"`
	id   peer.ID                 `json:"-"`
	Mv   *ayame.MembershipVector `json:"-"`
	Cert []byte                  `json:"cert"`
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
	r.Key = p2p.NewKey(aux.AliasKey)
	r.Mv = ayame.NewMembershipVectorFromBinary(aux.AliasMv)
	return nil
}
