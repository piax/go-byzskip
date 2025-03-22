package ayame

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"math/big"
	"math/rand"
	"net/url"

	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	p2p "github.com/piax/go-byzskip/ayame/p2p/pb"
)

const (
	JITTER_VALUE       = 100000
	UNIFIED_KEY_LENGTH = 512 // bits
	UNIFIED_KEY_SEP    = ":"
	MOD_FACTOR         = 10007 // prime number
)

type UnifiedKey struct {
	val *big.Int
	id  peer.ID
}

func randomBytes(len int) []byte {
	ret := make([]byte, len)
	for i := 0; i < len; i++ {
		ret[i] = byte(int(rand.Float32() * 8))
	}
	return ret
}

func randomBigInt() *big.Int {
	ret := make([]byte, UNIFIED_KEY_LENGTH/8)
	for i := 0; i < UNIFIED_KEY_LENGTH/8; i++ {
		ret[i] = byte(int(rand.Float32() * 8))
	}
	return big.NewInt(0).SetBytes(ret)
}

func ZeroID() peer.ID { // the smallest byte
	return peer.ID([]byte{0})
}

func MaxID() peer.ID { // zero length byte means max ID
	return peer.ID([]byte{})
}

func RandomID() peer.ID {
	b := randomBytes(UNIFIED_KEY_LENGTH)
	var alg uint64 = mh.SHA2_256
	hash, _ := mh.Sum(b, alg, -1)
	return peer.ID(hash)
}

func (t *UnifiedKey) Less(elem interface{}) bool {
	if v, ok := elem.(*UnifiedKey); ok {
		cmp := t.val.Cmp(v.val)
		if cmp == 0 {
			if len(v.id) == 0 { // max
				return true
			} else if len(t.id) == 0 { // max
				return false
			}
			return bytes.Compare([]byte(t.id), []byte(v.id)) < 0
		} else {
			return cmp < 0
		}
	}
	panic("not a unified key comparison")
}

func (t *UnifiedKey) Equals(elem any) bool {
	if v, ok := elem.(*UnifiedKey); ok {
		return t.val.Cmp(v.val) == 0 && bytes.Equal([]byte(t.id), []byte(v.id))
	}
	//panic(fmt.Sprintf("not a unified key comparison %s and %v", t, elem))
	return false
}

func (t *UnifiedKey) GetBigInt() *big.Int {
	return t.val
}

func (t *UnifiedKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(*UnifiedKey); ok {
		cmp := t.val.Cmp(v.val)
		if cmp == 0 {
			if len(v.id) == 0 { // max
				return true
			} else if len(t.id) == 0 { // max
				return false
			}
			return bytes.Compare([]byte(t.id), []byte(v.id)) <= 0
		} else {
			return t.val.Cmp(v.val) <= 0
		}
	}
	panic(fmt.Sprintf("not a unified key comparison %s and %v", t, elem))
}

func (t *UnifiedKey) UnescapedString() string {
	b := t.val.Bytes()
	last := 0
	for i := 0; i < len(b); i++ {
		if b[i] != 0 {
			last = i
		}
	}
	n := make([]byte, last+1)
	copy(n, b)
	str := string(n)
	str = str + UNIFIED_KEY_SEP

	if t.id == peer.ID([]byte{0}) {
		str += "<MIN_ID>"
	} else if t.id == peer.ID([]byte{}) {
		str += "<MAX_ID>"
	} else {
		str += t.id.String()
	}
	return str
}

func (t *UnifiedKey) String() string {
	//str := base32.RawStdEncoding.EncodeToString(t.val.Bytes())
	b := t.val.Bytes()
	last := 0
	for i := 0; i < len(b); i++ {
		if b[i] != 0 {
			last = i
		}
	}
	n := make([]byte, last+1)
	copy(n, b)
	str := url.QueryEscape(string(n))
	str = str + UNIFIED_KEY_SEP

	if t.id == peer.ID([]byte{0}) {
		str += "<MIN_ID>"
	} else if t.id == peer.ID([]byte{}) {
		str += "<MAX_ID>"
	} else {
		str += t.id.String()
	}
	//return "<" + str[0:15] + ">" // for readability
	return str
}

func (t *UnifiedKey) Encode() *p2p.Key {
	val := make([]byte, UNIFIED_KEY_LENGTH/8+len(t.id))
	kb := t.val.Bytes()
	copy(val, kb)
	for i := 0; i < len(t.id); i++ {
		val[len(kb)+i] = t.id[i]
	}
	return &p2p.Key{
		Type: p2p.KeyType_UNIFIED,
		Body: val,
	}
}

func (t *UnifiedKey) Value() []byte {
	return t.val.Bytes()
}

func (t *UnifiedKey) ID() peer.ID {
	return t.id
}

func (t *UnifiedKey) SetID(i peer.ID) {
	t.id = i
}

func (t *UnifiedKey) Pretty() string {
	return t.String()[:16] + ":" + t.id.String()
}

//func DecodeUnifiedKey(pk *p2p.Key) Key {
//	return NewUnifiedKeyFromBytes(pk.Body)
//}

func makeBytes(str string) ([]byte, error) {
	max := UNIFIED_KEY_LENGTH / 8
	ret := make([]byte, UNIFIED_KEY_LENGTH/8)
	bstr := []byte(str)
	if len(bstr) > max {
		return nil, fmt.Errorf("too long string: %s", str)
	}
	for i := 0; i < len(ret); i++ {
		if i < len(bstr) {
			ret[i] = bstr[i]
		} else {
			ret[i] = 0
		}
	}
	return ret, nil
}

func adjustBytes(b []byte) ([]byte, error) {
	max := UNIFIED_KEY_LENGTH / 8
	ret := make([]byte, UNIFIED_KEY_LENGTH/8)
	if len(b) > max {
		return nil, fmt.Errorf("too long bytes: %s", b)
	}
	for i := 0; i < len(ret); i++ {
		if i < len(b) {
			ret[i] = b[i]
		} else {
			ret[i] = 0
		}
	}
	return ret, nil
}

func NewUnifiedRangeKeyForPrefix(prefix string) *RangeKey {
	s := NewUnifiedKeyFromString(prefix, ZeroID())
	eBytes := make([]byte, UNIFIED_KEY_LENGTH/8)
	p := []byte(prefix)
	copy(eBytes, p)
	for i := len(p); i < UNIFIED_KEY_LENGTH/8; i++ {
		eBytes[i] = 0xff
	}
	e := NewUnifiedKeyFromByteValue(eBytes, MaxID())
	return NewRangeKey(s, true, e, true)
}

func NewUnifiedKeyBetween(start, end *UnifiedKey) Key {
	// need to copy the big integer struct because Sub and Add are destructive
	sint := new(big.Int)
	sint = sint.Set(start.val)
	eint := new(big.Int)
	eint = eint.Set(end.val)
	cmp := eint.Cmp(sint)
	if cmp < 0 { // wrap-around case
		eint = sint
		sint = big.NewInt(0)
	} else if cmp == 0 { // same value (returned id is not between start and end)
		return NewUnifiedKey(sint, RandomID())
	}
	diff := eint.Sub(eint, sint)
	r, _ := crand.Int(crand.Reader, diff)
	added := sint.Add(sint, r)
	//	ret := sint.Cmp(added)
	//	fmt.Println(ret)
	return NewUnifiedKey(added, RandomID())
}

func NewUnifiedKeyFromStringWithJitter(str string) Key {
	z := new(big.Int)
	b, err := makeBytes(str)
	if err != nil {
		panic(err) // XXX
	}
	z.SetBytes(b)
	z.Add(z, big.NewInt(int64(rand.Float64()*JITTER_VALUE)))
	return NewUnifiedKey(z, RandomID())
}

func NewRandomUnifiedKey() Key {
	/*ret := make([]byte, UNIFIED_KEY_LENGTH)
	for i := 0; i < UNIFIED_KEY_LENGTH; i++ {
		ret[i] = byte(int(rand.Float32() * 8))
	}
	return NewUnifiedKeyFromBytes(ret)*/
	return NewUnifiedKey(randomBigInt(), RandomID())
}

func NewUnifiedKey(v *big.Int, id peer.ID) Key {
	return &UnifiedKey{val: v, id: id}
}

func NewUnifiedKeyFromBytes(arr []byte) Key {
	keypart := arr[:UNIFIED_KEY_LENGTH/8]
	idpart := arr[UNIFIED_KEY_LENGTH/8:]
	z := new(big.Int)
	z.SetBytes(keypart)
	i := peer.ID(idpart)
	return NewUnifiedKey(z, i)
}

func modKey(bi *big.Int, id peer.ID) (*UnifiedKey, error) {
	// Extract the bigint part from the key
	keyBigInt := bi

	// Calculate max value for the bigint (2^256 - 1)
	maxBigInt := new(big.Int).Lsh(big.NewInt(1), 256)
	maxBigInt.Sub(maxBigInt, big.NewInt(1))

	// Calculate maxBigInt / MOD_FACTOR
	divisor := new(big.Int).Div(maxBigInt, big.NewInt(MOD_FACTOR))

	// Calculate keyBigInt % divisor
	modResult := new(big.Int).Mod(keyBigInt, divisor)

	// Create a new unified key with the modified bigint value
	modifiedKey := NewUnifiedKey(modResult, id).(*UnifiedKey)

	return modifiedKey, nil
}

func NewUnifiedKeyFromByteValue(value []byte, id peer.ID) Key {
	z := new(big.Int)
	z.SetBytes(value)
	return NewUnifiedKey(z, id)
}

func NewUnifiedKeyFromString(str string, id peer.ID) Key {
	z := new(big.Int)
	b, err := makeBytes(str)
	if err != nil {
		panic(err) // XXX
	}
	z.SetBytes(b)

	//modKey, err := modKey(z, id)
	//if err != nil {
	//	panic(err) // XXX
	//}

	return NewUnifiedKey(z, id)
}

func NewUnifiedKeyFromIdKey(id IdKey) Key {
	z := new(big.Int)
	b, err := adjustBytes(id)
	if err != nil {
		panic(err) // XXX
	}
	z.SetBytes(b)
	return NewUnifiedKey(z, peer.ID(id))
}

func NewUnifiedKeyFromInt(i int, id peer.ID) Key {
	z := new(big.Int)
	z.SetInt64(int64(i))
	return NewUnifiedKey(z, id)
}
