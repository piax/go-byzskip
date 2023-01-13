package ayame

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"unsafe"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-base32"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
)

type Key interface {
	// Less reports whether the element is less than b
	Less(elem interface{}) bool
	// Equals reports whether the element equals to b
	Equals(elem any) bool
	// Suger.
	LessOrEquals(elem interface{}) bool
	String() string
	Encode() *pb.Key
}

func NewKey(key *pb.Key) Key {
	if key == nil {
		return nil
	}
	switch key.Type {
	case pb.KeyType_FLOAT:
		return NewFloatKeyFromBytes(key.Body)
	case pb.KeyType_INT:
		return NewIntKeyFromBytes(key.Body)
	case pb.KeyType_STRING:
		return NewStringKeyFromBytes(key.Body)
	case pb.KeyType_ID:
		return NewIdKeyFromBytes(key.Body)
	case pb.KeyType_UNIFIED:
		return NewUnifiedKeyFromBytes(key.Body)
	case pb.KeyType_RANGE:
		return NewRangeKeyFromBytes(key.Body)
	}
	return nil
}

// Integer Key
type IntKey int

func (t IntKey) Less(elem interface{}) bool {
	if v, ok := elem.(IntKey); ok {
		return int(t) < int(v)
	}
	return false
}

func (t IntKey) Equals(elem any) bool {
	if v, ok := elem.(IntKey); ok {
		return int(t) == int(v)
	}
	return false
}

func (t IntKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(IntKey); ok {
		return int(t) <= int(v)
	}
	return false
}

func (t IntKey) String() string {
	return strconv.Itoa(int(t))
}

func (t IntKey) Encode() *pb.Key {
	return &pb.Key{
		Type: pb.KeyType_INT,
		Body: IntToByteArray(int(t))}
}

func IntToByteArray(num int) []byte {
	size := int(unsafe.Sizeof(num))
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		byt := *(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&num)) + uintptr(i)))
		arr[i] = byt
	}
	return arr
}

func NewIntKeyFromBytes(arr []byte) Key {
	val := int64(0)
	size := len(arr)
	for i := 0; i < size; i++ {
		*(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&val)) + uintptr(i))) = arr[i]
	}
	return IntKey(val)
}

type FloatKey float64

func (t FloatKey) Less(elem interface{}) bool {
	if v, ok := elem.(FloatKey); ok {
		return float64(t) < float64(v)
	}
	return false
}

func (t FloatKey) Equals(elem any) bool {
	if v, ok := elem.(FloatKey); ok {
		return float64(t) == float64(v)
	}
	return false
}

func (t FloatKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(FloatKey); ok {
		return float64(t) <= float64(v)
	}
	return false
}

func (t FloatKey) String() string {
	return fmt.Sprintf("%f", float64(t))
}

func (t FloatKey) Encode() *pb.Key {
	bits := math.Float64bits(float64(t))
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return &pb.Key{
		Type: pb.KeyType_FLOAT,
		Body: bytes}
}

func NewFloatKeyFromBytes(arg []byte) Key {
	bits := binary.LittleEndian.Uint64(arg)
	float := math.Float64frombits(bits)
	return FloatKey(float)
}

type StringKey string

func (t StringKey) Less(elem interface{}) bool {
	if v, ok := elem.(StringKey); ok {
		return t < v
	}
	return false
}

func (t StringKey) Equals(elem any) bool {
	if v, ok := elem.(StringKey); ok {
		return t == v
	}
	return false
}

func (t StringKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(StringKey); ok {
		return t <= v
	}
	return false
}

func (t StringKey) String() string {
	return string(t)
}

func (t StringKey) Encode() *pb.Key {
	bytes := []byte(t)
	return &pb.Key{
		Type: pb.KeyType_STRING,
		Body: bytes}
}

func NewStringKeyFromBytes(arg []byte) Key {
	return StringKey(string(arg))
}

type IdKey []byte

func NewStringIdKey(key string) IdKey {
	hash := sha256.Sum256([]byte(key))
	return IdKey(hash[:])
}

func NewIdKey(id peer.ID) IdKey {
	hash := sha256.Sum256([]byte(id))
	return IdKey(hash[:])
}

func (t IdKey) Less(elem interface{}) bool {
	if v, ok := elem.(IdKey); ok {
		return bytes.Compare(t, v) < 0
	}
	return false
}

func (t IdKey) Equals(elem any) bool {
	if v, ok := elem.(IdKey); ok {
		return bytes.Equal(t, v)
	}
	return false
}

func (t IdKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(IdKey); ok {
		return bytes.Compare(t, v) <= 0
	}
	return false
}

func (t IdKey) String() string {
	str := base32.RawStdEncoding.EncodeToString(t)
	return "<" + str[0:15] + ">" // for readability
}

func (t IdKey) Encode() *pb.Key {
	bytes := []byte(t)
	return &pb.Key{
		Type: pb.KeyType_ID,
		Body: bytes}
}

func NewIdKeyFromBytes(arg []byte) Key {
	return IdKey(arg)
}
