package ayame

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"unsafe"

	p2p "github.com/piax/go-ayame/ayame/p2p/pb"
)

type Key interface {
	// Less reports whether the element is less than b
	Less(elem interface{}) bool
	// Equals reports whether the element equals to b
	Equals(elem interface{}) bool
	// Suger.
	LessOrEquals(elem interface{}) bool
	String() string
	Encode() *p2p.Key
}

// Integer Key
type IntKey int

func (t IntKey) Less(elem interface{}) bool {
	if v, ok := elem.(IntKey); ok {
		return int(t) < int(v)
	}
	return false
}

func (t IntKey) Equals(elem interface{}) bool {
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

func (t IntKey) Encode() *p2p.Key {
	return &p2p.Key{
		Type: p2p.KeyType_INT,
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
		return int(t) < int(v)
	}
	return false
}

func (t FloatKey) Equals(elem interface{}) bool {
	if v, ok := elem.(FloatKey); ok {
		return int(t) == int(v)
	}
	return false
}

func (t FloatKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(FloatKey); ok {
		return int(t) <= int(v)
	}
	return false
}

func (t FloatKey) String() string {
	return fmt.Sprintf("%f", float64(t))
}

func (t FloatKey) Encode() *p2p.Key {
	bits := math.Float64bits(float64(t))
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return &p2p.Key{
		Type: p2p.KeyType_INT,
		Body: bytes}
}

func NewFloatKeyFromBytes(arg []byte) Key {
	bits := binary.LittleEndian.Uint64(arg)
	float := math.Float64frombits(bits)
	return FloatKey(float)
}
