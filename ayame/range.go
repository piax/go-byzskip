package ayame

import (
	//proto "github.com/gogo/protobuf/proto"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
	"google.golang.org/protobuf/proto"
)

func isOrderedInclusive(a, b, c Key) bool {
	if a.LessOrEquals(b) && b.LessOrEquals(c) {
		return true
	}
	if b.LessOrEquals(c) && c.LessOrEquals(a) {
		return true
	}
	if c.LessOrEquals(a) && a.LessOrEquals(b) {
		return true
	}
	return false
}

func IsOrdered(start Key, startInclusive bool, val Key, end Key, endInclusive bool) bool {
	if start.Equals(end) {
		return (startInclusive != endInclusive) || (start.Equals(val))
	}
	rc := isOrderedInclusive(start, val, end)
	if rc {
		if start.Equals(val) {
			rc = startInclusive
		}
	}
	if rc {
		if val.Equals(end) {
			rc = endInclusive
		}
	}
	return rc
}

type RangeKey struct {
	start          Key
	startInclusive bool
	startExtent    Key
	end            Key
	endInclusive   bool
	endExtent      Key
}

// start and end are not inclusive by default
func NewRangeKey(start Key, startInclusive bool, end Key, endInclusive bool) *RangeKey {
	return &RangeKey{start: start, startInclusive: startInclusive, end: end, endInclusive: endInclusive}
}

func (t *RangeKey) SetEndExtent(key Key) {
	t.endExtent = key
}

func (t *RangeKey) SetStartExtent(key Key) {
	t.startExtent = key
}

func (t *RangeKey) EndExtent() Key {
	return t.endExtent
}

func (t *RangeKey) StartExtent() Key {
	return t.startExtent
}

func (t *RangeKey) Less(elem interface{}) bool {
	if v, ok := elem.(RangeKey); ok {
		return t.start.Less(v.start)
	}
	return false
}

func (t *RangeKey) ContainsKey(k Key) bool {
	return IsOrdered(t.Start(), t.startInclusive, k, t.End(), t.endInclusive)
}

func (t *RangeKey) Start() Key {
	return t.start
}

func (t *RangeKey) StartInclusive() bool {
	return t.startInclusive
}

func (t *RangeKey) End() Key {
	return t.end
}

func (t *RangeKey) EndInclusive() bool {
	return t.endInclusive
}

func (t *RangeKey) Equals(elem any) bool {
	if v, ok := elem.(RangeKey); ok {
		return t.start.Equals(v.start)
	}
	return false
}

func (t *RangeKey) LessOrEquals(elem interface{}) bool {
	if v, ok := elem.(RangeKey); ok {
		return t.start.Equals(v.start) || t.start.Less(v.start)
	}
	return false
}

func (t *RangeKey) String() string {
	str := "[" + t.start.String() + "," + t.end.String() + "]" // for readability
	if t.startExtent != nil {
		str = "[" + t.startExtent.String() + "#" + str
	}
	if t.endExtent != nil {
		str = str + "#" + t.endExtent.String() + "]"
	}
	return str
}

func (t *RangeKey) Encode() *pb.Key {
	var encSE, encEE *pb.Key
	if t.startExtent != nil {
		encSE = t.startExtent.Encode()
	}
	if t.endExtent != nil {
		encEE = t.endExtent.Encode()
	}
	pr := pb.Range{
		Start:          t.start.Encode(),
		StartInclusive: t.startInclusive,
		StartExtent:    encSE,
		End:            t.end.Encode(),
		EndInclusive:   t.endInclusive,
		EndExtent:      encEE,
	}
	bytes, _ := proto.Marshal(&pr)
	return &pb.Key{
		Type: pb.KeyType_RANGE,
		Body: bytes}
}

func NewRangeKeyFromBytes(arg []byte) Key {
	rng := &pb.Range{}
	proto.Unmarshal(arg, rng)
	ret := NewRangeKey(NewKey(rng.Start), rng.StartInclusive, NewKey(rng.End), rng.EndInclusive)
	ret.SetStartExtent(NewKey(rng.StartExtent))
	ret.SetStartExtent(NewKey(rng.EndExtent))
	return ret
}
