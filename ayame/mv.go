package ayame

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"
)

const (
	MembershipVectorSize = 32
	DefaultAlpha         = 2
)

type MembershipVector struct {
	Alpha int
	Val   [MembershipVectorSize]int
}

func NewMembershipVector(alpha int) *MembershipVector {
	var v [MembershipVectorSize]int
	for i := 0; i < MembershipVectorSize; i++ {
		v[i] = rand.Intn(alpha)
	}
	return &MembershipVector{Val: v, Alpha: alpha}
}

func NewMembershipVectorLiteral(alpha int, literal []int) *MembershipVector {
	var v [MembershipVectorSize]int
	for i := 0; i < len(literal); i++ {
		v[i] = literal[i]
	}
	return &MembershipVector{Val: v, Alpha: alpha}
}

func NewMembershipVectorFromBinary(bin []byte) *MembershipVector {
	if len(bin) == 0 {
		return nil
	}
	val := make([]int, MembershipVectorSize)
	for i := 0; i < len(bin); i++ {
		v := int(bin[i])
		for j := 7; j >= 0; j-- {
			thisVal := int(math.Pow(float64(2), float64(j)))
			thisBit := v / int(math.Pow(float64(2), float64(j)))
			val[i*8+7-j] = thisBit
			//Log.Debugf("%d=>%d\n", i*8+7-j, thisBit)
			if thisBit == 1 {
				v -= thisVal
			}
		}
	}
	return NewMembershipVectorLiteral(2, val)
}

func (mv *MembershipVector) CommonPrefixLength(another *MembershipVector) int {
	for i := 0; i < len(mv.Val); i++ {
		if mv.Val[i] != another.Val[i] {
			return i
		}
	}
	return len(mv.Val)
}

// returns the byte representation.
func (mv *MembershipVector) Encode() []byte {
	if mv.Alpha != 2 {
		panic(fmt.Errorf("only binary mv is allowed"))
	}
	ret := make([]byte, MembershipVectorSize/8)
	for i := 0; i < MembershipVectorSize; i += 8 { // pack every 8 bits.
		byteVal := 0
		for j := 7; j >= 0; j-- {
			byteVal += int(math.Pow(float64(2), float64(7-j))) * mv.Val[i+j]
		}
		//Log.Debugf("%d=>%d\n", i/8, byteVal)
		ret[i/8] = byte(byteVal)
	}
	return ret
}

func (mv *MembershipVector) String() string {
	r := ""
	for i := 0; i < MembershipVectorSize; i++ {
		r += strconv.Itoa(mv.Val[i])
	}
	return r
}
