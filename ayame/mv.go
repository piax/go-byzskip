package ayame

import (
	"math/rand"
	"strconv"
)

const (
	MembershipVectorSize = 32
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

func (mv *MembershipVector) CommonPrefixLength(another *MembershipVector) int {
	for i := 0; i < MembershipVectorSize; i++ {
		if mv.Val[i] != another.Val[i] {
			return i
		}
	}
	return MembershipVectorSize
}

func (mv *MembershipVector) String() string {
	r := ""
	for i := 0; i < MembershipVectorSize; i++ {
		r += strconv.Itoa(mv.Val[i])
	}
	return r
}
