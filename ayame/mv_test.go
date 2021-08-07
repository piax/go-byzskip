package ayame_test

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/piax/go-ayame/ayame"
	ast "github.com/stretchr/testify/assert"
)

func TestMembershipVector(t *testing.T) {
	mv := ayame.NewMembershipVector(2)
	mv2 := *mv
	mv3 := ayame.NewMembershipVector(2)
	if mv.CommonPrefixLength(&mv2) != ayame.MembershipVectorSize {
		t.Errorf("unexpected common prefix")
	}
	common := mv.CommonPrefixLength(mv3)
	if common == ayame.MembershipVectorSize {
		t.Errorf("not a random membership vector")
	}
	fmt.Println(mv)
	fmt.Println(mv3)
	fmt.Printf("common= %d\n", common)
}

func TestEncode(t *testing.T) {
	mv := ayame.NewMembershipVectorLiteral(2,
		[]int{0, 0, 0, 0, 0, 1, 0, 0,
			0, 0, 0, 0, 1, 0, 0, 0,
			0, 0, 0, 1, 0, 0, 0, 0,
			0, 0, 1, 0, 0, 0, 0, 0})
	bytes := mv.Encode()
	fmt.Println(hex.Dump(bytes))
	decoded := ayame.NewMembershipVectorFromBinary(bytes)
	fmt.Println(decoded)
	ast.Equal(t, mv, decoded, "expected the same mv")
	ast.Equal(t, mv.CommonPrefixLength(decoded), ayame.MembershipVectorSize, fmt.Sprintf("expected %d", ayame.MembershipVectorSize))
}
