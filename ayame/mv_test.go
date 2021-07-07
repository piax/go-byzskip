package ayame_test

import (
	"fmt"
	"testing"

	"github.com/piax/go-ayame/ayame"
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
