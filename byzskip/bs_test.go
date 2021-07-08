package byzskip

import (
	"fmt"
	"testing"

	"github.com/piax/go-ayame/ayame"
	ast "github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	rt := NewRoutingTable(&IntKeyMV{Intkey: 1, Mvdata: ayame.NewMembershipVector(2)})
	rt.EnsureHeight(3)
	rt.EnsureHeight(2)
	fmt.Println(rt.String())
	ast.Equal(t, rt.Height(), 4, "expected 4")
}

func TestSorted(t *testing.T) {
	InitK(2)
	rt := NewRoutingTable(&IntKeyMV{Intkey: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt.Add(&IntKeyMV{Intkey: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0})})
	rt.Add(&IntKeyMV{Intkey: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0})})
	rt.Add(&IntKeyMV{Intkey: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0})})
	rt.Add(&IntKeyMV{Intkey: 5, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0})})
	rt.Add(&IntKeyMV{Intkey: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0})})
	rt.Add(&IntKeyMV{Intkey: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})})
	rt.Add(&IntKeyMV{Intkey: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})})
	rslt := rt.GetCloserCandidates()
	fmt.Println(ayame.SliceString(rslt))
}
