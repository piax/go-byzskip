package byzskip

import (
	"fmt"
	"testing"

	"github.com/piax/go-ayame/ayame"
	ast "github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	rt := NewSkipRoutingTable(&IntKeyMV{Intkey: 1, Mvdata: ayame.NewMembershipVector(2)})
	rt.ensureHeight(3)
	rt.ensureHeight(2)
	fmt.Println(rt.String())
	ast.Equal(t, rt.Height(), 4, "expected 4")
}

func TestLessCircular(t *testing.T) {
	fmt.Println(less(ayame.Int(5), ayame.Int(0), ayame.Int(10), ayame.Int(6), ayame.Int(3)))
	fmt.Println(less(ayame.Int(5), ayame.Int(0), ayame.Int(10), ayame.Int(3), ayame.Int(6)))
}

func TestSortCircular(t *testing.T) {
	lst := []KeyMV{}
	for i := -10; i < 0; i++ {
		lst = append(lst, &IntKeyMV{Intkey: ayame.Int(i), Mvdata: ayame.NewMembershipVector(2)})
	}
	SortC(ayame.Int(-2), lst)

	/*lst2 := []KeyMV{}
	for i := -10; i < 0; i++ {
		lst2 = append(lst2, &IntKeyMV{Intkey: Int(i), Mvdata: ayame.NewMembershipVector(2)})
	}
	SortCircular(-2, lst2)*/

	fmt.Println(ayame.SliceString(lst))
	/*fmt.Println(ayame.SliceString(lst2))*/
}

func TestSorted(t *testing.T) {
	InitK(2)
	rt := NewSkipRoutingTable(&IntKeyMV{Intkey: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt.Add(&IntKeyMV{Intkey: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0})})
	rt.Add(&IntKeyMV{Intkey: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0})})
	rt.Add(&IntKeyMV{Intkey: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0})})
	rt.Add(&IntKeyMV{Intkey: 5, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0})})
	rt.Add(&IntKeyMV{Intkey: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0})})
	rt.Add(&IntKeyMV{Intkey: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})})
	rt.Add(&IntKeyMV{Intkey: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})})
	rslt := rt.GetCloserCandidates()
	fmt.Println(ayame.SliceString(rslt))
	rslt = rt.GetCommonNeighbors(&IntKeyMV{Intkey: 9, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})})
	fmt.Println(ayame.SliceString(rslt))
}
