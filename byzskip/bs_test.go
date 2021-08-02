package byzskip

import (
	"fmt"
	"testing"

	"github.com/piax/go-ayame/ayame"
	ast "github.com/stretchr/testify/assert"
)

func TestTable(t *testing.T) {
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVector(2)})
	rt.ensureHeight(3)
	rt.ensureHeight(2)
	fmt.Println(rt.String())
	ast.Equal(t, rt.Height(), 4, "expected 4")
}

func TestLessCircular(t *testing.T) {
	fmt.Println(less(ayame.IntKey(5), ayame.IntKey(0), ayame.IntKey(10), ayame.IntKey(6), ayame.IntKey(3)))
	fmt.Println(less(ayame.IntKey(5), ayame.IntKey(0), ayame.IntKey(10), ayame.IntKey(3), ayame.IntKey(6)))
}

func TestSortCircular(t *testing.T) {
	lst := []KeyMV{}
	for i := -10; i < 0; i++ {
		lst = append(lst, &IntKeyMV{key: ayame.IntKey(i), Mvdata: ayame.NewMembershipVector(2)})
	}
	SortC(ayame.IntKey(-2), lst)

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
	rt := NewSkipRoutingTable(&IntKeyMV{key: 1, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 0, 0})})
	rt.Add(&IntKeyMV{key: 2, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 0, 0})})
	rt.Add(&IntKeyMV{key: 3, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 0, 0})})
	rt.Add(&IntKeyMV{key: 4, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 0, 0})})
	rt.Add(&IntKeyMV{key: 5, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 0, 1, 0})})
	rt.Add(&IntKeyMV{key: 6, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 0, 1, 0})})
	rt.Add(&IntKeyMV{key: 7, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})})
	rt.Add(&IntKeyMV{key: 8, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{1, 1, 1, 0})})
	rslt := rt.GetCloserCandidates()
	fmt.Println(ayame.SliceString(rslt))
	rslt = rt.GetCommonNeighbors(&IntKeyMV{key: 9, Mvdata: ayame.NewMembershipVectorLiteral(2, []int{0, 1, 1, 0})})
	fmt.Println(ayame.SliceString(rslt))
}
