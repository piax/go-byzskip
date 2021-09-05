package byzskip

import (
	"fmt"
	"strings"

	"github.com/piax/go-ayame/ayame"
	"github.com/thoas/go-funk"
)

var (
	K                         int = 4
	ALPHA                     int = 2
	LEFT_HALF_K, RIGHT_HALF_K int = halvesOfK(K)
)

func InitK(k int) {
	K = k
	LEFT_HALF_K, RIGHT_HALF_K = halvesOfK(k)
}

const (
	RIGHT int = iota
	LEFT
)

/*const (
	SINGLE int = iota
	PRUNE
)*/

func halvesOfK(k int) (int, int) {
	if k%2 == 0 {
		return k / 2, k / 2
	} else {
		return (k + 1) / 2, (k+1)/2 - 1
	}
}

type KeyMV interface {
	Key() ayame.Key
	MV() *ayame.MembershipVector
	Equals(other KeyMV) bool
	String() string
}

type IntKeyMV struct {
	key    ayame.IntKey
	Mvdata *ayame.MembershipVector
}

func (km IntKeyMV) Key() ayame.Key {
	return km.key
}

func (km IntKeyMV) MV() *ayame.MembershipVector {
	return km.Mvdata
}

func (km IntKeyMV) Equals(other KeyMV) bool {
	return km.key.Equals(other.Key())
}

func (km IntKeyMV) String() string {
	return km.key.String()
}

type NeighborList struct {
	owner     KeyMV
	Neighbors [2]([]KeyMV) // RIGHT, LEFT
	level     int
}

type NeighborRequest struct {
	// requester's key
	Key ayame.Key
	// requester's Membership Vector. can be nil.
	MV *ayame.MembershipVector
	// table index for requester's closest
	ClosestIndex *TableIndex
	// table index for requester's neighbors
	NeighborListIndex []*TableIndex
}

type RoutingTable interface {
	// access entries

	// Returns k closest and its level
	KClosest(req *NeighborRequest) ([]KeyMV, int)
	// Returns requested neighbor entry list
	Neighbors(req *NeighborRequest) []KeyMV
	// Returns all disjoint neighbor entry list.
	// If includeSelf is true, returns a list which include owner entry.
	// If sorted is true, returns a sorted list in order of closeness.
	AllNeighbors(includeSelf bool, sorted bool) []KeyMV

	HasSufficientNeighbors() bool

	// Deprecated: should use KClosest with request including key
	KClosestWithKey(key ayame.Key) ([]KeyMV, int)
	// Deprecated: should use Neighbors with request including mv
	GetCommonNeighbors(mv *ayame.MembershipVector) []KeyMV // get neighbors which have common prefix with kmv

	GetTableIndex() []*TableIndex
	GetClosestIndex() *TableIndex

	Add(c KeyMV)
	Delete(c ayame.Key)

	// neighbor list API
	GetNeighborLists() []*NeighborList
	AddNeighborList(s *NeighborList)

	// util
	String() string
	Size() int
}

type SkipRoutingTable struct {
	km            KeyMV           // self
	NeighborLists []*NeighborList // level 0 to top
}

func NewSkipRoutingTable(km KeyMV) *SkipRoutingTable {
	rt := &SkipRoutingTable{km: km, NeighborLists: []*NeighborList{}}
	rt.ensureHeight(1) // level 0
	return rt
}

func NodesEquals(a, b []KeyMV) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Key().Equals(b[i].Key()) {
			return false
		}
	}
	return true
}

func RoutingTableEquals(t1 RoutingTable, t2 RoutingTable) bool {
	for i := 0; i < len(t1.GetNeighborLists()); i++ {
		// terminate
		if i != 0 && len(t1.GetNeighborLists()[i].Neighbors[LEFT]) == 0 && len(t1.GetNeighborLists()[i].Neighbors[RIGHT]) == 0 {
			return true
		}
		if i != 0 && len(t2.GetNeighborLists()[i].Neighbors[LEFT]) == 0 && len(t2.GetNeighborLists()[i].Neighbors[RIGHT]) == 0 {
			return true
		}
		if !NodesEquals(t1.GetNeighborLists()[i].Neighbors[LEFT], t2.GetNeighborLists()[i].Neighbors[LEFT]) {
			fmt.Printf("%s, %s\n", ayame.SliceString(t1.GetNeighborLists()[i].Neighbors[LEFT]),
				ayame.SliceString(t2.GetNeighborLists()[i].Neighbors[LEFT]))
			return false
		}
		if !NodesEquals(t1.GetNeighborLists()[i].Neighbors[RIGHT], t2.GetNeighborLists()[i].Neighbors[RIGHT]) {
			fmt.Printf("%s, %s\n", ayame.SliceString(t1.GetNeighborLists()[i].Neighbors[RIGHT]),
				ayame.SliceString(t2.GetNeighborLists()[i].Neighbors[RIGHT]))
			return false
		}
	}
	return true
}

func (table *SkipRoutingTable) GetNeighborLists() []*NeighborList {
	return table.NeighborLists
}

func (table *SkipRoutingTable) AddNeighborList(s *NeighborList) {
	table.NeighborLists = append(table.NeighborLists, s)
}

func (table *SkipRoutingTable) KClosestWithKey(key ayame.Key) ([]KeyMV, int) {
	var ret []KeyMV
	var level int
	// find the lowest level
	for i, singleLevel := range table.NeighborLists {
		kn, exists := singleLevel.PickupKNodes(key)
		if exists {
			ret = kn
			level = i
			break
		}
	}
	return ret, level
}

func (table *SkipRoutingTable) KClosest(req *NeighborRequest) ([]KeyMV, int) {
	var ret []KeyMV
	var level int
	var clst []KeyMV
	// find the lowest level
	for i, singleLevel := range table.NeighborLists {
		kn, exists := singleLevel.PickupKNodes(req.Key)
		if exists {
			clst = kn
			level = i
			break
		}
	}
	// filter only if the level is 0
	if level == 0 {
		for _, n := range clst {
			if req.ClosestIndex == nil || isOrderedSimple(req.ClosestIndex.Min, n.Key(), req.Key) ||
				isOrderedSimple(req.Key, n.Key(), req.ClosestIndex.Max) {
				ret = append(ret, n)
			}
		}
		ret = excludeNeighborsInRequest(table.km, ret, req)
		return ret, 0
	}
	return clst, level
}

func (table *SkipRoutingTable) GetFindNodeRequest(k int) *NeighborRequest {
	return &NeighborRequest{
		Key:               table.km.Key(),
		MV:                table.km.MV(),
		ClosestIndex:      table.GetClosestIndex(),
		NeighborListIndex: table.GetTableIndex(),
	}
}

func (table *SkipRoutingTable) GetClosestIndex() *TableIndex {
	// XXX should be optimized because the level must be 0
	clst, lvl := table.KClosestWithKey(table.km.Key())
	if len(clst) == 0 {
		return nil
	} else {
		if lvl == 0 {
			return &TableIndex{Level: lvl, Min: clst[0].Key(), Max: clst[len(clst)-1].Key()}
		} else {
			return nil
		}

	}
}

func (table *SkipRoutingTable) GetTableIndex() []*TableIndex {
	ret := []*TableIndex{}
	for _, singleLevel := range table.NeighborLists {
		var min ayame.Key = nil
		var max ayame.Key = nil
		if singleLevel.hasSufficientNodes(LEFT) {
			last := len(singleLevel.Neighbors[LEFT]) - 1
			min = singleLevel.Neighbors[LEFT][last].Key()
		}
		if singleLevel.hasSufficientNodes(RIGHT) {
			last := len(singleLevel.Neighbors[RIGHT]) - 1
			max = singleLevel.Neighbors[RIGHT][last].Key()
		}
		if min != nil && max != nil { // both must be specified
			ret = append(ret, &TableIndex{Min: min, Max: max, Level: singleLevel.level})
		}
	}
	return ret
}

func excludeNodes(nodes []KeyMV, queried []ayame.Key) []KeyMV {
	ret := []KeyMV{}
	for _, n := range nodes {
		found := false
		for _, m := range queried {
			if n.Key().Equals(m) {
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, n)
		}
	}
	return ret
}

func excludeNeighborsInRequest(self KeyMV, lst []KeyMV, req *NeighborRequest) []KeyMV {
	ir := []ayame.Key{self.Key()}
	ir = append(ir, req.Key)
	if req.ClosestIndex != nil {
		ir = append(ir, req.ClosestIndex.Min)
		ir = append(ir, req.ClosestIndex.Max)
	}
	for _, idx := range req.NeighborListIndex {
		if idx != nil {
			ir = append(ir, idx.Min)
			ir = append(ir, idx.Max)
		}
	}
	return excludeNodes(lst, ir)
}

func appendKeyMVIfMissing(lst []KeyMV, node KeyMV) []KeyMV {
	for _, ele := range lst {
		if ele.Equals(node) {
			return lst
		}
	}
	return append(lst, node)
}

func (table *SkipRoutingTable) AllNeighbors(includeSelf bool, sorted bool) []KeyMV {
	ret := []KeyMV{}
	if sorted {
		if includeSelf {
			ret = []KeyMV{table.km}
		}
		right := []KeyMV{}
		left := []KeyMV{}
		// sorted list from bottom.
		for _, singleLevel := range table.NeighborLists {
			for _, n := range singleLevel.Neighbors[RIGHT] {
				right = appendKeyMVIfMissing(right, n)
			}
			for _, n := range singleLevel.Neighbors[LEFT] {
				left = appendKeyMVIfMissing(left, n)
			}
		}
		for len(right) > 0 || len(left) > 0 {
			if len(right) > 0 {
				ret = appendKeyMVIfMissing(ret, right[0])
				right = right[1:]
			}
			if len(left) > 0 {
				ret = appendKeyMVIfMissing(ret, left[0])
				left = left[1:]
			}
		}
	} else {
		for _, singleLevel := range table.NeighborLists {
			for _, n := range singleLevel.concatenate(includeSelf) {
				ret = appendKeyMVIfMissing(ret, n)
			}
		}
	}
	return ret
}

func (req *NeighborRequest) findIndexWithLevel(level int) *TableIndex {
	for _, ti := range req.NeighborListIndex {
		if ti.Level == level {
			return ti
		}
	}
	return nil
}

func (rts *NeighborList) concatenateWithIndex(req *NeighborRequest, includeSelf bool) []KeyMV {
	ret := []KeyMV{}
	idx := req.findIndexWithLevel(rts.level)
	for _, n := range rts.Neighbors[LEFT] {
		if idx == nil || isOrderedSimple(idx.Min, n.Key(), req.Key) ||
			isOrderedSimple(req.Key, n.Key(), idx.Max) {
			ret = append(ret, n)
		}
	}
	if len(ret) > 0 {
		reverseSlice(ret)
	}
	if includeSelf {
		ret = append(ret, rts.owner)
	}
	for _, n := range rts.Neighbors[RIGHT] {
		if idx == nil || isOrderedSimple(idx.Min, n.Key(), req.Key) ||
			isOrderedSimple(req.Key, n.Key(), idx.Max) {
			ret = append(ret, n)
		}
	}
	if idx != nil {
		ayame.Log.Debugf("req@%d=(%s %s %s),resp=%s\n", idx.Level, idx.Min, req.Key, idx.Max, ayame.SliceString(ret))
	} else {
		ayame.Log.Debugf("@%d,resp=%s\n", rts.level, ayame.SliceString(ret))
	}
	return ret
}

func (table *SkipRoutingTable) Neighbors(req *NeighborRequest) []KeyMV {
	ret := []KeyMV{}
	commonLen := table.km.MV().CommonPrefixLength(req.MV)
	ayame.Log.Debugf("key=%s: %s", table.km.Key(), table)
	for l, singleLevel := range table.NeighborLists {
		if l > commonLen { // no match
			break
		}
		for _, n := range singleLevel.concatenateWithIndex(req, true) {
			ret = appendKeyMVIfMissing(ret, n)
		}
		//fmt.Printf("level %d, table=%d, key=%d, common=%d, can=%s\n", l, table.km.Key(), kmv.Key(), commonLen, ayame.SliceString(ret))
	}
	ret = excludeNeighborsInRequest(table.km, ret, req)
	return ret
}

func (table *SkipRoutingTable) GetCommonNeighbors(mv *ayame.MembershipVector) []KeyMV {
	ret := []KeyMV{}
	commonLen := table.km.MV().CommonPrefixLength(mv)
	ayame.Log.Debugf("key=%s: %s", table.km.Key(), table)
	for l, singleLevel := range table.NeighborLists {
		if l > commonLen { // no match
			break
		}
		for _, n := range singleLevel.concatenate(true) {
			ret = appendKeyMVIfMissing(ret, n)
		}
		//fmt.Printf("level %d, table=%d, key=%d, common=%d, can=%s\n", l, table.km.Key(), kmv.Key(), commonLen, ayame.SliceString(ret))
	}
	return ret
}

func (table *SkipRoutingTable) ensureHeight(level int) {
	nextLevel := len(table.NeighborLists) // if current max is 1, nextLevel is 2
	for i := nextLevel; i <= level; i++ {
		table.NeighborLists = append(table.NeighborLists,
			NewNeighborList(table.km, len(table.NeighborLists)))
	}
}

func (table *SkipRoutingTable) Add(c KeyMV) {
	if table.km.Equals(c) {
		return // cannot add self
	}
	// ensure the height for the matched prefix length
	commonLen := table.km.MV().CommonPrefixLength(c.MV())
	table.ensureHeight(commonLen + 1)
	// add to levels from 0 to common prefix level.
	for i := 0; i < commonLen+1; i++ {
		// trimmed if needed
		table.NeighborLists[i].Add(RIGHT, c)
		table.NeighborLists[i].Add(LEFT, c)
	}
	// trim the height
	/* XXX
	table.neighborLists = funk.Filter(table.neighborLists, func(lv *NeighborList) bool {
		return !lv.hasDuplicatesInLeftsAndRights()
	}).([]*NeighborList)
	*/
}

// Delete an entry from KeyMV slice that matches the key.
// returns modified slice and modified status (true if modified)
func delNode(kms []KeyMV, key ayame.Key) ([]KeyMV, bool) {
	for i := range kms {
		if kms[i].Key().Equals(key) {
			return append(kms[:i], kms[i+1:]...), true
		}
	}
	return kms, false
}

// Delete entries which have specified key
func (table *SkipRoutingTable) Delete(key ayame.Key) {
	if table.km.Key().Equals(key) {
		return // cannot delete self
	}
	for _, levelTable := range table.NeighborLists {
		deleted, modified := delNode(levelTable.Neighbors[LEFT], key)
		if modified {
			levelTable.Neighbors[LEFT] = deleted
		}
		deleted, modified = delNode(levelTable.Neighbors[RIGHT], key)
		if modified {
			levelTable.Neighbors[RIGHT] = deleted
		}
	}
}

//func NewKeyMV(key int, mv *ayame.MembershipVector) *KeyMV {
//return &KeyMV{key: key, mv: mv}
//}

func (table *SkipRoutingTable) Size() int {
	lst := []ayame.Key{}

	for _, levelTable := range table.NeighborLists {
		for _, node := range levelTable.Neighbors[LEFT] {
			lst = ayame.AppendIfMissing(lst, node.Key())
		}
		for _, node := range levelTable.Neighbors[RIGHT] {
			lst = ayame.AppendIfMissing(lst, node.Key())
		}
	}
	return len(lst)
}

/*
func (table *SkipRoutingTable) AllKeys() []int {
	lst := []int{}

	for _, levelTable := range table.NeighborLists {
		for _, node := range levelTable.Neighbors[LEFT] {
			lst = ayame.AppendIfMissing(lst, node.Key())
		}
		for _, node := range levelTable.Neighbors[RIGHT] {
			lst = ayame.AppendIfMissing(lst, node.Key())
		}
	}
	return lst
}*/

func (table *SkipRoutingTable) Height() int {
	return len(table.NeighborLists)
}

func (table *SkipRoutingTable) String() string {
	ret := ""
	for _, sl := range table.NeighborLists {
		ret += sl.String() + "\n"
	}
	return ret
}

//func (km *KeyMV) equals(other *KeyMV) bool {
//return km.key == other.key
//}
/*
func delNode(kms []KeyMV, km KeyMV) []KeyMV {
	for i := range kms {
		if kms[i].Equals(km) {
			return append(kms[:i], kms[i+1:]...)
		}
	}
	return nil
}

func lenLessThanExists(buf [][]KeyMV) bool {
	for _, n := range buf {
		if len(n) < K-1 {
			return true
		}
	}
	return false
}
*/

func minMaxNode(kms []KeyMV) (KeyMV, KeyMV) {
	var max KeyMV = kms[0]
	var min KeyMV = kms[0]
	for _, s := range kms {
		if max.Key().Less(s.Key()) {
			max = s
		}
		if s.Key().Less(min.Key()) {
			min = s
		}
	}
	return min, max
}

func less(base, min, max, x, y ayame.Key) bool {
	if base.Less(min) {
		min = base
	}
	if max.Less(base) {
		max = base
	}
	if x.Equals(max) && y.Equals(min) {
		return true
	}
	if (y.LessOrEquals(base)) && (base.Less(x)) {
		return true
	}
	if (x.LessOrEquals(base)) && (base.Less(y)) {
		return false
	}
	return x.Less(y)
}

// much faster version of SortCircular
func SortC(base ayame.Key, kms []KeyMV) {
	min, max := minMaxNode(kms)
	eNum := len(kms)
	for i := eNum; i > 0; i-- {
		for j := 0; j < i-1; j++ {
			if less(base, min.Key(), max.Key(), kms[j+1].Key(), kms[j].Key()) {
				kms[j], kms[j+1] = kms[j+1], kms[j]
			}
		}
	}
}

/*
func SortCircular(base int, kms []KeyMV) {
	_, max := minMaxNode(kms)
	sort.Slice(kms, func(x, y int) bool {
		var xval, yval int
		xval = kms[x].Key()
		if kms[x].Key() <= base {
			xval += max.Key() + 1
		}
		yval = kms[y].Key()
		if kms[y].Key() <= base {
			yval += max.Key() + 1
		}
		return xval < yval
	})
}*/

// Deprecated: merged to AllNeighbors
func (rt *SkipRoutingTable) GetCloserCandidates() []KeyMV {
	right := []KeyMV{}
	left := []KeyMV{}
	// sorted list from bottom.
	for _, singleLevel := range rt.NeighborLists {
		for _, n := range singleLevel.Neighbors[RIGHT] {
			right = appendKeyMVIfMissing(right, n)
		}
		for _, n := range singleLevel.Neighbors[LEFT] {
			left = appendKeyMVIfMissing(left, n)
		}
	}
	ret := []KeyMV{}
	for len(right) > 0 || len(left) > 0 {
		if len(right) > 0 {
			ret = appendKeyMVIfMissing(ret, right[0])
			right = right[1:]
		}
		if len(left) > 0 {
			ret = appendKeyMVIfMissing(ret, left[0])
			left = left[1:]
		}
	}
	return ret
}

func isOrderedSimple(a, b, c ayame.Key) bool {
	if a.Less(b) && b.Less(c) {
		return true
	}
	if b.Less(c) && c.Less(a) {
		return true
	}
	if c.Less(a) && a.Less(b) {
		return true
	}
	return false
}

func isOrderedInclusive(a, b, c ayame.Key) bool {
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

func isOrdered(start ayame.Key, startInclusive bool, val ayame.Key, end ayame.Key, endInclusive bool) bool {
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

func NewNeighborList(owner KeyMV, level int) *NeighborList {
	var ns [2][]KeyMV
	ns[0] = make([]KeyMV, 0, 10)
	ns[1] = make([]KeyMV, 0, 10)
	return &NeighborList{owner: owner, Neighbors: ns, level: level}
}

func (rts NeighborList) String() string {
	ret := ""
	ret += fmt.Sprintf("Level {%d}: ", rts.level)
	ret += "LEFT=["
	ret += strings.Join(funk.Map(rts.Neighbors[LEFT], func(n KeyMV) string {
		return n.String()
	}).([]string), ",")
	ret += "], RIGHT=["
	ret += strings.Join(funk.Map(rts.Neighbors[RIGHT], func(n KeyMV) string {
		return n.String()
	}).([]string), ",")
	ret += "]"
	return ret
}

func reverseSlice(a []KeyMV) {
	for i, j := 0, len(a)-1; i < j; i, j = i+1, j-1 {
		a[i], a[j] = a[j], a[i]
	}
}

func SortCircularAppend(base ayame.Key, list []KeyMV, elem KeyMV) []KeyMV {
	var ret []KeyMV
	if len(list) == 0 {
		ret = []KeyMV{elem}
	} else if isOrderedSimple(base, elem.Key(), list[0].Key()) { //isOrdered(base, false, elem.Key(), list[0].Key(), false) {
		ret = append([]KeyMV{elem}, list...)
	} else {
		inserted := false
		ret = []KeyMV{}
		for i := 0; i < len(list)-1; i++ {
			if isOrderedSimple(list[i].Key(), elem.Key(), list[i+1].Key()) { //isOrdered(list[i].Key(), false, elem.Key(), list[i+1].Key(), false) {
				ret = append(ret, list[0:i+1]...)
				ret = append(ret, elem)
				ret = append(ret, list[i+1:]...)
				inserted = true
				break
			}
		}
		if !inserted {
			ret = append(list, elem)
		}
	}
	return ret
}

func (rts *NeighborList) Add(d int, u KeyMV) {
	for _, a := range rts.Neighbors[d] {
		if a.Equals(u) {
			return
		}
	}
	//rts.Neighbors[d] = append(rts.Neighbors[d], u)
	//SortC(rts.owner.Key(), rts.Neighbors[d])
	if d == LEFT {
		reverseSlice(rts.Neighbors[d])
	}
	rts.Neighbors[d] = SortCircularAppend(rts.owner.Key(), rts.Neighbors[d], u)
	if d == LEFT {
		reverseSlice(rts.Neighbors[d])
	}
	i := rts.satisfactionIndex(d)
	if i > 0 {
		rts.Neighbors[d] = rts.Neighbors[d][0 : i+1]
	}
}

func (rts *NeighborList) concatenate(includeSelf bool) []KeyMV {
	copied := append([]KeyMV{}, rts.Neighbors[LEFT]...)
	if len(copied) > 0 {
		reverseSlice(copied)
	}
	ret := copied
	if includeSelf {
		ret = append(ret, rts.owner)
	}
	ret = append(ret, rts.Neighbors[RIGHT]...)
	return ret
}

func contains(nodes []KeyMV, node KeyMV) bool {
	for _, n := range nodes {
		if node.Equals(n) {
			return true
		}
	}
	return false
}

func isDisjoint(a, b []KeyMV) bool {
	for _, v := range a {
		if contains(b, v) {
			return false
		}
	}
	return true
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func closestKNodesDisjoint(target ayame.Key, nodes []KeyMV) []KeyMV {
	sortedNodes := uniqueNodes(nodes) //append([]KeyMV{}, nodes...)
	SortC(target, sortedNodes)
	leftLen := min(len(sortedNodes), LEFT_HALF_K)
	rightLen := min(len(sortedNodes)-leftLen, RIGHT_HALF_K)
	lefts := sortedNodes[len(sortedNodes)-leftLen:]
	rights := sortedNodes[:rightLen]
	return append(lefts, rights...)
}

func uniqueNodes(nodes []KeyMV) []KeyMV {
	ret := []KeyMV{}
	for _, n := range nodes {
		ret = appendKeyMVIfMissing(ret, n)
	}
	return ret
}

func (rts *NeighborList) hasDuplicatesInLeftsAndRights() bool {
	return !isDisjoint(rts.Neighbors[RIGHT], rts.Neighbors[LEFT])
}

//func (km *KeyMV) Key() int {
//	return km.key
//}

//func (km *KeyMV) MV() *ayame.MembershipVector {
//	return km.mv
//}

//func (km KeyMV) String() string {
//	return strconv.Itoa(km.key)
//}

func (rts *NeighborList) PickupKNodes(target ayame.Key) ([]KeyMV, bool) {
	nodes := rts.concatenate(true)
	if rts.hasDuplicatesInLeftsAndRights() {
		//ayame.Log.Debugf("%d: picking up KNodes: level=%d, target=%d, nodes=%s\n", rts.owner.Key(), rts.level, target, ayame.SliceString(nodes))
		return closestKNodesDisjoint(target, nodes), true
	} else {
		//ayame.Log.Debugf("%d: picking up KNodes: level=%d, target=%d, nodes=%s\n", rts.owner.Key(), rts.level, target, ayame.SliceString(nodes))
		if len(nodes) < K { // if number of nodes is less than K, return all
			return nodes, true
		}
		for i := LEFT_HALF_K - 1; i < len(nodes)-RIGHT_HALF_K; i++ {
			curNode := nodes[i].Key()
			nextNode := nodes[i+1].Key()
			//ayame.Log.Debugf("cur=%d, next=%d, ordered? %s, %d:%d\n", curNode, nextNode, ayame.SliceString(nodes[i-LEFT_HALF_K+1:i+RIGHT_HALF_K+1]), i-LEFT_HALF_K+1, i+RIGHT_HALF_K+1)
			if isOrdered(curNode, true, target, nextNode, false) {
				return nodes[i-LEFT_HALF_K+1 : i+RIGHT_HALF_K+1], true
			}
		}
		return nil, false // empty
	}
}

func lessThanExists(lst []int, x int) bool {
	for _, v := range lst {
		if v < x {
			return true
		}
	}
	return false
}

// Returns negative value if all
func (rts *NeighborList) satisfactionIndex(d int) int {
	lst := rts.Neighbors[d]
	counts := make([]int, ALPHA)
	for i, n := range lst {
		digit := n.MV().Val[rts.level]
		counts[digit]++
		if !lessThanExists(counts, K-1) { // all is greater than (or equals) k - 1
			return i
		}
	}
	return -1
}

func (rts *NeighborList) hasSufficientNodes(d int) bool {
	return rts.satisfactionIndex(d) > 0
}

const (
	MIN_RT_THRESHOLD = 10
)

func (table *SkipRoutingTable) HasSufficientNeighbors() bool {
	if table.Size() < MIN_RT_THRESHOLD {
		return false
	}
	for _, nl := range table.NeighborLists {
		if !nl.hasDuplicatesInLeftsAndRights() {
			if !nl.hasSufficientNodes(RIGHT) {
				return false
			}
			if !nl.hasSufficientNodes(LEFT) {
				return false
			}
		} else { // first occurance
			return true
		}
	}
	return true
}

func (table *SkipRoutingTable) ExtendRoutingTable(level int) {
	for len(table.NeighborLists) <= level {
		maxLevel := len(table.NeighborLists) - 1
		newLevel := maxLevel + 1
		s := NewNeighborList(table.km, newLevel)
		table.NeighborLists = append(table.NeighborLists, s)
		// normal skip graph doesn't require thiis
		if maxLevel >= 0 {
			for _, n := range append(table.NeighborLists[maxLevel].Neighbors[RIGHT],
				table.NeighborLists[maxLevel].Neighbors[LEFT]...) {
				if n.MV().CommonPrefixLength(table.km.MV()) >= newLevel {
					s.Add(RIGHT, n)
					s.Add(LEFT, n)
				}
			}
		}
	}
}
