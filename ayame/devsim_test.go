package ayame_test

import (
	"container/heap"
	"fmt"
	"math/rand"
	"testing"

	"github.com/piax/go-byzskip/ayame"
	ast "github.com/stretchr/testify/assert"
)

func newTestEv(i int, t int64) ayame.Event {
	ev := ayame.NewSchedEvent(nil, nil, nil)
	ev.SetTime(t)
	ev.SetSender(ayame.NewLocalNode(ayame.IntKey(i), nil))
	ev.SetReceiver(ayame.NewLocalNode(ayame.IntKey(0), nil))
	return ev
}

func TestTopmost(t *testing.T) {
	times := 10
	num := 10000
	length := 1
	sum := 0
	rand.Seed(10)
	lens := make(map[int]int)
	lvls := make(map[int]int)
	lvl1times := 0
	for j := 0; j < times; j++ {
		mvs := make([]*ayame.MembershipVector, num)
		for i := 0; i < num; i++ {
			mvs[i] = ayame.NewMembershipVector(2)
		}
		lvl := 1
		for {
			count := 0
			for i := 1; i < num; i++ {
				if mvs[0].CommonPrefixLength((mvs[i])) >= lvl {
					count++
				}
			}
			//lens := make(map[int]int)
			if count <= length {
				if lvl == 1 {
					lens[count]++
					lvl1times++
				}
				lvls[lvl]++
				sum += lvl
				break
			}
			lvl++
		}
	}
	for i, c := range lens {
		fmt.Println("len", i, float64(c)/float64(lvl1times))
	}
	for i, c := range lvls {
		fmt.Println(i, float64(c)/float64(times))
	}
	fmt.Println(float64(sum) / float64(times))
}

func TestEventQueue(t *testing.T) {

	eq := make(ayame.EventQueue, 0)
	rand.Seed(0)

	testEvTime := []int64{1, 2, 2, 3, 4, 34, 98}

	for i, t := range testEvTime {
		ev := newTestEv(i, t)
		heap.Push(&eq, ev)
	}
	ev := heap.Pop(&eq).(ayame.Event)

	ast.Equal(t, ev.Sender().String(), "0", "expected 0")
	ast.Equal(t, ev.Time(), int64(1), "expected 1")

	// test adding earliest time.
	heap.Push(&eq, newTestEv(7, 1))
	ev = heap.Pop(&eq).(ayame.Event)
	ast.Equal(t, ev.Sender().String(), "7", "expected 7")

	var prevTime int64
	prevTime = 0
	for eq.Len() > 0 {
		ev = heap.Pop(&eq).(ayame.Event)
		ast.LessOrEqual(t, prevTime, ev.Time(), "expected smaller number")
		prevTime = ev.Time()
	}

}

func TestAppendAbsent(t *testing.T) {
	lst := []ayame.IntKey{}
	lst = ayame.AppendIfAbsent(lst, ayame.IntKey(1))
	lst = ayame.AppendIfAbsent(lst, ayame.IntKey(1))
	lst = ayame.AppendIfAbsent(lst, ayame.IntKey(2))
	ast.Equal(t, len(lst), 2, "expected 2")
}
