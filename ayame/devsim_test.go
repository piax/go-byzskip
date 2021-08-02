package ayame_test

import (
	"container/heap"
	"math/rand"
	"strconv"
	"testing"

	"github.com/piax/go-ayame/ayame"
	ast "github.com/stretchr/testify/assert"
)

func newTestEv(i int, t int64) ayame.Event {
	ev := ayame.NewSchedEvent()
	ev.SetTime(t)
	ev.SetSender(ayame.GetLocalNode(strconv.Itoa(i)))
	ev.SetReceiver(ayame.GetLocalNode(strconv.Itoa(0)))
	return ev
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

	ast.Equal(t, ev.Sender().Id(), "0", "expected 0")
	ast.Equal(t, ev.Time(), int64(1), "expected 1")

	// test adding earliest time.
	heap.Push(&eq, newTestEv(7, 1))
	ev = heap.Pop(&eq).(ayame.Event)
	ast.Equal(t, ev.Sender().Id(), "7", "expected 7")

	var prevTime int64
	prevTime = 0
	for eq.Len() > 0 {
		ev = heap.Pop(&eq).(ayame.Event)
		ast.LessOrEqual(t, prevTime, ev.Time(), "expected smaller number")
		prevTime = ev.Time()
	}

}

func TestAppendIf(t *testing.T) {
	lst := []ayame.Key{}
	lst = ayame.AppendIfMissing(lst, ayame.Int(1))
	lst = ayame.AppendIfMissing(lst, ayame.Int(1))
	lst = ayame.AppendIfMissing(lst, ayame.Int(2))
	ast.Equal(t, len(lst), 2, "expected 2")
}
