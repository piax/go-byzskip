package ayame

import "container/heap"

type EventExecutor struct {
	scheduled  EventQueue
	running    bool
	time       int64
	finishTime int64
	finishCh   chan bool
	EventCount int
}

var GlobalEventExecutor = NewEventExecutor()
var NETWORK_LATENCY = int64(1)

func NewEventExecutor() *EventExecutor {
	return &EventExecutor{scheduled: make(EventQueue, 0), running: false, time: 0, finishTime: 0, finishCh: make(chan bool), EventCount: 0}
}

func (ee *EventExecutor) Reset() {
	ee.EventCount = 0
}

func (ee *EventExecutor) RunForever() {
	ee.running = true
	for ee.scheduled.Len() > 0 && ee.running {
		sev := heap.Pop(&ee.scheduled).(SchedEvent)
		//Log.Debugf("got next event time:%d\n", sev.Time())
		if sev.Receiver() != nil {
			ee.EventCount++
		}
		ee.time = sev.Time()
		if !sev.IsCanceled() {
			n := sev.Receiver()
			sev.Run(n)
		}
		if ee.finishTime <= ee.time {
			ee.running = false
		}
	}
	ee.finishCh <- true
}

func (ee *EventExecutor) RegisterEvent(ev SchedEvent, latency int64) error {
	ev.SetSendTime(ee.time)
	ev.SetTime(ee.time + latency)
	//ee.scheduled.Push(ev)
	heap.Push(&ee.scheduled, ev)
	return nil
}

func (ee *EventExecutor) Stop() {
	ee.running = false
}

func (ee *EventExecutor) AwaitFinish() bool {
	return <-ee.finishCh
}

func (ee *EventExecutor) Sim(simTime int64, verbose bool) {
	ee.finishTime = simTime
	go ee.RunForever()
}
