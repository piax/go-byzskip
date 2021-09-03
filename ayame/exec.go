package ayame

import (
	"container/heap"
	"context"
)

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
	ee.finishCh = make(chan bool)
	ee.time = 0
}

func (ee *EventExecutor) RunForever() {
	ee.running = true
	len := ee.scheduled.Len()

	for len > 0 && ee.running {
		sev := heap.Pop(&ee.scheduled).(SchedEvent)
		if sev.Receiver() != nil { // not a timeout event
			ee.EventCount++
		}
		ee.time = sev.Time()
		if !sev.IsCanceled() {
			n := sev.Receiver()
			if sev.IsRequest() {
				resp := sev.ProcessRequest(context.TODO(), n)
				resp.SetSender(n)
				resp.SetReceiver(sev.Sender())
				ee.RegisterEvent(resp, NETWORK_LATENCY)
			} else {
				sev.Run(context.TODO(), n)
			}
		}
		if ee.finishTime <= ee.time {
			ee.running = false
		}
		len = ee.scheduled.Len()
	}
	ee.finishCh <- true
}

func (ee *EventExecutor) RegisterEvent(ev SchedEvent, latency int64) error {
	ev.SetSendTime(ee.time)
	ev.SetTime(ee.time + latency)
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
