package ayame

import (
	"fmt"

	p2p "github.com/piax/go-ayame/ayame/p2p/pb"
)

type Event interface {
	Sender() Node
	SetSender(Node)
	Receiver() Node
	SetReceiver(Node)
	SendTime() int64
	SetSendTime(int64)
	Time() int64
	SetTime(int64)
	Encode() *p2p.Message
	SetVerified(bool)
	IsVerified() bool
}

type AbstractEvent struct {
	sender     Node
	receiver   Node
	sendTime   int64
	vTime      int64
	isVerified bool
}

func (ev *AbstractEvent) Sender() Node {
	return ev.sender
}

func (ev *AbstractEvent) SetSender(n Node) {
	ev.sender = n
}

func (ev *AbstractEvent) Receiver() Node {
	return ev.receiver
}

func (ev *AbstractEvent) SetReceiver(n Node) {
	ev.receiver = n
}

func (ev *AbstractEvent) SendTime() int64 {
	return ev.sendTime
}

func (ev *AbstractEvent) SetSendTime(t int64) {
	ev.sendTime = t
}

func (ev *AbstractEvent) Time() int64 {
	return ev.vTime
}

func (ev *AbstractEvent) SetTime(t int64) {
	ev.vTime = t
}

func (ev *AbstractEvent) SetVerified(v bool) {
	ev.isVerified = v
}

func (ev *AbstractEvent) IsVerified() bool {
	return ev.isVerified
}

func NewEvent() *AbstractEvent {
	return &AbstractEvent{sender: nil, receiver: nil, sendTime: -1, vTime: -1}
}

type SchedEvent interface {
	SetJob(job func())
	Job() func()
	Run(node Node)
	SetCanceled(c bool)
	IsCanceled() bool
	Event
}

type AbstractSchedEvent struct {
	//job        func(se SchedEvent, node Node)
	job        func()
	isCanceled bool
	AbstractEvent
}

type AsyncJobEvent struct {
	asyncJob func(chan bool)
	AbstractSchedEvent
}

//func (n LocalNode) Sched(delay int64, job func(node Node)) SchedEvent {
//ev := NewSchedEvent(job)
//GlobalEventExecutor.RegisterEvent(ev, delay)
//return ev
//}

func NewSchedEvent() *AbstractSchedEvent {
	return &AbstractSchedEvent{AbstractEvent: *NewEvent(), job: nil, isCanceled: false}
}

func NewSchedEventWithJob(job func()) *AbstractSchedEvent {
	return &AbstractSchedEvent{AbstractEvent: *NewEvent(), job: job, isCanceled: false}
}

func NewAsyncJobEvent(job func(chan bool)) *AsyncJobEvent {
	return &AsyncJobEvent{AbstractSchedEvent: *NewSchedEvent(), asyncJob: job}
}

func (aj *AsyncJobEvent) Run(node Node) {
	ch := make(chan bool)
	aj.asyncJob(ch)
	<-ch // wait for the job
}

func (se *AbstractSchedEvent) Run(node Node) {
	//se.Job()(se, node)
	se.Job()()
}

func (se *AbstractSchedEvent) IsCanceled() bool {
	return se.isCanceled
}

func (se *AbstractSchedEvent) SetCanceled(c bool) {
	se.isCanceled = c
}

//func (se *AbstractSchedEvent) Job() func(se SchedEvent, node Node) {
func (se *AbstractSchedEvent) Job() func() {
	return se.job
}

//func (se *AbstractSchedEvent) SetJob(j func(se SchedEvent, node Node)) {
func (se *AbstractSchedEvent) SetJob(j func()) {
	se.job = j
}

func (se *AbstractSchedEvent) Cancel() {
	se.isCanceled = true
}

func (ev *AbstractSchedEvent) String() string {
	return fmt.Sprintf("%d:%s->%s", ev.vTime, ev.sender, ev.receiver)
}

func (ev *AbstractSchedEvent) Encode() *p2p.Message {
	return nil
}
