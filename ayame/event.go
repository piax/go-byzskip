package ayame

import (
	"context"
	"fmt"

	p2p "github.com/piax/go-ayame/ayame/p2p/pb"
)

type Event interface {
	Author() Node
	AuthorSign() []byte
	AuthorPubKey() []byte
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
	SetRequest(bool)
	IsRequest() bool
	IsResponse() bool
}

type AbstractEvent struct {
	author       Node // the origin (creator) of this event.
	authorSign   []byte
	authorPubKey []byte
	sender       Node
	receiver     Node
	sendTime     int64
	vTime        int64
	isVerified   bool
	isRequest    bool
}

func (ev *AbstractEvent) Author() Node {
	return ev.author
}

func (ev *AbstractEvent) AuthorSign() []byte {
	return ev.authorSign
}

func (ev *AbstractEvent) AuthorPubKey() []byte {
	return ev.authorPubKey
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

func (ev *AbstractEvent) SetRequest(v bool) {
	ev.isRequest = v
}

func (ev *AbstractEvent) IsRequest() bool {
	return ev.isRequest
}

func (ev *AbstractEvent) IsResponse() bool {
	return false
}

func NewEvent(author Node, authorSign []byte, authorPubKey []byte) *AbstractEvent {
	return &AbstractEvent{author: author, authorSign: authorSign, authorPubKey: authorPubKey, sender: nil, receiver: nil, isVerified: false, isRequest: false, sendTime: -1, vTime: -1}
}

func NewEventNoAuthor() *AbstractEvent {
	return &AbstractEvent{sender: nil, receiver: nil, isVerified: false, isRequest: false, sendTime: -1, vTime: -1}
}

type SchedEvent interface {
	SetJob(job func())
	Job() func()
	Run(ctx context.Context, node Node)
	ProcessRequest(ctx context.Context, node Node) SchedEvent
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

func NewSchedEvent(author Node, authorSign []byte, authorPubKey []byte) *AbstractSchedEvent {
	return &AbstractSchedEvent{AbstractEvent: *NewEvent(author, authorSign, authorPubKey), job: nil, isCanceled: false}
}

func NewSchedEventWithJob(job func()) *AbstractSchedEvent {
	return &AbstractSchedEvent{AbstractEvent: *NewEventNoAuthor(), job: job, isCanceled: false}
}

func (aj *AsyncJobEvent) Run(node Node) {
	ch := make(chan bool)
	aj.asyncJob(ch)
	<-ch // wait for the job
}

func (se *AbstractSchedEvent) Run(ctx context.Context, n Node) {
	se.Job()()
	//n.Yield(ctx)
}

func (se *AbstractSchedEvent) ProcessRequest(ctx context.Context, n Node) SchedEvent {
	//se.Job()(se, node)
	return nil
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
