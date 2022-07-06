package main

import (
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	"github.com/piax/go-byzskip/ayame"
)

// XXX not used yet.

type KADUnicastEvent struct {
	targetKey    kbucket.ID
	messageId    int
	path         []*KADNode
	path_lengths []int
	hop          int
	children     []*KADUnicastEvent
	findChannel  chan []*KADNode // channel to wait for the children
	root         *KADUnicastEvent
	// root only
	results                    []*KADNode
	channel                    chan bool
	numberOfMessages           int
	numberOfDuplicatedMessages int
	finishTime                 int64
	ayame.AbstractSchedEvent
}

var nextMessageId int = 0

func NewKADUnicastEvent(receiver *KADNode, target kbucket.ID) *KADUnicastEvent {
	nextMessageId++
	ev := &KADUnicastEvent{
		targetKey:   target,
		messageId:   nextMessageId,
		path:        []*KADNode{receiver},
		hop:         0,
		children:    []*KADUnicastEvent{},
		findChannel: make(chan []*KADNode),
		// only for root
		results:                    []*KADNode{},
		path_lengths:               []int{},
		channel:                    make(chan bool),
		numberOfMessages:           0,
		numberOfDuplicatedMessages: 0,
		finishTime:                 0,
		AbstractSchedEvent:         *ayame.NewSchedEvent(receiver, nil, nil)}
	ev.root = ev
	ev.SetSender(receiver)
	ev.SetReceiver(receiver)
	return ev
}
