package dht

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
	bs "github.com/piax/go-byzskip/byzskip"
)

type BSPutEvent struct {
	messageId string
	Record    *pb.Record
	Timestamp int64
	Request   bool // true if request
	ayame.AbstractSchedEvent
}

type BSGetEvent struct {
	messageId string
	Record    *pb.Record
	Timestamp int64
	Request   bool // true if request
	ayame.AbstractSchedEvent
}

func NewBSPutEvent(sender ayame.Node, messageId string, isRequest bool, record *pb.Record) *BSPutEvent {
	ev := &BSPutEvent{
		messageId:          messageId,
		Record:             record,
		Request:            isRequest,
		Timestamp:          time.Now().Unix(),
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	ev.SetSender(sender)
	ev.SetReceiver(sender) // XXX weird
	return ev
}

func (ue *BSPutEvent) String() string {
	return ue.Sender().String() + "<PUT " + string(ue.Record.GetKey()) + ":" + strconv.Itoa(len(ue.Record.Value)) + ">"
}

func (ue *BSPutEvent) Encode() *pb.Message {
	sender := ue.Sender().(*bs.BSNode).Parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.messageId,
		pb.MessageType_PUT_VALUE, sender, nil, nil,
		ue.Receiver().Key(), nil)
	ret.IsRequest = ue.Request
	ret.IsResponse = !ue.Request
	ret.Data.Record = ue.Record
	return ret
}

func (ue *BSPutEvent) Run(ctx context.Context, node ayame.Node) error {
	ayame.Log.Debugf("running put response handler.")
	if ue.IsResponse() {
		if err := handlePutResEvent(ctx, node.App().(*BSDHT), ue); err != nil {
			return err
		}
		return nil
	}
	return errors.New("protocol implementation error (this should not be happen)")
}

func (ue *BSPutEvent) IsRequest() bool {
	return ue.Request
}

func (ue *BSPutEvent) IsResponse() bool {
	return !ue.Request
}

func (ue *BSPutEvent) MessageId() string {
	return ue.messageId
}

func (ev *BSPutEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	ayame.Log.Debugf("running put request handler.")
	return handlePutRequest(ctx, node.App().(*BSDHT), ev)
}

func NewBSGetEvent(sender ayame.Node, messageId string, isRequest bool, rec *pb.Record) *BSGetEvent {
	ev := &BSGetEvent{
		messageId:          messageId,
		Record:             rec,
		Request:            isRequest,
		Timestamp:          time.Now().Unix(), // XXX no meaning
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	ev.SetSender(sender)
	ev.SetReceiver(sender) // XXX weird
	return ev
}

func (ue *BSGetEvent) String() string {
	return ue.Sender().String() + "<GET " + string(ue.Record.GetKey()) + ">"
}

func (ue *BSGetEvent) Encode() *pb.Message {
	sender := ue.Sender().(*bs.BSNode).Parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.messageId,
		pb.MessageType_GET_VALUE, sender, nil, nil,
		ue.Receiver().Key(), nil)
	ret.IsRequest = ue.Request
	ret.IsResponse = !ue.Request
	ret.Data.Record = ue.Record
	return ret
}

func (ue *BSGetEvent) Run(ctx context.Context, node ayame.Node) error {
	if ue.IsResponse() {
		ayame.Log.Debugf("running get response handler.")
		if err := handleGetResEvent(ctx, node.App().(*BSDHT), ue); err != nil {
			return err
		}
		return nil
	}
	return errors.New("protocol implementation error (this should not be happen)")
}

func (ue *BSGetEvent) IsRequest() bool {
	return ue.Request
}

func (ue *BSGetEvent) IsResponse() bool {
	return !ue.Request
}

func (ue *BSGetEvent) MessageId() string {
	return ue.messageId
}

func (ev *BSGetEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	ayame.Log.Debugf("running get request handler.")
	return handleGetRequest(ctx, node.App().(*BSDHT), ev)
}
