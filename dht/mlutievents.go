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

type BSMultiPutEvent struct {
	messageId string
	Record    *pb.Record
	Timestamp int64
	Request   bool // true if request
	ayame.AbstractSchedEvent
}

type BSMultiGetEvent struct {
	messageId string
	Record    []*pb.Record
	Timestamp int64
	Request   bool // true if request
	ayame.AbstractSchedEvent
}

func NewBSMultiPutEvent(sender ayame.Node, messageId string, isRequest bool, record *pb.Record) *BSMultiPutEvent {
	ev := &BSMultiPutEvent{
		messageId:          messageId,
		Record:             record,
		Request:            isRequest,
		Timestamp:          time.Now().Unix(),
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	ev.SetSender(sender)
	ev.SetReceiver(sender) // XXX weird
	return ev
}

func (ue *BSMultiPutEvent) String() string {
	return ue.Sender().String() + "<PUT " + string(ue.Record.GetKey()) + ":" + strconv.Itoa(len(ue.Record.Value)) + ">"
}

func (ue *BSMultiPutEvent) Encode() *pb.Message {
	sender := ue.Sender().(*bs.BSNode).Parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.messageId,
		pb.MessageType_PUT_MULTI_VALUE, sender, nil, nil,
		ue.Receiver().Key(), nil)
	ret.IsRequest = ue.Request
	ret.IsResponse = !ue.Request
	ret.Data.Record = []*pb.Record{ue.Record}
	return ret
}

func (ue *BSMultiPutEvent) Run(ctx context.Context, node ayame.Node) error {
	log.Debugf("running put response handler.")
	if ue.IsResponse() {
		if err := node.App().(AppDHT).HandleMultiPutResEvent(ctx, ue); err != nil {
			return err
		}
		return nil
	}
	return errors.New("protocol implementation error (this should not be happen)")
}

func (ue *BSMultiPutEvent) IsRequest() bool {
	return ue.Request
}

func (ue *BSMultiPutEvent) IsResponse() bool {
	return !ue.Request
}

func (ue *BSMultiPutEvent) MessageId() string {
	return ue.messageId
}

func (ev *BSMultiPutEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	log.Debugf("running multi-put request handler on %s@%v", node.Name(), node.Id())
	return node.App().(AppDHT).HandleMultiPutRequest(ctx, ev)
}

func NewBSMultiGetEvent(sender ayame.Node, messageId string, isRequest bool, rec []*pb.Record) *BSMultiGetEvent {
	ev := &BSMultiGetEvent{
		messageId:          messageId,
		Record:             rec,
		Request:            isRequest,
		Timestamp:          time.Now().Unix(), // XXX no meaning
		AbstractSchedEvent: *ayame.NewSchedEvent(sender, nil, nil)}
	ev.SetSender(sender)
	ev.SetReceiver(sender) // XXX weird
	return ev
}

func (ue *BSMultiGetEvent) String() string {
	if len(ue.Record) == 0 {
		return ue.Sender().String() + "<GET empty>"
	}
	return ue.Sender().String() + "<GET " + string(ue.Record[0].GetKey()) + ">"
}

func (ue *BSMultiGetEvent) Encode() *pb.Message {
	sender := ue.Sender().(*bs.BSNode).Parent.(*p2p.P2PNode)
	ret := sender.NewMessage(ue.messageId,
		pb.MessageType_GET_MULTI_VALUE, sender, nil, nil,
		ue.Receiver().Key(), nil)
	ret.IsRequest = ue.Request
	ret.IsResponse = !ue.Request
	ret.Data.Record = ue.Record
	return ret
}

func (ue *BSMultiGetEvent) Run(ctx context.Context, node ayame.Node) error {
	if ue.IsResponse() {
		log.Debugf("running get response handler.")
		if err := node.App().(AppDHT).HandleMultiGetResEvent(ctx, ue); err != nil {
			return err
		}
		return nil
	}
	return errors.New("protocol implementation error (this should not be happen)")
}

func (ue *BSMultiGetEvent) IsRequest() bool {
	return ue.Request
}

func (ue *BSMultiGetEvent) IsResponse() bool {
	return !ue.Request
}

func (ue *BSMultiGetEvent) MessageId() string {
	return ue.messageId
}

func (ev *BSMultiGetEvent) ProcessRequest(ctx context.Context, node ayame.Node) ayame.SchedEvent {
	log.Debugf("running get request handler.")
	return node.App().(AppDHT).HandleMultiGetRequest(ctx, ev)
}
