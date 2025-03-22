package ayame

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio/pbio"
	"google.golang.org/protobuf/proto"

	"github.com/libp2p/go-msgio"
	ayame "github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p/pb"
	// "github.com/gogo/protobuf/proto"
)

// Node type - a p2p host implementing one or more p2p protocols
type P2PNode struct {
	host.Host // libp2p host
	key       ayame.Key
	name      string
	mv        *ayame.MembershipVector
	converter func(*p2p.Message, *P2PNode, bool) ayame.SchedEvent
	child     ayame.Node
	Cert      []byte
	//CertSign           []byte // for cache
	//CertValidAfter     int64  // for cache
	//CertValidBefore    int64  // for cache
	Validator          func(peer.ID, ayame.Key, string, *ayame.MembershipVector, []byte) bool
	VerifyIntegrity    bool
	DetailedStatistics bool
	KeyInBytes         map[string]int64

	OutBytes int64
	InBytes  int64
	InCount  int64
	OutCount int64
	mutex    sync.Mutex

	protocol protocol.ID

	app interface{}
}

const (
	CONNMGR_LOW          = 100
	CONNMGR_HIGH         = 400
	RECORD_BYTES_PER_KEY = true
)

func New(h host.Host, key ayame.Key, name string, mv *ayame.MembershipVector, cert []byte,
	converter func(*p2p.Message, *P2PNode, bool) ayame.SchedEvent,
	validator func(peer.ID, ayame.Key, string, *ayame.MembershipVector, []byte) bool,
	verifyIntegrity bool, detailedStatistics bool,
	sendMessageProto string) *P2PNode {

	var keyInBytes map[string]int64
	if detailedStatistics {
		keyInBytes = make(map[string]int64)
	} else {
		keyInBytes = nil
	}

	node := &P2PNode{Host: h, key: key, name: name, mv: mv, converter: converter, Validator: validator,
		VerifyIntegrity: verifyIntegrity, DetailedStatistics: detailedStatistics, KeyInBytes: keyInBytes, OutBytes: 0, InBytes: 0, InCount: 0, OutCount: 0,
		protocol: protocol.ID(sendMessageProto)}
	node.SetStreamHandler(protocol.ID(sendMessageProto), node.onReceiveMessage)
	node.Cert = cert
	//sig, va, vb, err := authority.ExtractCert(cert)
	//if err != nil {
	//	ayame.Log.Warningf("failed to extract cert: verification may fail")
	//}
	//node.CertSign = sig
	//node.CertValidAfter = va
	//node.CertValidBefore = vb
	ayame.Log.Debug("node", node)
	return node

}

/*

var USE_QUIC = true // if false, TCP.

func NewNode(ctx context.Context, locator string, key ayame.Key, mv *ayame.MembershipVector,
	priv crypto.PrivKey,
	converter func(*p2p.Message, *P2PNode, bool) ayame.SchedEvent,
	validator func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool,
	verifyIntegrity bool) (*P2PNode, error) {

	listen, err := multiaddr.NewMultiaddr(locator)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	// public key is omitted because libp2p extracts pubkey from priv.GetPublic()
	if priv == nil {
		priv, _, err = crypto.GenerateKeyPair(crypto.Secp256k1, 256)
		if err != nil {
			ayame.Log.Errorf("%s\n", err)
			return nil, err
		}
	}
	var host host.Host

	connmgr, err := connmgr.NewConnManager(
		100, // Lowwater
		400, // HighWater,
		connmgr.WithGracePeriod(time.Minute),
	)
	if err != nil {
		panic(err)
	}
	if USE_QUIC { // XXX future work: option constructor.
		h, err := libp2p.New(
			//ctx,
			libp2p.ListenAddrs(listen),
			libp2p.Identity(priv),
			// support QUIC
			libp2p.Transport(libp2pquic.NewTransport),
			libp2p.ConnectionManager(connmgr),
		)
		if err != nil {
			ayame.Log.Errorf("libp2p.New %s\n", err)
			return nil, err
		}
		host = h
	} else {
		h, err := libp2p.New(
			//ctx,
			libp2p.ListenAddrs(listen),
			libp2p.Identity(priv),
			libp2p.Transport(libp2ptcp.NewTCPTransport),
			libp2p.ConnectionManager(connmgr),
		)
		if err != nil {
			ayame.Log.Errorf("libp2p.New %s\n", err)
			return nil, err
		}
		host = h
	}

	var keyInBytes map[string]int64
	if RECORD_BYTES_PER_KEY {
		keyInBytes = make(map[string]int64)
	} else {
		keyInBytes = nil
	}

	node := &P2PNode{Host: host, key: key, mv: mv, converter: converter, Validator: validator,
		VerifyIntegrity: verifyIntegrity,
		KeyInBytes:      keyInBytes, OutBytes: 0, InBytes: 0, InCount: 0, OutCount: 0}
	node.SetStreamHandler(sendMessageProto, node.onReceiveMessage)

	return node, nil
}
*/

// ayame.Node interface
func (n *P2PNode) Key() ayame.Key {
	return n.key
}

func (n *P2PNode) SetKey(key ayame.Key) {
	n.key = key
}

func (n *P2PNode) Name() string {
	return n.name
}

func (n *P2PNode) SetName(name string) {
	n.name = name
}

func (n *P2PNode) MV() *ayame.MembershipVector {
	return n.mv
}
func (n *P2PNode) SetMV(mv *ayame.MembershipVector) {
	n.mv = mv
}

func (n *P2PNode) String() string {
	return fmt.Sprintf("%s,%s", n.key, n.Addrs())
}

func (n *P2PNode) Id() peer.ID { // Endpoint
	return n.ID()
}

func IfNeededSign(verifyIntegrity bool, cert []byte) []byte {
	if verifyIntegrity {
		return cert
	}
	return nil
}

func (n *P2PNode) Encode() *p2p.Peer {
	return &p2p.Peer{
		Id:         n.ID().String(),
		Addrs:      EncodeAddrs(n.Addrs()),
		Mv:         n.mv.Encode(),
		Key:        n.key.Encode(),
		Name:       n.name,
		Cert:       IfNeededSign(n.VerifyIntegrity, n.Cert),
		Connection: p2p.ConnectionType_CONNECTED, // myself is always connected
	}
}

func (n *P2PNode) Close() error {
	ayame.Log.Debugf("%s's host is closed\n", n.Key())
	return n.Host.Close()
}

func (n *P2PNode) SetApp(app interface{}) {
	n.app = app
}

func (n *P2PNode) App() interface{} {
	return n.app
}

func (n *P2PNode) MessageIdPrefix() string {
	return n.Id().String()
}

func (n *P2PNode) Equals(o ayame.Node) bool {
	return n.Id() == o.Id()
}

func (n *P2PNode) SetChild(c ayame.Node) {
	n.child = c
}

// a stream handler
func (n *P2PNode) onReceiveMessage(s network.Stream) {
	// get request data
	mes := &p2p.Message{}
	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)
	defer s.Close()
	buf, err := r.ReadMsg()
	if err != nil {
		r.ReleaseMsg(buf)
		if err == io.EOF {
			return
		}
		s.Reset()
		ayame.Log.Errorf("%s: %s", n, err)
		return
	}

	ayame.Log.Debugf("receved mes len=%d", len(buf))
	// unmarshal it
	err = proto.Unmarshal(buf, mes)
	if err != nil {
		ayame.Log.Error(err)
		return
	}
	n.mutex.Lock()
	n.InBytes += int64(len(buf))
	n.InCount++
	if n.KeyInBytes != nil {
		n.KeyInBytes[mes.Data.Id] += int64(len(buf))
	}
	n.mutex.Unlock()
	//ayame.Log.Infof("%s: store %s->%s", s.Conn().LocalPeer(), s.Conn().RemotePeer(), s.Conn().RemoteMultiaddr())
	r.ReleaseMsg(buf)
	//ayame.Log.Infof("%s: Received from %s. size=%d\n", s.Conn().LocalPeer(), s.Conn().RemotePeer(), len(buf))
	var valid bool = true
	if n.VerifyIntegrity { // if failed, valid becomes false.
		valid = n.validateMessage(mes, s)
	}
	ev := n.converter(mes, n, valid)
	if ev == nil {
		return // Failed to convert message. Ignore.
	}
	if ev.Sender() == nil {
		return // Failed to convert message. Ignore.
		//panic("sender is nil")
	}
	n.Host.Peerstore().AddAddr(ev.Sender().Id(), s.Conn().RemoteMultiaddr(), peerstore.ConnectedAddrTTL)
	if mes.IsRequest {
		resp := ev.ProcessRequest(context.TODO(), n.child)
		//		resp.SetSender(n.child)
		if resp != nil {
			mes := resp.Encode()
			mes.Sender = n.Encode()
			ayame.Log.Debugf("%s@%s: PROCESSED REQUEST\n", n.Name(), s.Conn().LocalPeer())
			// no sign
			n.sendMsgToStream(s, mes)
		} else {
			ayame.Log.Warningf("Request %s is not processed", mes.Data.Id)
		}
	} else {
		if err := ev.Run(context.TODO(), n.child); err != nil {
			ayame.Log.Error(err)
			return
		}
	}
}

func (n *P2PNode) sign(ev ayame.SchedEvent, sign bool) proto.Message {
	mes := ev.Encode()
	mes.Sender = n.Encode()
	if sign { // if verification conscious
		if ev.Originator().Key().Equals(n.Key()) { // author
			if n.VerifyIntegrity {
				mes.Data.OriginatorSign = nil // ensure empty
				mes.Data.Originator.Connection = p2p.ConnectionType_NOT_CONNECTED
				senderData := mes.Data.SenderAppData // backup
				path := mes.Data.Path                // backup
				ts := mes.Data.Timestamp             // backup
				mes.Data.SenderAppData = ""          // ensure empty
				mes.Data.Path = nil                  // backup
				mes.Data.Timestamp = 0               // ignored
				//ayame.Log.Debugf("%s: AUTHOR %s, sign msg=%v", n, n.Id(), mes.Data)
				mes.Data.OriginatorSign, _ = n.signProtoMessage(mes.Data)
				ayame.Log.Debugf("ORIGINATOR %s signed: %v", n, mes.Data.OriginatorSign)
				mes.Data.SenderAppData = senderData // restore
				mes.Data.Path = path                // restore
				mes.Data.Timestamp = ts
			}
		} //else { // not author
		//ayame.Log.Debugf("NOT AUTHOR. FORWARD msg=%v", mes.Data)
		//}
		if n.VerifyIntegrity {
			mes.SenderSign, _ = n.signProtoMessage(mes)
		}
	}
	return mes
}

// Node API
func (n *P2PNode) Send(ctx context.Context, ev ayame.SchedEvent, sign bool) error {
	ayame.Log.Debugf("sending mes=%v/%s", ev.Receiver().Id(), ev.Receiver().Key())

	id := ev.Receiver().Id()

	s, err := n.NewStream(ctx, id, n.protocol)
	if err != nil {
		ayame.Log.Errorf("%s NewStream to %s: %s\n", n.Key(), id, err)
		return err
	}
	ayame.Log.Debugf("%s NewStream to %s\n", n.Key(), id)
	if err := n.sendMsgToStream(s, n.sign(ev, sign)); err != nil {
		ayame.Log.Errorf("%s send to %s: %s\n", n.Key(), id, err)
		return err
	}
	if ev.IsRequest() {
		n.onReceiveMessage(s)
	}
	ayame.Log.Debugf("sent mes=%v/%s", ev.Receiver().Id(), ev.Receiver().Key())
	return nil
}

// Validate incoming p2p message
// returns true if the message has valid signature
// message: a protobufs go data object
func (n *P2PNode) validateMessage(message *p2p.Message, s network.Stream) bool {
	// store a temp ref to signature and remove it from message data
	data := message.Data

	senderSign := message.SenderSign
	message.SenderSign = nil
	senderBin, err := proto.Marshal(message)
	if err != nil {
		ayame.Log.Error(err, "failed to marshal pb message")
		return false
	}
	message.SenderSign = senderSign

	bakOriginatorSign := data.OriginatorSign
	//bakAuthorPubKey := data.AuthorPubKey
	bakSenderAppData := data.SenderAppData
	bakTimestamp := data.Timestamp
	path := data.Path
	data.Timestamp = 0 // XXX ignored
	data.OriginatorSign = nil
	data.Originator.Connection = p2p.ConnectionType_NOT_CONNECTED
	//data.AuthorPubKey = nil
	data.SenderAppData = ""
	data.Path = nil

	// marshall data without the signature to protobufs3 binary format
	//ayame.Log.Debugf("%s: verifing author message=%v", n, data)
	bin, err := proto.Marshal(data)
	if err != nil {
		ayame.Log.Error(err, "failed to marshal pb message")
		return false
	}

	// restore the message data (for possible future use)
	data.OriginatorSign = bakOriginatorSign
	//data.AuthorPubKey = bakAuthorPubKey
	data.SenderAppData = bakSenderAppData
	data.Timestamp = bakTimestamp
	data.Path = path

	// restore peer id binary format from base58 encoded node id data
	senderId, err := peer.Decode(message.Sender.Id)
	if err != nil {
		ayame.Log.Error(err, "Failed to decode sender node id from base58")
		return false
	}

	authorId, err := peer.Decode(data.Originator.Id)
	if err != nil {
		ayame.Log.Error(err, "Failed to decode author node id from base58")
		return false
	}

	var authorPubKey crypto.PubKey
	if data.OriginatorPubKey != nil {
		apk, err := crypto.UnmarshalPublicKey(data.OriginatorPubKey)
		if err != nil {
			ayame.Log.Error(err, "Failed to extract key from message key data")
			return false
		}
		authorPubKey = apk
	} else {
		ayame.Log.Errorf("no author public key in message %s", message.Data.Id)
		return false
	}

	// verify the data was authored by the signing peer identified by the public key
	// and signature included in the message
	if message.SenderSign != nil {
		senderPubKey := s.Conn().RemotePublicKey()
		if !n.verifyData(senderBin, []byte(message.SenderSign), senderId, senderPubKey) {
			ayame.Log.Errorf("Failed to verify sender's signature")
			return false
		}
	} else {
		ayame.Log.Debugf("no sender sign in message %s", message.Data.Id)
		return false
	}

	if message.Data.OriginatorSign != nil {
		if !n.verifyData(bin, []byte(message.Data.OriginatorSign), authorId, authorPubKey) {
			ayame.Log.Errorf("%s: failed to verify the signature of msgid=%s, author %s", n, message.Data.Id, authorId)
			return false
		}
	} else {
		ayame.Log.Errorf("%s: no author sign in message %s", n, message.Data.Id)
		return false
	}
	ayame.Log.Debugf("%s: succeeded author msgid=%s author=%s, sender=%s", n, message.Data.Id, authorId, senderId)
	return true
}

// sign an outgoing p2p message payload
func (n *P2PNode) signProtoMessage(message proto.Message) ([]byte, error) {
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	return n.signData(data)
}

// sign binary data using the local node's private key
func (n *P2PNode) signData(data []byte) ([]byte, error) {
	key := n.Peerstore().PrivKey(n.ID())
	res, err := key.Sign(data)
	//res, err := authority.Sign(data, n.CertValidAfter, n.CertValidBefore, key)
	return res, err
}

// Verify incoming p2p message data integrity
// data: data to verify
// signature: author signature provided in the message payload
// peerId: author peer id from the message payload
// pubKeyData: author public key from the message payload
func (n *P2PNode) verifyData(data []byte, signature []byte, peerId peer.ID, pubKey crypto.PubKey) bool {

	// extract node id from the provided public key
	idFromKey, err := peer.IDFromPublicKey(pubKey)

	if err != nil {
		ayame.Log.Errorf("Failed to extract peer id from public key")
		return false
	}

	// verify that message author node id matches the provided node public key
	if idFromKey != peerId {
		//ayame.Log.Errorf("Node id=%v and provided public key=%v mismatch", peerId, idFromKey)
		return false
	}

	res, err := pubKey.Verify(data, signature)

	if err != nil {
		ayame.Log.Errorf("Verify error: %s\n", err)
		return false
	}

	return res
}

// helper method - generate message data
// messageId: unique for requests, copied from request to responses
func (n *P2PNode) NewMessage(messageId string, mtype p2p.MessageType,
	originator ayame.Node, originatorSign []byte, originatorPubKey []byte,
	key ayame.Key, mv *ayame.MembershipVector) *p2p.Message {
	// Add protobufs bin data for message author public key
	// this is useful for authenticating  messages forwarded by a node authored by another node
	/*var nodePubKey []byte = nil
	if includePubKey {
		pKey, err := crypto.MarshalPublicKey(n.Peerstore().PubKey(n.ID()))
		if err != nil {
			panic("Failed to get public key for sender from local peer store.")
		}
		nodePubKey = pKey
	}*/

	if originatorPubKey == nil {
		pKey, err := crypto.MarshalPublicKey(n.Peerstore().PubKey(n.ID()))
		if err != nil {
			panic("Failed to get public key for sender from local peer store.")
		}
		originatorPubKey = pKey
	}

	var mvData []byte
	if mv != nil {
		mvData = mv.Encode()
	}
	var keyData *p2p.Key
	if key != nil {
		keyData = key.Encode()
	}
	var originatorPeer *p2p.Peer
	if originator != nil {
		originatorPeer = originator.Encode()
		originatorPeer.Connection = p2p.ConnectionType_NOT_CONNECTED
	} else {
		originatorPeer = n.Encode()
		originatorPeer.Connection = p2p.ConnectionType_NOT_CONNECTED
	}

	originatorPubKey = IfNeededSign(n.VerifyIntegrity, originatorPubKey)
	data := &p2p.MessageData{
		Version:          string(n.protocol),
		Type:             mtype,
		Timestamp:        time.Now().Unix(),
		Id:               messageId,
		Key:              keyData,
		Mv:               mvData,
		Originator:       originatorPeer,
		OriginatorSign:   originatorSign,
		OriginatorPubKey: originatorPubKey}

	return &p2p.Message{Data: data}
}

// XXX context is not used
func (n *P2PNode) sendMsgToStream(s network.Stream, msg proto.Message) error {
	writer := pbio.NewDelimitedWriter(s)
	ayame.Log.Debugf("%s: writing msg", n)
	//fmt.Printf("%s: writing msg %s\n", n, msg)
	err := writer.WriteMsg(msg)
	ayame.Log.Debugf("%s: written msg", n)
	if err != nil {
		ayame.Log.Errorf("%s: write msg %s", n, err)
		s.Reset()
		return err
	}
	// XXX this is just for measurement.
	data, err := proto.Marshal(msg)
	if err != nil {
		ayame.Log.Errorf("%s: write msg %s", n, err)
		return err
	}
	length := int64(len(data))
	n.OutBytes += length
	n.OutCount++
	//fmt.Printf("%s: written msg %s\n", n, msg)

	return nil
}

/*
func (n *P2PNode) sendProtoMessage(ctx context.Context, id peer.ID, p protocol.ID, data proto.Message) error {
	s, err := n.NewStream(context.Background(), id, p)
	if err != nil {
		//ayame.Log.Errorf("%s NewStream to %s: %s\n", n.Key(), id, err)
		return err
	}
	defer func() {
		time.Sleep(time.Duration(100 * time.Millisecond))
		s.Close()
	}()
	return n.sendMsgToStream(ctx, s, data)
}
*/
