package ayame

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	//libp2pquic "github.com/libp2p/go-libp2p-quic-transport"

	//libp2pquic "github.com/libp2p/go-libp2p-quic-transport"

	"github.com/libp2p/go-msgio"
	"github.com/libp2p/go-msgio/protoio"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p/pb"

	"github.com/gogo/protobuf/proto"
)

// protocol version
const Version = "/ayame/0.0.0"

// Node type - a p2p host implementing one or more p2p protocols
type P2PNode struct {
	host.Host       // libp2p host
	key             ayame.Key
	mv              *ayame.MembershipVector
	converter       func(*p2p.Message, *P2PNode, bool) ayame.SchedEvent
	child           ayame.Node
	Cert            []byte
	Validator       func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool
	VerifyIntegrity bool
	KeyInBytes      map[string]int64

	OutBytes int64
	InBytes  int64
	InCount  int64
	OutCount int64
	mutex    sync.Mutex

	app interface{}
}

const (
	sendMessageProto     = "/msg/0.0.0"
	CONNMGR_LOW          = 100
	CONNMGR_HIGH         = 400
	RECORD_BYTES_PER_KEY = true
)

func New(h host.Host, key ayame.Key, mv *ayame.MembershipVector, cert []byte,
	converter func(*p2p.Message, *P2PNode, bool) ayame.SchedEvent,
	validator func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool,
	verifyIntegrity bool) *P2PNode {

	var keyInBytes map[string]int64
	if RECORD_BYTES_PER_KEY {
		keyInBytes = make(map[string]int64)
	} else {
		keyInBytes = nil
	}

	node := &P2PNode{Host: h, key: key, mv: mv, converter: converter, Validator: validator,
		VerifyIntegrity: verifyIntegrity, KeyInBytes: keyInBytes, OutBytes: 0, InBytes: 0, InCount: 0, OutCount: 0}
	node.SetStreamHandler(sendMessageProto, node.onReceiveMessage)
	node.Cert = cert
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
		Id:         peer.Encode(n.ID()),
		Addrs:      EncodeAddrs(n.Addrs()),
		Mv:         n.mv.Encode(),
		Key:        n.key.Encode(),
		Cert:       IfNeededSign(n.VerifyIntegrity, n.Cert), // XXX not yet
		Connection: p2p.ConnectionType_CONNECTED,            // myself is always connected
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

func (n *P2PNode) SetChild(c ayame.Node) {
	n.child = c
}

func NewKey(key *p2p.Key) ayame.Key {
	switch key.Type {
	case p2p.KeyType_FLOAT:
		return ayame.NewFloatKeyFromBytes(key.Body)
	case p2p.KeyType_INT:
		return ayame.NewIntKeyFromBytes(key.Body)
	case p2p.KeyType_STRING:
		return ayame.NewStringKeyFromBytes(key.Body)
	case p2p.KeyType_ID:
		return ayame.NewIdKeyFromBytes(key.Body)
	}
	return nil
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

	// unmarshal it
	err = proto.Unmarshal(buf, mes)
	if err != nil {
		ayame.Log.Error(err)
		return
	}
	n.mutex.Lock()
	n.InBytes += int64(len(buf))
	n.InCount++
	n.KeyInBytes[mes.Data.Id] += int64(len(buf))
	n.mutex.Unlock()
	//ayame.Log.Infof("%s: store %s->%s", s.Conn().LocalPeer(), s.Conn().RemotePeer(), s.Conn().RemoteMultiaddr())
	r.ReleaseMsg(buf)
	//ayame.Log.Infof("%s: Received from %s. size=%d\n", s.Conn().LocalPeer(), s.Conn().RemotePeer(), len(buf))
	var valid bool = true
	if n.VerifyIntegrity { // if failed, valid becomes false.
		valid = n.authenticateMessage(mes, s)
	}
	ev := n.converter(mes, n, valid)
	n.Host.Peerstore().AddAddr(ev.Sender().Id(), s.Conn().RemoteMultiaddr(), peerstore.PermanentAddrTTL)
	if mes.IsRequest {
		resp := ev.ProcessRequest(context.TODO(), n.child)
		//		resp.SetSender(n.child)
		mes := resp.Encode()
		mes.Sender = n.Encode()
		ayame.Log.Debugf("%s: PROCESSED REQUEST. response size=%d\n", s.Conn().LocalPeer(), mes.Size())
		// no sign
		n.sendMsgToStream(context.TODO(), s, mes)
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
		if ev.Author().Key().Equals(n.Key()) { // author
			if n.VerifyIntegrity {
				mes.Data.AuthorSign = nil // ensure empty
				mes.Data.Author.Connection = p2p.ConnectionType_NOT_CONNECTED
				senderData := mes.Data.SenderAppData // backup
				path := mes.Data.Path                // backup
				ts := mes.Data.Timestamp             // backup
				mes.Data.SenderAppData = ""          // ensure empty
				mes.Data.Path = nil                  // backup
				mes.Data.Timestamp = 0               // ignored
				//ayame.Log.Debugf("%s: AUTHOR %s, sign msg=%v", n, n.Id(), mes.Data)
				mes.Data.AuthorSign, _ = n.signProtoMessage(mes.Data)
				//ayame.Log.Debugf("AUTHOR %s signed: %v", n, mes.Data.AuthorSign)
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

	s, err := n.NewStream(ctx, id, sendMessageProto)
	if err != nil {
		//ayame.Log.Errorf("%s NewStream to %s: %s\n", n.Key(), id, err)
		return err
	}
	/* close is not needed by using connmgr */
	/*defer func() {
		// XXX too fast close may lose a packet
		time.Sleep(time.Duration(100 * time.Millisecond))
		s.Close()
	}()*/
	n.sendMsgToStream(ctx, s, n.sign(ev, sign))
	if ev.IsRequest() {
		n.onReceiveMessage(s)
	}
	return nil
}

// Authenticate incoming p2p message
// message: a protobufs go data object
func (n *P2PNode) authenticateMessage(message *p2p.Message, s network.Stream) bool {
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

	bakAuthorSign := data.AuthorSign
	//bakAuthorPubKey := data.AuthorPubKey
	bakSenderAppData := data.SenderAppData
	bakTimestamp := data.Timestamp
	path := data.Path
	data.Timestamp = 0 // XXX ignored
	data.AuthorSign = nil
	data.Author.Connection = p2p.ConnectionType_NOT_CONNECTED
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
	data.AuthorSign = bakAuthorSign
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

	authorId, err := peer.Decode(data.Author.Id)
	if err != nil {
		ayame.Log.Error(err, "Failed to decode author node id from base58")
		return false
	}

	var authorPubKey crypto.PubKey
	if data.AuthorPubKey != nil {
		apk, err := crypto.UnmarshalPublicKey(data.AuthorPubKey)
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

	if message.Data.AuthorSign != nil {
		if !n.verifyData(bin, []byte(message.Data.AuthorSign), authorId, authorPubKey) {
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
	author ayame.Node, authorSign []byte, authorPubKey []byte,
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

	if authorPubKey == nil {
		pKey, err := crypto.MarshalPublicKey(n.Peerstore().PubKey(n.ID()))
		if err != nil {
			panic("Failed to get public key for sender from local peer store.")
		}
		authorPubKey = pKey
	}

	var mvData []byte
	if mv != nil {
		mvData = mv.Encode()
	}
	var keyData *p2p.Key
	if key != nil {
		keyData = key.Encode()
	}
	var authorPeer *p2p.Peer
	if author != nil {
		authorPeer = author.Encode()
		authorPeer.Connection = p2p.ConnectionType_NOT_CONNECTED
	} else {
		authorPeer = n.Encode()
		authorPeer.Connection = p2p.ConnectionType_NOT_CONNECTED
	}

	authorPubKey = IfNeededSign(n.VerifyIntegrity, authorPubKey)
	data := &p2p.MessageData{
		Version:      Version,
		Type:         mtype,
		Timestamp:    time.Now().Unix(),
		Id:           messageId,
		Key:          keyData,
		Mv:           mvData,
		Author:       authorPeer,
		AuthorSign:   authorSign,
		AuthorPubKey: authorPubKey}

	return &p2p.Message{Data: data}

}

func (n *P2PNode) sendMsgToStream(ctx context.Context, s network.Stream, msg proto.Message) error {
	writer := protoio.NewDelimitedWriter(s)
	err := writer.WriteMsg(msg)
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
	//	ayame.Log.Infof("%s: written msg %s", n, msg)

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
