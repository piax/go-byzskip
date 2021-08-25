package ayame

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	"github.com/libp2p/go-msgio"
	"github.com/libp2p/go-msgio/protoio"
	"github.com/multiformats/go-multiaddr"
	"github.com/piax/go-ayame/ayame"
	p2p "github.com/piax/go-ayame/ayame/p2p/pb"

	"github.com/gogo/protobuf/proto"
)

// protocol version
const Version = "/go-ayame/message/0.0.0"

// Node type - a p2p host implementing one or more p2p protocols
type P2PNode struct {
	host.Host // libp2p host
	key       ayame.Key
	mv        *ayame.MembershipVector
	converter func(*p2p.Message, *P2PNode, bool) ayame.SchedEvent
	child     ayame.Node
	Cert      []byte
	Validator func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool

	OutBytes int64
	InBytes  int64
	InCount  int64
	mutex    sync.Mutex
}

const (
	sendMessageProto = "/msg/0.0.0"
)

func NewNode(ctx context.Context, locator string, key ayame.Key, mv *ayame.MembershipVector,
	converter func(*p2p.Message, *P2PNode, bool) ayame.SchedEvent,
	validator func(peer.ID, ayame.Key, *ayame.MembershipVector, []byte) bool) (*P2PNode, error) {

	listen, err := multiaddr.NewMultiaddr(locator)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	// public key is omitted because libp2p extracts pubkey from priv.GetPublic()
	priv, _, err := crypto.GenerateKeyPair(crypto.Secp256k1, 256)
	if err != nil {
		ayame.Log.Errorf("%s\n", err)
		return nil, err
	}
	host, err := libp2p.New(
		ctx,
		libp2p.ListenAddrs(listen),
		libp2p.Identity(priv),
		// support QUIC
		libp2p.Transport(libp2pquic.NewTransport),
	)
	if err != nil {
		ayame.Log.Errorf("libp2p.New %s\n", err)
		return nil, err
	}
	host.ID()
	node := &P2PNode{Host: host, key: key, mv: mv, converter: converter, Validator: validator, OutBytes: 0, InBytes: 0, InCount: 0}
	node.SetStreamHandler(sendMessageProto, node.onReceiveMessage)

	return node, nil
}

// ayame.Node interface
func (n *P2PNode) Key() ayame.Key {
	return n.key
}
func (n *P2PNode) MV() *ayame.MembershipVector {
	return n.mv
}
func (n *P2PNode) String() string {
	return fmt.Sprintf("%s,%s", n.key, n.Addrs())
}

func (n *P2PNode) Id() peer.ID { // Endpoint
	return n.ID()
}

func IfNeededSign(cert []byte) []byte {
	if ayame.SecureKeyMV {
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
		Cert:       IfNeededSign(n.Cert),         // XXX not yet
		Connection: p2p.ConnectionType_CONNECTED, // myself is always connected
	}
}

func (n *P2PNode) Close() {
	n.Host.Close()
	ayame.Log.Debugf("%s's host is closed\n", n.Key())
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
		panic("StringKey not implemented yet.")
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
		ayame.Log.Error(err)
		return
	}
	n.mutex.Lock()
	n.InBytes += int64(len(buf))
	n.InCount++
	n.mutex.Unlock()
	//ayame.Log.Infof("%s: store %s->%s", s.Conn().LocalPeer(), s.Conn().RemotePeer(), s.Conn().RemoteMultiaddr())

	// unmarshal it
	err = proto.Unmarshal(buf, mes)
	if err != nil {
		ayame.Log.Error(err)
		return
	}
	r.ReleaseMsg(buf)
	ayame.Log.Infof("%s: Received from %s. size=%d\n", s.Conn().LocalPeer(), s.Conn().RemotePeer(), len(buf))
	var valid bool = false
	if ayame.SecureKeyMV {
		valid = n.authenticateMessage(mes, mes.Data, s)
	}
	ev := n.converter(mes, n, valid)
	n.Host.Peerstore().AddAddr(ev.Sender().Id(), s.Conn().RemoteMultiaddr(), peerstore.PermanentAddrTTL)
	if mes.IsRequest {
		resp := ev.ProcessRequest(context.TODO(), n.child)
		//		resp.SetSender(n.child)
		mes := resp.Encode()
		mes.Sender = n.Encode()
		ayame.Log.Infof("%s: PROCESSED REQUEST. response size=%d\n", s.Conn().LocalPeer(), mes.Size())
		// no sign
		n.sendMsgToStream(context.TODO(), s, mes)
	} else {
		ev.Run(context.TODO(), n.child)
	}
}

func (n *P2PNode) sign(ev ayame.SchedEvent, sign bool) proto.Message {
	mes := ev.Encode()
	mes.Sender = n.Encode()
	if sign { // if verification conscious
		if ev.Sender().Key().Equals(n.Key()) { // author
			mes.Data.AuthorSign = nil            // ensure empty
			senderData := mes.Data.SenderAppData // ensure empty
			mes.Data.SenderAppData = ""          // ensure empty
			mes.Data.AuthorSign, _ = n.signProtoMessage(mes.Data)
			mes.Data.AuthorSign = IfNeededSign(mes.Data.AuthorSign)
			mes.Data.SenderAppData = senderData // restore
		}
		mes.SenderSign, _ = n.signProtoMessage(mes)
		mes.SenderSign = IfNeededSign(mes.SenderSign) // if needed by configuration
	}
	return mes
}

// Node API
func (n *P2PNode) Send(ctx context.Context, ev ayame.SchedEvent, sign bool) {
	ayame.Log.Infof("sending mes=%v/%s", ev.Receiver().Id(), ev.Receiver().Key())

	id := ev.Receiver().Id()

	s, err := n.NewStream(ctx, id, sendMessageProto)
	if err != nil {
		ayame.Log.Errorf("%s NewStream to %s: %s\n", n.Key(), id, err)
		return
	}
	defer s.Close()
	n.sendMsgToStream(ctx, s, n.sign(ev, sign))
	if ev.IsRequest() {
		n.onReceiveMessage(s)
	}
}

// Authenticate incoming p2p message
// message: a protobufs go data object
func (n *P2PNode) authenticateMessage(message *p2p.Message, data *p2p.MessageData, s network.Stream) bool {
	// store a temp ref to signature and remove it from message data

	senderSign := message.SenderSign
	message.SenderSign = nil
	senderBin, err := proto.Marshal(message)
	if err != nil {
		ayame.Log.Error(err, "failed to marshal pb message")
		return false
	}
	message.SenderSign = senderSign

	authorSign := data.AuthorSign
	senderAppData := data.SenderAppData
	data.AuthorSign = nil
	data.SenderAppData = ""

	// marshall data without the signature to protobufs3 binary format
	bin, err := proto.Marshal(data)
	if err != nil {
		ayame.Log.Error(err, "failed to marshal pb message")
		return false
	}

	// restore the message data (for possible future use)
	data.AuthorSign = authorSign
	data.SenderAppData = senderAppData

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
	}

	// verify the data was authored by the signing peer identified by the public key
	// and signature included in the message
	if message.SenderSign != nil {
		senderPubKey := s.Conn().RemotePublicKey()
		if !n.verifyData(senderBin, []byte(message.SenderSign), senderId, senderPubKey) {
			ayame.Log.Errorf("Failed to verify sender")
			return false
		}
	}
	if authorSign != nil {
		if !n.verifyData(bin, []byte(authorSign), authorId, authorPubKey) {
			ayame.Log.Errorf("Failed to verify author")
			return false
		}
	}
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
		log.Println(err, "Failed to extract peer id from public key")
		return false
	}

	// verify that message author node id matches the provided node public key
	if idFromKey != peerId {
		log.Println(err, "Node id and provided public key mismatch")
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
func (n *P2PNode) NewMessage(messageId string, mtype p2p.MessageType, key ayame.Key, mv *ayame.MembershipVector, includePubKey bool) *p2p.Message {
	// Add protobufs bin data for message author public key
	// this is useful for authenticating  messages forwarded by a node authored by another node
	var nodePubKey []byte = nil
	if includePubKey {
		pKey, err := crypto.MarshalPublicKey(n.Peerstore().PubKey(n.ID()))
		if err != nil {
			panic("Failed to get public key for sender from local peer store.")
		}
		nodePubKey = pKey
	}

	var mvData []byte
	if mv != nil {
		mvData = mv.Encode()
	}
	var keyData *p2p.Key
	if key != nil {
		keyData = key.Encode()
	}

	nodePubKey = IfNeededSign(nodePubKey)
	data := &p2p.MessageData{
		Version:   Version,
		Type:      mtype,
		Timestamp: time.Now().Unix(),
		Id:        messageId,
		Key:       keyData,
		Mv:        mvData,
		Author: &p2p.Peer{
			Id:         peer.Encode(n.ID()),
			Mv:         n.mv.Encode(),
			Key:        n.key.Encode(),
			Addrs:      EncodeAddrs(n.Host.Addrs()),
			Cert:       IfNeededSign(n.Cert),
			Connection: p2p.ConnectionType_CONNECTED, // XXX myself is always connected
		},
		AuthorPubKey: nodePubKey}

	return &p2p.Message{Data: data}

}

func (n *P2PNode) sendMsgToStream(ctx context.Context, s network.Stream, msg proto.Message) bool {
	writer := protoio.NewDelimitedWriter(s)
	err := writer.WriteMsg(msg)
	if err != nil {
		ayame.Log.Errorf("write msg %s\n", err)
		s.Reset()
		return false
	}
	return true
}

func (n *P2PNode) sendProtoMessage(ctx context.Context, id peer.ID, p protocol.ID, data proto.Message) bool {
	s, err := n.NewStream(context.Background(), id, p)
	if err != nil {
		ayame.Log.Errorf("%s NewStream to %s: %s\n", n.Key(), id, err)
		return false
	}
	defer s.Close()
	return n.sendMsgToStream(ctx, s, data)
}
