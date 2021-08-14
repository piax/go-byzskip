package ayame

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	peerstore "github.com/libp2p/go-libp2p-peerstore"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	"github.com/multiformats/go-multiaddr"
	"github.com/piax/go-ayame/ayame"
	p2p "github.com/piax/go-ayame/ayame/p2p/pb"

	ggio "github.com/gogo/protobuf/io"
	"github.com/gogo/protobuf/proto"
)

// protocol version
const Version = "/go-ayame/message/0.0.0"

// Node type - a p2p host implementing one or more p2p protocols
type P2PNode struct {
	host.Host // libp2p host
	key       ayame.Key
	mv        *ayame.MembershipVector
	converter func(*p2p.Message, *P2PNode) ayame.SchedEvent
	child     ayame.Node
	OutBytes  int64
	InBytes   int64
	InCount   int64
}

const MessageProto = "/message/0.0.0"

// Create a new node with its implemented protocols
func NewNode(ctx context.Context, locator string, key ayame.Key, mv *ayame.MembershipVector,
	converter func(*p2p.Message, *P2PNode) ayame.SchedEvent) (*P2PNode, error) {

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
	node := &P2PNode{Host: host, key: key, mv: mv, converter: converter, OutBytes: 0, InBytes: 0, InCount: 0}
	node.SetStreamHandler(MessageProto, node.onReceiveMessage)

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

func (n *P2PNode) Encode() *p2p.Peer {
	return &p2p.Peer{
		Id:         peer.Encode(n.ID()),
		Addrs:      EncodeAddrs(n.Addrs()),
		Mv:         n.mv.Encode(),
		Key:        n.key.Encode(),
		Cert:       nil,                          // XXX not yet
		Connection: p2p.ConnectionType_CONNECTED, // myself is always connected
	}
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
	buf, err := ioutil.ReadAll(s)
	if err != nil {
		s.Reset()
		ayame.Log.Error(err)
		return
	}
	n.InBytes += int64(len(buf))
	n.InCount++
	//ayame.Log.Infof("%s: store %s->%s", s.Conn().LocalPeer(), s.Conn().RemotePeer(), s.Conn().RemoteMultiaddr())
	s.Close()

	// unmarshal it
	err = proto.Unmarshal(buf, mes)
	if err != nil {
		ayame.Log.Error(err)
		return
	}
	//ayame.Log.Debugf("%s: Received from %s. size=%d\n", s.Conn().LocalPeer(), s.Conn().RemotePeer(), len(buf))

	valid := n.authenticateMessage(mes, mes.Data, s)
	if !valid {
		ayame.Log.Error("Failed to authenticate message")
		return
	}
	ev := n.converter(mes, n)
	//ayame.Log.Infof("%s: storing %s->%s", s.Conn().LocalPeer(), ev.Sender().Id(), s.Conn().RemoteMultiaddr())
	ayame.Log.Debugf("%s: Received from %s. size=%d\n", n.Key(), ev.Sender().Key(), len(buf))
	n.Host.Peerstore().AddAddr(ev.Sender().Id(), s.Conn().RemoteMultiaddr(), peerstore.PermanentAddrTTL)
	ev.Run(n.child)
}

func (n *P2PNode) Send(ev ayame.SchedEvent) { // send to self..

	mes := ev.Encode()
	mes.Sender = n.Encode()
	if ev.Sender().Key().Equals(n.Key()) { // author
		mes.Data.AuthorSign = nil            // ensure empty
		senderData := mes.Data.SenderAppData // ensure empty
		mes.Data.SenderAppData = ""          // ensure empty
		mes.Data.AuthorSign, _ = n.signProtoMessage(mes.Data)
		mes.Data.SenderAppData = senderData // restore
	}
	mes.SenderSign, _ = n.signProtoMessage(mes)
	//ayame.Log.Infof("sending mes=%v", ev)
	n.sendProtoMessage(ev.Receiver().Id(), MessageProto, mes)
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

	authorPubKey, err := crypto.UnmarshalPublicKey(data.AuthorPubKey)
	if err != nil {
		ayame.Log.Error(err, "Failed to extract key from message key data")
		return false
	}

	// verify the data was authored by the signing peer identified by the public key
	// and signature included in the message
	senderPubKey := s.Conn().RemotePublicKey()
	if !n.verifyData(senderBin, []byte(message.SenderSign), senderId, senderPubKey) {
		ayame.Log.Errorf("Failed to verify sender")
		return false
	}
	if !n.verifyData(bin, []byte(authorSign), authorId, authorPubKey) {
		ayame.Log.Errorf("Failed to verify author")
		return false
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

// helper method - generate message data (only the common part)
// messageId: unique for requests, copied from request to responses
func (n *P2PNode) NewMessage(messageId string, mtype p2p.MessageType, key ayame.Key, mv *ayame.MembershipVector) *p2p.Message {
	// Add protobufs bin data for message author public key
	// this is useful for authenticating  messages forwarded by a node authored by another node
	nodePubKey, err := crypto.MarshalPublicKey(n.Peerstore().PubKey(n.ID()))

	if err != nil {
		panic("Failed to get public key for sender from local peer store.")
	}

	data := &p2p.MessageData{
		Version:   Version,
		Type:      mtype,
		Timestamp: time.Now().Unix(),
		Id:        messageId,
		Key:       key.Encode(),
		Mv:        mv.Encode(),
		Author: &p2p.Peer{
			Id:         peer.Encode(n.ID()),
			Mv:         n.mv.Encode(),
			Key:        n.key.Encode(),
			Addrs:      EncodeAddrs(n.Host.Addrs()),
			Cert:       nil,                          // XXX not yet
			Connection: p2p.ConnectionType_CONNECTED, // myself is always connected
		},
		AuthorPubKey: nodePubKey}

	return &p2p.Message{Data: data}

}

// XXX Open stream, Send, and Close
func (n *P2PNode) sendProtoMessage(id peer.ID, p protocol.ID, data proto.Message) bool {
	s, err := n.NewStream(context.Background(), id, p)
	if err != nil {
		ayame.Log.Errorf("NewStream to %s: %s\n", id, err)
		return false
	}
	defer s.Close()

	writer := ggio.NewFullWriter(s)
	err = writer.WriteMsg(data)
	if err != nil {
		ayame.Log.Errorf("write msg %s\n", err)
		s.Reset()
		return false
	}
	return true
}
