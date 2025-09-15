package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"math"

	u "github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-base32"
	"github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
	"github.com/multiformats/go-multihash"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
	bs "github.com/piax/go-byzskip/byzskip"
	"go.opentelemetry.io/otel"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/ipfs/boxo/gateway"
	config "github.com/ipfs/kubo/config"
	doh "github.com/libp2p/go-doh-resolver"
)

var log = logging.Logger("dht")

// assertion
var (
	_ routing.Routing     = (*BSDHT)(nil)
	_ madns.BasicResolver = (*BSDHT)(nil)
)

const VALID_TIME = 24 * time.Hour

// many functions are imported from the FullRT in go-libp2p-kad-dht

type AppDHT interface {
	HandlePutProviderEvent(ctx context.Context, ev *BSPutProviderEvent) error
	HandleGetProvidersRequest(ctx context.Context, ev *BSGetProvidersEvent) ayame.SchedEvent
	HandleGetProvidersResEvent(ctx context.Context, ev *BSGetProvidersEvent) error
	HandlePutRequest(ctx context.Context, ev *BSPutEvent) ayame.SchedEvent
	HandlePutResEvent(ctx context.Context, ev *BSPutEvent) error
	HandleGetRequest(ctx context.Context, ev *BSGetEvent) ayame.SchedEvent
	HandleGetResEvent(ctx context.Context, ev *BSGetEvent) error
	HandleMultiPutRequest(ctx context.Context, ev *BSMultiPutEvent) ayame.SchedEvent
	HandleMultiPutResEvent(ctx context.Context, ev *BSMultiPutEvent) error
	HandleMultiGetRequest(ctx context.Context, ev *BSMultiGetEvent) ayame.SchedEvent
	HandleMultiGetResEvent(ctx context.Context, ev *BSMultiGetEvent) error
}

type BSDHT struct {
	ctx    context.Context
	cancel context.CancelFunc

	Node *bs.BSNode

	idFinder func(ctx context.Context, dht *BSDHT, id string) ([]*bs.BSNode, error)

	RecordValidator record.Validator
	ProviderManager *records.ProviderManager
	datastore       ds.Datastore
	madns.Resolver
}

func IdKeyFinder(ctx context.Context, dht *BSDHT, id string) ([]*bs.BSNode, error) {
	return dht.Node.Lookup(ctx, ayame.NewStringIdKey(id))
}

func New(h host.Host, options ...Option) (*BSDHT, error) {
	return NewWithoutDefaults(h, append(options, FallbackDefaults)...)
}

func NewDHTFromConfig(h host.Host, certFile string, authPubKey string, options ...Option) (routing.Routing, error) {
	certFile = expandPath(certFile)
	opts := []Option{
		AuthValidator(authority.AuthValidator(authPubKey)),
		Authorizer(authority.FileAuthAuthorizer(certFile)),
	}
	opts = append(opts, options...)
	bsdht, err := New(h, opts...)

	if err != nil {
		return nil, err
	}
	fmt.Printf("Starting ByzSkip-DHT: \n")
	fmt.Printf("  Cert File: %s\n", certFile)
	fmt.Printf("  Public Key: %s\n", authPubKey)
	return bsdht, nil
}

func NewWithoutDefaults(h host.Host, options ...Option) (*BSDHT, error) {
	var cfg Config

	if err := cfg.Apply(options...); err != nil {
		return nil, err
	}

	return cfg.NewDHT(h)
}

func (dht *BSDHT) Id() peer.ID {
	return dht.Node.Id()
}

func (dht *BSDHT) KnownIds() []peer.ID {
	neighbors := []peer.ID{}
	for _, pi := range dht.Node.RoutingTable.AllNeighbors(false, false) {
		p := pi.(*bs.BSNode)
		neighbors = append(neighbors, p.Id())
	}
	return neighbors
}

func (dht *BSDHT) HandlePutProviderEvent(ctx context.Context, ev *BSPutProviderEvent) error {
	log.Debugf("put provider from=%v\n", ev.Sender())
	mh, err := multihash.FromB58String(ev.Key)
	if err != nil {
		return err
	}
	for _, p := range ev.Providers {

		log.Debugf("adding provider mh=%s, id=%v\n", ev.Key, p.Id())
		dht.ProviderManager.AddProvider(ctx,
			mh, peer.AddrInfo{ID: p.Id(), Addrs: p.Addrs()})
	}
	return nil
}

func (dht *BSDHT) HandleGetProvidersResEvent(ctx context.Context, ev *BSGetProvidersEvent) error {
	//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
	dht.Node.ProcsMutex.RLock()
	proc, exists := dht.Node.Procs[ev.MessageId()]
	dht.Node.ProcsMutex.RUnlock()
	if exists && proc.Ch != nil { // sync
		log.Debugf("get providers finished from=%v\n", ev.Sender())
		proc.Ch <- ev
		return nil
	} // XXX async is not implemented yet
	return fmt.Errorf("%v: unregistered response msgid=%s received from %s", dht, ev.MessageId(), ev.Sender())
}

func (dht *BSDHT) HandleGetProvidersRequest(ctx context.Context, ev *BSGetProvidersEvent) ayame.SchedEvent {
	mh, err := multihash.FromB58String(ev.Key)
	if err != nil {
		log.Debugf("failed to make multihash %s \n", ev.Key)
		return nil
	}
	log.Debugf("getting providers from=%v, mh=%s\n", ev.Sender(), string(mh))
	provs, err := dht.ProviderManager.GetProviders(ctx, mh)
	log.Debugf("found providers for %s len=%d\n", mh, len(provs))
	if err != nil {
		log.Debugf("failed to get providers %s \n", ev.Key)
		return nil
	}
	peers := []*pb.Peer{}
	for _, p := range provs {
		peers = append(peers, &pb.Peer{
			Id:    p.ID.String(),
			Addrs: p2p.EncodeAddrs(p.Addrs),
		})
	}
	return NewBSGetProvidersEvent(dht.Node, ev.messageId, false, ev.Key, peers)
}

// func (dht *BSDHT) HandleGetRequest(ctx context.Context, ev *BSGetEvent) ayame.SchedEvent {
// 	ayame.Log.Debugf("get from=%v, key=%s\n", ev.Sender(), string(ev.Record[0].GetKey()))
// 	rec, err := dht.getRecordFromDatastore(ctx, mkDsKey(string(ev.Record[0].GetKey())))

// 	if err != nil { // XXX ignore
// 		rec = nil
// 	}

// 	ayame.Log.Debugf("got rec=%v", rec)
// 	ret := NewBSGetEvent(dht.Node, ev.MessageId(), false, []*pb.Record{rec})
// 	return ret
// }

func (dht *BSDHT) HandleGetRequest(ctx context.Context, ev *BSGetEvent) ayame.SchedEvent {
	log.Infof("get from=%v, key=%s @%s\n", ev.Sender(), ev.Record[0].GetKey(), dht.Node.Id())
	//	fmt.Printf("getting key: %s @ %s\n", ev.Record[0].GetKey(), dh.Node.Id())

	rec, err := dht.getRecordFromDatastore(ctx, mkDsKey(string(ev.Record[0].GetKey())))

	if err != nil { // XXX ignore
		rec = nil
	}
	log.Debugf("got rec=%v", rec)
	ret := NewBSGetEvent(dht.Node, ev.MessageId(), false, []*pb.Record{rec})
	return ret
}

func (dht *BSDHT) HandleGetResEvent(ctx context.Context, ev *BSGetEvent) error {
	//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
	dht.Node.ProcsMutex.RLock()
	proc, exists := dht.Node.Procs[ev.MessageId()]
	dht.Node.ProcsMutex.RUnlock()
	if exists && proc.Ch != nil { // sync
		log.Debugf("get finished from=%v\n", ev.Sender())
		proc.Ch <- ev
		return nil
	} // XXX async is not implemented yet
	return fmt.Errorf("%v: unregistered response msgid=%s received from %s", dht, ev.MessageId(), ev.Sender())
}

func (dht *BSDHT) HandlePutResEvent(ctx context.Context, ev *BSPutEvent) error {
	//strKey := base32.RawStdEncoding.EncodeToString(ev.Key.(ayame.IdKey))
	dht.Node.ProcsMutex.RLock()
	proc, exists := dht.Node.Procs[ev.MessageId()]
	dht.Node.ProcsMutex.RUnlock()
	if exists && proc.Ch != nil { // sync
		log.Debugf("put finished from=%v\n", ev.Sender())
		proc.Ch <- ev
		return nil
	} // XXX async is not implemented yet
	return nil
}

func (dht *BSDHT) HandlePutRequest(ctx context.Context, ev *BSPutEvent) ayame.SchedEvent {
	//strKey := base32.RawStdEncoding.EncodeToString(ev.Key.(ayame.IdKey))
	dht.putLocal(ctx, string(ev.Record.GetKey()), ev.Record) // why key?
	log.Debugf("put finished on %v from=%v, key=%s, len=%d\n", dht.Node.Id(), ev.Sender(), string(ev.Record.GetKey()), len(ev.Record.Value))

	rec, err := dht.getRecordFromDatastore(ctx, mkDsKey(string(ev.Record.GetKey())))

	if err != nil { // XXX ignore
		log.Debugf("checking datastore failed on %v from=%v, key=%s, len=%d\n", dht.Node.Id(), ev.Sender(), string(ev.Record.GetKey()), len(ev.Record.Value))
		rec = nil
	}
	ret := NewBSPutEvent(dht.Node, ev.MessageId(), false, rec)
	log.Debugf("returning put record on %v to=%v, key=%s, len=%d\n", dht.Node.Id(), ev.Sender(), string(ret.Record.GetKey()), len(ret.Record.Value))
	return ret
}

func (dht *BSDHT) getRecordFromDatastore(ctx context.Context, dskey ds.Key) (*pb.Record, error) {
	buf, err := dht.datastore.Get(ctx, dskey)
	log.Debugf("getting record from datastore: key=%s, len=%d", dskey, len(buf))
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		log.Errorf("error retrieving record from datastore: key=%s, err=%s", dskey, err)
		return nil, err
	}
	rec := new(pb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		// Bad data in datastore, log it but don't return an error, we'll just overwrite it
		return nil, err
	}
	err = dht.RecordValidator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		// Invalid record in datastore, probably expired but don't return an error,
		// we'll just overwrite it
		log.Debugf("invalid record in datastore: key=%s, err=%s", dskey, err)
		return nil, nil
	}

	return rec, nil
}

func (dht *BSDHT) getLocal(ctx context.Context, key string) (*pb.Record, error) {
	log.Debugf("finding value in datastore for key %s", key)

	return dht.getRecordFromDatastore(ctx, mkDsKey(key))
}

func (dht *BSDHT) getLocalRaw(ctx context.Context, key string) (*pb.Record, error) {
	rec, err := dht.getRecordFromDatastore(ctx, mkRawDsKey(key))
	if err != nil {
		log.Debugf("checking local datastore failed on %v key=%s, len=%d\n", dht.Node.Id(), key, len(rec.Value))
		return nil, err
	}
	//ayame.Log.Debugf("found value in datastore for key %s, len=%d", key, len(rec.Value))
	return rec, nil
}

// XXX somewhat tricky using "recpb"
func (dht *BSDHT) putLocal(ctx context.Context, key string, rec *pb.Record) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		return err
	}

	return dht.datastore.Put(ctx, mkDsKey(key), data)
}

func (dht *BSDHT) putLocalRaw(ctx context.Context, key string, rec *pb.Record) error {
	data, err := proto.Marshal(rec)
	log.Debugf("putting local raw key=%s, len=%d", key, len(data))
	if err != nil {
		return err
	}

	return dht.datastore.Put(ctx, mkRawDsKey(key), data)
}

func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

func mkRawDsKey(s string) ds.Key {
	return ds.NewKey(s)
}

func MakePutRecord(key string, value []byte) *pb.Record {
	record := new(pb.Record)
	record.Key = []byte(key) //key.Encode()
	record.Value = value
	return record
}

func (dht *BSDHT) makeNamedValueRecord(name string, value []byte, pcert *authority.PCert) (*pb.Record, error) {
	record := new(pb.Record)
	record.Key = []byte(name) //key.Encode()
	now := time.Now()
	record.TimeReceived = u.FormatRFC3339(now)
	pcertBytes, err := authority.PCertToBytes(pcert)
	if err != nil {
		return nil, err
	}
	pubKey, err := crypto.MarshalPublicKey(dht.Node.Parent.(*p2p.P2PNode).Host.Peerstore().PubKey(dht.Node.Id()))
	if err != nil {
		return nil, err
	}
	verifyData := append(pcertBytes, value...)
	sign, err := dht.Node.Parent.(*p2p.P2PNode).Host.Peerstore().PrivKey(dht.Node.Id()).Sign(verifyData)
	if err != nil {
		return nil, err
	}
	namedValue := pb.NamedValue{
		Value:  value,
		PubKey: pubKey,
		Pcert:  pcertBytes,
		Sign:   sign,
	}
	data, err := proto.Marshal(&namedValue)
	if err != nil {
		return nil, err
	}
	record.Value = data
	return record, nil
}

func (dht *BSDHT) verifySign(sign []byte, pubKey crypto.PubKey, value []byte) error {
	if v, err := pubKey.Verify(value, sign); err != nil || !v {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}

func (dht *BSDHT) extractNamedValue(record *pb.Record) (ayame.Key, []byte, error) {
	namedValue := new(pb.NamedValue)
	err := proto.Unmarshal(record.Value, namedValue)
	if err != nil {
		return nil, nil, err
	}
	if namedValue.Pcert == nil {
		return nil, nil, fmt.Errorf("no pcert found")
	}
	// Verify the PCert before returning the value
	pcert, err := authority.BytesToPCert(namedValue.Pcert)
	if err != nil {
		return nil, nil, fmt.Errorf("PCert unmarshal failed: %w", err)
	}
	// Use the node's authorizer to verify the PCert
	if dht.Node.Parent.(*p2p.P2PNode).Validator != nil {
		if !dht.Node.Parent.(*p2p.P2PNode).Validator(pcert.ID, pcert.Key, pcert.Name, pcert.Mv, pcert.Cert) {
			return nil, nil, fmt.Errorf("PCert validation failed using node's AuthValidator")
		}
	}
	pubKey, err := crypto.UnmarshalPublicKey(namedValue.PubKey)
	if err != nil {
		return nil, nil, fmt.Errorf("public key unmarshal failed: %w", err)
	}
	// Create a combined byte slice containing pcert and value for verification
	verifyData := append(namedValue.Pcert, namedValue.Value...)
	if err := dht.verifySign(namedValue.Sign, pubKey, verifyData); err != nil {
		return nil, nil, fmt.Errorf("signature verification failed: %w", err)
	}
	//return pcert.Key, namedValue.Value, nil
	parsed, err := ParseName(string(record.Key))
	if err != nil {
		return nil, nil, fmt.Errorf("invalid recorded key: %w", err)
	}
	return parsed, namedValue.Value, nil
}

// PutValue
func (dht *BSDHT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	log.Debugf("Put key=%s, len=%d", key, len(value))
	if err := dht.RecordValidator.Validate(key, value); err != nil {
		log.Debugf("validation failure key=%s, len=%d", key, len(value))
		return err
	}
	old, err := dht.getLocal(ctx, key)
	if err != nil {
		log.Debugf("get local failure key=%s, %s", key, err)
		return err
	}
	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// Check to see if the new one is better.
		i, err := dht.RecordValidator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			log.Debugf("select failure key=%s, %s", key, err)
			return err
		}
		if i != 0 {
			return fmt.Errorf("can't replace a newer value with an older value")
		}
	}

	rec := MakePutRecord(key, value)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dht.putLocal(ctx, key, rec)
	if err != nil {
		log.Debugf("put local failure key=%s, %s", key, err)
		return err
	}

	//keyByte, _ := base32.RawStdEncoding.DecodeString(key)
	//idKey := ayame.NewStringIdKey(key)
	//peers, err := dht.Node.Lookup(ctx, idKey)
	peers, err := dht.idFinder(ctx, dht, key)

	if err != nil {
		return err
	}
	log.Debugf("lookup done: %v", peers)

	/*
		successes := dht.execOnMany(ctx, func(ctx context.Context, p ayame.Node) error {
			err := dht.sendPutValue(ctx, p, rec)
			return err
		}, nsToIs(peers), true)

		log.Debugf("put succeeded on %d nodes", successes)
		if successes == 0 {
			return fmt.Errorf("failed to complete put")
		}
	*/
	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p ayame.Node) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			err := dht.sendPutValue(ctx, p, rec)
			if err != nil {
				log.Debugf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()

	return nil
}

// perhaps using generic is better
func nsToIs(lst []*bs.BSNode) []ayame.Node {
	ret := []ayame.Node{}
	for _, ele := range lst {
		ret = append(ret, ele)
	}
	return ret
}

func (dht *BSDHT) sendGetProviders(ctx context.Context, p ayame.Node, key multihash.Multihash) ([]*peer.AddrInfo, error) {
	log.Debugf("send get providers")
	if dht.Node.Id() == p.Id() { // put to self. This should be already done.
		return nil, nil
	}
	mes := NewBSGetProvidersEvent(dht.Node, dht.Node.NewMessageId(), true, key.B58String(), nil)
	resp := dht.Node.SendRequest(ctx, p, mes)
	if ev, ok := resp.(*BSGetProvidersEvent); ok {
		addrs := []*peer.AddrInfo{}
		for _, p := range ev.Providers {
			pid, err := peer.Decode(p.Id)
			if err != nil {
				return nil, err
			}
			addrs = append(addrs, &peer.AddrInfo{ID: pid, Addrs: p2p.Addresses(p.Addrs)})
		}
		return addrs, nil
	}
	if ev, ok := resp.(*bs.FailureResponse); ok {
		return nil, ev.Err
	}
	return nil, fmt.Errorf("invalid response type %s", resp)
}

func (dht *BSDHT) sendPutProvider(ctx context.Context, p ayame.Node, key multihash.Multihash) error {
	if dht.Node.Id() == p.Id() { // put to self. This should be already done.
		return nil
	}
	log.Debugf("sending provider to %s", p)
	mes := NewBSPutProviderEvent(dht.Node, dht.Node.NewMessageId(), key.B58String(), []*bs.BSNode{dht.Node})
	dht.Node.SendEventAsync(ctx, p, mes, false)
	return nil
}

func (dht *BSDHT) sendPutValue(ctx context.Context, p ayame.Node, rec *pb.Record) error {
	log.Debugf("send put value")
	if dht.Node.Id() == p.Id() { // put to self. This should be already done.
		return nil
	}
	mes := NewBSPutEvent(dht.Node, dht.Node.NewMessageId(), true, rec)
	resp := dht.Node.SendRequest(ctx, p, mes)
	if ev, ok := resp.(*BSPutEvent); ok {
		if !bytes.Equal(rec.Value, ev.Record.Value) {
			const errStr = "value not put correctly"
			log.Info(errStr, "put-message", rec, "get-message", ev.Record)
			return errors.New(errStr)
		}
		log.Debugf("exec put successfully on: %s for key=%s", p.Id(), string(rec.Key))
		return nil
	}
	if ev, ok := resp.(*bs.FailureResponse); ok {
		return ev.Err
	}
	return fmt.Errorf("invalid response type %s", resp)
}

func (dht *BSDHT) sendGetValue(ctx context.Context, p ayame.Node, key string) (*pb.Record, error) {
	log.Debugf("send get values")
	if dht.Node.Id() == p.Id() { // put to self.
		return dht.getRecordFromDatastore(ctx, mkDsKey(key))
	}
	mes := NewBSGetEvent(dht.Node, dht.Node.NewMessageId(), true, []*pb.Record{{Key: []byte(key)}})
	resp := dht.Node.SendRequest(ctx, p, mes)
	if ev, ok := resp.(*BSGetEvent); ok {
		return ev.Record[0], nil
	}
	if ev, ok := resp.(*bs.FailureResponse); ok {
		return nil, ev.Err
	}
	return nil, fmt.Errorf("invalid response type %s", resp)
}

const (
	WAIT_FRAC      = 0.3
	TIMEOUT_PER_OP = 5 * time.Second
	MAX_RECORD_AGE = time.Hour * 36
)

// execOnMany executes the given function on each of the peers, although it may only wait for a certain chunk of peers
// to respond before considering the results "good enough" and returning.
//
// If sloppyExit is true then this function will return without waiting for all of its internal goroutines to close.
// If sloppyExit is true then the passed in function MUST be able to safely complete an arbitrary amount of time after
// execOnMany has returned (e.g. do not write to resources that might get closed or set to nil and therefore result in
// a panic instead of just returning an error).
func (dht *BSDHT) execOnMany(ctx context.Context, fn func(context.Context, ayame.Node) error, peers []ayame.Node, sloppyExit bool) int {
	if len(peers) == 0 {
		return 0
	}

	// Create a buffered channel to avoid goroutine leaks
	errCh := make(chan error, len(peers))
	numSuccessfulToWaitFor := int(float64(len(peers)) * WAIT_FRAC)

	// Create a timeout context for the entire operation
	putctx, cancel := context.WithTimeout(ctx, TIMEOUT_PER_OP)
	defer cancel()

	// Launch goroutines for each peer
	for _, p := range peers {
		go func(p ayame.Node) {
			// Create a new context for each operation to ensure proper timeout handling
			opCtx, opCancel := context.WithTimeout(putctx, TIMEOUT_PER_OP)
			defer opCancel()

			err := fn(opCtx, p)
			select {
			case errCh <- err:
			case <-putctx.Done():
				// Context was cancelled, don't block on sending error
			}
		}(p)
	}

	var numDone, numSuccess int

	// Wait for either enough successes or all operations to complete
	for numDone < len(peers) {
		select {
		case err := <-errCh:
			numDone++
			if err == nil {
				numSuccess++
				// If we have enough successes and sloppyExit is true, we can return early
				if sloppyExit && numSuccess >= numSuccessfulToWaitFor {
					return numSuccess
				}
			}
		case <-putctx.Done():
			// Context was cancelled or timed out
			if putctx.Err() == context.DeadlineExceeded {
				log.Debugf("execOnMany timed out after %v", TIMEOUT_PER_OP)
			} else {
				log.Debugf("execOnMany cancelled: %v", putctx.Err())
			}
			return numSuccess
		}
		log.Debugf("execOnMany loop, numDone=%d, numSuccess=%d", numDone, numSuccess)
	}
	log.Debugf("execOnMany done, numSuccess=%d", numSuccess)

	return numSuccess
}

// GetValue
func (dht *BSDHT) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	responses, err := dht.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	var best []byte

	for r := range responses {
		best = r
	}

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound
	}
	log.Debugf("GetValue %v %x", key, best)
	return best, nil
}

type RecvdVal struct {
	Key  []byte
	Val  []byte
	From peer.ID
}

func (r RecvdVal) String() string {
	return fmt.Sprintf("RecvdVal{Key: %s, Val: %s, From: %s}", string(r.Key), string(r.Val), r.From)
}

func (dht *BSDHT) processValues(ctx context.Context, key string, vals <-chan RecvdVal,
	newVal func(ctx context.Context, v RecvdVal, better bool) bool) (best []byte, peersWithBest map[peer.ID]struct{}, aborted bool) {
loop:
	for {
		if aborted {
			return
		}

		select {
		case v, ok := <-vals:
			if !ok {
				break loop
			}

			// Select best value
			if best != nil {
				if bytes.Equal(best, v.Val) {
					peersWithBest[v.From] = struct{}{}
					aborted = newVal(ctx, v, false)
					continue
				}
				sel, err := dht.RecordValidator.Select(key, [][]byte{best, v.Val})
				if err != nil {
					log.Debugf("failed to select best value key %s err=%s", key, err)
					continue
				}
				if sel != 1 {
					aborted = newVal(ctx, v, false)
					continue
				}
			}
			peersWithBest = make(map[peer.ID]struct{})
			peersWithBest[v.From] = struct{}{}
			best = v.Val
			aborted = newVal(ctx, v, true)
		case <-ctx.Done():
			return
		}
	}

	return
}

func (dht *BSDHT) searchValueQuorum(ctx context.Context, key string, valCh <-chan RecvdVal, out chan<- []byte, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v RecvdVal, better bool) bool {
			numResponses++
			if better {
				select {
				case out <- v.Val:
				case <-ctx.Done():
					return false
				}
			}

			if nvals > 0 && numResponses > nvals {
				//close(stopCh)
				return true
			}
			return false
		})
}

// SearchValue
func (dht *BSDHT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = GetQuorum(&cfg)
	}

	//stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key)

	out := make(chan []byte)
	go func() {
		defer close(out)
		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, out, responsesNeeded)
		if best == nil || aborted {
			return
		}

		updatePeers := make([]ayame.Node, 0, bs.K)
		select {
		case l := <-lookupRes:
			if l == nil {
				return
			}

			for _, p := range l.peers {
				if _, ok := peersWithBest[p.Id()]; !ok {
					updatePeers = append(updatePeers, p)
				}
			}
		case <-ctx.Done():
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		dht.updatePeerValues(ctx, key, best, updatePeers)
		cancel()
	}()

	return out, nil
}

type lookupWithFollowupResult struct {
	peers []ayame.Node // the top K not unreachable peers at the end of the query
}

// stopQuery is not implemented.
func (dht *BSDHT) getValues(ctx context.Context, key string) (<-chan RecvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan RecvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	log.Debug("finding value", "key", key)

	if rec, err := dht.getLocal(ctx, key); rec != nil && err == nil {
		select {
		case valCh <- RecvdVal{
			Key:  rec.GetKey(),
			Val:  rec.GetValue(),
			From: dht.Node.Id(),
		}:
		case <-ctx.Done():
		}
	}

	//idKey := ayame.NewStringIdKey(key)
	//ps, _ := dht.Node.Lookup(ctx, &idKey)

	ps, _ := dht.idFinder(ctx, dht, key)
	peers := nsToIs(ps)

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		queryFn := func(ctx context.Context, p ayame.Node) error {
			rec, err := dht.sendGetValue(ctx, p, key)
			if err != nil {
				return err
			}
			// TODO: What should happen if the record is invalid?
			// Pre-existing code counted it towards the quorum, but should it?
			if rec != nil && rec.GetValue() != nil {
				rv := RecvdVal{
					Key:  rec.GetKey(),
					Val:  rec.GetValue(),
					From: p.Id(),
				}

				select {
				case valCh <- rv:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}

		dht.execOnMany(ctx, queryFn, peers, false)

		lookupResCh <- &lookupWithFollowupResult{peers: peers}
	}()
	return valCh, lookupResCh
}

func (dht *BSDHT) updatePeerValues(ctx context.Context, key string, val []byte, peers []ayame.Node) {
	log.Debugf("update peer values")
	fixupRec := MakePutRecord(key, val)
	for _, p := range peers {
		go func(p ayame.Node) {
			//TODO: Is this possible?
			if p.Id() == dht.Node.Id() {
				err := dht.putLocal(ctx, key, fixupRec)
				if err != nil {
					log.Error("Error correcting local dht entry:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			err := dht.sendPutValue(ctx, p, fixupRec)
			if err != nil {
				log.Debug("Error correcting DHT entry: ", err)
			}
		}(p)
	}
}

var Tracer = otel.Tracer("")

// Provide
func (dht *BSDHT) Provide(ctx context.Context, key cid.Cid, brdcst bool) error {
	log.Debugf("Put Provider key=%s", key)
	keyMH := key.Hash()

	// add self locally
	log.Debugf("adding providers for %s as %s\n", keyMH, dht.Node.Id())
	dht.ProviderManager.AddProvider(ctx, keyMH, peer.AddrInfo{ID: dht.Node.Id()})
	if !brdcst {
		return nil
	}

	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// timed out
			return context.DeadlineExceeded
		} else if timeout < 10*time.Second {
			// Reserve 10% for the final put.
			deadline = deadline.Add(-timeout / 10)
		} else {
			// Otherwise, reserve a second (we'll already be
			// connected so this should be fast).
			deadline = deadline.Add(-time.Second)
		}
		var cancel context.CancelFunc
		closerCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	var exceededDeadline bool
	//idKey := ayame.NewStringIdKey(string(keyMH))
	//ps, err := dht.Node.Lookup(closerCtx, idKey)

	ps, err := dht.idFinder(closerCtx, dht, string(keyMH))

	peers := nsToIs(ps)
	log.Debugf("put providers on %s", ayame.SliceString(peers))

	switch err {
	case context.DeadlineExceeded:
		// If the _inner_ deadline has been exceeded but the _outer_
		// context is still fine, provide the value to the closest peers
		// we managed to find, even if they're not the _actual_ closest peers.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		exceededDeadline = true
	case nil:
	default:
		return err
	}
	/*
		successes := dht.execOnMany(ctx, func(ctx context.Context, p ayame.Node) error {
			err := dht.sendPutProvider(context.Background(), p, keyMH) //dht.protoMessenger.PutProvider(ctx, p, keyMH, dht.h)
			return err
		}, peers, true)
	*/

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p ayame.Node) {
			defer wg.Done()
			log.Debugf("putProvider(%s, %s)", keyMH, p)
			err := dht.sendPutProvider(ctx, p, keyMH)
			if err != nil {
				log.Debug(err)
			}
		}(p)
	}
	wg.Wait()

	if exceededDeadline {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}

// FindProvidersAsync
func (dht *BSDHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	log.Debugf("Find Provider key=%s, count=%d", key, count)
	if !key.Defined() {
		peerOut := make(chan peer.AddrInfo)
		close(peerOut)
		return peerOut
	}

	chSize := count
	if count == 0 {
		chSize = 1
	}
	peerOut := make(chan peer.AddrInfo, chSize)

	keyMH := key.Hash()

	go dht.findProvidersAsyncRoutine(ctx, keyMH, count, peerOut)
	return peerOut
}

func (dht *BSDHT) findProvidersAsyncRoutine(ctx context.Context, key multihash.Multihash, count int, peerOut chan peer.AddrInfo) {
	defer close(peerOut)

	findAll := count == 0
	ps := make(map[peer.ID]struct{})
	psLock := &sync.Mutex{}
	psTryAdd := func(p peer.ID) bool {
		psLock.Lock()
		defer psLock.Unlock()
		_, ok := ps[p]
		if !ok && (len(ps) < count || findAll) {
			ps[p] = struct{}{}
			return true
		}
		return false
	}
	psSize := func() int {
		psLock.Lock()
		defer psLock.Unlock()
		return len(ps)
	}

	provs, err := dht.ProviderManager.GetProviders(ctx, key)
	log.Debugf("found providers for %s len=%d\n", key, len(provs))

	if err != nil {
		return
	}
	for _, p := range provs {
		// NOTE: Assuming that this list of peers is unique
		if psTryAdd(p.ID) {
			log.Debugf("using provider: %s", p)
			select {
			case peerOut <- p:
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, don't bother with remote RPC
		// TODO: is this a DOS vector?
		if !findAll && psSize() >= count {
			log.Debugf("got enough providers (%d/%d)", psSize(), count)
			return
		}
		log.Debugf("found %d/%d providers on local", psSize(), count)
	}

	//idKey := ayame.NewStringIdKey(string(key))
	//peers, _ := dht.Node.Lookup(ctx, idKey)

	peers, _ := dht.idFinder(ctx, dht, string(key))

	queryctx, cancelquery := context.WithCancel(ctx)
	defer cancelquery()

	fn := func(ctx context.Context, p ayame.Node) error {
		provs, err := dht.sendGetProviders(context.Background(), p, key)
		if err != nil {
			return err
		}

		log.Debugf("%d provider entries", len(provs))

		// Add unique providers from request, up to 'count'
		for _, prov := range provs {
			dht.maybeAddAddrs(prov.ID, prov.Addrs, peerstore.TempAddrTTL)
			log.Debugf("got provider: %s", prov)
			if psTryAdd(prov.ID) {
				log.Debugf("using provider: %s", prov)
				select {
				case peerOut <- *prov:
				case <-ctx.Done():
					log.Debug("context timed out sending more providers")
					return ctx.Err()
				}
			} else {
				log.Debugf("already have provider: %s", prov)
			}
			if !findAll && psSize() >= count {
				log.Debugf("got enough providers (%d/%d)", psSize(), count)
				cancelquery()
				return nil
			}
			log.Debugf("found %d/%d providers on remote", psSize(), count)
		}
		return nil
	}
	dht.execOnMany(queryctx, fn, nsToIs(peers), false)
}

func (dht *BSDHT) maybeAddAddrs(p peer.ID, addrs []multiaddr.Multiaddr, ttl time.Duration) {
	// Don't add addresses for self or our connected peers. We have better ones.
	if p == dht.Node.Id() {
		return
	}
	self := dht.Node.Parent.(*p2p.P2PNode)
	self.Host.Peerstore().AddAddrs(p, addrs, ttl)
}

func (dht *BSDHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}
	//idkey := ayame.NewIdKey(id)
	//clst, _ := dht.Node.Lookup(ctx, idkey)

	clst, _ := dht.idFinder(ctx, dht, id.String())
	for _, n := range clst {
		//if n.Key().Equals(idkey) {
		if n.Id() == id {
			return peer.AddrInfo{ID: n.Id(), Addrs: n.Addrs()}, nil
		}
	}
	return peer.AddrInfo{}, routing.ErrNotFound
}

// run as the bootstrap node.
func (dht *BSDHT) RunAsBootstrap(ctx context.Context) error {
	dht.Node.RunBootstrap(ctx)
	return nil
}

// bootstrap the node.
func (dht *BSDHT) Bootstrap(ctx context.Context) error {
	return dht.Node.Join(ctx)
}

func (dht *BSDHT) Close() error {
	dht.cancel()
	return dht.Node.Close()
}

func (dht *BSDHT) GetPublicKey(ctx context.Context, p peer.ID) (crypto.PubKey, error) {
	// no need to get publckey
	return nil, nil
}

func ConvertMessage(mes *pb.Message, self *p2p.P2PNode, valid bool) ayame.SchedEvent {
	var ev ayame.SchedEvent
	needValidation := self.VerifyIntegrity
	originator, _ := bs.ConvertPeer(self, mes.Data.Originator, needValidation)
	log.Debugf("received msgid=%s,author=%s", mes.Data.Id, mes.Data.Originator.Id)
	switch mes.Data.Type {
	case pb.MessageType_GET_VALUE:
		ev = NewBSGetEvent(originator, mes.Data.Id, mes.IsRequest, mes.Data.Record)
		p, err := bs.ConvertPeer(self, mes.Sender, needValidation)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
		ev.SetVerified(true) // always verified
		return ev
	case pb.MessageType_PUT_VALUE:
		ev = NewBSPutEvent(originator, mes.Data.Id, mes.IsRequest, mes.Data.Record[0])
		p, err := bs.ConvertPeer(self, mes.Sender, needValidation)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
		ev.SetVerified(true) // always verified
		return ev
	case pb.MessageType_PUT_MULTI_VALUE:
		p, err := bs.ConvertPeer(self, mes.Sender, needValidation)
		ev = NewBSMultiPutEvent(originator, mes.Data.Id, mes.IsRequest, mes.Data.Record[0])
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
		ev.SetVerified(true) // always verified
		return ev
	case pb.MessageType_GET_MULTI_VALUE:
		p, err := bs.ConvertPeer(self, mes.Sender, needValidation)
		ev = NewBSMultiGetEvent(originator, mes.Data.Id, mes.IsRequest, mes.Data.Record)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
		ev.SetVerified(true) // always verified
		return ev
	case pb.MessageType_ADD_PROVIDER:
		ev = NewBSPutProviderEvent(originator, mes.Data.Id, mes.Data.SenderAppData,
			bs.ConvertPeers(self, mes.Data.CandidatePeers))
		p, err := bs.ConvertPeer(self, mes.Sender, needValidation)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
		ev.SetVerified(true) // always verified
		return ev
	case pb.MessageType_GET_PROVIDERS:
		ev = NewBSGetProvidersEvent(originator, mes.Data.Id, mes.IsRequest, mes.Data.SenderAppData,
			mes.Data.CandidatePeers)
		p, err := bs.ConvertPeer(self, mes.Sender, needValidation)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
		ev.SetVerified(true) // always verified
		return ev

	}
	return bs.ConvertMessage(mes, self, valid)
}

// a DNS resolver by BSDHT, replace the default resolver by decorating the original resolver
// This function waits for the BSDHT routing to be initialized before applying the resolver
func WithBSDHTResolver() fx.Option {
	return fx.Decorate(func(original *madns.Resolver) (*madns.Resolver, error) {
		// Get BSDHT instance from global variable
		// This avoids creating additional Host instances
		bs := getGlobalBSDHT()
		if bs == nil {
			return original, nil
		}

		// Create a new resolver instance that delegates to BSDHT
		newResolver, err := madns.NewResolver(
			madns.WithDefaultResolver(bs),
		)
		if err != nil {
			return original, nil // Fall back to original on error
		}

		return newResolver, nil
	})
}

// getGlobalBSDHT returns the globally stored BSDHT instance
func getGlobalBSDHT() *BSDHT {
	// This function will be implemented in the routing.go file
	// For now, return nil to avoid compilation errors
	return nil
}

// BSDHTResolver creates a new DNS resolver that uses BSDHT as the default resolver
// This function provides BSDHT as a resolver without using fx.Decorate
func BSDHTResolver(bs *BSDHT) (*madns.Resolver, error) {
	if bs == nil {
		// Fall back to default resolver if BSDHT is not available
		return madns.NewResolver()
	}

	// Create a new resolver instance that delegates to BSDHT
	newResolver, err := madns.NewResolver(
		madns.WithDefaultResolver(bs),
	)
	if err != nil {
		// Fall back to default resolver on error
		return madns.NewResolver()
	}

	return newResolver, nil
}

// BSDHTDNSResolver replaces the default DNSResolver function in Kubo
// This function can be used to replace fx.Provide(DNSResolver) in the dependency injection
func BSDHTDNSResolver(cfg *config.Config, bs *BSDHT) (*madns.Resolver, error) {
	if bs == nil {
		// Fall back to original DNSResolver if BSDHT is not available
		return originalDNSResolver(cfg)
	}

	// Create a new resolver instance that delegates to BSDHT
	newResolver, err := madns.NewResolver(
		madns.WithDefaultResolver(bs),
	)
	if err != nil {
		// Fall back to original DNSResolver on error
		return originalDNSResolver(cfg)
	}

	return newResolver, nil
}

// originalDNSResolver is a copy of the original DNSResolver function from Kubo
// This is used as a fallback when BSDHT is not available
func originalDNSResolver(cfg *config.Config) (*madns.Resolver, error) {
	var dohOpts []doh.Option
	if !cfg.DNS.MaxCacheTTL.IsDefault() {
		dohOpts = append(dohOpts, doh.WithMaxCacheTTL(cfg.DNS.MaxCacheTTL.WithDefault(time.Duration(math.MaxUint32)*time.Second)))
	}

	return gateway.NewDNSResolver(cfg.DNS.Resolvers, dohOpts...)
}
