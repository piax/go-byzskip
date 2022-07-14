package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/routing"
	"github.com/libp2p/go-libp2p-kad-dht/providers"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-base32"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
	bs "github.com/piax/go-byzskip/byzskip"
)

// assertion
var (
	_ routing.ContentRouting = (*BSDHT)(nil)
	_ routing.Routing        = (*BSDHT)(nil)
	_ routing.PeerRouting    = (*BSDHT)(nil)
	_ routing.PubKeyFetcher  = (*BSDHT)(nil)
	_ routing.ValueStore     = (*BSDHT)(nil)

//	_ ayame.Node             = (*BSDHT)(nil)
)

// many functions are imported from go-libp2p-kad-dht FullRT

type BSDHT struct {
	node *bs.BSNode

	RecordValidator record.Validator
	ProviderManager *providers.ProviderManager
	datastore       ds.Datastore
}

func New(h host.Host, options ...Option) (*BSDHT, error) {
	return NewWithoutDefaults(h, append(options, FallbackDefaults)...)
}

func NewWithoutDefaults(h host.Host, options ...Option) (*BSDHT, error) {
	var cfg Config

	if err := cfg.Apply(options...); err != nil {
		return nil, err
	}

	return cfg.NewDHT(h)
}

func handleGetRequest(ctx context.Context, dht *BSDHT, ev *BSGetEvent) ayame.SchedEvent {
	rec, err := dht.checkLocalDatastore(ctx, mkDsKey(string(ev.Record.GetKey())))

	if err != nil { // XXX ignore
		rec = nil
	}
	ret := NewBSGetEvent(dht.node, ev.MessageId(), false, rec)
	return ret
}

func (dht *BSDHT) checkLocalDatastore(ctx context.Context, dskey ds.Key) (*pb.Record, error) {
	buf, err := dht.datastore.Get(ctx, dskey)
	ayame.Log.Debugf("%s checking data store got: %v", dht.node.Key(), buf)

	if err == ds.ErrNotFound {
		return nil, nil
	}

	// if we got an unexpected error, bail.
	if err != nil {
		return nil, err
	}

	// if we have the value, send it back
	ayame.Log.Debugf("%s handleGetValue success!", dht.node.Key())

	rec := new(pb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		ayame.Log.Debug("failed to unmarshal DHT record from datastore")
		return nil, err
	}

	var recordIsBad bool
	recvtime := time.Unix(rec.GetTimestamp(), 0)

	if time.Since(recvtime) > MAX_RECORD_AGE {
		ayame.Log.Debug("old record found, tossing.")
		recordIsBad = true
	}

	// NOTE: We do not verify the record here beyond checking these timestamps.
	// we put the burden of checking the records on the requester as checking a record
	// may be computationally expensive

	if recordIsBad {
		err := dht.datastore.Delete(ctx, dskey)
		if err != nil {
			ayame.Log.Error("Failed to delete bad record from datastore: ", err)
		}

		return nil, nil // can treat this as not having the record at all
	}

	return rec, nil
}

func handleGetResEvent(ctx context.Context, dht *BSDHT, ev *BSGetEvent) error {
	//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
	dht.node.ProcsMutex.RLock()
	proc, exists := dht.node.Procs[ev.MessageId()]
	dht.node.ProcsMutex.RUnlock()
	if exists && proc.Ch != nil { // sync
		ayame.Log.Debugf("get finished from=%v\n", ev.Sender())
		proc.Ch <- ev
		return nil
	} // XXX async is not implemented yet
	return fmt.Errorf("%v: unregistered response msgid=%s received from %s", dht, ev.MessageId(), ev.Sender())
}

func handlePutResEvent(ctx context.Context, dht *BSDHT, ev *BSPutEvent) error {
	//strKey := base32.RawStdEncoding.EncodeToString(ev.Key.(ayame.IdKey))
	dht.node.ProcsMutex.RLock()
	proc, exists := dht.node.Procs[ev.MessageId()]
	dht.node.ProcsMutex.RUnlock()
	if exists && proc.Ch != nil { // sync
		ayame.Log.Debugf("put finished from=%v\n", ev.Sender())
		proc.Ch <- ev
		return nil
	} // XXX async is not implemented yet
	return nil
}

func handlePutRequest(ctx context.Context, dht *BSDHT, ev *BSPutEvent) ayame.SchedEvent {
	//strKey := base32.RawStdEncoding.EncodeToString(ev.Key.(ayame.IdKey))
	dht.putLocal(ctx, string(ev.Record.GetKey()), ev.Record) // why key?
	ayame.Log.Debugf("put finished on %v from=%v\n", dht.node.Id(), ev.Sender())

	rec, err := dht.checkLocalDatastore(ctx, mkDsKey(string(ev.Record.GetKey())))

	if err != nil { // XXX ignore
		rec = nil
	}
	ret := NewBSPutEvent(dht.node, ev.MessageId(), false, rec)
	return ret
}

func (dht *BSDHT) getRecordFromDatastore(ctx context.Context, dskey ds.Key) (*pb.Record, error) {
	buf, err := dht.datastore.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		ayame.Log.Errorf("error retrieving record from datastore: key=%s, err=%s", dskey, err)
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
		return nil, err
	}

	return rec, nil
}

func (dht *BSDHT) getLocal(ctx context.Context, key string) (*pb.Record, error) {
	ayame.Log.Debugf("finding value in datastore for key %s", key)

	return dht.getRecordFromDatastore(ctx, mkDsKey(key))
}

func (dht *BSDHT) putLocal(ctx context.Context, key string, rec *pb.Record) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		return err
	}

	return dht.datastore.Put(ctx, mkDsKey(key), data)
}

func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

func MakePutRecord(key string, value []byte) *pb.Record {
	record := new(pb.Record)
	record.Key = []byte(key) //key.Encode()
	record.Value = value
	return record
}

// PutValue
func (dht *BSDHT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	if err := dht.RecordValidator.Validate(key, value); err != nil {
		return err
	}
	old, err := dht.getLocal(ctx, key)
	if err != nil {
		return err
	}
	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// Check to see if the new one is better.
		i, err := dht.RecordValidator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			return err
		}
		if i != 0 {
			return fmt.Errorf("can't replace a newer value with an older value")
		}
	}

	rec := MakePutRecord(key, value)
	rec.Timestamp = time.Now().Unix()
	err = dht.putLocal(ctx, key, rec)
	if err != nil {
		return err
	}

	//keyByte, _ := base32.RawStdEncoding.DecodeString(key)
	idKey := ayame.NewStringIdKey(key)

	peers := dht.node.Lookup(ctx, idKey)
	ayame.Log.Debugf("lookup done: %v", peers)

	successes := dht.execOnMany(ctx, func(ctx context.Context, p ayame.Node) error {
		err := dht.sendPutValue(ctx, p, rec)
		return err
	}, nsToIs(peers), true)

	ayame.Log.Debugf("put succeeded on %d nodes", successes)
	if successes == 0 {
		return fmt.Errorf("failed to complete put")
	}
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

func (dht *BSDHT) sendPutValue(ctx context.Context, p ayame.Node, rec *pb.Record) error {
	if dht.node.Id() == p.Id() { // put to self. This should be already done.
		return nil
	}
	mes := NewBSPutEvent(dht.node, dht.node.NewMessageId(), true, rec)
	resp := dht.node.SendRequest(ctx, p, mes)
	if ev, ok := resp.(*BSPutEvent); ok {
		if !bytes.Equal(rec.Value, ev.Record.Value) {
			const errStr = "value not put correctly"
			ayame.Log.Info(errStr, "put-message", rec, "get-message", ev.Record)
			return errors.New(errStr)
		}
		ayame.Log.Debugf("exec put successfully on: %s", p.Id())
		return nil
	}
	if ev, ok := resp.(*bs.FailureResponse); ok {
		return ev.Err
	}
	return fmt.Errorf("invalid response type %s", resp)
}

func (dht *BSDHT) sendGetValue(ctx context.Context, p ayame.Node, key string) (*pb.Record, error) {
	if dht.node.Id() == p.Id() { // put to self.
		return dht.checkLocalDatastore(ctx, mkDsKey(key))
	}
	mes := NewBSGetEvent(dht.node, dht.node.NewMessageId(), true, &pb.Record{Key: []byte(key)})
	resp := dht.node.SendRequest(ctx, p, mes)
	if ev, ok := resp.(*BSGetEvent); ok {
		return ev.Record, nil
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

// (picked from IPFS)
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

	// having a buffer that can take all of the elements is basically a hack to allow for sloppy exits that clean up
	// the goroutines after the function is done rather than before
	errCh := make(chan error, len(peers))
	numSuccessfulToWaitFor := int(float64(len(peers)) * WAIT_FRAC)

	putctx, cancel := context.WithTimeout(ctx, TIMEOUT_PER_OP)
	defer cancel()

	for _, p := range peers {
		go func(p ayame.Node) {
			errCh <- fn(putctx, p)
		}(p)
	}

	var numDone, numSuccess, successSinceLastTick int
	var ticker *time.Ticker
	var tickChan <-chan time.Time

	for numDone < len(peers) {
		ayame.Log.Debugf("numDone=%d < len(peers)=%d", numDone, len(peers))
		select {
		case err := <-errCh:
			numDone++
			if err == nil {
				numSuccess++
				if numSuccess >= numSuccessfulToWaitFor && ticker == nil {
					// Once there are enough successes, wait a little longer
					ticker = time.NewTicker(time.Millisecond * 500)
					defer ticker.Stop()
					tickChan = ticker.C
					successSinceLastTick = numSuccess
				}
				// This is equivalent to numSuccess * 2 + numFailures >= len(peers) and is a heuristic that seems to be
				// performing reasonably.
				// TODO: Make this metric more configurable
				// TODO: Have better heuristics in this function whether determined from observing static network
				// properties or dynamically calculating them
				if numSuccess+numDone >= len(peers) {
					cancel()
					if sloppyExit {
						return numSuccess
					}
				}
			}
		case <-tickChan:
			if numSuccess > successSinceLastTick {
				// If there were additional successes, then wait another tick
				successSinceLastTick = numSuccess
			} else {
				cancel()
				if sloppyExit {
					return numSuccess
				}
			}
		}
	}
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
	ayame.Log.Debugf("GetValue %v %x", key, best)
	return best, nil
}

type RecvdVal struct {
	Val  []byte
	From peer.ID
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
					ayame.Log.Debugf("failed to select best value key %s err=%s", key, err)
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

func (dht *BSDHT) searchValueQuorum(ctx context.Context, key string, valCh <-chan RecvdVal, stopCh chan struct{},
	out chan<- []byte, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
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
				close(stopCh)
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

	stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key, stopCh)

	out := make(chan []byte)
	go func() {
		defer close(out)
		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)
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

func (dht *BSDHT) getValues(ctx context.Context, key string, stopQuery chan struct{}) (<-chan RecvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan RecvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	ayame.Log.Debug("finding value", "key", key)

	if rec, err := dht.getLocal(ctx, key); rec != nil && err == nil {
		select {
		case valCh <- RecvdVal{
			Val:  rec.GetValue(),
			From: dht.node.Id(),
		}:
		case <-ctx.Done():
		}
	}

	idKey := ayame.NewStringIdKey(key)
	peers := nsToIs(dht.node.Lookup(ctx, &idKey))

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
	fixupRec := MakePutRecord(key, val)
	for _, p := range peers {
		go func(p ayame.Node) {
			//TODO: Is this possible?
			if p.Id() == dht.node.Id() {
				err := dht.putLocal(ctx, key, fixupRec)
				if err != nil {
					ayame.Log.Error("Error correcting local dht entry:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			err := dht.sendPutValue(ctx, p, fixupRec)
			if err != nil {
				ayame.Log.Debug("Error correcting DHT entry: ", err)
			}
		}(p)
	}
}

// Provide
func (dht *BSDHT) Provide(ctx context.Context, key cid.Cid, brdcst bool) error {
	return routing.ErrNotSupported
}

// FindProvidersAsync
func (dht *BSDHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	keyMH := key.Hash()
	idKey := ayame.IdKey(keyMH)
	go func(key ayame.IdKey) {
		clst := dht.node.Lookup(ctx, key)
		for _, n := range clst {
			ch <- peer.AddrInfo{ID: n.Id(), Addrs: n.Addrs()}
		}
	}(idKey)
	return ch
}

func (dht *BSDHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}
	idkey := ayame.NewIdKey(id)
	clst := dht.node.Lookup(ctx, idkey)
	for _, n := range clst {
		if n.Key().Equals(idkey) {
			return peer.AddrInfo{ID: n.Id(), Addrs: n.Addrs()}, nil
		}
	}
	return peer.AddrInfo{}, routing.ErrNotFound
}

// nothing to do.
func (dht *BSDHT) Bootstrap(context.Context) error {
	return nil
}

func (dht *BSDHT) Close() error {
	return dht.node.Close()
}

func (dht *BSDHT) GetPublicKey(ctx context.Context, p peer.ID) (crypto.PubKey, error) {
	// no need to get publckey
	return nil, nil
}

func ConvertMessage(mes *pb.Message, self *p2p.P2PNode, valid bool) ayame.SchedEvent {
	var ev ayame.SchedEvent
	author, _ := bs.ConvertPeer(self, mes.Data.Author)
	ayame.Log.Debugf("received msgid=%s,author=%s", mes.Data.Id, mes.Data.Author.Id)
	switch mes.Data.Type {
	case pb.MessageType_GET_VALUE:
		ev = NewBSGetEvent(author, mes.Data.Id, mes.IsRequest, mes.Data.Record)
		p, err := bs.ConvertPeer(self, mes.Sender)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
		ev.SetVerified(true) // always verified
		return ev
	case pb.MessageType_PUT_VALUE:
		ev = NewBSPutEvent(author, mes.Data.Id, mes.IsRequest, mes.Data.Record)
		p, err := bs.ConvertPeer(self, mes.Sender)
		if err != nil {
			panic(fmt.Sprintf("Failed to convert node: %s\n", err))
		}
		ev.SetSender(p)
		ev.SetVerified(true) // always verified
		return ev
	}
	return bs.ConvertMessage(mes, self, valid)
}
