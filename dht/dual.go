package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/piax/go-byzskip/authority"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
	bs "github.com/piax/go-byzskip/byzskip"
	"google.golang.org/protobuf/proto"
)

// assertion
const (
	GET_TIMEOUT = 20 * time.Second
	PUT_TIMEOUT = 10 * time.Second
)

func MVIdFinder(ctx context.Context, dht *BSDHT, id string) ([]*bs.BSNode, error) {
	return dht.Node.LookupMV(ctx, ayame.NewMembershipVectorFromId(id))
}

func (dht *BSDHT) LookupIPAddr(ctx context.Context, name string) ([]net.IPAddr, error) {
	fmt.Printf("*** LookupIPAddr %s\n", name)
	return net.DefaultResolver.LookupIPAddr(ctx, name)
}

func (dht *BSDHT) LookupLocal(ctx context.Context, name string) (bool, error) {
	fmt.Printf("*** LookupLocal %s\n", name)
	var key string
	var err error
	if key, err = Normalize(name); err != nil {
		return false, err
	}
	if rec, err := dht.getNamedLocal(ctx, key); rec != nil && err == nil {
		return true, nil
	}
	return false, nil
}

func (dht *BSDHT) LookupName(ctx context.Context, name string) ([]*bs.BSNode, error) {
	var key string
	var err error
	if key, err = Normalize(name); err != nil {
		return nil, err
	}
	var ps []*bs.BSNode
	pkey, err := ParseName(key)
	if err == nil {
		ukey := pkey
		if len(ukey.ID()) == 0 {
			log.Debugf("looking up key: %s @ %s\n", ukey.Value(), dht.Node.Id())
			ps, _ = dht.Node.LookupRange(ctx,
				ayame.NewRangeKey(
					ayame.NewUnifiedKeyFromByteValue(ukey.Value(), ayame.ZeroID()),
					false,
					ayame.NewUnifiedKeyFromByteValue(ukey.Value(), ayame.MaxID()),
					false))

		} else {
			ps, _ = dht.Node.Lookup(ctx, ukey)
		}
	} else {
		log.Errorf(fmt.Sprintf("namespace error: %s", err))
	}
	return ps, nil
}

func (dht *BSDHT) LookupTXT(ctx context.Context, name string) (values []string, err error) {
	fmt.Printf("***LookupTXT %s\n", name)
	_, values, err = dht.LookupNames(ctx, name, false)
	return values, err
}

// Not compatible with BasicResolver
func (dht *BSDHT) LookupNames(ctx context.Context, name string, isPrefix bool) ([]string, []string, error) {
	log.Debugf("LookupNames %s %v", name, isPrefix)
	// DNSLink specific handling
	name = strings.TrimPrefix(name, "_dnslink.")
	name = strings.TrimSuffix(name, ".")
	rslt, err := dht.GetNamedValues(ctx, name, isPrefix)
	var keys []string = []string{}
	var values []string = []string{}
	for _, r := range rslt {
		str, err := url.QueryUnescape(string(r.Key))
		if err != nil {
			str = string(r.Key)
		}
		keys = append(keys, str)
		values = append(values, string(r.Val))
	}
	return keys, values, err
}

func (dht *BSDHT) getRecordWithPrefixFromDatastore(ctx context.Context, key string) ([]*pb.Record, error) {
	q := query.Query{Filters: []query.Filter{query.FilterKeyPrefix{Prefix: ds.NewKey(key).String()}}}
	results, err := dht.datastore.Query(ctx, q)
	//results, err := dht.datastore.Get(ctx, ds.NewKey(key))
	if err != nil {
		return nil, err
	}
	es, _ := results.Rest()
	ret := []*pb.Record{}
	for _, e := range es {
		rec := new(pb.Record)
		err = proto.Unmarshal(e.Value, rec)
		if err != nil {
			// Bad data in datastore, log it but don't return an error, we'll just overwrite it
			return nil, err
		}
		err = dht.RecordValidator.Validate(string(rec.GetKey()), rec.GetValue())
		if err != nil {
			return nil, err
		}
		ret = append(ret, rec)
	}
	return ret, nil
}

func (dht *BSDHT) getNamedLocal(ctx context.Context, key string) ([]*pb.Record, error) {
	log.Debugf("finding value in datastore for key %s", key)
	return dht.getRecordWithPrefixFromDatastore(ctx, key)
}

func (dht *BSDHT) getPCert() (*authority.PCert, error) {
	cbytes := dht.Node.Parent.(*p2p.P2PNode).Cert
	_, va, vb, err := authority.ExtractCert(cbytes)
	if err != nil {
		return nil, err
	}
	ret := &authority.PCert{
		Key:         dht.Node.Key(),
		ID:          dht.Node.Id(),
		Mv:          dht.Node.MV(),
		Name:        dht.Node.Name(),
		ValidAfter:  time.Unix(va, 0),
		ValidBefore: time.Unix(vb, 0),
		Cert:        cbytes,
	}
	return ret, nil
}

func (dht *BSDHT) sendMultiPutValue(ctx context.Context, p ayame.Node, rec *pb.Record) error {
	log.Debugf("send multi put value")
	if dht.Node.Id() == p.Id() { // put to self. This should be already done.
		return nil
	}
	mes := NewBSMultiPutEvent(dht.Node, dht.Node.NewMessageId(), true, rec)
	resp := dht.Node.SendRequest(ctx, p, mes)
	if ev, ok := resp.(*BSMultiPutEvent); ok {
		if !bytes.Equal(rec.Value, ev.Record.Value) {
			const errStr = "value not put correctly"
			log.Info(errStr, "put-message", rec, "get-message", ev.Record)
			return errors.New(errStr)
		}
		log.Debugf("exec multi put %s=%s successfully on: %s", rec.Key, rec.Value, p.Id())
		return nil
	}
	if ev, ok := resp.(*bs.FailureResponse); ok {
		return ev.Err
	}
	return fmt.Errorf("invalid response type %s", resp)
}

// PutNamedValue
func (dht *BSDHT) PutNamedValue(ctx context.Context, name string, value []byte, opts ...routing.Option) error {
	// /hrns/name
	n, err := Normalize(name)
	if err != nil {
		return err
	}
	k, err := ParseName(n)
	if err != nil {
		return err
	}
	k.SetID(dht.Node.Id())

	// Authorization succeeds only if the peer has already been authorized for the IdKey.
	//netKey, m, cert, err := Authorizer(dht.Node.Id(), k)
	//log.Debugf("k=%v key=%v, %v=%v? %s => %s cert=%v\n", k, netKey, m, dht.Node.Id(), netKey, dht.Node.Key(), cert)

	// at this point, netKey is the permitted key.
	// name:peer.ID
	key := ayame.NewUnifiedKeyFromString(name, dht.Node.Id())
	//key, _ = Normalize(key)
	recordKey, err := Normalize(key.String())
	if err != nil {
		return err
	}

	//log.Debugf("Put key=%s, value len1=%d, len2=%d", recordKey, len(value), len(rec.GetValue()))
	/*
		rec := MakePutRecord(recordKey, value)
		rec.TimeReceived = u.FormatRFC3339(time.Now())
	*/

	pcert, err := dht.getPCert()
	if err != nil {
		return err
	}
	rec, err := dht.makeNamedValueRecord(recordKey, value, pcert)
	if err != nil {
		return err
	}

	err = dht.RecordValidator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		log.Debugf("before local put, validate failure key=%s, %s", key, err)
		return err
	}

	// put to local
	old, err := dht.getLocalRaw(ctx, recordKey)
	if err != nil {
		log.Debugf("get local failure key=%s, %s", key, err)
		return err
	}
	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		log.Debugf("comparing local value, old len=%d, new len=%d", len(old.GetValue()), len(value))
		// Check to see if the new one is better.
		i, err := dht.RecordValidator.Select(recordKey, [][]byte{value, old.GetValue()})
		if err != nil {
			log.Debugf("select failure key=%s, %s", key, err)
			return err
		}
		if i != 0 {
			return fmt.Errorf("can't replace a newer value with an older value")
		}
	}

	err = dht.putLocalRaw(ctx, recordKey, rec)
	if err != nil {
		log.Debugf("put local failure key=%s, %s", key, err)
		return err
	}

	ps, err := dht.idFinder(ctx, dht, recordKey)
	if err != nil {
		return err
	}
	log.Debugf("lookup done: %v", ps)

	wg := sync.WaitGroup{}
	for _, p := range ps {
		wg.Add(1)
		go func(p ayame.Node) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			err := dht.sendMultiPutValue(ctx, p, rec)
			if err != nil {
				log.Debugf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()
	/*	peers := nsToIs(ps)

		numSuccess := dht.execOnManyMulti(ctx, func(ctx context.Context, p ayame.Node) error {
			return dht.sendMultiPutValue(ctx, p, rec)
		}, peers)
		log.Debugf("execOnManyMulti done, numSuccess=%d", numSuccess) */
	return nil
}

func (dh *BSDHT) sendGetValues(ctx context.Context, p ayame.Node, key string) ([]*pb.Record, error) {
	if dh.Node.Id() == p.Id() { // put to self.
		return dh.getRecordWithPrefixFromDatastore(ctx, key)
	}
	mes := NewBSMultiGetEvent(dh.Node, dh.Node.NewMessageId(), true, []*pb.Record{{Key: []byte(key)}})
	//fmt.Printf("*** context to send request: %s\n", ctx.Err())
	getCtx, cancel := context.WithTimeout(context.Background(), GET_TIMEOUT)
	defer cancel()
	resp := dh.Node.SendRequest(getCtx, p, mes)

	if ev, ok := resp.(*BSMultiGetEvent); ok {
		log.Debugf("%s: got ev=%v", dh.Node.Id(), ev)
		if len(ev.Record) > 0 {
			log.Debugf("%s: got first key=%s, value len=%d", dh.Node.Id(), string(ev.Record[0].Key), len(ev.Record[0].Value))
		}
		return ev.Record, nil
	}
	if ev, ok := resp.(*bs.FailureResponse); ok {
		return nil, ev.Err
	}
	return nil, fmt.Errorf("invalid response type %s", resp)
}

// exec fn on all peers, with TIMEOUT_PER_OP.
// returns number of successful execution of fn.
func (dht *BSDHT) execOnManyMulti(ctx context.Context, fn func(context.Context, ayame.Node) error, peers []ayame.Node) int {
	if len(peers) == 0 {
		return 0
	}

	// Create a buffered channel to avoid goroutine leaks
	errCh := make(chan error, len(peers))
	numSuccessfulToWaitFor := int(float64(len(peers)) * WAIT_FRAC)

	// Create a timeout context for the entire operation
	putctx, cancel := context.WithTimeout(ctx, TIMEOUT_PER_OP)
	defer cancel()

	// Use a WaitGroup to track goroutines
	var wg sync.WaitGroup
	wg.Add(len(peers))

	// Launch goroutines for each peer
	for _, p := range peers {
		go func(p ayame.Node) {
			defer wg.Done()
			// Create a new context for each operation
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
				// If we have enough successes, we can return early
				if numSuccess >= numSuccessfulToWaitFor {
					return numSuccess
				}
			}
		case <-putctx.Done():
			// Context was cancelled or timed out
			if putctx.Err() == context.DeadlineExceeded {
				log.Debugf("execOnManyMulti timed out after %v", TIMEOUT_PER_OP)
			} else {
				log.Debugf("execOnManyMulti cancelled: %v", putctx.Err())
			}
			return numSuccess
		}
		log.Debugf("execOnManyMulti loop, numDone=%d, numSuccess=%d", numDone, numSuccess)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	log.Debugf("execOnManyMulti done, numSuccess=%d", numSuccess)
	return numSuccess
}

// the key should have the following format.
// /hrns/<name>[:<peerID>]
func (dht *BSDHT) GetNamedValues(ctx context.Context, key string, isPrefix bool, opts ...routing.Option) ([]RecvdVal, error) {
	// normalize the key
	key, err := Normalize(url.QueryEscape(key))
	if err != nil {
		return nil, err
	}
	responses, err := dht.searchNamedValue(ctx, key, isPrefix, opts...)
	if err != nil {
		return nil, err
	}
	ret := []RecvdVal{}

	for r := range responses {
		ret = append(ret, r)
	}

	if ctx.Err() != nil {
		return ret, ctx.Err()
	}

	log.Debugf("GetNamedValue %s %v", key, ret)
	return ret, nil
}

func allAbove(recs map[string][]RecvdVal, nvals int) bool {
	for _, v := range recs {
		if len(v) < nvals {
			return false
		}
	}
	return true
}

func (dht *BSDHT) searchMultipleValues(ctx context.Context, key string, valCh <-chan RecvdVal, stopCh chan struct{},
	out chan<- RecvdVal, nvals int) (map[string]RecvdVal, bool) {
	return dht.processMultipleValues(ctx, key, valCh,
		func(ctx context.Context, keystr string, v RecvdVal, recs map[string]RecvdVal, collected map[string][]RecvdVal, better bool) bool {
			if better {
				select {
				case out <- v:
				case <-ctx.Done():
					return false
				}
			}

			if nvals > 0 && allAbove(collected, nvals) {
				close(stopCh)
				return true
			}

			return false
		})
}

func (dht *BSDHT) processMultipleValues(ctx context.Context, _ string, vals <-chan RecvdVal,
	newVal func(ctx context.Context, keystr string, v RecvdVal, recs map[string]RecvdVal, collected map[string][]RecvdVal, better bool) bool) (best map[string]RecvdVal, aborted bool) {
	bestmap := make(map[string]RecvdVal)
	collected := make(map[string][]RecvdVal)
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
			// aggregate records for each key
			keystr := string(v.Key)
			if cur, ok := collected[keystr]; !ok {
				collected[keystr] = []RecvdVal{v}
			} else {
				collected[keystr] = append(cur, v)
			}

			if cur, ok := bestmap[keystr]; !ok {
				bestmap[keystr] = v
			} else {
				if bytes.Equal(cur.Val, v.Val) {
					aborted = newVal(ctx, keystr, v, bestmap, collected, false)
					continue
				}
				sel, err := dht.RecordValidator.Select(keystr, [][]byte{cur.Val, v.Val})
				if err != nil {
					log.Debugf("failed to select best value key %s err=%s", keystr, err)
					continue
				}
				if sel != 1 {
					aborted = newVal(ctx, keystr, v, bestmap, collected, false)
					continue
				}
			}
			aborted = newVal(ctx, keystr, v, bestmap, collected, true)
		case <-ctx.Done():
			return
		}
	}

	return
}

func (dh *BSDHT) searchNamedValue(ctx context.Context, key string, isPrefix bool, opts ...routing.Option) (<-chan RecvdVal, error) {
	//key := "/v/" + ukey.String()

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = GetQuorum(&cfg)
	}

	stopCh := make(chan struct{})
	valCh, lookupRes := dh.getNamedValues(ctx, key, isPrefix, stopCh)

	out := make(chan RecvdVal)
	go func() {
		defer close(out)
		_, aborted := dh.searchMultipleValues(ctx, key, valCh, stopCh, out, responsesNeeded)
		if aborted {
			return
		}

		select {
		case l := <-lookupRes:
			if l == nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}()

	return out, nil
}

func (dht *BSDHT) getNamedValues(ctx context.Context, key string, isPrefix bool, stopQuery chan struct{}) (<-chan RecvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan RecvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	var ps []*bs.BSNode
	pkey, err := ParseName(key)
	if err == nil {
		ukey := pkey
		if len(ukey.ID()) == 0 {
			log.Debugf("looking up key: %s @ %s\n", ukey.Value(), dht.Node.Id())
			var rng *ayame.RangeKey
			if isPrefix {
				rng = ayame.NewUnifiedRangeKeyForPrefix(string(ukey.Value()))
			} else {
				rng = ayame.NewRangeKey(
					ayame.NewUnifiedKeyFromByteValue(ukey.Value(), ayame.ZeroID()),
					false,
					ayame.NewUnifiedKeyFromByteValue(ukey.Value(), ayame.MaxID()),
					false)
			}
			ps, _ = dht.Node.LookupRange(ctx, rng)
		} else {
			ps, _ = dht.Node.Lookup(ctx, ukey)
		}
	} else {
		log.Errorf(fmt.Sprintf("namespace error: %s", err))
	}

	peers := nsToIs(ps)
	if len(peers) == 0 {
		close(valCh)
		close(lookupResCh)
		return valCh, lookupResCh
	}

	log.Debugf("get range done: for %s, %v\n", key, peers)

	// Create channels and mutex for synchronization
	done := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var channelsClosed bool

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() {
			mu.Lock()
			if !channelsClosed {
				close(valCh)
				close(lookupResCh)
				channelsClosed = true
			}
			mu.Unlock()
		}()

		queryCtx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		queryFn := func(ctx context.Context, p ayame.Node) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
				return nil
			default:
				recs, err := dht.sendGetValues(ctx, p, key)
				if err != nil {
					return err
				}
				for _, rec := range recs {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-done:
						return nil
					default:
						key, value, err := dht.extractNamedValue(rec)
						if err != nil {
							log.Warningf("invalid record: %s", err)
							continue
						}
						if rec != nil && rec.GetValue() != nil {
							rv := RecvdVal{
								Key:  []byte(key.String()),
								Val:  value,
								From: p.Id(),
							}

							// Check if channels are closed before sending
							mu.Lock()
							if channelsClosed {
								mu.Unlock()
								return nil
							}

							// Non-blocking send with timeout
							timer := time.NewTimer(100 * time.Millisecond)
							select {
							case valCh <- rv:
								timer.Stop()
								log.Debugf("Successfully sent value to channel")
							case <-timer.C:
								log.Debugf("Timeout while sending value to channel")
							case <-ctx.Done():
								timer.Stop()
								mu.Unlock()
								return ctx.Err()
							case <-done:
								timer.Stop()
								mu.Unlock()
								return nil
							}
							mu.Unlock()
						}
					}
				}
				return nil
			}
		}

		success := dht.execOnManyMulti(queryCtx, queryFn, peers)
		log.Debugf("execOnManyMulti completed with %d successes", success)

		// Check if channels are closed before sending lookup result
		mu.Lock()
		if !channelsClosed {
			// Non-blocking send for lookup result
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case lookupResCh <- &lookupWithFollowupResult{peers: peers}:
				timer.Stop()
				log.Debugf("Successfully sent lookup result")
			case <-timer.C:
				log.Debugf("Timeout while sending lookup result")
			case <-ctx.Done():
				timer.Stop()
				log.Debugf("Context cancelled while sending lookup result")
			}
		}
		mu.Unlock()
	}()

	// Start a goroutine to handle cleanup
	go func() {
		select {
		case <-ctx.Done():
			close(done)
		case <-stopQuery:
			close(done)
		}
		wg.Wait()
	}()

	return valCh, lookupResCh
}

func (dht *BSDHT) ValidateRecord(record *pb.Record, sender ayame.Node) bool {
	// Validate that the record key matches the sender's ID and name
	key, err := ParseName(string(record.GetKey()))
	if err != nil {
		log.Warningf("Invalid record key format: %v", err)
		return false
	}
	if key.ID() != sender.Id() {
		log.Warningf("Unauthorized request: Id in the event=%s, but sender=%s", key, sender)
		return false
	}
	keyName := strings.TrimRight(string(key.Value()), "\x00")
	senderName := sender.Name()
	if keyName != senderName {
		log.Warningf("Unauthorized request: name in the event key='%s', but sender's name='%s'", keyName, senderName)
		return false
	}
	return true
}

func (dht *BSDHT) HandleMultiGetRequest(ctx context.Context, ev *BSMultiGetEvent) ayame.SchedEvent {
	log.Infof("get from=%v, key=%s @%s\n", ev.Sender(), ev.Record[0].GetKey(), dht.Node.Id())
	//	fmt.Printf("getting key: %s @ %s\n", ev.Record[0].GetKey(), dh.Node.Id())

	rec, err := dht.getRecordWithPrefixFromDatastore(ctx, string(ev.Record[0].GetKey()))

	if err != nil { // XXX ignore
		rec = nil
	}
	log.Debugf("got rec=%v", rec)
	ret := NewBSMultiGetEvent(dht.Node, ev.MessageId(), false, rec)
	return ret
}

func (dht *BSDHT) HandleMultiGetResEvent(ctx context.Context, ev *BSMultiGetEvent) error {
	//log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
	dht.Node.ProcsMutex.RLock()
	proc, exists := dht.Node.Procs[ev.MessageId()]
	dht.Node.ProcsMutex.RUnlock()
	if exists && proc.Ch != nil { // sync
		proc.Ch <- ev
		return nil
	} // XXX async is not implemented yet
	return fmt.Errorf("%v: unregistered response msgid=%s received from %s", dht, ev.MessageId(), ev.Sender())
}

func (dht *BSDHT) HandleMultiPutRequest(ctx context.Context, ev *BSMultiPutEvent) ayame.SchedEvent {
	if !dht.ValidateRecord(ev.Record, ev.Sender()) {
		return NewBSMultiPutEvent(dht.Node, ev.MessageId(), false, nil)
	}
	err := dht.putLocalRaw(ctx, string(ev.Record.GetKey()), ev.Record)
	if err != nil {
		log.Warningf("failed to put local raw: %s", err)
		return NewBSMultiPutEvent(dht.Node, ev.MessageId(), false, nil)
	}
	log.Debugf("handle multi-put finished on %s@%v from=%v, key=%s, len=%d\n", dht.Node.Name(), dht.Node.Id(), ev.Sender(), string(ev.Record.GetKey()), len(ev.Record.Value))

	rec, err := dht.getLocalRaw(ctx, string(ev.Record.GetKey()))
	if err != nil {
		rec = nil
	}
	recs, err := dht.getRecordWithPrefixFromDatastore(ctx, string(ev.Record.GetKey()))
	log.Debugf("got prefixed recs after put=%v", recs)
	if err != nil {
		log.Warningf("failed to get record with prefix from datastore: %s", err)
		rec = nil
	}
	if rec == nil {
		return NewBSMultiPutEvent(dht.Node, ev.MessageId(), false, nil)
	}
	if rec.GetValue() != nil && ev.Record.GetValue() != nil && bytes.Equal(rec.GetValue(), ev.Record.GetValue()) {
		log.Debugf("Record is correctly stored")
		return NewBSMultiPutEvent(dht.Node, ev.MessageId(), false, rec)
	}

	ret := NewBSMultiPutEvent(dht.Node, ev.MessageId(), false, rec)
	log.Debugf("returning multi-put record on %s@%v to=%v, key=%s, len=%d\n",
		dht.Node.Name(), dht.Node.Id(), ev.Sender(), string(ret.Record.GetKey()), len(ret.Record.GetValue()))
	return ret
}

func (dht *BSDHT) HandleMultiPutResEvent(ctx context.Context, ev *BSMultiPutEvent) error {
	dht.Node.ProcsMutex.RLock()
	proc, exists := dht.Node.Procs[ev.MessageId()]
	dht.Node.ProcsMutex.RUnlock()
	if exists && proc.Ch != nil {
		log.Debugf("put finished from=%v\n", ev.Sender())
		proc.Ch <- ev
		return nil
	}
	return nil
}

//func (dht *DualDHT) ProvideMH(ctx context.Context, keyMH multihash.Multihash, brdcst bool) error {

//idKey := ayame.NewStringIdKey(string(keyMH))
//ps, err := dh.Node.Lookup(closerCtx, idKey)
// TO->
//	ps, err := dht.Node.LookupMV(closerCtx, ayame.NewMembershipVectorFromId(string(keyMH)), dht.Node.Key())
//	peers := nsToIs(ps)
//

//func (dht *DualDHT) FindPeer(ctx context.Context, id peer.ID) (peer.AddrInfo, error) {
//idkey := ayame.NewIdKey(id)
// TO->
//clst, _ := dh.Node.Lookup(ctx, idkey)
//	clst, _ := dht.Node.LookupMV(ctx, ayame.NewMembershipVectorFromId(string(id)), dht.Node.Key())
//}
