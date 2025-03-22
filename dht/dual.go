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
	return net.DefaultResolver.LookupIPAddr(ctx, name)
}

func (dht *BSDHT) LookupLocal(ctx context.Context, name string) (bool, error) {
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
			ayame.Log.Debugf("looking up key: %s @ %s\n", ukey.Value(), dht.Node.Id())
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
		ayame.Log.Errorf(fmt.Sprintf("namespace error: %s", err))
	}
	return ps, nil
}

func (dht *BSDHT) LookupTXT(ctx context.Context, name string) (values []string, err error) {
	_, values, err = dht.LookupNames(ctx, name, false)
	return values, err
}

// Not compatible with BasicResolver
func (dht *BSDHT) LookupNames(ctx context.Context, name string, isPrefix bool) ([]string, []string, error) {
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
	ayame.Log.Debugf("finding value in datastore for key %s", key)
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
	ayame.Log.Debugf("send multi put value")
	if dht.Node.Id() == p.Id() { // put to self. This should be already done.
		return nil
	}
	mes := NewBSMultiPutEvent(dht.Node, dht.Node.NewMessageId(), true, rec)
	resp := dht.Node.SendRequest(ctx, p, mes)
	if ev, ok := resp.(*BSMultiPutEvent); ok {
		if !bytes.Equal(rec.Value, ev.Record.Value) {
			const errStr = "value not put correctly"
			ayame.Log.Info(errStr, "put-message", rec, "get-message", ev.Record)
			return errors.New(errStr)
		}
		ayame.Log.Debugf("exec multi put successfully on: %s", p.Id())
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
	//ayame.Log.Debugf("k=%v key=%v, %v=%v? %s => %s cert=%v\n", k, netKey, m, dht.Node.Id(), netKey, dht.Node.Key(), cert)

	// at this point, netKey is the permitted key.
	// name:peer.ID
	key := ayame.NewUnifiedKeyFromString(name, dht.Node.Id())
	//key, _ = Normalize(key)
	recordKey, err := Normalize(key.String())
	if err != nil {
		return err
	}

	ayame.Log.Debugf("Put key=%s, value len=%d", recordKey, len(value))
	if err := dht.RecordValidator.Validate(recordKey, value); err != nil {
		ayame.Log.Debugf("validation failure key=%s, value len=%d", key, len(value))
		return err
	}
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
	/* XXXX Is it necessary to put on local?
	// put to local
	old, err := dht.getLocalRaw(ctx, recordKey)
	if err != nil {
		ayame.Log.Debugf("get local failure key=%s, %s", key, err)
		return err
	}
	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// Check to see if the new one is better.
		i, err := dht.RecordValidator.Select(recordKey, [][]byte{value, old.GetValue()})
		if err != nil {
			ayame.Log.Debugf("select failure key=%s, %s", key, err)
			return err
		}
		if i != 0 {
			return fmt.Errorf("can't replace a newer value with an older value")
		}
	}*/

	/*
		err = dht.putLocalRaw(ctx, recordKey, rec)
		if err != nil {
			ayame.Log.Debugf("put local failure key=%s, %s", key, err)
			return err
		}
	*/

	peers, err := dht.Node.Lookup(ctx, key)
	if err != nil {
		return err
	}
	fmt.Printf("key lookup done: for %s, %v\n", key, peers)

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p ayame.Node) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			err := dht.sendMultiPutValue(ctx, p, rec)
			if err != nil {
				ayame.Log.Debugf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()

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
		ayame.Log.Debugf("%s: got ev=%v", dh.Node.Id(), ev)
		if len(ev.Record) > 0 {
			ayame.Log.Debugf("%s: got first key=%s, value len=%d", dh.Node.Id(), string(ev.Record[0].Key), len(ev.Record[0].Value))
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

	// having a buffer that can take all of the elements is basically a hack to allow for sloppy exits that clean up
	// the goroutines after the function is done rather than before
	errCh := make(chan error, len(peers))

	putctx, cancel := context.WithTimeout(ctx, TIMEOUT_PER_OP)
	defer cancel()

	for _, p := range peers {
		go func(p ayame.Node) {
			errCh <- fn(putctx, p)
		}(p)
	}

	var numDone, numSuccess int

	for numDone < len(peers) {
		ayame.Log.Debugf("numDone=%d < len(peers)=%d", numDone, len(peers))
		select {
		case err := <-errCh:
			numDone++
			if err == nil {
				numSuccess++
				ayame.Log.Debugf("numSuccess=%d numDone=%d len=%d", numSuccess, numDone, len(peers))
			}
		case <-ctx.Done():
			ayame.Log.Debugf("execution ended with:%s", ctx.Err())
		}
	}
	return numSuccess
}

// the key should have the following format.
// /hrns/<name>[:<peerID>]
func (dht *BSDHT) GetNamedValues(ctx context.Context, key string, isPrefix bool, opts ...routing.Option) ([]RecvdVal, error) {
	// normalize the key
	key, err := Normalize(key)
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

	ayame.Log.Debugf("GetNamedValue %s %v", key, ret)
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

func (dht *BSDHT) processMultipleValues(ctx context.Context, key string, vals <-chan RecvdVal,
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
					ayame.Log.Debugf("failed to select best value key %s err=%s", keystr, err)
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
	//key := "/v/" + ukey.String()
	valCh := make(chan RecvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)
	/* XXX local repository is not yet.
	fn := func() {
		ayame.Log.Debugf("finding value with prefix %s", key)
		if recs, err := dh.getNamedLocal(ctx, key); recs != nil && err == nil {
			for _, rec := range recs {
				ayame.Log.Debugf("found local value for key=%s: %s", key, rec)
				if rec != nil && rec.GetValue() != nil {
					rv := RecvdVal{
						Key:  rec.GetKey(),
						Val:  rec.GetValue(),
						From: dh.Node.Id(),
					}
					select {
					case valCh <- rv:
						ayame.Log.Debugf("sent to channel local value for key=%s: %s", key, rec)
					case <-ctx.Done():
						ayame.Log.Debugf("context done.")
					}
				}
			}
		}
	}
	// find local
	go fn()
	*/
	var ps []*bs.BSNode
	//idKey := ayame.NewStringIdKey(key)
	//ps, _ := dh.Node.Lookup(ctx, &idKey)
	pkey, err := ParseName(key)
	if err == nil {
		ukey := pkey
		if len(ukey.ID()) == 0 {
			ayame.Log.Debugf("looking up key: %s @ %s\n", ukey.Value(), dht.Node.Id())
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
		ayame.Log.Errorf(fmt.Sprintf("namespace error: %s", err))
	}

	peers := nsToIs(ps)

	fmt.Printf("get range done: for %s, %v\n", key, peers)

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		queryFn := func(ctx context.Context, p ayame.Node) error {
			recs, err := dht.sendGetValues(ctx, p, key)
			if err != nil {
				return err
			}
			// TODO: What should happen if the record is invalid?
			// Pre-existing code counted it towards the quorum, but should it?
			for _, rec := range recs {
				key, value, err := dht.extractNamedValue(rec)
				if err != nil {
					ayame.Log.Warningf("invalid record: %s", err)
					continue
				}
				if rec != nil && rec.GetValue() != nil {
					rv := RecvdVal{
						Key:  []byte(key.String()),
						Val:  value,
						From: p.Id(),
					}
					select {
					case valCh <- rv:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
			return nil
		}

		dht.execOnManyMulti(ctx, queryFn, peers)
		lookupResCh <- &lookupWithFollowupResult{peers: peers}
	}()
	return valCh, lookupResCh
}

func (dht *BSDHT) ValidateRecord(record *pb.Record, sender ayame.Node) bool {
	// Validate that the record key matches the sender's ID and name
	key, err := ParseName(string(record.GetKey()))
	if err != nil {
		ayame.Log.Warningf("Invalid record key format: %v", err)
		return false
	}
	if key.ID() != sender.Id() {
		ayame.Log.Warningf("Unauthorized request: Id in the event=%s, but sender=%s", key, sender)
		return false
	}
	keyName := strings.TrimRight(string(key.Value()), "\x00")
	senderName := sender.Name()
	if keyName != senderName {
		ayame.Log.Warningf("Unauthorized request: name in the event key='%s', but sender's name='%s'", keyName, senderName)
		return false
	}
	return true
}

func (dht *BSDHT) HandleMultiGetRequest(ctx context.Context, ev *BSMultiGetEvent) ayame.SchedEvent {
	ayame.Log.Infof("get from=%v, key=%s @%s\n", ev.Sender(), ev.Record[0].GetKey(), dht.Node.Id())
	//	fmt.Printf("getting key: %s @ %s\n", ev.Record[0].GetKey(), dh.Node.Id())

	rec, err := dht.getRecordWithPrefixFromDatastore(ctx, string(ev.Record[0].GetKey()))

	if err != nil { // XXX ignore
		rec = nil
	}
	ayame.Log.Debugf("got rec=%v", rec)
	ret := NewBSMultiGetEvent(dht.Node, ev.MessageId(), false, rec)
	return ret
}

func (dht *BSDHT) HandleMultiGetResEvent(ctx context.Context, ev *BSMultiGetEvent) error {
	//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
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
	dht.putLocalRaw(ctx, string(ev.Record.GetKey()), ev.Record)
	ayame.Log.Debugf("handle multi-put finished on %s@%v from=%v, key=%s, len=%d\n", dht.Node.Name(), dht.Node.Id(), ev.Sender(), string(ev.Record.GetKey()), len(ev.Record.Value))

	rec, err := dht.getLocalRaw(ctx, string(ev.Record.GetKey()))
	if err != nil {
		rec = nil
	}

	ret := NewBSMultiPutEvent(dht.Node, ev.MessageId(), false, rec)
	ayame.Log.Debugf("returning multi-put record on %s@%v to=%v, key=%s, len=%d\n", dht.Node.Name(), dht.Node.Id(), ev.Sender(), string(ret.Record.GetKey()), len(ret.Record.Value))
	return ret
}

func (dht *BSDHT) HandleMultiPutResEvent(ctx context.Context, ev *BSMultiPutEvent) error {
	dht.Node.ProcsMutex.RLock()
	proc, exists := dht.Node.Procs[ev.MessageId()]
	dht.Node.ProcsMutex.RUnlock()
	if exists && proc.Ch != nil {
		ayame.Log.Debugf("put finished from=%v\n", ev.Sender())
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
