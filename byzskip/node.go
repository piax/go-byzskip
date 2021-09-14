// Copyright 2021 Yuuichi Teranishi, NICT, Japan
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

// Package byzskip provides a ByzSkip implementation.
//
// You can create a Node, Join to a network, Lookup a node, and Unicast a message to a node.
package byzskip

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/piax/go-ayame/ayame"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
	"github.com/thoas/go-funk"
)

type JoinStats struct {
	runningQueries int       // running queries
	closest        []*BSNode // the current closest nodes
	candidates     []*BSNode // the candidate nodes
	queried        []*BSNode // store the queried remote nodes
	failed         []*BSNode // store the queried (but failed) remote nodes
}

func (s *JoinStats) String() string {
	return fmt.Sprintf("closers=%s, candidates=%s, queried=%s, failed=%s", ayame.SliceString(s.closest), ayame.SliceString(s.candidates), ayame.SliceString(s.queried), ayame.SliceString(s.failed))
}

type FindNodeResponse struct {
	id         string    // the request id
	sender     *BSNode   // the responder
	closest    []*BSNode // the current closer nodes
	candidates []*BSNode // the candidate nodes
	level      int
	isFailure  bool // the responder is in failure
}

// -- find node
type FindNodeProc struct {
	id string
	ev *BSFindNodeEvent
	ch chan *FindNodeResponse
}

type BSNode struct {
	parent ayame.Node
	key    ayame.Key
	mv     *ayame.MembershipVector
	//bs.IntKeyMV
	stats *JoinStats

	RoutingTable RoutingTable
	IsFailure    bool
	QuerySeen    map[string]int

	Procs      map[string]*FindNodeProc
	procsMutex sync.RWMutex // for r/w procs status
	rtMutex    sync.RWMutex // for r/w routing table
	//statsMutex sync.RWMutex // for r/w join status
	seenMutex       sync.RWMutex                               // for r/w QuerySeen
	unicastHandler  func(*BSNode, *BSUnicastEvent, bool, bool) // received event, already seen, next already on the path
	messageReceiver func(*BSNode, *BSUnicastEvent)             // a function when received a message

	proc               goprocess.Process
	DisableFixLowPeers bool
}

func (n *BSNode) Key() ayame.Key {
	return n.key
}

func (node *BSNode) Id() peer.ID { // Endpoint
	return node.parent.Id()
}

func (node *BSNode) Addrs() []ma.Multiaddr { // Endpoint
	return node.parent.Addrs()
}

func (node *BSNode) Send(ctx context.Context, ev ayame.SchedEvent, sign bool) error {
	return node.parent.Send(ctx, ev, sign)
}

func (n *BSNode) MV() *ayame.MembershipVector {
	return n.mv
}

func (n *BSNode) Equals(m KeyMV) bool {
	//return m.Key().Equals(n.key)
	mn := m.(*BSNode)
	return mn.Id() == n.Id() && m.Key().Equals(n.key)
}

func (n *BSNode) String() string {
	if n.Key() == nil {
		return "+"
	}
	ret := n.Key().String()
	if n.IsFailure {
		ret += "*"
	}
	return ret
}

func (n *BSNode) Encode() *pb.Peer {
	return n.parent.Encode()
}

func (n *BSNode) Close() error {
	n.NotifyDeletion(context.Background())
	time.Sleep(time.Duration(DURATION_BEFORE_CLOSE) * time.Millisecond)
	n.parent.Close()
	// process close
	return n.proc.Close()
}

type BSRoutingTable struct {
	//	nodes map[int]*BSNode // key=>node
	SkipRoutingTable
}

func NewBSRoutingTable(keyMV KeyMV) RoutingTable {
	t := NewSkipRoutingTable(keyMV)
	return &BSRoutingTable{SkipRoutingTable: *t} //, nodes: make(map[int]*BSNode)}
}

func ksToNs(lst []KeyMV) []*BSNode {
	ret := []*BSNode{}
	for _, ele := range lst {
		ret = append(ret, ele.(*BSNode))
	}
	return ret
}

func nsToKs(lst []*BSNode) []KeyMV {
	ret := []KeyMV{}
	for _, ele := range lst {
		ret = append(ret, ele)
	}
	return ret
}

// returns k-neighbors, the level found k-neighbors, neighbor candidates for s
func (node *BSNode) GetNeighborsAndCandidates(key ayame.Key, mv *ayame.MembershipVector) ([]*BSNode, int, []*BSNode) {
	ret, level := node.RoutingTable.KClosestWithKey(key)
	//can := node.routingTable.GetAll()
	can := node.RoutingTable.GetCommonNeighbors(mv)
	return ksToNs(ret), level, ksToNs(can)
}

func NewBSNode(parent ayame.Node, rtMaker func(KeyMV) RoutingTable, isFailure bool) *BSNode {
	ret := &BSNode{key: parent.Key(), mv: parent.MV(),
		//LocalNode: ayame.GetLocalNode(strconv.Itoa(key)),
		parent: parent,
		// stats is not generated at this time
		QuerySeen:          make(map[string]int),
		Procs:              make(map[string]*FindNodeProc),
		IsFailure:          isFailure,
		DisableFixLowPeers: false}
	ret.RoutingTable = rtMaker(ret)

	return ret
}

func (node *BSNode) GetClosestNodes(key ayame.Key) ([]*BSNode, int) {
	nb, lv := node.RoutingTable.KClosestWithKey(key)
	return ksToNs(nb), lv
}

func (node *BSNode) GetList(includeSelf bool, sorted bool) []*BSNode {
	return ksToNs(node.RoutingTable.AllNeighbors(includeSelf, sorted))
}

/*
func (node *BSNode) GetCandidates() []*BSNode {
	return ksToNs(node.RoutingTable.GetList())
}

func (node *BSNode) RoutingTableString() string {
	return node.RoutingTable.String()
}
*/
func UnincludedNodes(nodes []*BSNode, queried []*BSNode) []*BSNode {
	ret := []*BSNode{}
	for _, n := range nodes {
		found := false
		for _, m := range queried {
			if n.Equals(m) {
				found = true
				break
			}
		}
		if !found {
			ret = append(ret, n)
		}
	}
	//ayame.Log.Debugf("%d/%d queried\n", found, len(curKNodes))
	return ret
}

func AllContained(curKNodes []*BSNode, queried []*BSNode) bool {
	for _, n := range curKNodes {
		contained := false
		for _, m := range queried {
			if n.Equals(m) {
				//				ayame.Log.Debugf("%s is contained", n)
				contained = true
				break
			}
		}
		if !contained {
			//			ayame.Log.Debugf("%s is NOT contained", n)
			return false
		}
	}
	return true
}

func Contains(node *BSNode, nodes []*BSNode) bool {
	for _, n := range nodes {
		if n.Equals(node) {
			return true
		}
	}
	return false
}

func ContainsKey(key ayame.Key, nodes []*BSNode) bool {
	for _, n := range nodes {
		if n.Key().Equals(key) {
			return true
		}
	}
	return false
}

func AppendNodesIfMissing(lst []*BSNode, nodes []*BSNode) []*BSNode {
	for _, ele := range nodes {
		lst = AppendNodeIfMissing(lst, ele)
	}
	return lst
}

func AppendNodeIfMissing(lst []*BSNode, node *BSNode) []*BSNode {
	for _, ele := range lst {
		if ele.Equals(node) {
			return lst
		}
	}
	return append(lst, node)
}

func PathsString(paths [][]PathEntry) string {
	ret := ""
	for _, path := range paths {
		ret += "[" + strings.Join(funk.Map(path, func(pe PathEntry) string {
			return fmt.Sprintf("%s@%d", pe.Node, pe.Level)
		}).([]string), ",") + "]"
	}
	return ret
}

func PathString(path []PathEntry) string {
	return "[" + strings.Join(funk.Map(path, func(pe PathEntry) string {
		return fmt.Sprintf("%s@%d", pe.Node, pe.Level)
	}).([]string), ",") + "]"
}

func (pe PathEntry) String() string {
	return pe.Node.String()
}

const (
	JOIN_TIMEOUT          = 30
	LOOKUP_TIMEOUT        = 30
	DELNODE_TIMEOUT       = 30
	FIND_NODE_TIMEOUT     = 10
	FIND_NODE_PARALLELISM = 5
	FIND_NODE_INTERVAL    = 1
	UNICAST_SEND_TIMEOUT  = 1
	DELNODE_SEND_TIMEOUT  = 300 // millisecond
	DURATION_BEFORE_CLOSE = 100 // millisecond
)

var SeqNo int = 0

func NextId() string {
	SeqNo++
	return strconv.Itoa(SeqNo)
}

func (n *BSNode) pickCandidates(stat *JoinStats, count int) []*BSNode {
	ret := []*BSNode{}
	///if !stat.reached {
	for _, c := range stat.closest {
		if len(ret) == count {
			return ret
		}
		ret = AppendNodeIfMissing(ret, c)
		ret = UnincludedNodes(ret, stat.queried)
		ret = UnincludedNodes(ret, []*BSNode{n}) // remove self.
	}
	///}
	for _, c := range stat.candidates {
		if len(ret) == count {
			return ret
		}
		ret = AppendNodeIfMissing(ret, c)
		ret = UnincludedNodes(ret, stat.queried)
		ret = UnincludedNodes(ret, []*BSNode{n}) // remove self.
	}
	// not enough number
	return ret
}

func (n *BSNode) IsIntroducer() bool {
	return n.Key() == nil
}

func NewSimNode(key ayame.Key, mv *ayame.MembershipVector) *BSNode {
	return NewBSNode(ayame.NewLocalNode(key, mv), NewBSRoutingTable, false)
}

// Join a node join to the network.
// introducer's multiaddr is specified as a addr argument
func (n *BSNode) Join(ctx context.Context, addr string) error {
	introducer, _ := n.IntroducerNode(addr)
	return n.JoinWithNode(ctx, introducer)
}

var ResponseCount int // for sim

func (n *BSNode) RefreshRTWait(ctx context.Context) error {
	joinCtx, cancel := context.WithTimeout(ctx, time.Duration(JOIN_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()

	findCh := make(chan *FindNodeResponse, 1)
	for !n.allQueried() {
		reqCount := 0
		cs := n.pickCandidates(n.stats, FIND_NODE_PARALLELISM)
		if len(cs) == 1 && cs[0].IsIntroducer() { // remove the special node
			//n.statsMutex.Lock()
			n.stats.closest = []*BSNode{}
			//n.statsMutex.Unlock()
		}
		for _, c := range cs {
			node := c // candidate
			reqCount++
			id := NextId()
			go func() {
				n.FindNode(joinCtx, findCh, node, id)
			}()
		}
	L:
		for i := 0; i < reqCount; i++ {
			// continue to simulation for the responses reqCount n.ch<-
			select {
			case <-joinCtx.Done(): // after JOIN_TIMEOUT
				ayame.Log.Debugf("%s: Join process ended with %s\n", n, joinCtx.Err())
				break L
			case response := <-findCh: // FindNode finished
				ayame.Log.Debugf("%s: %d th/ %d FindNode for %s to %s finished", n, i+1, reqCount, response.id, response.sender)
				err := n.processResponse(response)
				if err != nil {
					return err // fatal error occured during the process
				}
			}
		}
		//XXX if needed
		//time.Sleep(time.Duration(FIND_NODE_INTERVAL) * time.Second)
		ayame.Log.Debugf("%s: join stats=%s", n.Key(), n.stats)
	}
	return nil
}

func (n *BSNode) JoinWithNode(ctx context.Context, introducer *BSNode) error {

	n.stats = &JoinStats{
		runningQueries: 0,
		closest:        []*BSNode{introducer},
		candidates:     []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}
	err := n.RefreshRTWait(ctx)
	if err != nil {
		return err
	}

	n.proc = goprocessctx.WithContext(ctx)
	if !n.DisableFixLowPeers {
		n.proc.Go(n.fixLowPeersRoutine)
	}
	return nil
}

func (n *BSNode) RunBootstrap(ctx context.Context) {

	n.stats = &JoinStats{
		runningQueries: 0,
		closest:        []*BSNode{n},
		candidates:     []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}
	n.RefreshRTWait(ctx)

	n.proc = goprocessctx.WithContext(ctx)
	if !n.DisableFixLowPeers {
		n.proc.Go(n.fixLowPeersRoutine)
	}
}

func (n *BSNode) JoinAsync(ctx context.Context, introducer *BSNode) {
	n.stats = &JoinStats{runningQueries: 0,
		closest:    []*BSNode{introducer},
		candidates: []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}

	if !n.allQueried() {
		n.sendNextParalellRequest(ctx, 1) // XXX one failure kills the join process
		ayame.Log.Debugf("%s: join stats=%s", n.Key(), n.stats)
	} else {
		ayame.Log.Debugf("%s: finish\n%s", n.Key(), n.RoutingTable)
	}
}

func (n *BSNode) sendNextParalellRequest(ctx context.Context, parallel int) int {
	reqCount := 0
	cs := n.pickCandidates(n.stats, parallel)
	if len(cs) == 1 && cs[0].IsIntroducer() { // remove the special node
		//n.statsMutex.Lock()
		n.stats.closest = []*BSNode{}
		//n.statsMutex.Unlock()
	}
	for _, c := range cs {
		node := c // candidate
		reqCount++
		n.stats.runningQueries++
		requestId := NextId()
		ev := NewBSFindNodeReqEvent(n, requestId, n.Key(), n.MV())
		n.procsMutex.Lock()
		n.Procs[requestId] = &FindNodeProc{ev: ev, id: requestId, ch: nil}
		n.procsMutex.Unlock()
		err := n.SendEvent(ctx, node, ev, false)
		if err != nil {
			n.stats.failed = append(n.stats.failed, node)
		}
	}
	return reqCount
}

func isFaultySet(nodes []*BSNode) bool {
	if len(nodes) == 0 {
		return false
	}
	for _, n := range nodes {
		if !n.IsFailure {
			return false
		}
	}
	// all faulty nodes!
	return true
}

func (n *BSNode) processResponse(response *FindNodeResponse) error {
	if response.isFailure { // timed out
		n.stats.queried = AppendNodeIfMissing(n.stats.queried, response.sender)
		n.stats.failed = AppendNodeIfMissing(n.stats.failed, response.sender)
		// delete response.sender from the routing table if already exists.
		n.rtMutex.Lock()
		//n.RoutingTable.Delete(response.sender.Key())
		n.RoutingTable.Del(response.sender)
		n.rtMutex.Unlock()
		return nil
	}
	if response.level == 0 {
		ayame.Log.Debugf("reached matched nodes %s", ayame.SliceString(response.candidates))
		initialNodes := append(n.stats.candidates, n.stats.closest...)
		if !n.IsFailure && isFaultySet(initialNodes) {
			ayame.Log.Infof("initial nodes hijacked: %s\n", initialNodes)
		}
		for _, m := range response.closest {
			if m.Key().Equals(n.Key()) {
				return fmt.Errorf("%s: Key already in use", n.Key())
			}
			fmt.Printf("%s: initial: %s", n.Key(), m.Key())
		}
	}
	n.rtMutex.Lock()
	n.RoutingTable.Add(response.sender) // XXX lock
	ayame.Log.Debugf("%s: added response sender %s", n, response.sender)
	n.rtMutex.Unlock()

	for _, c := range response.candidates {
		n.rtMutex.Lock()
		n.RoutingTable.Add(c)
		n.rtMutex.Unlock()
	}
	if EXCLUDE_CLOSEST_IN_NEIGHBORS {
		for _, c := range response.closest {
			n.rtMutex.Lock()
			n.RoutingTable.Add(c)
			n.rtMutex.Unlock()
		}
	}

	n.stats.closest = AppendNodesIfMissing(n.stats.closest, response.closest)
	// read from routing table
	n.rtMutex.RLock()
	n.stats.candidates = n.GetList(false, true)
	n.rtMutex.RUnlock()
	//n.stats.candidates = append(n.stats.candidates, response.candidates...)

	n.stats.queried = AppendNodeIfMissing(n.stats.queried, response.sender)

	ayame.Log.Debugf("%s: received closest=%s, level=%d, candidates=%s, next candidates=%s\n", n.Key(), ayame.SliceString(response.closest), response.level,
		ayame.SliceString(response.candidates),
		ayame.SliceString(n.stats.candidates))
	n.procsMutex.Lock()
	delete(n.Procs, response.id)
	n.procsMutex.Unlock()

	if response.level == 0 { // just for debug
		ayame.Log.Debugf("%d: start neighbor collection from %s, queried=%s\n", n.Key(),
			ayame.SliceString(UnincludedNodes(n.stats.candidates, n.stats.queried)),
			ayame.SliceString(n.stats.queried))
	}

	return nil
}

func (n *BSNode) NotifyDeletion(ctx context.Context) {
	deleteCtx, cancel := context.WithTimeout(ctx, time.Duration(DELNODE_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()
	// all entries without myself
	n.rtMutex.RLock()
	entries, _ := delNode(n.RoutingTable.AllNeighbors(false, false), n.Key())
	n.rtMutex.RUnlock()
	pos := 0
	ch := make(chan struct{}, 1)
	total := 0
	for pos < len(entries) {
		reqCount := 0
		for i := 0; i < FIND_NODE_PARALLELISM; i++ {
			ayame.Log.Debugf("%s: %d/%d\n", n.Key(), pos, len(entries))
			if pos == len(entries) {
				ayame.Log.Debugf("%d/%d break!\n", pos, len(entries))
				break
			}
			ev := NewBSDelNodeEvent(n, NextId(), n.Key())
			reqCount++
			go func(to *BSNode, ev *BSDelNodeEvent) {
				n.sendDelNodeEvent(ctx, ch, to, ev)
			}(entries[pos].(*BSNode), ev)
			pos++
		}
	L:
		for i := 0; i < reqCount; i++ {
			select {
			case <-deleteCtx.Done(): // after UNICAST_SEND_TIMEOUT
				ayame.Log.Debugf("DelNode %s ended with %s during %d/%d\n", n.Key(), deleteCtx.Err(), i, reqCount)
				break L
			case <-ch:
				total++
				ayame.Log.Debugf("%s: DelNode sent %d/%d\n", n.Key(), total, len(entries))
			}
		}
	}
	ayame.Log.Debugf("%s: DelNode process finished %d/%d\n", n.Key(), total, len(entries))
}

func (n *BSNode) allQueried() bool {
	ret := false
	//n.statsMutex.RLock()
	closestExceptMe := UnincludedNodes(n.stats.closest, []*BSNode{n})
	candidatesExceptMe := UnincludedNodes(n.stats.candidates, []*BSNode{n})
	ret = AllContained(closestExceptMe, n.stats.queried) &&
		AllContained(candidatesExceptMe, n.stats.queried)
	//n.statsMutex.RUnlock()
	return ret
}

func (n *BSNode) Lookup(ctx context.Context, key ayame.Key) []*BSNode {
	lookupCtx, cancel := context.WithTimeout(ctx, time.Duration(LOOKUP_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()

	closers, level := n.GetClosestNodes(key)
	if level == 0 {
		ayame.Log.Debugf("FindNode ended locally")
		return closers
	}
	queried := []*BSNode{}
	results := []*BSNode{}
	lookupCh := make(chan *FindNodeResponse)

	for !AllContained(closers, queried) {
		reqCount := 0
		cs := UnincludedNodes(closers, queried)

		for _, c := range cs {
			node := c // candidate
			reqCount++
			id := NextId()
			go func() { n.findNodeWithKey(lookupCtx, lookupCh, node, key, id) }()
		}
	L:
		for i := 0; i < reqCount; i++ {
			select {
			case <-lookupCtx.Done(): // after JOIN_TIMEOUT
				ayame.Log.Debugf("FindNode ended with %s", lookupCtx.Err())
				break L
			case response := <-lookupCh: // FindNode finished
				if response.isFailure { // timed out
					queried = AppendNodeIfMissing(queried, response.sender)
					n.stats.failed = AppendNodeIfMissing(n.stats.failed, response.sender)
					// delete response.sender from the routing table if already exists.
					n.rtMutex.Lock()
					//n.RoutingTable.Delete(response.sender.Key())
					n.RoutingTable.Del(response.sender)
					n.rtMutex.Unlock()
					continue L
				}
				if response.level == 0 {
					results = AppendNodesIfMissing(results, response.closest)
					if ContainsKey(key, results) {
						return results // cancel other FindNode
					}
				}
				ayame.Log.Debugf("%d th/ %d FindNode finished", i+1, reqCount)
				queried = AppendNodeIfMissing(queried, response.sender)
				closers = AppendNodesIfMissing(closers, response.closest)
			}
		}
	}
	return results
}

func (n *BSNode) SetMessageReceiver(receiver func(*BSNode, *BSUnicastEvent)) {
	n.messageReceiver = receiver
}

func (n *BSNode) SetUnicastHandler(handler func(*BSNode, *BSUnicastEvent, bool, bool)) {
	n.unicastHandler = handler
}

// unicast a message with payload to a node with key
func (n *BSNode) Unicast(ctx context.Context, key ayame.Key, payload []byte) {
	unicastCtx, cancel := context.WithTimeout(ctx, time.Duration(UNICAST_SEND_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()
	closers, level := n.GetClosestNodes(key)
	ayame.Log.Debugf("from=%s,to=%s,first level=%d %s\n", n.Key(), key, level, ayame.SliceString(closers))
	ch := make(chan struct{}, 1)
	mid := n.String() + "." + NextId()
	for _, c := range closers {
		localc := c
		ev := NewBSUnicastEvent(n, nil, nil, mid, level, key, payload)
		n.sendUnicastEvent(ctx, ch, localc, ev)
	}
L:
	for i := 0; i < len(closers); i++ {
		select {
		case <-unicastCtx.Done(): // after UNICAST_SEND_TIMEOUT ;; perhaps during corrupted connection attempt
			ayame.Log.Debugf("A unicast ended with %s\n", unicastCtx.Err())
			break L
		case <-ch:
			ayame.Log.Debugf("A unicast sent %d/%d\n", i, len(closers))
		}
	}
}

func (n *BSNode) sendDelNodeEvent(ctx context.Context, ch chan struct{}, node *BSNode, ev *BSDelNodeEvent) {
	sendCtx, cancel := context.WithTimeout(ctx, time.Duration(DELNODE_SEND_TIMEOUT)*time.Millisecond)
	defer cancel()
	sch := make(chan struct{}, 1)
	go func() {
		ayame.Log.Debugf("sending delete %s => %s\n", ev.TargetKey, node.Key())
		n.SendEvent(ctx, node, ev, false) // ignore errors
		sch <- struct{}{}
	}()
	select {
	case <-sendCtx.Done():
		ayame.Log.Debugf("%s: delnode ended with %s, continue anyway\n", n.Key(), sendCtx.Err())
	case <-sch:
	}
	ch <- struct{}{}
}

func (n *BSNode) sendUnicastEvent(ctx context.Context, ch chan struct{}, node *BSNode, ev *BSUnicastEvent) {
	go func() {
		if node.Equals(n) {
			n.handleUnicast(ctx, ev, true)
		} else {
			err := n.SendEvent(ctx, node, ev, true)
			if err != nil {
				n.stats.failed = append(n.stats.failed, node)
			}
		}
		ch <- struct{}{}
	}()
}

func (n *BSNode) SendEvent(ctx context.Context, receiver ayame.Node, ev ayame.SchedEvent, sign bool) error {
	ev.SetSender(n)
	ev.SetReceiver(receiver)
	return n.Send(ctx, ev, sign)
}

func (n *BSNode) findNodeWithKey(ctx context.Context, findCh chan *FindNodeResponse, node *BSNode, key ayame.Key, requestId string) chan *FindNodeResponse {
	ev := NewBSFindNodeReqEvent(n, requestId, key, nil)
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second)
	defer cancel()
	ch := make(chan *FindNodeResponse, 1)
	n.procsMutex.Lock()
	n.Procs[requestId] = &FindNodeProc{ev: ev, id: requestId, ch: ch}
	n.procsMutex.Unlock()
	n.SendEvent(ctx, node, ev, false) // ignore errors (detect by timeout)
	select {
	case <-findCtx.Done(): // after FIND_NODE_TIMEOUT
		ayame.Log.Debugf("%s: FindNode ended with %s", n, findCtx.Err())
		if findCtx.Err() == context.DeadlineExceeded {
			// set the node as failure
			findCh <- &FindNodeResponse{sender: node, isFailure: true}
		} else {
			findCh <- &FindNodeResponse{}
		}
	case response := <-ch: // FindNode finished
		ayame.Log.Debugf("%s: FindNode ended normally", n)
		findCh <- response
	}
	return ch
}

func (n *BSNode) FindNode(ctx context.Context, findCh chan *FindNodeResponse, node *BSNode, requestId string) {
	ev := NewBSFindNodeReqEvent(n, requestId, n.Key(), n.MV())
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second)
	defer cancel()
	ch := make(chan *FindNodeResponse, 1)
	n.procsMutex.Lock()
	//n.Procs[requestId] = &FindNodeProc{ev: ev, id: requestId, ch: ch}
	n.Procs[requestId] = &FindNodeProc{ev: ev, id: requestId, ch: ch}
	n.procsMutex.Unlock()
	//ayame.Log.Infof("%s: registered msgid=%s to %s\n", n, requestId, node)
	ayame.Log.Debugf("%s: FindNode to %s started", n, node)
	n.SendEvent(ctx, node, ev, false) // ignore errors (detect by timeout)
	// wait for the next event happens
	select {
	case <-findCtx.Done(): // after FIND_NODE_TIMEOUT
		ayame.Log.Debugf("%s: FindNode ended with %s", n, findCtx.Err())
		if findCtx.Err() == context.DeadlineExceeded {
			// set the node as failure
			findCh <- &FindNodeResponse{sender: node, isFailure: true}
		} else {
			findCh <- &FindNodeResponse{}
		}
	case response := <-ch: // FindNode finished
		ayame.Log.Debugf("%s: FindNode ended normally", n)
		findCh <- response
	}
}

var USE_TABLE_INDEX = true

func (n *BSNode) handleFindNode(ctx context.Context, ev ayame.SchedEvent) {
	if ev.IsResponse() {
		n.handleFindNodeResponse(ctx, ev)
	}
	//else {
	//	n.SendEvent(ctx, ev.Sender(), n.handleFindNodeRequest(ctx, ev), false)
	//}
}

func (n *BSNode) handleFindNodeResponse(ctx context.Context, ev ayame.SchedEvent) {
	ue := ev.(*BSFindNodeEvent)
	ayame.Log.Debugf("find node response from=%v\n", ue.Sender().(*BSNode))
	//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
	n.procsMutex.RLock()
	proc, exists := n.Procs[ue.MessageId]
	n.procsMutex.RUnlock()
	if exists {
		if proc.ch != nil { // sync
			ayame.Log.Debugf("find node finished from=%v\n", ue.Sender().(*BSNode))
			proc.ch <- &FindNodeResponse{id: ue.MessageId, sender: ue.Sender().(*BSNode), closest: ue.closers, candidates: ue.candidates, level: ue.level, isFailure: false}
		} else { // async
			n.processResponse(&FindNodeResponse{id: ue.MessageId, sender: ue.Sender().(*BSNode), closest: ue.closers, candidates: ue.candidates, level: ue.level, isFailure: false})
			ResponseCount += (len(ue.closers) + len(ue.candidates))
			ayame.Log.Debugf("%d", ResponseCount)

			if !n.allQueried() {
				ayame.Log.Debugf("%s: not finished\n%s", n.Key(), n.RoutingTable)
				n.sendNextParalellRequest(ctx, 1)
			} else {
				ayame.Log.Debugf("%s: finish\n%s", n.Key(), n.RoutingTable)
			}
		}
	} else {
		ayame.Log.Errorf("%s: unregistered response msgid=%s received from %s\n", n, ue.MessageId, ue.Sender().(*BSNode))
		panic("unregisterd response") // it's implementation error
	}

}

var EXCLUDE_CLOSEST_IN_NEIGHBORS bool = false

func (n *BSNode) handleFindNodeRequest(ctx context.Context, ev ayame.SchedEvent) ayame.SchedEvent {
	ue := ev.(*BSFindNodeEvent)
	//kmv := ue.Sender().(*BSNode)
	var closest, candidates []*BSNode
	var level int
	if ue.req.MV == nil {
		n.rtMutex.RLock()
		closest, level = n.GetClosestNodes(ue.req.Key)
		n.rtMutex.RUnlock()
	} else {
		n.rtMutex.RLock()
		if USE_TABLE_INDEX {
			candidates = ksToNs(n.RoutingTable.Neighbors(ue.req))
			//closers, level = n.GetClosestNodes(ue.req.Key)
			clst, lvl := n.RoutingTable.KClosest(ue.req) // GetClosestNodes(ue.req.Key)
			closest = ksToNs(clst)
			level = lvl
			if EXCLUDE_CLOSEST_IN_NEIGHBORS {
				candidates = UnincludedNodes(candidates, closest)
			}
		} else {
			closest, level, candidates = n.GetNeighborsAndCandidates(ue.req.Key, ue.req.MV)
		}
		n.rtMutex.RUnlock()
	}
	ayame.Log.Debugf("returns closest=%s, level=%d, candidates=%s\n", ayame.SliceString(closest), level, ayame.SliceString(candidates))
	n.rtMutex.Lock()
	n.RoutingTable.Add(ue.Sender().(*BSNode))
	n.rtMutex.Unlock()
	res := NewBSFindNodeResEvent(n, ue, closest, level, candidates)
	res.SetSender(n)
	return res
}

func (n *BSNode) handleUnicast(ctx context.Context, sev ayame.SchedEvent, sendToSelf bool) error {
	msg := sev.(*BSUnicastEvent)
	ayame.Log.Debugf("handling msg to %d on %s level %d\n", msg.TargetKey, n, msg.level)
	if ayame.SecureKeyMV {
		if !sendToSelf && !msg.IsVerified() {
			return fmt.Errorf("%s: failed to verify message %s, throw it all away", n, msg.MessageId)
		}
	}

	if !sendToSelf && msg.CheckAlreadySeen(n) {
		if n.unicastHandler != nil {
			n.unicastHandler(n, msg, true, false)
		}
		ayame.Log.Debugf("called already seen handler\n")
		return nil
	}
	if msg.level == 0 { // level 0 means destination
		msg.SetAlreadySeen(n)
		//ayame.Log.Debugf("node: %d, m: %s\n", n, msg)
		if n.unicastHandler != nil {
			n.unicastHandler(n, msg, false, false)
		}
		if n.messageReceiver != nil {
			n.messageReceiver(n, msg)
		}
	} else {
		nextMsgs := msg.findNextHops(n)
		//ayame.Log.Debugf("next msgs: %v\n", nextMsgs)
		for _, next := range nextMsgs {
			node := next.Receiver().(*BSNode)
			ayame.Log.Debugf("node: %d, m: %d\n", node.key, n.key)
			// already via the path.
			if node.Equals(n) {
				// myself
				ayame.Log.Debugf("I, %d, am one of the dest: %d, pass to myself\n", n.key, node.key)
				n.handleUnicast(ctx, next, true)
			} else {
				if Contains(node, funk.Map(msg.Path, func(pe PathEntry) *BSNode { return pe.Node.(*BSNode) }).([]*BSNode)) {
					if n.unicastHandler != nil {
						n.unicastHandler(n, msg, false, true)
					}
				} else {
					msg.SetAlreadySeen(n)
					ayame.Log.Debugf("I, %d@%d, am not one of the dest: %d, forward\n", n.key, msg.level, node.key)
					//ev := msg.createSubMessage(n)
					err := n.SendEvent(ctx, node, next, true)
					if err != nil {
						n.stats.failed = append(n.stats.failed, node)
					}
				}
			}

		}
	}
	return nil
}

func (n *BSNode) handleDelNode(sev ayame.SchedEvent) error {
	ev := sev.(*BSDelNodeEvent)
	if ev.TargetKey.Equals(sev.Sender().Key()) { // only if the sender has the targetKey
		n.rtMutex.Lock()
		//n.RoutingTable.Delete(ev.TargetKey)
		n.RoutingTable.Del(ev.Sender().(*BSNode))
		n.rtMutex.Unlock()

		// need to re-construct the routing table. crawl again?
	}
	return nil
}
