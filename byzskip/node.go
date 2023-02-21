// Package byzskip is an implementation of ByzSkip on go-libp2p
//
// You can create a Node, Join to a network, Lookup a key, and Unicast a message.
package byzskip

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	pb "github.com/piax/go-byzskip/ayame/p2p/pb"
	"github.com/thoas/go-funk"
)

const (
	JOIN_TIMEOUT          = 30
	LOOKUP_TIMEOUT        = 30
	DELNODE_TIMEOUT       = 30
	REQUEST_TIMEOUT       = 10
	FIND_NODE_TIMEOUT     = 10
	FIND_NODE_PARALLELISM = 5
	FIND_NODE_INTERVAL    = 1
	UNICAST_SEND_TIMEOUT  = 1
	DELNODE_SEND_TIMEOUT  = 300 // millisecond
	DURATION_BEFORE_CLOSE = 100 // millisecond

	// Flags for implementation choices.

	// If true, modify the routing table without checking.
	// Setting this to true is not recommended.
	MODIFY_ROUTING_TABLE_BY_RESPONSE = false

	// If false, redundancy occurs in neighbors and closest.
	// Setting this to false is not recommended.
	EXCLUDE_CLOSEST_IN_NEIGHBORS = true

	// If false, table index is not used.
	// Setting this to false is not recommended.
	USE_TABLE_INDEX = true

	// If false, PRUNE_OPT2 forwards messages in the higher level even if the message is already seen.
	// Setting this to false is not recommended.
	IGNORE_LEVEL_IN_OPT = true
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

type FindNodeMVResponse struct {
	id        string    // the request id
	sender    *BSNode   // the responder
	neighbors []*BSNode // the current neighbor nodes
	isTopmost bool      // the response is from topmost level.
	isFailure bool      // the responder is in failure
}

type FindRangeResponse struct {
	id        string    // the request id
	sender    *BSNode   // the responder
	targets   []*BSNode // the current neighbor nodes
	isFailure bool      // the responder is in failure
}

// -- find node
type RequestProcess struct {
	Id string
	Ev ayame.SchedEvent //*BSFindNodeEvent
	Ch chan interface{} //*FindNodeResponse or *FindNodeMVResponse
}

type BSNode struct {
	Parent ayame.Node
	key    ayame.Key
	mv     *ayame.MembershipVector
	//bs.IntKeyMV
	stats *JoinStats

	RoutingTable RoutingTable

	EventForwarder func(context.Context, *BSNode, *BSNode, ayame.SchedEvent, bool)
	IsFailure      bool
	QuerySeen      map[string]int

	Procs            map[string]*RequestProcess
	ProcsMutex       sync.RWMutex                               // for r/w procs status
	rtMutex          sync.RWMutex                               // for r/w routing table
	failureMutex     sync.RWMutex                               // for r/w failure status
	seenMutex        sync.RWMutex                               // for r/w QuerySeen
	unicastHandler   func(*BSNode, *BSUnicastEvent, bool, bool) // received event, already seen, next already on the path
	messageReceiver  func(*BSNode, *BSUnicastEvent)             // a function when received a message
	responseReceiver func(*BSNode, *BSUnicastResEvent)

	proc               goprocess.Process
	DisableFixLowPeers bool
	app                interface{}
	BootstrapAddrs     []peer.AddrInfo

	lastPing    time.Time
	lastRefresh time.Time
}

func (n *BSNode) Key() ayame.Key {
	return n.key
}

func (n *BSNode) SetKey(key ayame.Key) {
	// should check running status
	n.key = key
}

func (node *BSNode) Id() peer.ID { // Endpoint
	if node == nil {
		panic("node is nil")
	}
	if node.Parent == nil {
		panic("parent is nil")
	}
	return node.Parent.Id()
}

func (node *BSNode) Addrs() []ma.Multiaddr { // Endpoint
	return node.Parent.Addrs()
}

func (node *BSNode) Send(ctx context.Context, ev ayame.SchedEvent, sign bool) error {
	return node.Parent.Send(ctx, ev, sign)
}

func (n *BSNode) MV() *ayame.MembershipVector {
	return n.mv
}

func (n *BSNode) SetMV(mv *ayame.MembershipVector) {
	n.mv = mv
}

func (n *BSNode) Equals(m any) bool {
	//return m.Key().Equals(n.key)
	if mn, ok := m.(*BSNode); ok {
		if n.isIntroducer() {
			return mn.isIntroducer()
		}
		if p, ok := mn.Parent.(*p2p.RemoteNode); ok {
			return p.Id() == n.Id()
		}
		return mn.Key().Equals(n.key)
	}
	return false
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
	return n.Parent.Encode()
}

func (n *BSNode) Close() error {
	n.NotifyDeletion(context.Background())
	time.Sleep(time.Duration(DURATION_BEFORE_CLOSE) * time.Millisecond)
	n.Parent.Close()
	// process close
	if n.proc == nil {
		panic("implementation error?")
	}
	return n.proc.Close()
}

func (n *BSNode) SetApp(app interface{}) {
	n.app = app
}

func (n *BSNode) App() interface{} {
	return n.app
}

func (n *BSNode) MessageIdPrefix() string {
	return n.Parent.MessageIdPrefix()
}

/*type BSRoutingTable struct {
	//	nodes map[int]*BSNode // key=>node
	SkipRoutingTable
}

func NewBSRoutingTable(keyMV KeyMV) RoutingTable {
	t := NewSkipRoutingTable(keyMV)
	return &BSRoutingTable{SkipRoutingTable: *t} //, nodes: make(map[int]*BSNode)}
}*/

func ksToNs(lst []KeyMV) []*BSNode {
	ret := []*BSNode{}
	for _, ele := range lst {
		ret = append(ret, ele.(*BSNode))
	}
	return ret
}

/*func nsToKs(lst []*BSNode) []KeyMV {
	ret := []KeyMV{}
	for _, ele := range lst {
		ret = append(ret, ele)
	}
	return ret
}*/

// returns k-neighbors, the level found k-neighbors, neighbor candidates for s
func (node *BSNode) GetNeighborsAndCandidates(key ayame.Key, mv *ayame.MembershipVector) ([]*BSNode, int, []*BSNode) {
	ret, level := node.RoutingTable.KClosestWithKey(key)
	//can := node.routingTable.GetAll()
	can := node.RoutingTable.GetCommonNeighbors(mv)
	return ksToNs(ret), level, ksToNs(can)
}

func New(h host.Host, options ...Option) (*BSNode, error) {
	return NewWithoutDefaults(h, append(options, FallbackDefaults)...)
}

func NewWithoutDefaults(h host.Host, options ...Option) (*BSNode, error) {
	var cfg Config

	if err := cfg.Apply(options...); err != nil {
		return nil, err
	}

	return cfg.NewNode(h)
}

func NewWithParent(parent ayame.Node, rtMaker func(KeyMV) RoutingTable,
	eventForwarder func(context.Context, *BSNode, *BSNode, ayame.SchedEvent, bool),
	isFailure bool) *BSNode {
	ret := &BSNode{key: parent.Key(), mv: parent.MV(),
		//LocalNode: ayame.GetLocalNode(strconv.Itoa(key)),
		Parent: parent,
		// stats is not generated at this time
		QuerySeen:          make(map[string]int),
		Procs:              make(map[string]*RequestProcess),
		IsFailure:          isFailure,
		DisableFixLowPeers: false,
	}
	ret.RoutingTable = rtMaker(ret)
	if eventForwarder == nil {
		ret.EventForwarder = NodeEventForwarder
	} else {
		ret.EventForwarder = eventForwarder
	}

	return ret
}

func (node *BSNode) GetClosestNodes(key ayame.Key) ([]*BSNode, int) {
	nb, lv := node.RoutingTable.KClosestWithKey(key)
	return ksToNs(nb), lv
}

func (node *BSNode) GetClosestNodesMV(mv *ayame.MembershipVector, src ayame.Key) ([]*BSNode, bool) {
	nb, fin := node.RoutingTable.KClosestWithMV(mv, src)
	return ksToNs(nb), fin
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

// Deprecated: Use Exclude instead
/*func UnincludedNodes2(nodes []*BSNode, queried []*BSNode) []*BSNode {
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
}*/

func AllContained2(curKNodes []*BSNode, queried []*BSNode) bool {
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

/*
func Contains(node *BSNode, nodes []*BSNode) bool {
	for _, n := range nodes {
		if n.Equals(node) {
			return true
		}
	}
	return false
}*/

func ContainsKey(key ayame.Key, nodes []*BSNode) bool {
	for _, n := range nodes {
		if n.Key().Equals(key) {
			return true
		}
	}
	return false
}

func ContainsMV(mv *ayame.MembershipVector, nodes []*BSNode) bool {
	for _, n := range nodes {
		if n.MV().Equals(mv) {
			return true
		}
	}
	return false
}

//func AppendIfAbsent(lst []*BSNode, nodes ...*BSNode) []*BSNode {
//	for _, ele := range nodes {
//	lst = ayame.AppendIfAbsent(lst, nodes...)
//	}
//	return lst
//}

//
//func AppendNodeIfMissing(lst []*BSNode, node *BSNode) []*BSNode {
//	for _, ele := range lst {
//		if ele.Equals(node) {
//			return lst
//		}
//	}
//	return append(lst, node)
//}

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

var seqNo int = 0

func NextId() string {
	seqNo++
	return strconv.Itoa(seqNo)
}

func (n *BSNode) pickCandidates(stat *JoinStats, count int) []*BSNode {
	ret := []*BSNode{}
	///if !stat.reached {
	for _, c := range stat.closest {
		if len(ret) == count {
			return ret
		}
		if MODIFY_ROUTING_TABLE_BY_RESPONSE || c.isIntroducer() || n.RoutingTable.PossiblyBeAdded(c) {
			ret = ayame.AppendIfAbsent(ret, c)
			ret = ayame.Exclude(ret, stat.queried)
			ret = ayame.Exclude(ret, []*BSNode{n}) // remove self.
		}
	}
	///}
	for _, c := range stat.candidates {
		if len(ret) == count {
			return ret
		}
		if MODIFY_ROUTING_TABLE_BY_RESPONSE || c.isIntroducer() || n.RoutingTable.PossiblyBeAdded(c) {
			ret = ayame.AppendIfAbsent(ret, c)
			ret = ayame.Exclude(ret, stat.queried)
			ret = ayame.Exclude(ret, []*BSNode{n}) // remove self.
		}
	}
	// not enough number
	return ret
}

func (n *BSNode) isIntroducer() bool {
	// introducer remote node initially don't have key information
	return n.Key() == nil
}

// New simulation node
func NewSimNode(key ayame.Key, mv *ayame.MembershipVector) *BSNode {
	return NewWithParent(ayame.NewLocalNode(key, mv), NewSkipRoutingTable, nil, false)
}

// Join a node join to the network.
// introducer's multiaddr candidates are specified as addrs argument
func (n *BSNode) Join(ctx context.Context) error {
	if len(n.BootstrapAddrs) == 0 {
		return errors.New("no bootstrap address specified")
	}
	introducer, err := n.IntroducerNode(ayame.PickRandomly(n.BootstrapAddrs))
	if err != nil {
		return err
	}
	if introducer.Id() == n.Id() {
		return n.RunBootstrap(ctx)
	}

	return n.JoinWithNode(ctx, introducer)
}

var ResponseCount int // for sim

type RequestEvent interface {
	MessageId() string
	ayame.SchedEvent
}

type FailureResponse struct {
	Err error
}

// SendRequest sends request event and wait for response.
// The type of response depends on the request.
func (n *BSNode) SendRequest(ctx context.Context, node ayame.Node, req RequestEvent) interface{} {
	reqCtx, cancel := context.WithTimeout(ctx, time.Duration(REQUEST_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()
	reqCh := make(chan interface{}, 1)
	n.ProcsMutex.Lock()
	mid := req.MessageId()
	n.Procs[mid] = &RequestProcess{Ev: req, Id: mid, Ch: reqCh}
	n.ProcsMutex.Unlock()
	start := time.Now()
	n.SendEventAsync(ctx, node, req, false)
	select {
	case <-reqCtx.Done(): // after REQUEST_TIMEOUT
		ayame.Log.Debugf("%s: request ended with %s", n, reqCtx.Err())
	case res := <-reqCh: // request finished
		n.Parent.(*p2p.P2PNode).Host.Peerstore().RecordLatency(node.Id(), time.Since(start))
		ayame.Log.Debugf("%s: request ended normally", n)
		return res
	}
	return &FailureResponse{Err: reqCtx.Err()}
}

func (n *BSNode) RefreshRTWait(ctx context.Context) error {
	joinCtx, cancel := context.WithTimeout(ctx, time.Duration(JOIN_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()

	findCh := make(chan *FindNodeResponse, 1)
	for !n.allQueried() {
		reqCount := 0
		cs := n.pickCandidates(n.stats, FIND_NODE_PARALLELISM)
		if len(cs) == 1 && cs[0].isIntroducer() { // remove the special node
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
		n.lastRefresh = time.Now()
		n.lastPing = time.Now()
	}
	return nil
}

func (n *BSNode) JoinWithNode(ctx context.Context, introducer *BSNode) error {

	n.stats = &JoinStats{
		runningQueries: 0,
		closest:        []*BSNode{introducer},
		candidates:     []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}
	err := n.RefreshRTWait(ctx)
	n.lastPing = time.Now()
	n.lastRefresh = time.Now()

	if err != nil {
		return err
	}

	n.proc = goprocessctx.WithContext(ctx)
	if !n.DisableFixLowPeers {
		n.proc.Go(n.fixLowPeersRoutine)
	}
	return nil
}

func (n *BSNode) RunBootstrap(ctx context.Context) error {

	n.stats = &JoinStats{
		runningQueries: 0,
		closest:        []*BSNode{n},
		candidates:     []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}
	n.RefreshRTWait(ctx)

	n.proc = goprocessctx.WithContext(ctx)
	if !n.DisableFixLowPeers {
		n.proc.Go(n.fixLowPeersRoutine)
	}
	return nil
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
	if len(cs) == 1 && cs[0].isIntroducer() { // remove the special node
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
		n.ProcsMutex.Lock()
		n.Procs[requestId] = &RequestProcess{Ev: ev, Id: requestId, Ch: nil}
		n.ProcsMutex.Unlock()
		n.SendEventAsync(ctx, node, ev, false)
	}
	return reqCount
}

/*
func (n *BSNode) sendNextParalellMVRequest(ctx context.Context, parallel int) int {
	reqCount := 0
	cs := n.pickCandidates(n.stats, parallel)
	if len(cs) == 1 && cs[0].isIntroducer() { // remove the special node
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
		n.ProcsMutex.Lock()
		n.Procs[requestId] = &RequestProcess{Ev: ev, Id: requestId, Ch: nil}
		n.ProcsMutex.Unlock()
		n.SendEventAsync(ctx, node, ev, false)
	}
	return reqCount
}*/

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

func sortByCloseness(key ayame.Key, nodes []*BSNode) []*BSNode {
	if len(nodes) == 0 {
		return nodes
	}
	ks := nodes
	SortC(key, ks)
	ret := []*BSNode{}

	for i, j := 0, len(ks)-1; i < j; i, j = i+1, j-1 {
		ret = append(ret, ks[i])
		ret = append(ret, ks[j])
	}
	return ret
}

func (n *BSNode) GetNeighborRequest() *NeighborRequest {
	return &NeighborRequest{
		Key:               n.Key(),
		MV:                n.MV(),
		ClosestIndex:      n.RoutingTable.GetClosestIndex(),
		NeighborListIndex: n.RoutingTable.GetTableIndex(),
	}
}

func (n *BSNode) processResponseIndirect(response *FindNodeResponse) error {
	if response.isFailure { // timed out
		n.stats.queried = ayame.AppendIfAbsent(n.stats.queried, response.sender)
		n.failureMutex.Lock()
		n.stats.failed = ayame.AppendIfAbsent(n.stats.failed, response.sender)
		n.failureMutex.Unlock()
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
		//ayame.Log.Infof("initial nodes size: %d\n", len(initialNodes))
		if !n.IsFailure && isFaultySet(initialNodes) {
			ayame.Log.Infof("initial nodes hijacked: %s\n", initialNodes)
		}
		for _, m := range response.closest {
			if m.Key().Equals(n.Key()) {
				return fmt.Errorf("%s: Key already in use", n.Key())
			}
		}
	}
	n.rtMutex.Lock()
	n.RoutingTable.Add(response.sender, true) // XXX lock
	ayame.Log.Debugf("%s: added response sender %s", n, response.sender)
	n.rtMutex.Unlock()
	/*
		for _, c := range response.candidates {
			n.rtMutex.Lock()
			n.stats.candidateTable.Add(c, true)
			n.rtMutex.Unlock()
		}
		if EXCLUDE_CLOSEST_IN_NEIGHBORS {
			for _, c := range response.closest {
				n.rtMutex.Lock()
				n.stats.candidateTable.Add(c, true)
				n.rtMutex.Unlock()
			}
		}
	*/
	n.stats.closest = ayame.AppendIfAbsent(n.stats.closest, response.closest...)

	n.stats.candidates = ayame.AppendIfAbsent(n.stats.candidates, response.candidates...)
	if EXCLUDE_CLOSEST_IN_NEIGHBORS {
		n.stats.candidates = ayame.AppendIfAbsent(n.stats.candidates, response.closest...)
	}
	/*
		for _, c := range response.candidates {
			if n.RoutingTable.PossiblyBeAdded(c) {
				ayame.Log.Debugf("%s possibly be added to {%s}%s\n", c.Key(), n.Key(), n.RoutingTable)
				n.stats.candidates = AppendNodeIfMissing(n.stats.candidates, c)
			} else {
				ayame.Log.Debugf("%s NOT possibly be added to {%s}%s\n", c.Key(), n.Key(), n.RoutingTable)
			}
		}
		if EXCLUDE_CLOSEST_IN_NEIGHBORS {
			for _, c := range response.closest {
				if n.RoutingTable.PossiblyBeAdded(c) {
					ayame.Log.Debugf("%s possibly be added to {%s}%s\n", c.Key(), n.Key(), n.RoutingTable)
					n.stats.candidates = AppendNodeIfMissing(n.stats.candidates, c)
				} else {
					ayame.Log.Debugf("%s NOT possibly be added to {%s}%s\n", c.Key(), n.Key(), n.RoutingTable)
				}
			}
		}*/
	/*	n.rtMutex.RLock()
		req := n.GetNeighborRequest()
		n.rtMutex.RUnlock()
		n.stats.candidates = ksToNs(excludeNeighborsInRequest(n, n.stats.candidateTable.AllNeighbors(false, true), req))*/
	//n.stats.candidates = ksToNs(n.stats.candidateTable.AllNeighbors(false, true)) //AppendNodesIfMissing(n.stats.candidates, ksToNs(n.stats.candidateTable.AllNeighbors(false, true)))
	//ksToNs(n.stats.candidateTable.AllNeighbors(false, true))
	//n.stats.candidates = AppendNodesIfMissing(n.stats.candidates, ksToNs(n.stats.candidateTable.AllNeighbors(false, true)))

	n.stats.queried = ayame.AppendIfAbsent(n.stats.queried, response.sender)

	ayame.Log.Debugf("%s: received closest=%s, level=%d, candidates=%s,  candidates=%s\n", n.Key(), ayame.SliceString(response.closest), response.level,
		ayame.SliceString(response.candidates),
		ayame.SliceString(n.stats.candidates))
	n.ProcsMutex.Lock()
	delete(n.Procs, response.id)
	n.ProcsMutex.Unlock()

	if response.level == 0 { // just for debug
		ayame.Log.Debugf("%s: start neighbor collection from %s, queried=%s\n", n.Key(),
			ayame.SliceString(ayame.Exclude(n.stats.candidates, n.stats.queried)),
			ayame.SliceString(n.stats.queried))
	}

	return nil
}

func (n *BSNode) processResponseRange(response *FindRangeResponse) error {
	if response.isFailure { // timed out
		n.stats.queried = ayame.AppendIfAbsent(n.stats.queried, response.sender)
		n.failureMutex.Lock()
		n.stats.failed = ayame.AppendIfAbsent(n.stats.failed, response.sender)
		n.failureMutex.Unlock()
		// delete response.sender from the routing table if already exists.
		n.rtMutex.Lock()
		//n.RoutingTable.Delete(response.sender.Key())
		n.RoutingTable.Del(response.sender)
		n.rtMutex.Unlock()
		return nil
	}

	//	n.rtMutex.RLock()
	n.stats.closest = ayame.AppendIfAbsent(n.stats.closest, response.targets...)
	//	n.rtMutex.RUnlock()
	n.stats.queried = ayame.AppendIfAbsent(n.stats.queried, response.sender)

	n.ProcsMutex.Lock()
	delete(n.Procs, response.id)
	n.ProcsMutex.Unlock()

	return nil
}

func (n *BSNode) processResponseMV(response *FindNodeMVResponse) error {
	if response.isFailure { // timed out
		n.stats.queried = ayame.AppendIfAbsent(n.stats.queried, response.sender)
		n.failureMutex.Lock()
		n.stats.failed = ayame.AppendIfAbsent(n.stats.failed, response.sender)
		n.failureMutex.Unlock()
		// delete response.sender from the routing table if already exists.
		n.rtMutex.Lock()
		//n.RoutingTable.Delete(response.sender.Key())
		n.RoutingTable.Del(response.sender)
		n.rtMutex.Unlock()
		return nil
	}

	if ADD_NODES_IN_LOOKUP {
		n.rtMutex.Lock()
		n.RoutingTable.Add(response.sender, true) // XXX lock
		ayame.Log.Debugf("%s: added response sender %s", n, response.sender)
		n.rtMutex.Unlock()
	}
	//	n.rtMutex.RLock()
	n.stats.closest = ayame.AppendIfAbsent(n.stats.closest, response.neighbors...)
	//	n.rtMutex.RUnlock()
	n.stats.queried = ayame.AppendIfAbsent(n.stats.queried, response.sender)

	n.ProcsMutex.Lock()
	delete(n.Procs, response.id)
	n.ProcsMutex.Unlock()

	return nil
}

func (n *BSNode) processResponse(response *FindNodeResponse) error {
	if response.isFailure { // timed out
		n.stats.queried = ayame.AppendIfAbsent(n.stats.queried, response.sender)
		n.failureMutex.Lock()
		n.stats.failed = ayame.AppendIfAbsent(n.stats.failed, response.sender)
		n.failureMutex.Unlock()
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
		}
	}
	n.rtMutex.Lock()
	n.RoutingTable.Add(response.sender, true) // XXX lock
	ayame.Log.Debugf("%s: added response sender %s", n, response.sender)
	n.rtMutex.Unlock()

	for _, c := range response.candidates {
		n.rtMutex.Lock()
		n.RoutingTable.Add(c, true)
		n.rtMutex.Unlock()
	}
	if EXCLUDE_CLOSEST_IN_NEIGHBORS {
		for _, c := range response.closest {
			n.rtMutex.Lock()
			n.RoutingTable.Add(c, true)
			n.rtMutex.Unlock()
		}
	}

	n.stats.closest = ayame.AppendIfAbsent(n.stats.closest, response.closest...)
	// read from routing table
	n.rtMutex.RLock()
	n.stats.candidates = n.GetList(false, true)
	n.rtMutex.RUnlock()
	//n.stats.candidates = append(n.stats.candidates, response.candidates...)

	n.stats.queried = ayame.AppendIfAbsent(n.stats.queried, response.sender)

	ayame.Log.Debugf("%s: received closest=%s, level=%d, candidates=%s, next candidates=%s\n", n.Key(), ayame.SliceString(response.closest), response.level,
		ayame.SliceString(response.candidates),
		ayame.SliceString(n.stats.candidates))
	n.ProcsMutex.Lock()
	delete(n.Procs, response.id)
	n.ProcsMutex.Unlock()

	if response.level == 0 { // just for debug
		ayame.Log.Debugf("%s: start neighbor collection from %s, queried=%s\n", n.Key(),
			ayame.SliceString(ayame.Exclude(n.stats.candidates, n.stats.queried)),
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

func (n *BSNode) LevelZeroNodes() []*BSNode {
	lists := n.RoutingTable.GetNeighborLists()[0]
	ret := lists.Neighbors[LEFT]
	for _, n := range lists.Neighbors[RIGHT] {
		ret = ayame.AppendIfAbsent(ret, n)
	}
	return ksToNs(ret)
}

func (n *BSNode) TopmostLevel() int {
	for i, lst := range n.RoutingTable.GetNeighborLists() {
		if lst.hasDuplicatesInLeftsAndRights() {
			return i
		}
	}
	return -1
}

func (n *BSNode) FilledTopmostLevel() bool {
	for _, lst := range n.RoutingTable.GetNeighborLists() {
		if lst.hasSufficientNodes(LEFT) && lst.hasSufficientNodes(RIGHT) && lst.hasDuplicatesInMyRing() {
			//fmt.Printf("%d: topmostevel is %d\n", n.Key(), lst.level)
			return true
		}
	}
	return false
}

func (n *BSNode) allQueried() bool {
	ret := false
	//n.statsMutex.RLock()
	closestExceptMe := ayame.Exclude(n.stats.closest, []*BSNode{n})
	candidatesExceptMe := ayame.Exclude(n.stats.candidates, []*BSNode{n})
	ret = ayame.IsSubsetOf(closestExceptMe, n.stats.queried) &&
		ayame.IsSubsetOf(candidatesExceptMe, n.stats.queried)
	//n.statsMutex.RUnlock()
	return ret
}

func (n *BSNode) Lookup(ctx context.Context, key ayame.Key) ([]*BSNode, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, time.Duration(LOOKUP_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()

	closers, level := n.GetClosestNodes(key)
	if level == 0 {
		ayame.Log.Debugf("FindNode ended locally")
		return closers, nil
	}
	queried := []*BSNode{}
	results := []*BSNode{}
	lookupCh := make(chan interface{})

	for !ayame.IsSubsetOf(closers, queried) {
		reqCount := 0
		cs := ayame.Exclude(closers, queried)

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
			case res := <-lookupCh: // FindNode finished
				response := res.(*FindNodeResponse)
				if response.isFailure { // timed out
					queried = ayame.AppendIfAbsent(queried, response.sender)
					n.failureMutex.Lock()
					n.stats.failed = ayame.AppendIfAbsent(n.stats.failed, response.sender)
					n.failureMutex.Unlock()
					// delete response.sender from the routing table if already exists.
					n.rtMutex.Lock()
					//n.RoutingTable.Delete(response.sender.Key())
					n.RoutingTable.Del(response.sender)
					n.rtMutex.Unlock()
					continue L
				}
				if response.level == 0 {
					results = ayame.AppendIfAbsent(results, response.closest...)
					//if ContainsKey(key, results) {
					//	return results, nil // cancel other FindNode
					//}
				}
				ayame.Log.Debugf("%d th/ %d FindNode finished", i+1, reqCount)
				queried = ayame.AppendIfAbsent(queried, response.sender)
				closers = ayame.AppendIfAbsent(closers, response.closest...)
			}
		}
	}
	return results, nil
}

func (n *BSNode) LookupName(ctx context.Context, name string) ([]*BSNode, error) {
	start := ayame.NewUnifiedKeyFromString(name, ayame.ZeroID())
	end := ayame.NewUnifiedKeyFromString(name, ayame.MaxID())
	return n.LookupRange(ctx, ayame.NewRangeKey(start, false, end, false))
}

func (n *BSNode) LookupRange(ctx context.Context, rng *ayame.RangeKey) ([]*BSNode, error) {
	closests, err := n.Lookup(ctx, rng.Start())
	if err != nil {
		return nil, err
	}
	//cl := nsToKs(closests)
	//	SortC(start, cl)
	//	ayame.ReverseSlice(cl)
	ayame.Log.Debugf("scan starts from: %s to: %s", closests[0].Key(), rng.End())
	return n.Scan(ctx, closests, rng)
}

func (n *BSNode) Scan(ctx context.Context, start []*BSNode, rng *ayame.RangeKey) ([]*BSNode, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, time.Duration(LOOKUP_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()

	/*	targets, err := n.Lookup(lookupCtx, min)
		if err != nil {
			return nil, err
		} */

	curRange := ayame.NewRangeKey(start[0].Key(), true, rng.End(), rng.EndInclusive())
	targets := start
	results := make([]*BSNode, len(start))
	copy(results, start)
	queried := []*BSNode{}
	lookupCh := make(chan interface{})
	var endExtent ayame.Key

	for !ayame.IsSubsetOf(targets, queried) {
		reqCount := 0
		nexts := ayame.Exclude(targets, queried)
		curRange.SetEndExtent(endExtent)
		for _, c := range nexts {
			node := c
			reqCount++
			id := NextId()
			ayame.Log.Debugf("sending FindRange to %s, with range %s", node, curRange)
			go func() { n.findRange(lookupCtx, lookupCh, node, curRange, id) }()
		}

	L:
		for i := 0; i < reqCount; i++ {
			select {
			case <-lookupCtx.Done(): // after JOIN_TIMEOUT
				ayame.Log.Debugf("FindRange ended with %s", lookupCtx.Err())
				break L
			case res := <-lookupCh: // FindNode finished
				response := res.(*FindRangeResponse)
				if response.isFailure { // timed out
					queried = ayame.AppendIfAbsent(queried, response.sender)
					n.failureMutex.Lock()
					n.stats.failed = ayame.AppendIfAbsent(n.stats.failed, response.sender)
					n.failureMutex.Unlock()
					// delete response.sender from the routing table if already exists.
					n.rtMutex.Lock()
					//n.RoutingTable.Delete(response.sender.Key())
					n.RoutingTable.Del(response.sender)
					n.rtMutex.Unlock()
					continue L
				}
				ayame.Log.Debugf("%s: got response from %s: %s", n.Key(), response.sender.Key(), ayame.SliceString(response.targets))

				// XXX need a filter?
				filtered := response.targets

				//if rng.ContainsKey(response.sender.Key()) {
				results = ayame.AppendIfAbsent(results, response.sender)

				et := EndExtentIn(curRange, results)
				if et != nil {
					endExtent = et
					ayame.Log.Debugf("got end extent %s: %s", et, ayame.SliceString(results))
				}
				//}
				//if ContainsMV(mv, response.neighbors) {
				//	results = ayame.AppendIfAbsent(results, response.neighbors...)
				//}
				ayame.Log.Debugf("%d th/ %d FindNode finished", i+1, reqCount)
				queried = ayame.AppendIfAbsent(queried, response.sender)
				targets = ayame.AppendIfAbsent(targets, filtered...)
			}
		}
	}
	return results, nil
}

func (n *BSNode) LookupMV(ctx context.Context, mv *ayame.MembershipVector, src ayame.Key) ([]*BSNode, error) {
	lookupCtx, cancel := context.WithTimeout(ctx, time.Duration(LOOKUP_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()

	neighbors, fin := n.GetClosestNodesMV(mv, src)
	results := []*BSNode{}
	if fin {
		results = neighbors
		ayame.Log.Debugf("directly found on %s, results=%s\n", src, ayame.SliceString(results))
		return results, nil
	}

	queried := []*BSNode{}
	lookupCh := make(chan interface{})

	for !ayame.IsSubsetOf(neighbors, queried) {
		reqCount := 0
		nexts := ayame.Exclude(neighbors, queried)
		ayame.Log.Debugf("nexts = %s\n", ayame.SliceString(nexts))

		for _, c := range nexts {
			node := c // candidate
			reqCount++
			id := NextId()
			go func() { n.findNodeMV(lookupCtx, lookupCh, node, mv, src, id) }()
		}
	L:
		for i := 0; i < reqCount; i++ {
			select {
			case <-lookupCtx.Done(): // after JOIN_TIMEOUT
				ayame.Log.Debugf("FindNode ended with %s", lookupCtx.Err())
				break L
			case res := <-lookupCh: // FindNode finished
				response := res.(*FindNodeMVResponse)
				if response.isFailure { // timed out
					queried = ayame.AppendIfAbsent(queried, response.sender)
					n.failureMutex.Lock()
					n.stats.failed = ayame.AppendIfAbsent(n.stats.failed, response.sender)
					n.failureMutex.Unlock()
					// delete response.sender from the routing table if already exists.
					n.rtMutex.Lock()
					//n.RoutingTable.Delete(response.sender.Key())
					n.RoutingTable.Del(response.sender)
					n.rtMutex.Unlock()
					continue L
				}
				if response.isTopmost { // finished the search
					ayame.Log.Debugf("got topmost response: %s", ayame.SliceString(response.neighbors))
					results = ayame.AppendIfAbsent(results, response.neighbors...)
				}
				//if ContainsMV(mv, response.neighbors) {
				//	results = ayame.AppendIfAbsent(results, response.neighbors...)
				//}
				ayame.Log.Debugf("%d th/ %d FindNode finished", i+1, reqCount)
				queried = ayame.AppendIfAbsent(queried, response.sender)
				neighbors = ayame.AppendIfAbsent(neighbors, response.neighbors...)
			}
		}
	}
	return results, nil
}

func (n *BSNode) SetMessageReceiver(receiver func(*BSNode, *BSUnicastEvent)) {
	n.messageReceiver = receiver
}

func (n *BSNode) SetResponseReceiver(receiver func(*BSNode, *BSUnicastResEvent)) {
	n.responseReceiver = receiver
}

func (n *BSNode) SetUnicastHandler(handler func(*BSNode, *BSUnicastEvent, bool, bool)) {
	n.unicastHandler = handler
}

func (n *BSNode) NewMessageId() string {
	return n.MessageIdPrefix() + "." + NextId() //n.Id().String() + "." + NextId()
}

// unicast a message with payload to a node with key
// returns message ID
func (n *BSNode) Unicast(ctx context.Context, key ayame.Key, mid string, payload []byte) {
	//unicastCtx, cancel := context.WithTimeout(ctx, time.Duration(UNICAST_SEND_TIMEOUT)*time.Second) // all request should be ended within
	//defer cancel()
	closers, level := n.GetClosestNodes(key)
	ayame.Log.Debugf("from=%s,to=%s,first level=%d %s\n", n.Key(), key, level, ayame.SliceString(closers))
	//ch := make(chan struct{}, 1)

	for _, c := range closers {
		localc := c
		ev := NewBSUnicastEvent(n, nil, nil, mid, level, key, payload)
		//n.sendUnicastEvent(ctx, ch, localc, ev)
		n.sendUnicastEvent(ctx, nil, localc, ev)
	}
	/*L:
	for i := 0; i < len(closers); i++ {
		select {
		case <-unicastCtx.Done(): // after UNICAST_SEND_TIMEOUT ;; perhaps during corrupted connection attempt
			ayame.Log.Debugf("A unicast ended with %s\n", unicastCtx.Err())
			break L
		case <-ch:
			ayame.Log.Debugf("A unicast sent %d/%d at %s\n", i, len(closers), time.Now().String())
		}
	}
	*/
}

func (n *BSNode) sendDelNodeEvent(ctx context.Context, ch chan struct{}, node *BSNode, ev *BSDelNodeEvent) {
	sendCtx, cancel := context.WithTimeout(ctx, time.Duration(DELNODE_SEND_TIMEOUT)*time.Millisecond)
	defer cancel()
	sch := make(chan struct{}, 1)
	go func() {
		ayame.Log.Debugf("sending delete %s => %s\n", ev.TargetKey, node.Key())
		n.SendEventAsync(ctx, node, ev, false) // ignore errors
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
	//	go func() {
	if node.Equals(n) {
		n.handleUnicast(ctx, ev, true)
	} else {
		ayame.Log.Debugf("sent event %s from=%s,to=%s %s\n", ev.MessageId, n.Key(), node.Key(), time.Now().String())
		n.SendEventAsync(ctx, node, ev, true)
	}
	//if ch != nil {
	//ch <- struct{}{}
	//}
	//	}()
}

func (n *BSNode) SendEventAsyncSim(ctx context.Context, receiver ayame.Node, ev ayame.SchedEvent, sign bool) {
	//go func() {
	ev.SetSender(n)
	ev.SetReceiver(receiver)
	err := n.Send(ctx, ev, sign)
	if err != nil {
		n.stats.failed = append(n.stats.failed, receiver.(*BSNode))
	}
	//}()
}

// normal behavior
func NodeEventForwarder(ctx context.Context, sender *BSNode, receiver *BSNode, ev ayame.SchedEvent, sign bool) {
	sender.SendEventAsync(ctx, receiver, ev, sign)
}

func (n *BSNode) SendEventAsync(ctx context.Context, receiver ayame.Node, ev ayame.SchedEvent, sign bool) {
	// XXX run asynchronously? it should be ended in a short time.
	//go func() {
	ev.SetSender(n)
	ev.SetReceiver(receiver)
	err := n.Send(ctx, ev, sign)
	if err != nil {
		n.failureMutex.Lock()
		n.stats.failed = append(n.stats.failed, receiver.(*BSNode))
		n.failureMutex.Unlock()
	}
	//}()
}

func (n *BSNode) findRange(ctx context.Context, findCh chan interface{}, node *BSNode, rng *ayame.RangeKey, requestId string) {
	ev := NewBSFindRangeReqEvent(n, requestId, rng)
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second)
	defer cancel()
	ch := make(chan interface{}, 1)
	n.ProcsMutex.Lock()
	n.Procs[requestId] = &RequestProcess{Ev: ev, Id: requestId, Ch: ch}
	n.ProcsMutex.Unlock()
	if n.Id() == node.Id() { // query to self
		ev.SetSender(n)
		n.handleFindRangeResponse(ctx, n.handleFindRangeRequest(ctx, ev))
	} else {
		n.SendEventAsync(ctx, node, ev, false) // ignore errors (detect by timeout)
	}
	select {
	case <-findCtx.Done(): // after FIND_NODE_TIMEOUT
		ayame.Log.Debugf("%s: FindNode ended with %s", n, findCtx.Err())
		if findCtx.Err() == context.DeadlineExceeded {
			// set the node as failure
			findCh <- &FindRangeResponse{sender: node, isFailure: true}
		} else {
			findCh <- &FindRangeResponse{}
		}
	case response := <-ch: // FindNode finished
		ayame.Log.Debugf("%s: FindNode ended normally", n)
		findCh <- response
	}
}

func (n *BSNode) findNodeMV(ctx context.Context, findCh chan interface{}, node *BSNode, mv *ayame.MembershipVector, src ayame.Key, requestId string) {
	ev := NewBSFindNodeMVReqEvent(n, requestId, mv, src)
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second)
	defer cancel()
	ch := make(chan interface{}, 1)
	n.ProcsMutex.Lock()
	n.Procs[requestId] = &RequestProcess{Ev: ev, Id: requestId, Ch: ch}
	n.ProcsMutex.Unlock()
	if n.Id() == node.Id() { // query to self
		ev.SetSender(n)
		n.handleFindNodeMVResponse(ctx, n.handleFindNodeMVRequest(ctx, ev))
	} else {
		n.SendEventAsync(ctx, node, ev, false) // ignore errors (detect by timeout)
	}
	select {
	case <-findCtx.Done(): // after FIND_NODE_TIMEOUT
		ayame.Log.Debugf("%s: FindNode ended with %s", n, findCtx.Err())
		if findCtx.Err() == context.DeadlineExceeded {
			// set the node as failure
			findCh <- &FindNodeMVResponse{sender: node, isFailure: true}
		} else {
			findCh <- &FindNodeMVResponse{}
		}
	case response := <-ch: // FindNode finished
		ayame.Log.Debugf("%s: FindNode ended normally", n)
		findCh <- response
	}
}

func (n *BSNode) findNodeWithKey(ctx context.Context, findCh chan interface{}, node *BSNode, key ayame.Key, requestId string) {
	ev := NewBSFindNodeReqEvent(n, requestId, key, nil)
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second)
	defer cancel()
	ch := make(chan interface{}, 1)
	n.ProcsMutex.Lock()
	n.Procs[requestId] = &RequestProcess{Ev: ev, Id: requestId, Ch: ch}
	n.ProcsMutex.Unlock()

	if n.Id() == node.Id() { // query to self
		ev.SetSender(n)
		n.handleFindNodeResponse(ctx, n.handleFindNodeRequest(ctx, ev))
	} else {
		n.SendEventAsync(ctx, node, ev, false) // ignore errors (detect by timeout)
	}
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

func (n *BSNode) FindNode(ctx context.Context, findCh chan *FindNodeResponse, node *BSNode, requestId string) {
	ev := NewBSFindNodeReqEvent(n, requestId, n.Key(), n.MV())
	resp := n.SendRequest(ctx, node, ev)
	if fnr, ok := resp.(*FindNodeResponse); ok {
		findCh <- fnr
		return
	}
	if fr, ok := resp.(*FailureResponse); ok {
		ayame.Log.Debugf("%s: FindNode ended with %s", n, fr.Err)
		findCh <- &FindNodeResponse{sender: node, isFailure: true}
		return
	}
}

/* func (n *BSNode) findNodeOld(ctx context.Context, findCh chan *FindNodeResponse, node *BSNode, requestId string) {
	ev := NewBSFindNodeReqEvent(n, requestId, n.Key(), n.MV())
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second)
	defer cancel()
	ch := make(chan interface{}, 1)
	n.ProcsMutex.Lock()
	//n.Procs[requestId] = &FindNodeProc{ev: ev, id: requestId, ch: ch}
	n.Procs[requestId] = &RequestProcess{Ev: ev, Id: requestId, Ch: ch}
	n.ProcsMutex.Unlock()
	//ayame.Log.Infof("%s: registered msgid=%s to %s\n", n, requestId, node)
	ayame.Log.Debugf("%s: FindNode to %s started", n, node)
	start := time.Now()
	n.SendEventAsync(ctx, node, ev, false) // ignore errors (detect by timeout)
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
	case res := <-ch: // FindNode finished
		n.Parent.(*p2p.P2PNode).Host.Peerstore().RecordLatency(node.Id(), time.Since(start))
		response := res.(*FindNodeResponse)
		ayame.Log.Debugf("%s: FindNode ended normally", n)
		findCh <- response
	}
} */

func (n *BSNode) handleFindNode(ctx context.Context, ev ayame.SchedEvent) error {
	if ev.IsResponse() {
		return n.handleFindNodeResponse(ctx, ev)
	}
	return fmt.Errorf("protocol implmenentation error")
	//else {
	//	n.SendEvent(ctx, ev.Sender(), n.handleFindNodeRequest(ctx, ev), false)
	//}
}

func (n *BSNode) handleFindRange(ctx context.Context, ev ayame.SchedEvent) error {
	if ev.IsResponse() {
		return n.handleFindRangeResponse(ctx, ev)
	}
	return fmt.Errorf("protocol implmenentation error")
	//else {
	//	n.SendEvent(ctx, ev.Sender(), n.handleFindNodeRequest(ctx, ev), false)
	//}
}

func (n *BSNode) handleFindNodeMV(ctx context.Context, ev ayame.SchedEvent) error {
	if ev.IsResponse() {
		return n.handleFindNodeMVResponse(ctx, ev)
	}
	return fmt.Errorf("protocol implmenentation error")
}

func (n *BSNode) handleUnicastResponse(ctx context.Context, ev ayame.SchedEvent, sendToSelf bool) error {
	if !sendToSelf && !ev.IsVerified() {
		ayame.Log.Debugf("verify error. thrwon away")
		return fmt.Errorf("%s: failed to verify a message, throw it away", n)
	}
	ayame.Log.Debugf("verified")
	if ev.IsResponse() {
		n.responseReceiver(n, ev.(*BSUnicastResEvent))
	}
	//else {
	//	n.SendEvent(ctx, ev.Sender(), n.handleFindNodeRequest(ctx, ev), false)
	//}
	return nil
}

const ADD_NODES_IN_LOOKUP = false

func (n *BSNode) handleFindNodeMVRequest(ctx context.Context, ev ayame.SchedEvent) ayame.SchedEvent {
	ue := ev.(*BSFindNodeMVEvent)
	var ret []*BSNode
	n.rtMutex.RLock()
	neighbors, fin := n.RoutingTable.KClosestWithMV(ue.mv, ue.src)
	if fin {
		ayame.Log.Debugf("reached to topmost level")
	}
	ret = ksToNs(neighbors)
	n.rtMutex.RUnlock()
	ayame.Log.Debugf("%s returns neighbors=%s\n", n.key, ayame.SliceString(neighbors))
	if ADD_NODES_IN_LOOKUP {
		n.rtMutex.Lock()
		n.RoutingTable.Add(ue.Sender().(*BSNode), true)
		n.rtMutex.Unlock()
	}
	res := NewBSFindNodeMVResEvent(n, ue, ret, fin)
	res.SetSender(n)
	return res
}

func (n *BSNode) handleFindNodeMVResponse(ctx context.Context, ev ayame.SchedEvent) error {
	ue := ev.(*BSFindNodeMVEvent)
	ayame.Log.Debugf("find node response from=%v\n", ue.Sender().(*BSNode))
	//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
	n.ProcsMutex.RLock()
	proc, exists := n.Procs[ue.messageId]
	n.ProcsMutex.RUnlock()
	if n.Key().Equals(ev.Sender().Key()) && n.Id() != ev.Sender().Id() {
		fmt.Fprintf(os.Stderr, "Already joined key=%s exists. ID=%s", n.Key(), ev.Sender().Id().String())
		os.Exit(1) // rather than panic.
	}
	if exists {
		resp := &FindNodeMVResponse{id: ue.messageId, sender: ue.Sender().(*BSNode), neighbors: ue.neighbors, isFailure: false, isTopmost: ue.isTopmost}
		if proc.Ch != nil { // sync
			ayame.Log.Debugf("find node finished from=%v\n", ue.Sender().(*BSNode))
			proc.Ch <- resp
		} else { // async
			n.processResponseMV(resp)
			if !ayame.IsSubsetOf(n.stats.closest, n.stats.queried) {
				ayame.Log.Debugf("%s: not finished\n%s", n.Key(), n.RoutingTable)
				n.sendNextParalellRequest(ctx, 1)
			} else {
				ayame.Log.Debugf("%s: finish\n%s", n.Key(), n.RoutingTable)
			}
		}
	} else {
		return fmt.Errorf("%s: unregistered response msgid=%s received from %s", n, ue.messageId, ue.Sender().(*BSNode))
	}
	return nil
}

func (n *BSNode) handleFindRangeRequest(ctx context.Context, ev ayame.SchedEvent) ayame.SchedEvent {
	ue := ev.(*BSFindRangeEvent)
	n.rtMutex.RLock()
	targets := n.RoutingTable.GetNeighborLists()[0].Neighbors[0] // Right
	// CASE1: start, ... end, ... key
	// CASE2: end, ... key
	rng := ue.rng.(*ayame.RangeKey)
	//ayame.Log.Infof("%s: rng=%s right neighbors=%s\n", n.key, ue.rng, ayame.SliceString(targets))
	targets = pickRangeFrom(n.Key(), rng, targets)
	n.rtMutex.RUnlock()
	res := NewBSFindRangeResEvent(n, ue, ksToNs(targets))
	res.SetSender(n)
	return res
}

func (n *BSNode) handleFindRangeResponse(ctx context.Context, ev ayame.SchedEvent) error {
	ue := ev.(*BSFindRangeEvent)
	ayame.Log.Debugf("find node response from=%v\n", ue.Sender().(*BSNode))
	//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
	n.ProcsMutex.RLock()
	proc, exists := n.Procs[ue.messageId]
	n.ProcsMutex.RUnlock()
	if exists {
		resp := &FindRangeResponse{id: ue.messageId, sender: ue.Sender().(*BSNode), targets: ue.targets, isFailure: false}
		if proc.Ch != nil { // sync
			ayame.Log.Debugf("find node finished from=%v\n", ue.Sender().(*BSNode))
			proc.Ch <- resp
		} else { // async
			n.processResponseRange(resp)
			if !ayame.IsSubsetOf(n.stats.closest, n.stats.queried) {
				ayame.Log.Debugf("%s: not finished\n%s", n.Key(), n.RoutingTable)
				n.sendNextParalellRequest(ctx, 1)
			} else {
				ayame.Log.Debugf("%s: finish\n%s", n.Key(), n.RoutingTable)
			}
		}
	} else {
		return fmt.Errorf("%s: unregistered response msgid=%s received from %s", n, ue.messageId, ue.Sender().(*BSNode))
	}
	return nil
}

func (n *BSNode) handleFindNodeResponse(ctx context.Context, ev ayame.SchedEvent) error {
	ue := ev.(*BSFindNodeEvent)
	ayame.Log.Debugf("find node response from=%v\n", ue.Sender().(*BSNode))
	//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
	n.ProcsMutex.RLock()
	proc, exists := n.Procs[ue.messageId]
	n.ProcsMutex.RUnlock()
	if n.Key().Equals(ev.Sender().Key()) && n.Id() != ev.Sender().Id() {
		fmt.Fprintf(os.Stderr, "Already joined key=%s exists. ID=%s", n.Key(), ev.Sender().Id().String())
		os.Exit(1) // rather than panic.
	}
	if exists {
		if proc.Ch != nil { // sync
			ayame.Log.Debugf("find node finished from=%v\n", ue.Sender().(*BSNode))
			proc.Ch <- &FindNodeResponse{id: ue.messageId, sender: ue.Sender().(*BSNode), closest: ue.closers, candidates: ue.candidates, level: ue.level, isFailure: false}
		} else { // async
			if MODIFY_ROUTING_TABLE_BY_RESPONSE {
				n.processResponse(&FindNodeResponse{id: ue.messageId, sender: ue.Sender().(*BSNode), closest: ue.closers, candidates: ue.candidates, level: ue.level, isFailure: false})
			} else {
				n.processResponseIndirect(&FindNodeResponse{id: ue.messageId, sender: ue.Sender().(*BSNode), closest: ue.closers, candidates: ue.candidates, level: ue.level, isFailure: false})
			}
			ResponseCount += (len(ue.closers) + len(ue.candidates))

			if !n.allQueried() {
				//if !n.filledTopmostLevel() {
				ayame.Log.Debugf("%s: not finished\n%s", n.Key(), n.RoutingTable)
				n.sendNextParalellRequest(ctx, 1)
			} else {
				ayame.Log.Debugf("%s: finish\n%s", n.Key(), n.RoutingTable)
			}
		}
	} else {
		return fmt.Errorf("%s: unregistered response msgid=%s received from %s", n, ue.messageId, ue.Sender().(*BSNode))
	}
	return nil
}

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
				candidates = ayame.Exclude(candidates, closest)
			}
		} else {
			closest, level, candidates = n.GetNeighborsAndCandidates(ue.req.Key, ue.req.MV)
		}
		n.rtMutex.RUnlock()
	}
	ayame.Log.Debugf("returns closest=%s, level=%d, candidates=%s\n", ayame.SliceString(closest), level, ayame.SliceString(candidates))
	n.rtMutex.Lock()
	n.RoutingTable.Add(ue.Sender().(*BSNode), true)
	n.rtMutex.Unlock()
	res := NewBSFindNodeResEvent(n, ue, closest, level, candidates)
	res.SetSender(n)
	return res
}

func (n *BSNode) handleUnicast(ctx context.Context, sev ayame.SchedEvent, sendToSelf bool) error {
	msg := sev.(*BSUnicastEvent)
	ayame.Log.Debugf("handling msg to %d on %s level %d\n", msg.TargetKey, n, msg.level)

	if !sendToSelf && !msg.IsVerified() {
		return fmt.Errorf("%s: failed to verify message %s, throw it all away", n, msg.MessageId)
	}

	alreadySeen := msg.CheckAlreadySeen(n)
	if !sendToSelf && alreadySeen {
		if n.unicastHandler != nil {
			n.unicastHandler(n, msg, true, false)
		}
		ayame.Log.Debugf("called already seen handler on %s, seen=%s\n", n, msg.MessageId)
		return nil
	}
	if (RoutingType != SKIP_GRAPH && msg.level == 0) || (RoutingType == SKIP_GRAPH && msg.level < 0) { // level 0 means destination
		msg.SetAlreadySeen(n)
		//ayame.Log.Debugf("node: %d, m: %s\n", n, msg)
		if n.unicastHandler != nil {
			n.unicastHandler(n, msg, false, false)
		}
		if n.messageReceiver != nil { //&& !alreadySeen {
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
					//fmt.Printf("sent event %s from=%s,to=%s %s\n", next.MessageId, n.Key(), node.Key(), time.Now().String())
					//n.SendEventAsync(ctx, node, next, true)
					n.EventForwarder(ctx, n, node, next, true)
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
