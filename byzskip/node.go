package byzskip

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/piax/go-ayame/ayame"
	pb "github.com/piax/go-ayame/ayame/p2p/pb"
	"github.com/thoas/go-funk"
)

// UpdateNeighborsProc
// k-closests are updated
// 1. candidates are updated by k-closests
// after all candidates are queried,
// 2. candidates are updated by current routing table entries

// -- join
type JoinStats struct {
	closers    []*BSNode // the current closer nodes
	candidates []*BSNode // the candidate nodes
	queried    []*BSNode // store the queried remote nodes
	failed     []*BSNode // store the queried (but failed) remote nodes
}

func (s *JoinStats) String() string {
	return fmt.Sprintf("closers=%s, candidates=%s, queried=%s, failed=%s", ayame.SliceString(s.closers), ayame.SliceString(s.candidates), ayame.SliceString(s.queried), ayame.SliceString(s.failed))
}

type FindNodeResponse struct {
	id         string    // the request id
	sender     *BSNode   // the responder
	closers    []*BSNode // the current closer nodes
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
	seenMutex      sync.RWMutex                               // for r/w QuerySeen
	unicastHandler func(*BSNode, *BSUnicastEvent, bool, bool) // received event, already seen, next already on the path
}

func (n *BSNode) Key() ayame.Key {
	return n.key
}

func (node *BSNode) Id() peer.ID { // Endpoint
	return node.parent.Id()
}

func (node *BSNode) Send(ev ayame.SchedEvent) {
	node.parent.Send(ev)
}

func (n *BSNode) MV() *ayame.MembershipVector {
	return n.mv
}

func (n *BSNode) Equals(m KeyMV) bool {
	return m.Key().Equals(n.key)
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

func (n *BSNode) Close() {
	n.NotifyDeletion()
	n.parent.Close()
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

// returns k-neighbors, the level found k-neighbors, neighbor candidates for s
func (node *BSNode) GetNeighborsAndCandidates(key ayame.Key, mv *ayame.MembershipVector) ([]*BSNode, int, []*BSNode) {
	ret, level := node.RoutingTable.GetNeighbors(key)
	//can := node.routingTable.GetAll()
	can := node.RoutingTable.GetCommonNeighbors(mv)
	return ksToNs(ret), level, ksToNs(can)
}

func NewBSNode(parent ayame.Node, rtMaker func(KeyMV) RoutingTable, isFailure bool) *BSNode {
	ret := &BSNode{key: parent.Key(), mv: parent.MV(),
		//LocalNode: ayame.GetLocalNode(strconv.Itoa(key)),
		parent: parent,
		// stats is not generated at this time
		QuerySeen: make(map[string]int),
		Procs:     make(map[string]*FindNodeProc),
		IsFailure: isFailure}
	ret.RoutingTable = rtMaker(ret)
	return ret
}

// for local
func (node *BSNode) GetNeighbors(key ayame.Key) ([]*BSNode, int) {
	nb, lv := node.RoutingTable.GetNeighbors(key)
	return ksToNs(nb), lv
}

func (node *BSNode) GetCandidates() []*BSNode {
	return ksToNs(node.RoutingTable.GetAll())
}

func (node *BSNode) GetCloserCandidates() []*BSNode {
	return ksToNs(node.RoutingTable.GetCloserCandidates())
}

func (node *BSNode) RoutingTableString() string {
	return node.RoutingTable.String()
}

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
	FIND_NODE_TIMEOUT     = 10
	FIND_NODE_PARALLELISM = 4
	FIND_NODE_INTERVAL    = 1
	UNICAST_SEND_TIMEOUT  = 1
)

var SeqNo int = 0

func NextId() string {
	SeqNo++
	return strconv.Itoa(SeqNo)
}

func pickCandidates(stat *JoinStats, count int) []*BSNode {
	ret := []*BSNode{}
	for _, c := range stat.closers {
		if len(ret) == count {
			return ret
		}
		ret = AppendNodeIfMissing(ret, c)
		ret = UnincludedNodes(ret, stat.queried)
	}
	for _, c := range stat.candidates {
		if len(ret) == count {
			return ret
		}
		ret = AppendNodeIfMissing(ret, c)
		ret = UnincludedNodes(ret, stat.queried)
	}
	// not enough number
	return ret
}

func (n *BSNode) IsIntroducer() bool {
	return n.Key() == nil
}

//func (n *BSNode) Join(introducer *BSNode) {
func (n *BSNode) Join(ctx context.Context, addr string) {
	introducer, _ := n.IntroducerNode(addr)
	n.JoinWithNode(ctx, introducer)
}

func (n *BSNode) JoinWithNode(ctx context.Context, introducer *BSNode) {
	joinCtx, cancel := context.WithTimeout(ctx, time.Duration(JOIN_TIMEOUT)*time.Second) // all request should be ended within

	n.stats = &JoinStats{closers: []*BSNode{introducer},
		candidates: []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}

	defer cancel()

	findCh := make(chan *FindNodeResponse, 1)
	for !n.allQueried() {
		reqCount := 0
		cs := pickCandidates(n.stats, FIND_NODE_PARALLELISM)
		if len(cs) == 1 && cs[0].IsIntroducer() { // remove the special node
			//n.statsMutex.Lock()
			n.stats.closers = []*BSNode{}
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
		// wait for the process
	L:
		for i := 0; i < reqCount; i++ {
			select {
			case <-joinCtx.Done(): // after JOIN_TIMEOUT
				ayame.Log.Debugf("A FindNode ended with %s\n", joinCtx.Err())
				break L
			case response := <-findCh: // FindNode finished
				ayame.Log.Debugf("%d th/ %d FindNode for %s to %s finished", i+1, reqCount, response.id, response.sender)
				if response.isFailure { // timed out
					// XXX TODO: should we try several times?
					n.stats.queried = AppendNodeIfMissing(n.stats.queried, response.sender)
					n.stats.failed = AppendNodeIfMissing(n.stats.failed, response.sender)
					// XXX delete response.sender from the routing table if already exists.
					continue L
				}
				if response.level == 0 {
					ayame.Log.Debugf("reached matched nodes")
				}
				/*if response.sender == nil { // empty response; Find Node timed out
					ayame.Log.Debugf("A FindNode ended with timeout")
					break L
				}*/
				n.rtMutex.Lock()
				n.RoutingTable.Add(response.sender) // XXX lock
				n.rtMutex.Unlock()

				for _, c := range response.candidates {
					n.rtMutex.Lock()
					n.RoutingTable.Add(c)
					n.rtMutex.Unlock()
				}
				//n.statsMutex.Lock()
				// XXX should append if closer
				n.stats.closers = AppendNodesIfMissing(n.stats.closers, response.closers)
				n.rtMutex.RLock()
				n.stats.candidates = ksToNs(n.RoutingTable.GetCloserCandidates())
				n.rtMutex.RUnlock()
				n.stats.queried = AppendNodeIfMissing(n.stats.queried, response.sender)
				//n.statsMutex.Unlock()
				ayame.Log.Debugf("%s: received closers=%s, level=%d, candidates=%s\n", n.Key(), ayame.SliceString(response.closers), response.level, ayame.SliceString(response.candidates))
				n.procsMutex.Lock()
				delete(n.Procs, response.id)
				n.procsMutex.Unlock()
			}
		}

		//XXX if needed
		//time.Sleep(time.Duration(FIND_NODE_INTERVAL) * time.Second)
		ayame.Log.Debugf("%s: join stats=%s", n.Key(), n.stats)
	}
}

func (n *BSNode) NotifyDeletion() {
	for _, neighbor := range ksToNs(n.RoutingTable.GetAll()) {
		n.SendEvent(neighbor, nil) // XXX implement nil
	}
}

func (n *BSNode) allQueried() bool {
	ret := false
	//n.statsMutex.RLock()
	ret = AllContained(n.stats.closers, n.stats.queried) &&
		AllContained(n.stats.candidates, n.stats.queried)
	//n.statsMutex.RUnlock()
	return ret
}

func (n *BSNode) Lookup(ctx context.Context, key ayame.Key) []*BSNode {
	lookupCtx, cancel := context.WithTimeout(ctx, time.Duration(LOOKUP_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()

	closers, level := n.GetNeighbors(key)
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
			go func() { n.FindNodeWithKey(lookupCtx, lookupCh, node, key, id) }()
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
					// XXX delete response.sender from the routing table if already exists.
					continue L
				}
				if response.level == 0 {
					results = AppendNodesIfMissing(results, response.closers)
					if ContainsKey(key, results) {
						return results // cancel other FindNode
					}
				}
				ayame.Log.Debugf("%d th/ %d FindNode finished", i+1, reqCount)
				queried = AppendNodeIfMissing(queried, response.sender)
				closers = AppendNodesIfMissing(closers, response.closers)
			}
		}
	}
	return results
}

func (n *BSNode) SetUnicastHandler(receiver func(*BSNode, *BSUnicastEvent, bool, bool)) {
	n.unicastHandler = receiver
}

func (n *BSNode) Unicast(ctx context.Context, key ayame.Key, payload []byte) {
	unicastCtx, cancel := context.WithTimeout(ctx, time.Duration(UNICAST_SEND_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()
	closers, level := n.GetNeighbors(key)
	ayame.Log.Debugf("from=%s,to=%s,first level=%d %s\n", n.Key(), key, level, ayame.SliceString(closers))
	ch := make(chan struct{}, 1)
	mid := NextId()
	for _, c := range closers {
		localc := c
		ev := NewBSUnicastEvent(n, mid, level, key)
		n.unicastEvent(ch, localc, ev)
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

func (n *BSNode) unicastEvent(ch chan struct{}, node *BSNode, ev *BSUnicastEvent) {
	go func() {
		if node.Equals(n) {
			n.handleUnicast(ev, true)
		} else {
			n.SendEvent(node, ev)
		}
		ch <- struct{}{}
	}()
}

func (n *BSNode) SendEvent(receiver ayame.Node, ev ayame.SchedEvent) {
	ev.SetSender(n) // author of the message
	ev.SetReceiver(receiver)
	n.Send(ev)
}

func (n *BSNode) FindNodeWithKey(ctx context.Context, findCh chan *FindNodeResponse, node *BSNode, key ayame.Key, requestId string) chan *FindNodeResponse {
	ev := NewBSFindNodeReqEvent(n, requestId, key, nil)
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second)
	defer cancel()
	ch := make(chan *FindNodeResponse)
	n.procsMutex.Lock()
	n.Procs[requestId] = &FindNodeProc{ev: ev, id: requestId, ch: ch}
	n.procsMutex.Unlock()
	n.SendEvent(node, ev)
	select {
	case <-findCtx.Done(): // after FIND_NODE_TIMEOUT
		ayame.Log.Debugf("FindNode ended with %s", findCtx.Err())
		if findCtx.Err() == context.DeadlineExceeded {
			// set the node as failure
			findCh <- &FindNodeResponse{sender: node, isFailure: true}
		} else {
			findCh <- &FindNodeResponse{}
		}
	case response := <-ch: // FindNode finished
		ayame.Log.Debugf("FindNode ended normally")
		findCh <- response
	}
	return ch
}

func (n *BSNode) FindNode(ctx context.Context, findCh chan *FindNodeResponse, node *BSNode, requestId string) chan *FindNodeResponse {
	ev := NewBSFindNodeReqEvent(n, requestId, n.Key(), n.MV())
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second)
	defer cancel()
	ch := make(chan *FindNodeResponse)
	n.procsMutex.Lock()
	n.Procs[requestId] = &FindNodeProc{ev: ev, id: requestId, ch: ch}
	n.procsMutex.Unlock()
	n.SendEvent(node, ev)
	select {
	case <-findCtx.Done(): // after FIND_NODE_TIMEOUT
		ayame.Log.Debugf("FindNode ended with %s", findCtx.Err())
		if findCtx.Err() == context.DeadlineExceeded {
			// set the node as failure
			findCh <- &FindNodeResponse{sender: node, isFailure: true}
		} else {
			findCh <- &FindNodeResponse{}
		}
	case response := <-ch: // FindNode finished
		ayame.Log.Debugf("FindNode ended normally")
		findCh <- response
	}
	return ch
}

func (n *BSNode) handleFindNode(ev ayame.SchedEvent) {
	ue := ev.(*BSFindNodeEvent)
	if ue.isResponse {
		ayame.Log.Debugf("find node response from=%v\n", ue.Sender().(*BSNode))
		//ayame.Log.Debugf("stats=%s, table=%s\n", n.stats, n.RoutingTable)
		n.procsMutex.RLock()
		proc, exists := n.Procs[ue.MessageId]
		n.procsMutex.RUnlock()
		if exists {
			proc.ch <- &FindNodeResponse{id: ue.MessageId, sender: ue.Sender().(*BSNode), closers: ue.closers, candidates: ue.candidates, level: ue.level, isFailure: false}
		} else {
			ayame.Log.Errorf("unregistered response %s from %s\n", ue.MessageId, ue.Sender().(*BSNode))
			panic("unregisterd response") // it's implementation error
		}
	} else {
		kmv := ue.Sender().(*BSNode)
		var closers, candidates []*BSNode
		var level int
		if ue.TargetMV == nil {
			n.rtMutex.RLock()
			closers, level = n.GetNeighbors(ue.TargetKey)
			n.rtMutex.RUnlock()
		} else {
			n.rtMutex.RLock()
			closers, level, candidates = n.GetNeighborsAndCandidates(kmv.Key(), kmv.MV())
			n.rtMutex.RUnlock()
		}
		ayame.Log.Debugf("returns closers=%s, level=%d, candidates=%s\n", ayame.SliceString(closers), level, ayame.SliceString(candidates))
		n.rtMutex.Lock()
		n.RoutingTable.Add(ue.Sender().(*BSNode))
		n.rtMutex.Unlock()
		ev := NewBSFindNodeResEvent(ue, closers, level, candidates)
		n.SendEvent(ue.Sender(), ev)
	}
}

func SimUnicastHandler(n *BSNode, msg *BSUnicastEvent, alreadySeen bool, alreadyOnThePath bool) {
	if alreadySeen {
		msg.Root.NumberOfDuplicatedMessages++
		//msg.root.results = appendIfMissing(msg.root.results, m)
		msg.Root.Paths = append(msg.Root.Paths, msg.Path)
	} else if alreadyOnThePath {
		ayame.Log.Debugf("I, %d, found %d is on the path %s, do nothing\n", n.key, msg.Receiver().Key(),
			strings.Join(funk.Map(msg.Path, func(pe PathEntry) string {
				return fmt.Sprintf("%s@%d", pe.Node, pe.Level)
			}).([]string), ","))
		msg.Root.Results = AppendNodeIfMissing(msg.Root.Results, n)
		msg.Root.Paths = append(msg.Root.Paths, msg.Path)
	} else {
		ayame.Log.Debugf("level=0 on %d msg=%s\n", n.key, msg)
		// reached to the destination.
		if Contains(n, msg.Root.Destinations) { // already arrived.
			ayame.Log.Debugf("redundant result: %s, path:%s\n", msg, PathsString([][]PathEntry{msg.Path}))
		} else { // NEW!
			msg.Root.Destinations = append(msg.Root.Destinations, n)
			msg.Root.DestinationPaths = append(msg.Root.DestinationPaths, msg.Path)

			if len(msg.Root.Destinations) == msg.Root.ExpectedNumberOfResults {
				// XXX need to send UnicastReply
				ayame.Log.Debugf("dst=%d: completed %d, paths:%s\n", msg.TargetKey, len(msg.Root.Destinations),
					PathsString(msg.Root.DestinationPaths))
				//msg.root.channel <- true
				//msg.root.finishTime = sev.Time()
			} else {
				if len(msg.Root.Destinations) >= msg.Root.ExpectedNumberOfResults {
					ayame.Log.Debugf("redundant results: %s, paths:%s\n", ayame.SliceString(msg.Root.Destinations), PathsString(msg.Root.DestinationPaths))
				} else {
					ayame.Log.Debugf("wait for another result: currently %d\n", len(msg.Root.Destinations))
				}
			}
		}
		// add anyway to check redundancy & record number of messages
		msg.Root.Results = AppendNodeIfMissing(msg.Root.Results, n)
		msg.Root.Paths = append(msg.Root.Paths, msg.Path)
	}
}

func (n *BSNode) handleUnicast(sev ayame.SchedEvent, sendToSelf bool) error {
	msg := sev.(*BSUnicastEvent)
	ayame.Log.Debugf("handling msg to %d on %s level %d\n", msg.TargetKey, n, msg.level)
	if !sendToSelf && msg.CheckAlreadySeen(n) {
		n.unicastHandler(n, msg, true, false)
		ayame.Log.Debugf("called already seen handler\n")
		return nil
	}
	if msg.level == 0 { // level 0 means destination
		n.unicastHandler(n, msg, false, false)
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
				n.handleUnicast(next, true)
			} else {
				if Contains(node, funk.Map(msg.Path, func(pe PathEntry) *BSNode { return pe.Node.(*BSNode) }).([]*BSNode)) {
					n.unicastHandler(n, msg, false, true)
				} else {
					msg.SetAlreadySeen(n)
					ayame.Log.Debugf("I, %d, am not one of the dest: %d, forward\n", n.key, node.key)
					//ev := msg.createSubMessage(n)
					n.SendEvent(node, next)
				}
			}

		}
	}
	return nil
}
