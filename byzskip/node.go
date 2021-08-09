package byzskip

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/piax/go-ayame/ayame"
	"github.com/thoas/go-funk"
)

// -- find node
type FindNodeProc struct {
	ev         *BSFindNodeEvent
	candidates []*BSNode   // candidates
	closers    []*BSNode   // the current result
	queried    []*BSNode   // store the queried remote nodes
	ch         chan string // notify message-id when finished
}

type BSNode struct {
	parent ayame.Node
	key    ayame.Key
	mv     *ayame.MembershipVector
	//bs.IntKeyMV

	RoutingTable RoutingTable
	IsFailure    bool
	QuerySeen    map[string]int

	Procs map[string]*FindNodeProc
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
	ret := n.Key().String()
	if n.IsFailure {
		ret += "*"
	}
	return ret
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
func (node *BSNode) GetNeighborsAndCandidates(s *BSNode) ([]*BSNode, int, []*BSNode) {
	ret, level := node.RoutingTable.GetNeighbors(s.Key())
	//can := node.routingTable.GetAll()
	can := node.RoutingTable.GetCommonNeighbors(s)
	return ksToNs(ret), level, ksToNs(can)
}

func NewBSNode(parent ayame.Node, rtMaker func(KeyMV) RoutingTable, isFailure bool) *BSNode {
	ret := &BSNode{key: parent.Key(), mv: parent.MV(),
		//LocalNode: ayame.GetLocalNode(strconv.Itoa(key)),
		parent:    parent,
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
	found := 0
	for _, n := range curKNodes {
		contained := false
		for _, m := range queried {
			if n.Equals(m) {
				found++
				contained = true
				break
			}
		}
		if !contained {
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

func appendNodeIfMissing(lst []*BSNode, node *BSNode) []*BSNode {
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
			return fmt.Sprintf("%s@%d", pe.Node, pe.level)
		}).([]string), ",") + "]"
	}
	return ret
}

func (pe PathEntry) String() string {
	return pe.Node.String()
}

const (
	FIND_NODE_TIMEOUT  = 20
	FIND_NODE_INTERVAL = 1
)

func (n *BSNode) Join(introducer *BSNode) {
	ctx := context.Background()
	findCtx, cancel := context.WithTimeout(ctx, time.Duration(FIND_NODE_TIMEOUT)*time.Second) // all request should be ended within
	defer cancel()
	var ch chan string
	go func() { // run in background
		ch = n.FindNode(findCtx, introducer)
	}()
	// wait for the process
	select {
	case <-ctx.Done(): // after FIND_NODE_TIMEOUT
		ayame.Log.Infof("FIND_NODE process ended with timeout")
	case mid := <-ch: // FindNode finished
		ayame.Log.Infof("FIND_NODE process finished normally")
		proc := n.Procs[mid]
		ayame.Log.Infof("found %s\n", ayame.SliceString(proc.closers))
	}
}

func (n *BSNode) FindNode(ctx context.Context, introducer *BSNode) chan string {

	ev := NewBSFindNodeReqEvent(n, n.Key())
	ch := make(chan string, 1)
	n.Procs[ev.MessageId] = &FindNodeProc{ev: ev, queried: []*BSNode{}, ch: ch}
	ev.SetSender(n) // author of the message
	ev.SetReceiver(introducer)
	n.Send(ev)
	return ch
}

func (n *BSNode) handleFindNode(ev ayame.SchedEvent) {
	ue := ev.(*BSFindNodeEvent)
	if ue.isResponse {
		if ue.level == 0 {

		}
		for _, c := range ue.candidates {
			n.RoutingTable.Add(c)
		}
	} else {
		n.RoutingTable.Add(ue.Sender().(*BSNode))
		n.GetNeighborsAndCandidates(ue.Sender().(*BSNode))
		n.GetCandidates()
		n.GetNeighbors(ue.TargetKey)
	}
}

func (m *BSNode) handleUnicast(sev ayame.SchedEvent, sendToSelf bool) error {
	msg := sev.(*BSUnicastEvent)
	ayame.Log.Debugf("handling %s->%d on %s level %d\n", msg.Root.Sender().String(), msg.TargetKey, msg.Receiver(), msg.level)
	if !sendToSelf && msg.CheckAlreadySeen() {
		msg.Root.numberOfDuplicatedMessages++
		//msg.root.results = appendIfMissing(msg.root.results, m)
		msg.Root.Paths = append(msg.Root.Paths, msg.path)
		return nil
	}

	if msg.level == 0 { // level 0 means destination
		ayame.Log.Debugf("level=0 on %d msg=%s\n", m.key, msg)
		// reached to the destination.
		if Contains(m, msg.Root.Destinations) { // already arrived.
			ayame.Log.Debugf("redundant result: %s, path:%s\n", msg, PathsString([][]PathEntry{msg.path}))
		} else { // NEW!
			msg.Root.Destinations = append(msg.Root.Destinations, m)
			msg.Root.DestinationPaths = append(msg.Root.DestinationPaths, msg.path)

			if len(msg.Root.Destinations) == msg.Root.expectedNumberOfResults {
				// XXX need to send UnicastReply
				ayame.Log.Debugf("dst=%d: completed %d, paths:%s\n", msg.TargetKey, len(msg.Root.Destinations),
					PathsString(msg.Root.DestinationPaths))
				//msg.root.channel <- true
				//msg.root.finishTime = sev.Time()
			} else {
				if len(msg.Root.Destinations) >= msg.Root.expectedNumberOfResults {
					ayame.Log.Debugf("redundant results: %s, paths:%s\n", ayame.SliceString(msg.Root.Destinations), PathsString(msg.Root.DestinationPaths))
				} else {
					ayame.Log.Debugf("wait for another result: currently %d\n", len(msg.Root.Destinations))
				}
			}
		}
		// add anyway to check redundancy & record number of messages
		msg.Root.Results = appendNodeIfMissing(msg.Root.Results, m)
		msg.Root.Paths = append(msg.Root.Paths, msg.path)

	} else {
		nextMsgs := msg.findNextHops()
		//ayame.Log.Debugf("next msgs: %v\n", nextMsgs)
		for _, next := range nextMsgs {
			node := next.Receiver().(*BSNode)
			ayame.Log.Debugf("node: %d, m: %d\n", node.key, m.key)

			// already via the path.
			if node.Equals(m) {
				// myself
				ayame.Log.Debugf("I, %d, am one of the dest: %d, pass to myself\n", m.key, node.key)
				m.handleUnicast(next, true)
			} else {
				if Contains(node, funk.Map(msg.path, func(pe PathEntry) *BSNode { return pe.Node.(*BSNode) }).([]*BSNode)) {
					// do nothing next but record to path
					ayame.Log.Debugf("I, %d, found %d is on the path %s, do nothing\n", m.key, node.key,
						strings.Join(funk.Map(msg.path, func(pe PathEntry) string {
							return fmt.Sprintf("%s@%d", pe.Node, pe.level)
						}).([]string), ","))
					msg.Root.Results = appendNodeIfMissing(msg.Root.Results, m)
					msg.Root.Paths = append(msg.Root.Paths, msg.path)
				} else {
					msg.SetAlreadySeen()
					ayame.Log.Debugf("I, %d, am not one of the dest: %d, forward\n", m.key, node.key)
					//ev := msg.createSubMessage(n)
					m.Send(next)
				}
			}

		}
	}
	return nil
}
