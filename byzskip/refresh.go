package byzskip

import (
	"context"
	"sync"
	"time"

	"github.com/jbenet/goprocess"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
)

var periodicBootstrapInterval = 1 * time.Minute
var RefreshInterval = 2 * time.Minute
var PingInterval = 1 * time.Minute

func (n *BSNode) fixLowPeersRoutine(proc goprocess.Process) {
	ticker := time.NewTicker(periodicBootstrapInterval)
	defer ticker.Stop()

	for {
		select {
		//		case <-n.fixLowPeersChan:
		case <-ticker.C:
		case <-proc.Closing():
			return
		}
		ayame.Log.Debugf("%s: fixing low peers", n)
		n.fixLowPeers(context.Background())
	}

}

var peerPingTimeout = 10 * time.Second

func (n *BSNode) pingAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, node := range n.RoutingTable.AllNeighbors(false, true) {
		wg.Add(1)
		go func(node *BSNode) {
			defer wg.Done()
			livelinessCtx, cancel := context.WithTimeout(ctx, peerPingTimeout)
			if err := n.Parent.(*p2p.P2PNode).Host.Connect(livelinessCtx, peer.AddrInfo{ID: node.Id()}); err != nil {
				ayame.Log.Debug("evicting peer after failed ping", "peer", node.Id(), "error", err)

				n.rtMutex.Lock()
				//n.RoutingTable.Delete(ev.TargetKey)
				n.RoutingTable.Del(node)
				n.rtMutex.Unlock()
			}
			cancel()
		}(node.(*BSNode))
	}
	wg.Wait()
}

func (n *BSNode) fixLowPeers(ctx context.Context) {
	force := false

	if time.Since(n.lastPing) >= PingInterval {
		ayame.Log.Debugf("%s: ping to all", n)
		n.pingAll(ctx)
		n.lastPing = time.Now()
	}

	if time.Since(n.lastRefresh) >= RefreshInterval {
		force = true
	}

	if !force && len(n.stats.failed) == 0 && n.RoutingTable.HasSufficientNeighbors() {
		ayame.Log.Debugf("%s: has sufficient neighbors", n)
		return
	}

	clst, _ := n.RoutingTable.KClosestWithKey(n.Key())
	ayame.Log.Debugf("%s: refresh start nodes: %s", n, ayame.SliceString(clst))
	n.stats = &JoinStats{runningQueries: 0,
		closest:    ksToNs(clst),
		candidates: []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}
	n.RefreshRTWait(ctx)
}
