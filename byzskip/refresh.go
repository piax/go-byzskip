package byzskip

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/piax/go-byzskip/ayame"
	p2p "github.com/piax/go-byzskip/ayame/p2p"
	"go.uber.org/fx"
)

// var periodicBootstrapInterval = 1 * time.Minute // bootstrap the routing table
var refreshInterval = 12 * time.Hour // reconstruct the routing table
var pingInterval = 10 * time.Minute  // ping all neighbors

/*
func (n *BSNode) fixLowPeersRoutine(proc goprocess.Process) {
	log.Debugf("%s: fixLowPeersRoutine starting", n)
	ticker := time.NewTicker(periodicBootstrapInterval)
	defer ticker.Stop()

	for {
		select {
		//		case <-n.fixLowPeersChan:
		case <-ticker.C:
		case <-proc.Closing():
			log.Debugf("%s: fixLowPeersRoutine closing", n)
			return
		}
		log.Debugf("%s: fixing low peers", n)
		n.fixLowPeers(context.Background())
	}
}*/

var peerPingTimeout = 10 * time.Second

func (n *BSNode) pingAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, node := range n.RoutingTable.AllNeighbors(false, true) {
		wg.Add(1)
		go func(node *BSNode) {
			defer wg.Done()
			livelinessCtx, cancel := context.WithTimeout(ctx, peerPingTimeout)
			if err := n.Parent.(*p2p.P2PNode).Host.Connect(livelinessCtx, peer.AddrInfo{ID: node.Id()}); err != nil {
				log.Info("evicting peer after failed ping", "peer", node.Id(), "error", err)

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
	if time.Since(n.lastPing) >= pingInterval {
		log.Debugf("%s: ping to all", n)
		n.pingAll(ctx)
		n.lastPing = time.Now()
	}

	if time.Since(n.lastRefresh) >= refreshInterval {
		force = true
	}

	if !force && len(n.stats.failed) == 0 && n.RoutingTable.HasSufficientNeighbors() {
		log.Debugf("%s: has sufficient neighbors", n)
		return
	}
	introducer, err := n.IntroducerNode(ayame.PickRandomly(n.BootstrapAddrs))
	clst, _ := n.RoutingTable.KClosestWithKey(n.Key())
	if len(clst) < K && introducer.Id() != n.Id() {
		log.Infof("%s: has insufficient neighbors, use introducer to refresh", n)
		if err != nil {
			log.Errorf("%s: failed to get introducer", n)
			return
		}
		clst = []KeyMV{introducer}
	}
	log.Debugf("%s: refresh start nodes: %s", n, ayame.SliceString(clst))
	n.stats = &JoinStats{runningQueries: 0,
		closest:    ksToNs(clst),
		candidates: []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}
	n.RefreshRTWait(ctx)
}

// LowPeersFixer is a function to be used with fx.Invoke
// The interval parameter specifies the execution interval
func LowPeersFixer(interval time.Duration) func(lc fx.Lifecycle, node *BSNode) error {
	return func(lc fx.Lifecycle, node *BSNode) error {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go func() {
					ticker := time.NewTicker(interval)
					defer ticker.Stop()

					for {
						select {
						case <-ticker.C:
							node.fixLowPeers(context.Background())
						case <-ctx.Done():
							log.Debugf("%s: fixLowPeersRoutine closing", node)
							return
						}
					}
				}()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				log.Debugf("%s: LowPeersFixer stopping", node)
				return nil
			},
		})
		return nil
	}
}
