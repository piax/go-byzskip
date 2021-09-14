package byzskip

import (
	"context"
	"time"

	"github.com/jbenet/goprocess"
	"github.com/piax/go-ayame/ayame"
)

var periodicBootstrapInterval = 1 * time.Minute

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
		n.fixLowPeers()
	}

}

func (n *BSNode) fixLowPeers() {
	if len(n.stats.failed) == 0 && n.RoutingTable.HasSufficientNeighbors() {
		ayame.Log.Debugf("%s: has sufficient neighbors", n)
		return
	}
	clst, _ := n.RoutingTable.KClosestWithKey(n.Key())
	n.stats = &JoinStats{runningQueries: 0,
		closest:    ksToNs(clst),
		candidates: []*BSNode{}, queried: []*BSNode{}, failed: []*BSNode{}}
	n.RefreshRTWait(context.TODO())
}
