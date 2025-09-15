package sim

import (
	"fmt"
	"strings"

	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
	"github.com/thoas/go-funk"
)

func simUnicastHandler(n *bs.BSNode, msg *bs.BSUnicastEvent, alreadySeen bool, alreadyOnThePath bool) {
	if alreadySeen {
		msg.Root.NumberOfDuplicatedMessages++
		//msg.root.results = appendIfMissing(msg.root.results, m)
		msg.Root.Paths = append(msg.Root.Paths, msg.Path)
	} else if alreadyOnThePath {
		log.Debugf("I, %d, found %d is on the path %s, do nothing\n", n.Key(), msg.Receiver().Key(),
			strings.Join(funk.Map(msg.Path, func(pe bs.PathEntry) string {
				return fmt.Sprintf("%s@%d", pe.Node, pe.Level)
			}).([]string), ","))
		msg.Root.Results = ayame.AppendIfAbsent(msg.Root.Results, n)
		msg.Root.Paths = append(msg.Root.Paths, msg.Path)
	} else {
		log.Debugf("level=0 on %d msg=%s\n", n.Key(), msg)
		// reached to the destination.
		if bs.Contains(n, msg.Root.Destinations) { // already arrived.
			log.Debugf("redundant result: %s, path:%s\n", msg, bs.PathsString([][]bs.PathEntry{msg.Path}))
		} else { // NEW!
			msg.Root.Destinations = append(msg.Root.Destinations, n)
			msg.Root.DestinationPaths = append(msg.Root.DestinationPaths, msg.Path)

			if len(msg.Root.Destinations) == msg.Root.ExpectedNumberOfResults {
				// XXX need to send UnicastReply
				log.Debugf("dst=%d: completed %d, paths:%s\n", msg.TargetKey, len(msg.Root.Destinations),
					bs.PathsString(msg.Root.DestinationPaths))
				//msg.root.channel <- true
				//msg.root.finishTime = sev.Time()
			} else {
				if len(msg.Root.Destinations) >= msg.Root.ExpectedNumberOfResults {
					log.Debugf("redundant results: %s, paths:%s\n", ayame.SliceString(msg.Root.Destinations), bs.PathsString(msg.Root.DestinationPaths))
				} else {
					log.Debugf("wait for another result: currently %d\n", len(msg.Root.Destinations))
				}
			}
		}
		// add anyway to check redundancy & record number of messages
		msg.Root.Results = ayame.AppendIfAbsent(msg.Root.Results, n)
		msg.Root.Paths = append(msg.Root.Paths, msg.Path)
	}
}
