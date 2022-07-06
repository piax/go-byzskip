package main

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
		ayame.Log.Debugf("I, %d, found %d is on the path %s, do nothing\n", n.Key(), msg.Receiver().Key(),
			strings.Join(funk.Map(msg.Path, func(pe bs.PathEntry) string {
				return fmt.Sprintf("%s@%d", pe.Node, pe.Level)
			}).([]string), ","))
		msg.Root.Results = bs.AppendNodeIfMissing(msg.Root.Results, n)
		msg.Root.Paths = append(msg.Root.Paths, msg.Path)
	} else {
		ayame.Log.Debugf("level=0 on %d msg=%s\n", n.Key(), msg)
		// reached to the destination.
		if bs.Contains(n, msg.Root.Destinations) { // already arrived.
			ayame.Log.Debugf("redundant result: %s, path:%s\n", msg, bs.PathsString([][]bs.PathEntry{msg.Path}))
		} else { // NEW!
			msg.Root.Destinations = append(msg.Root.Destinations, n)
			msg.Root.DestinationPaths = append(msg.Root.DestinationPaths, msg.Path)

			if len(msg.Root.Destinations) == msg.Root.ExpectedNumberOfResults {
				// XXX need to send UnicastReply
				ayame.Log.Debugf("dst=%d: completed %d, paths:%s\n", msg.TargetKey, len(msg.Root.Destinations),
					bs.PathsString(msg.Root.DestinationPaths))
				//msg.root.channel <- true
				//msg.root.finishTime = sev.Time()
			} else {
				if len(msg.Root.Destinations) >= msg.Root.ExpectedNumberOfResults {
					ayame.Log.Debugf("redundant results: %s, paths:%s\n", ayame.SliceString(msg.Root.Destinations), bs.PathsString(msg.Root.DestinationPaths))
				} else {
					ayame.Log.Debugf("wait for another result: currently %d\n", len(msg.Root.Destinations))
				}
			}
		}
		// add anyway to check redundancy & record number of messages
		msg.Root.Results = bs.AppendNodeIfMissing(msg.Root.Results, n)
		msg.Root.Paths = append(msg.Root.Paths, msg.Path)
	}
}
