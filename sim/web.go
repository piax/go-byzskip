package sim

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"

	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
)

var allNodes []*bs.BSNode

func startWeb(ns []*bs.BSNode) {
	allNodes = ns

	http.Handle("/vis/", http.StripPrefix("/vis", http.FileServer(http.Dir(os.Getenv("BSSIM_WEBDIR")))))
	http.HandleFunc("/uni", uni)
	http.HandleFunc("/nodes", nodes)
	log.Fatal(http.ListenAndServe(":3000", nil))
}

func uni(w http.ResponseWriter, req *http.Request) {
	fmt.Fprint(w, DoUnicast())
}

func nodes(w http.ResponseWriter, req *http.Request) {
	fmt.Fprint(w, doNodes())
}

func doNodes() string {
	rval := make([]string, len(allNodes))
	for i, x := range allNodes {
		rval[i] = x.RoutingTable.JSONString()
	}
	return "[" + strings.Join(rval, ",") + "]"
}

func ConstructNodes(k int) string {
	bs.InitK(k)
	allNodes := ConstructOverlay(*numberOfNodes)
	rval := make([]string, len(allNodes))
	for i, x := range allNodes {
		rval[i] = x.RoutingTable.JSONString()
	}
	return "[" + strings.Join(rval, ",") + "]"
}

func DoUnicast() string {
	ayame.GlobalEventExecutor.Reset()
	src := NormalList[rand.Intn(len(NormalList))]
	dst := NormalList[rand.Intn(len(NormalList))]
	mid := src.String() + "." + NextId()
	msg := bs.NewBSUnicastEventNoOriginator(src, mid, ayame.MembershipVectorSize, dst.Key(), []byte("hello"))
	ayame.GlobalEventExecutor.RegisterEvent(msg, int64(1000))
	ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {
		ayame.Log.Debugf("id=%s,src=%s, dst=%s timed out\n", msg.MessageId, src, dst)
		msg.Root.Channel <- true
	}), int64(1000+100))

	go func(msg *bs.BSUnicastEvent) {
		<-msg.Root.Channel // wait for the timeout.
		//if *verbose {
		//avg, _ := meanOfPathLength(msg.root.paths)
		ayame.Log.Debugf("%d: started %s, finished: %d\n", msg.TargetKey, msg.MessageId, msg.Time())
		//}
		if bs.ContainsKey(msg.TargetKey, msg.Root.Destinations) {
			ayame.Log.Debugf("%s is included in %s\n", msg.TargetKey, msg.Root.Destinations)
		} else {
			ayame.Log.Debugf("%s->%s: FAILURE!!! %s\n", msg.Sender(), msg.TargetKey, ayame.SliceString(msg.Root.Destinations))
		}
		close(msg.Root.Channel)
	}(msg)
	ayame.GlobalEventExecutor.Sim(int64(2000), true)
	ayame.GlobalEventExecutor.AwaitFinish()

	ret := "["
	for x, path := range msg.Paths {
		ps := "["
		for i, p := range path {
			ps += fmt.Sprintf("[%d, %s]", p.Level, p.Node.Key())
			if i < len(path)-1 {
				ps += ","
			}
		}
		ps += "]"
		ret += ps
		if x < len(msg.Paths)-1 {
			ret += ","
		}
	}
	ret += "]"
	return ret
}
