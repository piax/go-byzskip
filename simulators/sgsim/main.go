package main

import (
	"fmt"
	"math/rand"
	"strconv"

	"github.com/montanaflynn/stats"
	"github.com/op/go-logging"
	"github.com/piax/go-byzskip/ayame"
	flag "github.com/spf13/pflag"
	funk "github.com/thoas/go-funk"
)

var alpha *int
var numberOfNodes *int
var trials *int
var seed *int64
var verbose *bool

func ConstructOverlay(numberOfNodes int, fast bool) []*SGNode {
	nodes := make([]*SGNode, 0, numberOfNodes)
	for i := 0; i < numberOfNodes; i++ {
		n := NewSGNode(i, ayame.NewMembershipVector(*alpha))
		nodes = append(nodes, n)
	}
	if *verbose {
		dumpNodesMV(nodes)
	}
	FastJoinAll(nodes)
	ave, _ := stats.Mean(funk.Map(nodes, func(n *SGNode) float64 { return float64(n.routingTableHeight()) }).([]float64))
	ayame.Log.Debugf("avg. routing table height: %f\n", ave)
	return nodes
}

func dumpNodesMV(nodes []*SGNode) {
	for i, n := range nodes {
		fmt.Printf("node[%d]=%s\n", i, n.mv)
	}
}

func main() {
	alpha = flag.IntP("alpha", "a", 2, "the alphabet size of the membership vector")
	numberOfNodes = flag.IntP("nodes", "n", 100, "number of nodes")
	seed = flag.Int64P("seed", "s", 3, "give a random seed")
	trials = flag.IntP("trials", "t", -1, "number of search trials (-1 means same as nodes)")
	verbose = flag.BoolP("verbose", "v", false, "verbose output")
	flag.Parse()

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	rand.Seed(*seed)
	nodes := ConstructOverlay(*numberOfNodes, true)
	if *verbose {
		for _, n := range nodes {
			fmt.Println("key=" + strconv.Itoa(n.key) + "\n" + n.routingTableString())
		}
	}
	if *trials < 0 {
		*trials = *numberOfNodes
	}
	msgs := []*UnicastEvent{}
	for i := 1; i <= *trials; i++ {
		src := rand.Intn(*numberOfNodes)
		dst := rand.Intn(*numberOfNodes)
		msg := NewUnicastEvent(nodes[src], dst)
		msgs = append(msgs, msg)
		if *verbose {
			ayame.Log.Debugf("id=%d,src=%d, dst=%d\n", msg.messageId, src, dst)
		}
		ayame.GlobalEventExecutor.RegisterEvent(msg, int64(i*1000))
	}
	for _, msg := range msgs {
		go func(msg *UnicastEvent) {
			<-msg.root.channel
			ayame.Log.Debugf("%d: %d, hops %d\n", msg.messageId, msg.Time(), len(msg.path))
		}(msg)
	}
	ayame.GlobalEventExecutor.Sim(int64(*trials*1000*2), true)
	ayame.GlobalEventExecutor.AwaitFinish()

	ave, _ := stats.Mean(funk.Map(msgs, func(msg *UnicastEvent) float64 { return float64(len(msg.path)) }).([]float64))
	ayame.Log.Infof("avg-match-hops: %f\n", ave)
}
