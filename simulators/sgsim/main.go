package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/montanaflynn/stats"
	"github.com/piax/go-ayame/ayame"
	funk "github.com/thoas/go-funk"
)

var alpha *int
var numberOfNodes *int
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
	fmt.Printf("avg. routing table height: %f\n", ave)
	return nodes
}

func dumpNodesMV(nodes []*SGNode) {
	for i, n := range nodes {
		fmt.Printf("node[%d]=%s\n", i, n.mv)
	}
}

func main() {
	alpha = flag.Int("alpha", 2, "the alphabet size (default: 2)")
	numberOfNodes = flag.Int("nodes", 16, "number of nodes (default: 16)")
	seed = flag.Int64("seed", 2, "give a random seed (default: 1)")
	verbose = flag.Bool("v", false, "verbose output")
	flag.Parse()

	rand.Seed(*seed)
	nodes := ConstructOverlay(*numberOfNodes, true)
	if *verbose {
		for _, n := range nodes {
			fmt.Println("key=" + strconv.Itoa(n.key) + "\n" + n.routingTableString())
		}
	}
	msgs := []*UnicastEvent{}
	numberOfTrials := *numberOfNodes * 4
	for i := 1; i <= numberOfTrials; i++ {
		src := rand.Intn(*numberOfNodes)
		dst := rand.Intn(*numberOfNodes)
		msg := NewUnicastEvent(nodes[src], dst)
		msgs = append(msgs, msg)
		if *verbose {
			fmt.Printf("id=%d,src=%d, dst=%d\n", msg.messageId, src, dst)
		}
		ayame.GlobalEventExecutor.RegisterEvent(msg, int64(i*1000))
	}
	for _, msg := range msgs {
		go func(msg *UnicastEvent) {
			<-msg.root.channel
			if *verbose {
				fmt.Printf("%d: %d, hops %d\n", msg.messageId, msg.Time(), len(msg.path))
			}
		}(msg)
	}
	ayame.GlobalEventExecutor.Sim(int64(numberOfTrials*1000*2), true)
	ayame.GlobalEventExecutor.AwaitFinish()

	ave, _ := stats.Mean(funk.Map(msgs, func(msg *UnicastEvent) float64 { return float64(len(msg.path)) }).([]float64))
	fmt.Printf("avg. path length: %f\n", ave)
}
