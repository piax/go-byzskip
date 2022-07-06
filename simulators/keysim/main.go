package main

import (
	"flag"
	"fmt"

	"github.com/op/go-logging"
	"github.com/piax/go-ayame/ayame"
	ki "github.com/piax/go-ayame/key_issuer"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

var k *int
var numberOfNodes *int
var seed *int64
var poolSize *int
var adversarialType *string
var issueType *string
var failureRatio *float64
var paramAlpha *float64
var paramBeta *float64
var verbose *bool

func allFailure(delegated []*ki.Node) bool {
	for _, d := range delegated {
		if !d.IsFailure() {
			return false
		}
	}
	return true
}

// burst key attack constants
const (
	BURST_START     = 1000
	BURST_START_KEY = 0.5
	BURST_INTERVAL  = 0.00001
)

const (
	JOIN_RATIO = 0.7
)

func GetKClosest(nodes Ring, logicalKey float64, k int) []*ki.Node {
	ret := []*ki.Node{}
	left, right := k/2, k/2

	index, cur := nodes.Find(logicalKey)

	start := index
	// cur is always non-nil
	for len(ret) < left {
		if cur.Key() != logicalKey { // myself (when as-is) is skipped.
			ret = append(ret, cur)
		}
		index, cur = nodes.Prev(index)
	}
	ayame.ReverseSlice(ret)
	index = start
	for len(ret) < left+right { // right side
		if cur.Key() != logicalKey { // myself (when as-is) is skipped.
			ret = append(ret, cur)
		}
		index, cur = nodes.Next(index)
	}
	return ret

}

func main() {
	k = flag.Int("k", 4, "the redundancy parameter")
	numberOfNodes = flag.Int("nodes", 100, "number of nodes")
	seed = flag.Int64("seed", 4, "give a random seed")
	poolSize = flag.Int("pool", 5, "the size of the pool")
	paramAlpha = flag.Float64("alpha", 1.0, "the parameter alpha of beta distribution.")
	paramBeta = flag.Float64("beta", 1.0, "the parameter beta of beta distribution.")
	adversarialType = flag.String("adv", "random", "adversarial attack type {burst|random}")
	issueType = flag.String("issue", "random", "key issuer type {shuffle|random|asis}")
	failureRatio = flag.Float64("f", 0.0, "failure ratio")
	verbose = flag.Bool("v", true, "verbose output")
	flag.Parse()

	if *verbose {
		ayame.InitLogger(logging.DEBUG)
	} else {
		ayame.InitLogger(logging.INFO)
	}

	n := *numberOfNodes // number of simulations

	r := rand.New(rand.NewSource(uint64(*seed)))
	/*dist := distuv.Normal{
		Mu:    0.5, // Mean of the normal distribution
		Sigma: 0.1, // Standard deviation of the normal distribution
		Src:   r,
	}*/

	/*dist := distuv.Exponential{
		Rate: 0.5,
		Src:  r,
	}*/

	dist := distuv.Beta{
		Alpha: *paramAlpha,
		Beta:  *paramBeta,
		Src:   r,
	}

	// use the defined variable

	z := make([]float64, n)
	for i := 0; i < n; i++ {
		z[i] = dist.Rand()
		//z[i] = rand.Float64()
	}

	paramsString := fmt.Sprintf("%d %d %f %f %f a:%s i:%s ", *numberOfNodes, *k, *failureRatio, *paramAlpha, *paramBeta, *adversarialType, *issueType)

	auth := ki.NewKeyIssuer(*issueType, *seed, *poolSize)

	// store nodes in order of obtained keys
	ring := make(Ring, 0)

	for i := 0; i < n; i++ {
		var node *ki.Node
		switch *adversarialType {
		case "burst":
			if BURST_START < i && i < int(*failureRatio*(float64(n)))+BURST_START {
				key := BURST_START_KEY + float64(i)*BURST_INTERVAL // burst key attack
				netKey := auth.GetKey(ayame.FloatKey(key))
				node = ki.NewNodeWithFailure(key, float64(netKey.(ayame.FloatKey)), true)
			} else {
				netKey := auth.GetKey(ayame.FloatKey(z[i]))
				//node = NewNodeWithFailure(z[i], netKey, r.Float64() < *failureRatio)
				node = ki.NewNodeWithFailure(z[i], float64(netKey.(ayame.FloatKey)), false)
			}
		case "random":
			netKey := auth.GetKey(ayame.FloatKey(z[i]))
			node = ki.NewNodeWithFailure(z[i], float64(netKey.(ayame.FloatKey)), r.Float64() < *failureRatio)
		}
		if rand.Float64() < JOIN_RATIO {
			ring.Push(node)
		}
	}

	ring.Update()

	// PUT the transfer function on k neighbor nodes for logical key.
	// If all neighbors are faulty, the node becomes faulty.
	// Note that this is just a simplified check: just check K neighbors (K/2 left and K/2 right are all faulty)
	allFaultyCount := 0
	for _, node := range ring {
		closestNodes := GetKClosest(ring, node.(*ki.Node).LogicalKey(), *k)
		for _, n := range closestNodes {
			n.Delegate(node.(*ki.Node))
		}
		if !node.(*ki.Node).IsFailure() && allFailure(closestNodes) {
			allFaultyCount++
			ayame.Log.Infof("all failure for node=%s, %s\n", node, ayame.SliceString(closestNodes))
		}
	}

	ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-float64(allFaultyCount)/float64(ring.Len()))

	// Jain's fairness.
	jain_num := 0
	jain_denom := 0
	min := *numberOfNodes
	max := 0
	sum := 0
	for _, node := range ring {
		//fmt.Printf("%s, %s\n", node, ayame.SliceString(node.(*Node).delegated))
		x := len(node.(*ki.Node).Delegated())
		jain_num += x
		jain_denom += x * x
		if min > x {
			min = x
		}
		if max < x {
			max = x
		}
		sum += x
	}
	ayame.Log.Infof("fairness: %s %f\n", paramsString, float64(jain_num*jain_num)/float64(ring.Len()*jain_denom))
	ayame.Log.Infof("min: %s %d\n", paramsString, min)
	ayame.Log.Infof("max: %s %d\n", paramsString, max)
	ayame.Log.Infof("ave: %s %f\n", paramsString, float64(sum)/float64(ring.Len()))

}
