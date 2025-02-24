package sim

import (
	"math/rand"
	"strconv"

	"github.com/montanaflynn/stats"
	"github.com/piax/go-byzskip/ayame"
	bs "github.com/piax/go-byzskip/byzskip"
	"github.com/thoas/go-funk"
)

const (
	EACH_UNICAST_TRIALS = 100
	EACH_UNICAST_TIMES  = 500
)

var SeqNo int = 0

func NextId() string {
	SeqNo++
	return strconv.Itoa(SeqNo)
}

func expUnicastRecursive(trials int) {
	ayame.GlobalEventExecutor.Reset()
	msgs := []*bs.BSUnicastEvent{}
	for i := 1; i <= trials; i++ {
		src := NormalList[rand.Intn(len(NormalList))]
		dst := NormalList[rand.Intn(len(NormalList))]
		mid := src.String() + "." + NextId()
		msg := bs.NewBSUnicastEventNoOriginator(src, mid, ayame.MembershipVectorSize, dst.Key(), []byte("hello")) // starts with the max level.
		msgs = append(msgs, msg)
		ayame.Log.Debugf("%d: nodes=%d, id=%s,src=%s, dst=%s\n", int64(i*1000), len(NormalList), msg.MessageId, src, dst)
		ayame.GlobalEventExecutor.RegisterEvent(msg, int64(i*1000))
		//nodes[src].SendEvent(msg)
		// time out after 200ms
		ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {
			ayame.Log.Debugf("id=%s,src=%s, dst=%s timed out\n", msg.MessageId, src, dst)
			msg.Root.Channel <- true
		}), int64(i*1000)+100)
	}
	recursiveUnicastExperiment(msgs, trials)
}

func calcMaxPathAve(msgs []*bs.BSUnicastEvent) float64 {
	curSrc := msgs[0].Sender()
	lengths := []float64{}
	sumMax := float64(0)
	count := 0
	for _, m := range msgs {
		if m.Sender().(*bs.BSNode).Equals(curSrc.(*bs.BSNode)) {
			mlen, _ := maxPathLength(m.Root.Paths)
			//fmt.Printf("%d->%d,%s,%f\n", m.Author().Key(), m.TargetKey, bs.PathsString(m.Root.Paths), mlen)
			lengths = append(lengths, mlen)
		} else { // cur src differs
			max := float64(0)
			for i, len := range lengths { // max len
				flen := float64(len)
				if i == 0 || flen > max {
					max = flen
				}
			}
			sumMax += max
			count++
			curSrc = m.Sender().(*bs.BSNode)
			lengths = []float64{}
		}
	}
	//the last one
	max := float64(0)
	for i, len := range lengths { // max len
		flen := float64(len)
		if i == 0 || flen > max {
			max = flen
		}
	}
	sumMax += max
	count++

	return sumMax / float64(count)
}

func calcMaxMsgsAve(msgs []*bs.BSUnicastEvent) float64 {
	curSrc := msgs[0].Sender()
	lengths := []float64{}
	sumMax := float64(0)
	count := 0
	for _, m := range msgs {
		if m.Sender().(*bs.BSNode).Equals(curSrc.(*bs.BSNode)) {
			mlen, _ := msgsByPath(m.Root.Paths)
			//fmt.Printf("%d->%d,%s,%f\n", m.Author().Key(), m.TargetKey, bs.PathsString(m.Root.Paths), mlen)
			lengths = append(lengths, mlen)
		} else { // cur src differs
			max := float64(0)
			for i, len := range lengths { // max len
				flen := float64(len)
				if i == 0 || flen > max {
					max = flen
				}
			}
			sumMax += max
			count++
			curSrc = m.Sender().(*bs.BSNode)
			lengths = []float64{}
		}
	}
	//the last one
	max := float64(0)
	for i, len := range lengths { // max len
		flen := float64(len)
		if i == 0 || flen > max {
			max = flen
		}
	}
	sumMax += max
	count++

	return sumMax / float64(count)
}

func expMVIterative(trials int) {
	path_lengths := []float64{}
	nums_msgs := []int{}
	match_lengths := []float64{}
	failures := 0

	///trials = 1
	for i := 1; i <= trials; i++ {
		src := NormalList[rand.Intn(len(NormalList))]
		dst := NormalList[rand.Intn(len(NormalList))]
		//founds, hops, msgs, hops_to_match, failure := FastNodeLookup(nodes[dst].routingTable.dhtId, nodes[src], *alpha)
		founds, hops, msgs, hops_to_match, failure := FastLookupMV(dst.MV(), src)
		//founds2, hops2, msgs2, hops_to_match2, failure2 := FastLookup(src.Key(), dst)
		ayame.Log.Debugf("%s->%s MV %s %d %d %d %v\n", src, dst, founds, hops, msgs, hops_to_match, failure)
		//ayame.Log.Infof("%s->%s Key: %s %d %d %d %v\n", dst, src, founds2, hops2, msgs2, hops_to_match2, failure2)
		path_lengths = append(path_lengths, float64(hops))
		nums_msgs = append(nums_msgs, msgs)
		if !failure {
			match_lengths = append(match_lengths, float64(hops_to_match))
		}
		if failure {
			failures++
			ayame.Log.Debugf("%s->%s: FAILURE!!! %s\n", src, dst, ayame.SliceString(founds))
		}
		ayame.Log.Debugf("%s->%s: avg. results: %d, hops: %d, msgs: %d, hops_to_match: %d, fails: %d\n", src, dst, len(founds), hops, msgs, hops_to_match, failures)
	}
	pmean, _ := stats.Mean(path_lengths)
	ayame.Log.Infof("avg-paths-length: %s %f\n", paramsString, pmean)
	hmean, _ := stats.Mean(match_lengths)
	ayame.Log.Infof("avg-match-hops: %s %f\n", paramsString, hmean)
	ayame.Log.Infof("avg-msgs: %s %f\n", paramsString, meanOfInt(nums_msgs))
	ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-float64(failures)/float64(trials))
}

func expIterative(trials int) {
	path_lengths := []float64{}
	nums_msgs := []int{}
	match_lengths := []float64{}
	failures := 0

	for i := 1; i <= trials; i++ {
		src := NormalList[rand.Intn(len(NormalList))]
		dst := NormalList[rand.Intn(len(NormalList))]
		//founds, hops, msgs, hops_to_match, failure := FastNodeLookup(nodes[dst].routingTable.dhtId, nodes[src], *alpha)
		founds, hops, msgs, hops_to_match, failure := FastLookup(dst.Key(), src)
		path_lengths = append(path_lengths, float64(hops))
		nums_msgs = append(nums_msgs, msgs)
		if !failure {
			match_lengths = append(match_lengths, float64(hops_to_match))
		}
		if failure {
			failures++
			ayame.Log.Debugf("%s->%s: FAILURE!!! %s\n", src, dst, ayame.SliceString(founds))
		}
		ayame.Log.Debugf("%s->%s: avg. results: %d, hops: %d, msgs: %d, hops_to_match: %d, fails: %d\n", src, dst, len(founds), hops, msgs, hops_to_match, failures)
	}
	pmean, _ := stats.Mean(path_lengths)
	ayame.Log.Infof("avg-paths-length: %s %f\n", paramsString, pmean)
	hmean, _ := stats.Mean(match_lengths)
	ayame.Log.Infof("avg-match-hops: %s %f\n", paramsString, hmean)
	ayame.Log.Infof("avg-msgs: %s %f\n", paramsString, meanOfInt(nums_msgs))
	ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-float64(failures)/float64(trials))
}

func expEachIterative() {
	match_lengths := []float64{}
	failures := 0
	count := 0
	sum_max_hops := 0
	sum_max_msgs := 0
	nums_msgs := []int{}
	for i := 1; i <= EACH_UNICAST_TRIALS; i++ {
		src := NormalList[rand.Intn(len(NormalList))]
		max_hops := 0
		max_msgs := 0
		for j := 1; j <= EACH_UNICAST_TIMES; j++ {
			count++
			dst := NormalList[rand.Intn(len(NormalList))]
			founds, hops, msgs, hops_to_match, failure := FastLookup(dst.Key(), src)
			if max_hops < hops {
				max_hops = hops
			}
			if max_msgs < msgs {
				max_msgs = msgs
			}
			nums_msgs = append(nums_msgs, msgs)
			if !failure {
				match_lengths = append(match_lengths, float64(hops_to_match))
			} else {
				failures++
				ayame.Log.Debugf("%s->%s: FAILURE!!! %s\n", src, dst, ayame.SliceString(founds))
			}
			ayame.Log.Debugf("%d: nodes=%d, src=%s, dst=%s\n", int64(count*1000), len(NormalList), src, dst)
		}
		sum_max_hops += max_hops
		sum_max_msgs += max_msgs
	}
	ayame.Log.Infof("avg-max-hops: %s %f\n", paramsString, float64(sum_max_hops)/float64(EACH_UNICAST_TRIALS))
	ayame.Log.Infof("avg-max-msgs: %s %f\n", paramsString, float64(sum_max_msgs)/float64(EACH_UNICAST_TRIALS))
	hmean, _ := stats.Mean(match_lengths)
	ayame.Log.Infof("avg-match-hops: %s %f\n", paramsString, hmean)
	ayame.Log.Infof("avg-msgs: %s %f\n", paramsString, meanOfInt(nums_msgs))
	ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-float64(failures)/float64(EACH_UNICAST_TRIALS*EACH_UNICAST_TIMES))
}

func expUnicastEachRecursive() {
	ayame.GlobalEventExecutor.Reset()

	msgs := []*bs.BSUnicastEvent{}
	count := 0
	for i := 1; i <= EACH_UNICAST_TRIALS; i++ {
		src := NormalList[rand.Intn(len(NormalList))]
		for j := 1; j <= EACH_UNICAST_TIMES; j++ {
			count++
			dst := NormalList[rand.Intn(len(NormalList))]
			mid := src.String() + "." + NextId()
			msg := bs.NewBSUnicastEventNoOriginator(src, mid, ayame.MembershipVectorSize, dst.Key(), []byte("hello")) // starts with the max level.
			msgs = append(msgs, msg)
			ayame.Log.Debugf("%d: nodes=%d, id=%s,src=%s, dst=%s\n", int64(count*1000), len(NormalList), msg.MessageId, src, dst)
			ayame.GlobalEventExecutor.RegisterEvent(msg, int64(count*1000))
			ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {
				ayame.Log.Debugf("id=%s,src=%s, dst=%s timed out\n", msg.MessageId, src, dst)
				msg.Root.Channel <- true
			}), int64(count*1000)+100)
		}
	}
	recursiveUnicastExperiment(msgs, count)
	ayame.Log.Infof("avg-max-hops: %s %f\n", paramsString, calcMaxPathAve(msgs))
	ayame.Log.Infof("avg-max-msgs: %s %f\n", paramsString, calcMaxMsgsAve(msgs))
}

func recursiveUnicastExperiment(msgs []*bs.BSUnicastEvent, trials int) {
	success := 0
	for _, msg := range msgs {
		go func(msg *bs.BSUnicastEvent) {
			<-msg.Root.Channel // wait for the timeout.
			//if *verbose {
			//avg, _ := meanOfPathLength(msg.root.paths)
			ayame.Log.Debugf("%d: started %s, finished: %d\n", msg.TargetKey, msg.MessageId, msg.Time())
			//}
			if bs.ContainsKey(msg.TargetKey, msg.Root.Destinations) {
				ayame.Log.Debugf("%s is included in %s\n", msg.TargetKey, msg.Root.Destinations)
				success++
			} else {
				ayame.Log.Debugf("%s->%s: FAILURE!!! %s\n", msg.Sender(), msg.TargetKey, ayame.SliceString(msg.Root.Destinations))
			}
			close(msg.Root.Channel)
		}(msg)
	}
	//	fmt.Printf("sim until= %d\n", int64(trials*1000*2))
	ayame.GlobalEventExecutor.Sim(int64(trials*1000*2), true)
	ayame.GlobalEventExecutor.AwaitFinish()

	ave, _ := stats.Mean(funk.Map(msgs, func(msg *bs.BSUnicastEvent) float64 {
		for _, path := range msg.DestinationPaths {
			ayame.Log.Debugf("%s->%s: path %s\n", msg.Root.Sender(), msg.TargetKey, ayame.SliceString(path))
		}
		min, _ := minHops(msg.DestinationPaths, msg.TargetKey)
		ayame.Log.Debugf("%s->%s: min. path length: %f\n", msg.Root.Sender(), msg.TargetKey, min)
		return min
	}).([]float64))
	// len(msgs) should be ignored because unicasts are initiated autonomously
	counts := ayame.GlobalEventExecutor.EventCount - len(msgs)

	ayame.Log.Infof("avg-match-hops: %s %f\n", paramsString, ave)
	ayame.Log.Infof("avg-msgs: %s %f\n", paramsString, float64(counts)/float64(trials))
	if FailureType == F_CALC && *failureRatio != 0.0 {
		// XXX
		probSum := 0.0
		count := 1000
		for _, msg := range msgs {
			prob := ComputeProbabilityMonteCarlo(msg, *failureRatio, count)
			ayame.Log.Debugf("%s->%d %f\n", msg.Sender(), msg.TargetKey, prob)
			probSum += prob
		}
		ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-probSum/float64(len(msgs)))
	} else {
		ayame.Log.Infof("success-ratio: %s %f\n", paramsString, float64(success)/float64(trials))
	}
}
