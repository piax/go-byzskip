package main

import (
	"math/rand"

	"github.com/montanaflynn/stats"
	"github.com/piax/go-ayame/ayame"
	"github.com/thoas/go-funk"
)

const (
	EACH_UNICAST_TRIALS = 10
	EACH_UNICAST_TIMES  = 100
)

func expUnicastRecursive(trials int) {
	msgs := []*BSUnicastEvent{}
	for i := 1; i <= trials; i++ {
		src := NormalList[rand.Intn(len(NormalList))]
		dst := NormalList[rand.Intn(len(NormalList))]
		msg := NewBSUnicastEvent(src, ayame.MembershipVectorSize, dst.key) // starts with the max level.
		msgs = append(msgs, msg)
		ayame.Log.Debugf("nodes=%d, id=%d,src=%s, dst=%s\n", len(NormalList), msg.messageId, src, dst)
		ayame.GlobalEventExecutor.RegisterEvent(msg, int64(i*1000))
		//nodes[src].SendEvent(msg)
		// time out after 200ms
		ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {
			ayame.Log.Debugf("id=%d,src=%s, dst=%s timed out\n", msg.messageId, src, dst)
			msg.root.channel <- true
		}), int64(i*1000)+100)
	}
	recursiveUnicastExperiment(msgs, trials)
}

func calcMaxPathAve(msgs []*BSUnicastEvent) float64 {
	curSrc := msgs[0].Sender()
	lengths := []float64{}
	sumMax := float64(0)
	count := 0
	for _, m := range msgs {
		if m.Sender().(*BSNode).Equals(curSrc.(*BSNode)) {
			mlen, _ := maxPathLength(m.root.paths)
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
			curSrc = m.Sender().(*BSNode)
		}
	}
	return sumMax / float64(count)
}

func expUnicastEachRecursive() {
	msgs := []*BSUnicastEvent{}
	count := 0
	for i := 1; i <= EACH_UNICAST_TRIALS; i++ {
		src := NormalList[rand.Intn(len(NormalList))]
		for j := 1; j <= EACH_UNICAST_TIMES; j++ {
			count++
			dst := NormalList[rand.Intn(len(NormalList))]
			msg := NewBSUnicastEvent(src, ayame.MembershipVectorSize, dst.key) // starts with the max level.
			msgs = append(msgs, msg)
			ayame.Log.Debugf("nodes=%d, id=%d,src=%s, dst=%s\n", len(NormalList), msg.messageId, src, dst)
			ayame.GlobalEventExecutor.RegisterEvent(msg, int64(count*1000))
			ayame.GlobalEventExecutor.RegisterEvent(ayame.NewSchedEventWithJob(func() {
				ayame.Log.Debugf("id=%d,src=%s, dst=%s timed out\n", msg.messageId, src, dst)
				msg.root.channel <- true
			}), int64(count*1000)+100)
		}
	}
	recursiveUnicastExperiment(msgs, count)
	ayame.Log.Infof("avg-max-hops: %s %f\n", paramsString, calcMaxPathAve(msgs))
}

func recursiveUnicastExperiment(msgs []*BSUnicastEvent, trials int) {
	success := 0
	for _, msg := range msgs {
		go func(msg *BSUnicastEvent) {
			<-msg.root.channel // wait for the timeout.
			//if *verbose {
			//avg, _ := meanOfPathLength(msg.root.paths)
			ayame.Log.Debugf("%d: started %d, finished: %d\n", msg.targetKey, msg.messageId, msg.Time())
			//}
			if ContainsKey(msg.targetKey, msg.root.destinations) {
				ayame.Log.Debugf("%s is included in %s\n", msg.targetKey, msg.root.destinations)
				success++
			} else {
				ayame.Log.Infof("%s->%s: FAILURE!!! %s\n", msg.Sender().Id(), msg.targetKey, ayame.SliceString(msg.root.destinations))
			}
			close(msg.root.channel)
		}(msg)
	}
	ayame.GlobalEventExecutor.Reset()
	ayame.GlobalEventExecutor.Sim(int64(trials*1000*2), true)
	ayame.GlobalEventExecutor.AwaitFinish()

	ave, _ := stats.Mean(funk.Map(msgs, func(msg *BSUnicastEvent) float64 {
		min, _ := minHops(msg.destinationPaths, msg.targetKey)
		ayame.Log.Debugf("%s->%s: min. path length: %f\n", msg.root.Sender(), msg.targetKey, min)
		return min
	}).([]float64))
	counts := ayame.GlobalEventExecutor.EventCount

	ayame.Log.Infof("avg-match-hops: %s %f\n", paramsString, ave)
	ayame.Log.Infof("avg-msgs: %s %f\n", paramsString, float64(counts)/float64(trials))
	if FailureType == F_CALC {
		// XXX
		probSum := 0.0
		count := 1000
		for _, msg := range msgs {
			prob := ComputeProbabilityMonteCarlo(msg, *failureRatio, count)
			ayame.Log.Debugf("%s->%d %f\n", msg.Sender().Id(), msg.targetKey, prob)
			probSum += prob
		}
		ayame.Log.Infof("success-ratio: %s %f\n", paramsString, 1-probSum/float64(len(msgs)))
	} else {
		ayame.Log.Infof("success-ratio: %s %f\n", paramsString, float64(success)/float64(trials))
	}
}
