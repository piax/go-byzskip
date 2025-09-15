package ayame

type EventQueue []SchedEvent

func (eq EventQueue) Len() int { return len(eq) }

func (eq EventQueue) Less(i, j int) bool {
	//fmt.Println("Less?", i, j)
	return eq[i].Time() < eq[j].Time()
}

func (eq EventQueue) Swap(i, j int) {
	eq[i], eq[j] = eq[j], eq[i]
}

func (eq *EventQueue) Push(x interface{}) {
	log.Debugf("queued event to time: %d\n", x.(SchedEvent).Time())
	*eq = append(*eq, x.(SchedEvent))
}

func (eq *EventQueue) Pop() interface{} {
	old := *eq
	n := len(old)
	ev := old[n-1]
	//old[n-1] = nil // avoid memory leak
	*eq = old[0 : n-1]
	//*eq = old[1:n]
	log.Debugf("got event at time: %d\n", ev.Time())
	return ev
}
