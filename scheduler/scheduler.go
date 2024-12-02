package scheduler

/*
1. Determine a set of candidate workers on which a task could run
2. Score the candidate workers from best to worst
3. Pick the worker with the best score
*/
type Scheduler interface {
	SelectCandidateNodes()
	Score()
	Pick()
}
