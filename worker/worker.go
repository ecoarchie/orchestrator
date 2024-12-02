package worker

import (
	"fmt"

	"github.com/ecoarchie/orchestrator/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

/*
1. Run tasks as Docker containers
2. Accept tasks to run from a manager
3. Provide relevant statistics to the manager for the purpose of
scheduling tasks
4. Keep track of its tasks and their state
*/
type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
}

func (w *Worker) CollectStats() {
	fmt.Println("Collect stats")
}

func (w *Worker) RunTask() {
	fmt.Println("Run the task")
}

func (w *Worker) StartTask() {
	fmt.Println("Start the task")
}

func (w *Worker) StopTask() {
	fmt.Println("Stop the task")
}
