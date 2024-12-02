package manager

import (
	"fmt"

	"github.com/ecoarchie/orchestrator/task"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
)

/*
1. Accept requests from users to start and stop tasks
2. Schedule tasks onto worker machines
3. Keep track of tasks, their states, and the machine on which they run
*/
type Manager struct {
	Pending       queue.Queue
	TaskDb        map[string][]*task.Task
	EventDb       map[string][]*task.TaskEvent
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
}

func (m *Manager) SelectWorker() {
	fmt.Println("Select worker")
}

func (m *Manager) UpdateTasks() {
	fmt.Println("Update tasks")
}

func (m *Manager) SendWork() {
	fmt.Println("Send work to workers")
}
