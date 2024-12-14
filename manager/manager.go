package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/ecoarchie/orchestrator/node"
	"github.com/ecoarchie/orchestrator/scheduler"
	"github.com/ecoarchie/orchestrator/store"
	"github.com/ecoarchie/orchestrator/task"
	"github.com/ecoarchie/orchestrator/worker"
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
	TaskDb        store.Store
	EventDb       store.Store
	Workers       []string
	WorkerTaskMap map[string][]uuid.UUID
	TaskWorkerMap map[uuid.UUID]string
	LastWorker    int
	WorkerNodes   []*node.Node
	Scheduler     scheduler.Scheduler
}

func New(workers []string, schedulerType string, dbType string) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)
	taskWorkerMap := make(map[uuid.UUID]string)

	var nodes []*node.Node
	for worker := range workers {
		workerTaskMap[workers[worker]] = []uuid.UUID{}

		nAPI := fmt.Sprintf("http://%v", workers[worker])
		n := node.NewNode(workers[worker], nAPI, "worker")
		nodes = append(nodes, n)
	}

	var s scheduler.Scheduler
	switch schedulerType {
	case "roundrobin":
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	case "epvm":
		s = &scheduler.Epvm{Name: "epvm"}
	default:
		s = &scheduler.RoundRobin{Name: "roundrobin"}
	}
	m := Manager{
		Pending:       *queue.New(),
		Workers:       workers,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		WorkerNodes:   nodes,
		Scheduler:     s,
	}

	var ts store.Store
	var es store.Store
	var err error
	switch dbType {
	case "memory":
		ts = store.NewInMemoryTaskStore()
		es = store.NewInMemoryTaskEventStore()
	case "persistent":
		ts, err = store.NewTaskStore("tasks.db", 0o600, "tasks")
		if err != nil {
			log.Fatalf("unable to create task store: %v", err)
		}
		es, err = store.NewEventStore("events.db", 0o600, "events")
		if err != nil {
			log.Fatalf("unable to create task event store: %v", err)
		}
	}

	m.TaskDb = ts
	m.EventDb = es
	return &m
}

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)
	if candidates == nil {
		msg := fmt.Sprintf("No available candidate match resource request for task %v", t.ID)
		err := errors.New(msg)
		return nil, err
	}

	scores := m.Scheduler.Score(t, candidates)
	if scores == nil {
		return nil, fmt.Errorf("no scores returned to task %v", t)
	}
	selectedNode := m.Scheduler.Pick(scores, candidates)
	return selectedNode, nil
}

func (m *Manager) SendWork() {
	if m.Pending.Len() > 0 {
		fmt.Printf("Pending queue len: %d\n", m.Pending.Len())
		e := m.Pending.Dequeue()
		te := e.(task.TaskEvent)
		err := m.EventDb.Put(te.ID.String(), &te)
		if err != nil {
			log.Printf("error attempting to store task event %s: %s\n", te.ID.String(), err)
			return
		}
		log.Printf("Pulled %v off pending queue\n", te)

		taskWorker, ok := m.TaskWorkerMap[te.Task.ID]
		if ok {
			res, err := m.TaskDb.Get(te.Task.ID.String())
			if err != nil {
				log.Printf("unable to schedule task: %s", err)
				return
			}

			persistedTask, ok := res.(*task.Task)
			if !ok {
				log.Println("unable to convert task to task.Task type")
				return
			}
			if te.State == task.Completed && task.ValidStateTransition(persistedTask.State, te.State) {
				m.stopTask(taskWorker, te.Task.ID.String())
				return
			}

			log.Printf("[Manager]: invalid request: exsisting task %s is in state %v and cannot transition to the completed state\n", persistedTask.ID.String(), persistedTask.State)
			return
		}

		t := te.Task
		w, err := m.SelectWorker(t)
		if err != nil {
			log.Printf("[Manager]: error selecting worker for task %s: %v\n", t.ID, err)
			return
		}
		log.Printf("[Manager] selected worker %s for task %s", w.Name, t.ID)

		m.WorkerTaskMap[w.Name] = append(m.WorkerTaskMap[w.Name], te.Task.ID)
		m.TaskWorkerMap[t.ID] = w.Name

		t.State = task.Scheduled
		m.TaskDb.Put(t.ID.String(), &t)

		data, err := json.Marshal(te)
		if err != nil {
			log.Printf("Unable to marshal task object: %v.\n", t)
		}

		url := fmt.Sprintf("http://%s/tasks", w.Name)
		fmt.Printf("[Manager]: Assign task to worker url: %s\n", url)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
		if err != nil {
			log.Printf("[Manager]: Error connecting to %v: %v\n", w, err)
			m.Pending.Enqueue(t)
			return
		}
		d := json.NewDecoder(resp.Body)
		if resp.StatusCode != http.StatusCreated {
			e := worker.ErrResponse{}
			err := d.Decode(&e)
			if err != nil {
				fmt.Printf("Error decoding response: %s\n", err.Error())
				return
			}
			fmt.Printf("Response error (%d): %s", e.HttpStatusCode, e.Msg)
			return
		}
		t = task.Task{}
		err = d.Decode(&t)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		w.TaskCount++
		log.Printf("[manager] received response from worker: %#v\n", t)
	} else {
		log.Println("[Manager]: No work in the queue")
	}
}

func (m *Manager) stopTask(worker, taskID string) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("[Manager]: error creating request to delete task %s: %v\n", taskID, err)
		return
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[Manager]: error connecting to worker at %s: %v\n", url, err)
		return
	}

	if resp.StatusCode != http.StatusNoContent {
		log.Printf("[Manager]: error sending request: %v\n", err)
		return
	}
	log.Printf("[Manager]: task %s has been scheduled to be stopped", taskID)
}

func (m *Manager) UpdateTasks() {
	for {
		log.Println("[Manager]: Checking for task updates from workers")
		m.updateTasks()
		log.Println("[Manager]: Task updates completed")
		log.Println("[Manager]: Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		log.Println("[Manager]: Processing any task in the queue")
		m.SendWork()
		log.Println("[Manager]: Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) updateTasks() {
	for _, worker := range m.Workers {
		log.Printf("[Manager]: Checking worker %v for task updates", worker)
		url := fmt.Sprintf("http://%s/tasks", worker)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("[Manager]: Error connecting to %v: %v\n", worker, err)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("[Manager]: Error sending request: %v\n", err)
			continue
		}

		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			log.Printf("[Manager]: Error unmarshaling tasks: %s\n", err.Error())
		}
		for _, t := range tasks {
			log.Printf("[Manager]: Attempting to update task %v\n", t.ID)
			res, err := m.TaskDb.Get(t.ID.String())
			if err != nil {
				log.Printf("[Manager]: Task with ID %s not found\n", t.ID)
				continue
			}
			taskPersisted, ok := res.(*task.Task)
			if !ok {
				log.Printf("cannot convert result %v to task.Task type\n", res)
				continue
			}

			if taskPersisted.State != t.State {
				taskPersisted.State = t.State
			}

			taskPersisted.StartTime = t.StartTime
			taskPersisted.FinishTime = t.FinishTime
			taskPersisted.ContainerID = t.ContainerID
			taskPersisted.HostPorts = t.HostPorts

			m.TaskDb.Put(taskPersisted.ID.String(), taskPersisted)
		}
	}
}

func (m *Manager) AddTask(te task.TaskEvent) {
	m.Pending.Enqueue(te)
}

func (m *Manager) GetTasks() []*task.Task {
	taskList, err := m.TaskDb.List()
	if err != nil {
		log.Printf("error getting list of tasks: %v\n", err)
		return nil
	}
	return taskList.([]*task.Task)
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	log.Printf("[Manager]: Calling healthvheck for task %s: %s\n", t.ID, t.HealthCheck)

	w := m.TaskWorkerMap[t.ID]
	hostPort := getHostport(t.HostPorts)
	worker := strings.Split(w, ":")
	if hostPort == nil {
		log.Printf("Have not collected task %s host port yet. Skipping.\n", t.ID)
		return nil
	}
	url := fmt.Sprintf("http://%s:%s%s", worker[0], *hostPort, t.HealthCheck)
	log.Printf("[Manager]: Calling healthcheck for task %s: %s\n", t.ID, url)
	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Sprintf("[Manager]: Error connecting to health check %s", url)
		log.Println(msg)
		return errors.New(msg)
	}

	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("[Manager]: Error health check for task %s didn't return 200\n", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	log.Printf("[Manager]: Task %s health check response: %v\n", t.ID, resp.StatusCode)
	return nil
}

func getHostport(ports nat.PortMap) *string {
	for k := range ports {
		x := &ports[k][0].HostPort

		return x
	}
	return nil
}

func (m *Manager) doHealthChecks() {
	tasks := m.GetTasks()
	for _, t := range tasks {
		if t.State == task.Running && t.RestartCount < 3 {
			err := m.checkTaskHealth(*t)
			if err != nil {
				if t.RestartCount < 3 {
					m.restartTask(t)
				}
			}
		} else if t.State == task.Failed && t.RestartCount < 3 {
			m.restartTask(t)
		}
	}
}

func (m *Manager) restartTask(t *task.Task) {
	w := m.TaskWorkerMap[t.ID]
	t.State = task.Scheduled
	t.RestartCount++
	m.TaskDb.Put(t.ID.String(), t)

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Running,
		Timestamp: time.Now(),
		Task:      *t,
	}
	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("[Manager]: Unable to marshal task object: %v.", t)
		return
	}

	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("[Manager]: Error connecting to %v: %v", w, err)
		m.Pending.Enqueue(t)
		return
	}

	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		log.Printf("[Manager]: Response error (%d): %s", e.HttpStatusCode, e.Msg)
		return
	}

	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		fmt.Printf("Error decoding response: %s\n", err.Error())
		return
	}
	log.Printf("[Manager]: Restarting task %#v\n", t)
}

func (m *Manager) DoHealthChecks() {
	for {
		log.Println("\n[Manager]: Performing task health check")
		m.doHealthChecks()
		log.Println("[Manager]: Task health checks completed")
		log.Println("[Manager]: Sleeping for 60 seconds")
		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) UpdateNodeStats() {
	for {
		for _, node := range m.WorkerNodes {
			log.Printf("Collecting stats for node %v", node.Name)
			_, err := node.GetStats()
			if err != nil {
				log.Printf("error updating node stats: %v", err)
			}
		}
		time.Sleep(15 * time.Second)
	}
}
