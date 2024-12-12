package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

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
	Stats     *Stats
	TaskCount int
}

func (w *Worker) CollectStats() {
	for {
		log.Println("[Worker]: Collecting stats")
		w.Stats = GetStats()
		w.Stats.TaskCount = w.TaskCount
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			res := w.runTask()
			if res.Error != nil {
				log.Printf("[Worker]: ERROR running task %v\n", res.Error)
			}
		} else {
			log.Println("[Worker]: No tasks to process for now")
		}

		log.Println("[Worker]: Sleeping for 10 seconds")
		time.Sleep(10 * time.Second)
	}
}

func (w *Worker) runTask() task.DockerResult {
	t := w.Queue.Dequeue()
	if t == nil {
		log.Println("[Worker]: No tasks in the queue")
		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)
	taskPersisted := w.Db[taskQueued.ID]
	if taskPersisted == nil {
		taskPersisted = &taskQueued
		w.Db[taskQueued.ID] = &taskQueued
	}

	var res task.DockerResult
	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.Scheduled:
			if taskQueued.ContainerID != "" {
				res = w.StopTask(taskQueued)
				if res.Error != nil {
					log.Printf("%v\n", res.Error)
				}
			}
			res = w.StartTask(taskQueued)
		case task.Completed:
			res = w.StopTask(taskQueued)
		default:
			fmt.Printf("This is a mistake. taskPersisted: %v, taskQueued: %v\n", taskPersisted, taskQueued)
			res.Error = errors.New("why did we get here?")
		}
	} else {
		err := fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
		res.Error = err
		return res
	}
	return res
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()
	c := task.NewConfig(&t)
	d := task.NewDocker(c)
	res := d.Run()
	if res.Error != nil {
		log.Printf("[Worker]: ERROR running container %v: %v\n", t.ContainerID, res.Error)
		t.State = task.Failed
		w.Db[t.ID] = &t
		return res
	}

	t.ContainerID = res.ContainerId
	t.State = task.Running
	w.Db[t.ID] = &t

	return res
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)

	res := d.Stop(t.ContainerID)
	if res.Error != nil {
		log.Printf("[Worker]: ERROR stopping container %v: %v\n", t.ContainerID, res.Error)
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.Completed
	w.Db[t.ID] = &t
	log.Printf("[Worker]: Stopped and removed container %v for task %v\n", t.ContainerID, t.ID)

	return res
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) GetTasks() []*task.Task {
	tasks := []*task.Task{}
	for _, t := range w.Db {
		tasks = append(tasks, t)
	}
	return tasks
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)
	d := task.NewDocker(config)
	return d.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("[Worker]: Checking status of tasks")
		w.updateTasks()
		log.Println("[Worker]: Task updates completed")
		log.Println("[Worker]: Sleeping for 15 seconds")
		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	for id, t := range w.Db {
		if t.State == task.Running {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("[Worker]: ERROR: %v\n", resp.Error)
			}

			if resp.Container == nil {
				log.Printf("[Worker]: No container for running task %s\n", id)
				w.Db[id].State = task.Failed
			}

			if resp.Container.State.Status == "exited" {
				log.Printf("[Worker]: Container for task %s in non running state %s", id, resp.Container.State.Status)
				w.Db[id].State = task.Failed
			}
			w.Db[id].HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
		}
	}
}
