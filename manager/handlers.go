package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ecoarchie/orchestrator/task"
	"github.com/go-chi/chi"
	"github.com/google/uuid"
)

func (a *Api) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	te := task.TaskEvent{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Println(msg)
		w.WriteHeader(http.StatusBadRequest)
		e := ErrResponse{
			HttpStatusCode: http.StatusBadRequest,
			Msg:            msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Manager.AddTask(te)
	log.Printf("\n[Manager add task handler]: Added task with ID: %v\n", te.Task.ID)
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(te.Task)
}

func (a *Api) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

func (a *Api) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Println("No task ID passed in request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tID, err := uuid.Parse(taskID)
	if err != nil {
		msg := fmt.Sprintf("Task ID - %v is not valid uuid\n", taskID)
		log.Println(msg)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	taskToStop, err := a.Manager.TaskDb.Get(tID.String())
	if err != nil {
		msg := fmt.Sprintf("Task with ID %v not found", taskID)
		log.Println(msg)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.Completed,
		Timestamp: time.Now(),
	}

	taskCopy := taskToStop.(*task.Task)
	// taskCopy.State = task.Completed
	te.Task = *taskCopy
	a.Manager.AddTask(te)

	log.Printf("Added task event %v to stop task %v\n", te.ID, taskCopy.ID)
	w.WriteHeader(http.StatusNoContent)
}

func (a *Api) GetNodesHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(a.Manager.WorkerNodes)
}
