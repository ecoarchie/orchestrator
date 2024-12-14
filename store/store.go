package store

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/boltdb/bolt"
	"github.com/ecoarchie/orchestrator/task"
)

type Store interface {
	Put(key string, value interface{}) error
	Get(key string) (interface{}, error)
	List() (interface{}, error)
	Count() (int, error)
}

type InMemoryTaskStore struct {
	Db map[string]*task.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{
		Db: make(map[string]*task.Task),
	}
}

func (ts *InMemoryTaskStore) Put(key string, value interface{}) error {
	t, ok := value.(*task.Task)
	if !ok {
		return fmt.Errorf("value %v is not a task.Task type", value)
	}
	ts.Db[key] = t
	return nil
}

func (ts *InMemoryTaskStore) Get(key string) (interface{}, error) {
	t, ok := ts.Db[key]
	if !ok {
		return nil, fmt.Errorf("task with key %s does not exist", key)
	}
	return t, nil
}

func (ts *InMemoryTaskStore) List() (interface{}, error) {
	var tasks []*task.Task
	for _, t := range ts.Db {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (ts *InMemoryTaskStore) Count() (int, error) {
	return len(ts.Db), nil
}

type InMemoryTaskEventStore struct {
	Db map[string]*task.TaskEvent
}

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		Db: make(map[string]*task.TaskEvent),
	}
}

func (i *InMemoryTaskEventStore) Put(key string, value interface{}) error {
	e, ok := value.(*task.TaskEvent)
	if !ok {
		return fmt.Errorf("value %v is not a task.TaskEvent type", value)
	}
	i.Db[key] = e
	return nil
}

func (i *InMemoryTaskEventStore) Get(key string) (interface{}, error) {
	e, ok := i.Db[key]
	if !ok {
		return nil, fmt.Errorf("task event with key %s does not exist", key)
	}

	return e, nil
}

func (i *InMemoryTaskEventStore) List() (interface{}, error) {
	var events []*task.TaskEvent
	for _, e := range i.Db {
		events = append(events, e)
	}
	return events, nil
}

func (i *InMemoryTaskEventStore) Count() (int, error) {
	return len(i.Db), nil
}

type TaskStore struct {
	Db         *bolt.DB
	DbFile     string
	FileMode   os.FileMode
	BucketName string
}

func NewTaskStore(file string, mode os.FileMode, bucketName string) (*TaskStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open %v", file)
	}
	t := TaskStore{
		Db:         db,
		DbFile:     file,
		FileMode:   mode,
		BucketName: bucketName,
	}
	err = t.CreateBucket()
	if err != nil {
		log.Println("bucket already exists, will use it instead of creating new")
	}

	return &t, nil
}

func (t *TaskStore) CreateBucket() error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(t.BucketName))
		if err != nil {
			return fmt.Errorf("error creating bucket %s: %s", t.BucketName, err)
		}
		return nil
	})
}

func (t *TaskStore) Close() {
	t.Db.Close()
}

func (t *TaskStore) Put(key string, value interface{}) error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.BucketName))

		buf, err := json.Marshal(value.(*task.Task))
		if err != nil {
			return err
		}

		err = b.Put([]byte(key), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (t *TaskStore) Get(key string) (interface{}, error) {
	var task task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.BucketName))
		t := b.Get([]byte(key))
		if t == nil {
			return fmt.Errorf("task %v not found", key)
		}
		err := json.Unmarshal(t, &task)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &task, nil
}

func (t *TaskStore) List() (interface{}, error) {
	var tasks []*task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.BucketName))
		b.ForEach(func(k, v []byte) error {
			var task task.Task
			err := json.Unmarshal(v, &task)
			if err != nil {
				return err
			}
			tasks = append(tasks, &task)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (t *TaskStore) Count() (int, error) {
	taskCount := 0
	incrCount := func(k, v []byte) error {
		taskCount++
		return nil
	}

	err := t.Db.View(func(tc *bolt.Tx) error {
		b := tc.Bucket([]byte("tasks"))
		b.ForEach(incrCount)
		return nil
	})
	if err != nil {
		return -1, err
	}
	return taskCount, nil
}

type EventStore struct {
	DBFile     string
	FileMode   os.FileMode
	Db         *bolt.DB
	BucketName string
}

func NewEventStore(file string, mode os.FileMode, bName string) (*EventStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open %v", file)
	}
	e := EventStore{
		DBFile:     file,
		FileMode:   mode,
		Db:         db,
		BucketName: bName,
	}
	err = e.CreateBucket()
	if err != nil {
		log.Println("bucket already exists, will use it")
	}
	return &e, nil
}

func (e *EventStore) Close() {
	e.Db.Close()
}

func (e *EventStore) CreateBucket() error {
	return e.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(e.BucketName))
		if err != nil {
			return fmt.Errorf("error creating bucket %s: %s", e.BucketName, err)
		}
		return nil
	})
}

func (e *EventStore) Put(key string, value interface{}) error {
	return e.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.BucketName))

		buf, err := json.Marshal(value.(*task.Task))
		if err != nil {
			return err
		}
		err = b.Put([]byte(key), buf)
		if err != nil {
			log.Printf("unable to save item %s", key)
			return err
		}
		return nil
	})
}

func (e *EventStore) Get(key string) (interface{}, error) {
	var event task.TaskEvent
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.BucketName))
		t := b.Get([]byte(key))
		if t == nil {
			return fmt.Errorf("event %v not found", key)
		}
		err := json.Unmarshal(t, &event)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &event, nil
}

func (e *EventStore) List() (interface{}, error) {
	var events []*task.TaskEvent
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.BucketName))
		b.ForEach(func(k, v []byte) error {
			var event task.TaskEvent
			err := json.Unmarshal(v, &event)
			if err != nil {
				return err
			}
			events = append(events, &event)
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (e *EventStore) Count() (int, error) {
	eventCount := 0
	err := e.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(e.BucketName))
		b.ForEach(func(k, v []byte) error {
			eventCount++
			return nil
		})
		return nil
	})
	if err != nil {
		return -1, err
	}
	return eventCount, nil
}
