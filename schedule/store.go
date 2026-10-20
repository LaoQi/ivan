package schedule

import (
	"errors"
	"sort"
	"sync"
)

type Store interface {
	List() ([]Job, error)
	Get(id string) (Job, bool, error)
	Put(j Job) error
	Delete(id string) error
	Close() error
}

type MemStore struct {
	mu   sync.Mutex
	jobs map[string]Job
}

func NewMemStore() *MemStore {
	return &MemStore{jobs: map[string]Job{}}
}

func (m *MemStore) List() ([]Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Job, 0, len(m.jobs))
	for _, j := range m.jobs {
		out = append(out, j)
	}
	sort.Slice(out, func(i, k int) bool {
		if out[i].State.NextRunAtMS != out[k].State.NextRunAtMS {
			return out[i].State.NextRunAtMS < out[k].State.NextRunAtMS
		}
		return out[i].ID < out[k].ID
	})
	return out, nil
}

func (m *MemStore) Get(id string) (Job, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	return j, ok, nil
}

func (m *MemStore) Put(j Job) error {
	if j.ID == "" {
		return errors.New("作业缺少 id")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.jobs[j.ID] = j
	return nil
}

func (m *MemStore) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.jobs, id)
	return nil
}

func (m *MemStore) Close() error { return nil }
