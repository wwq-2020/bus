package bus

import (
	"sync"
	"sync/atomic"
)

var (
	seq int64
)

// Storage 存储接口
type Storage interface {
	Sink(string, *Event) (int64, error)
	Fetch() ([]*Event, error)
	Delete(int64) error
}

type memStorage struct {
	m map[int64]*Event
	sync.Mutex
}

func newMemStorage() Storage {
	return &memStorage{
		m: make(map[int64]*Event),
	}
}

func (ms *memStorage) Sink(topic string, event *Event) (int64, error) {
	id := nextID()
	ms.m[id] = event
	return id, nil
}

func (ms *memStorage) Delete(id int64) error {
	ms.Lock()
	defer ms.Unlock()
	delete(ms.m, id)
	return nil
}

func (ms *memStorage) Fetch() ([]*Event, error) {
	return nil, nil
}

func nextID() int64 {
	return atomic.AddInt64(&seq, 1)
}
