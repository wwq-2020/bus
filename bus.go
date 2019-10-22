package bus

import (
	"context"
	"sync"
)

// OpType 操作类型
type OpType int

const (
	// DoOp 做
	DoOp OpType = iota + 1
	// DeleteOp 删除
	DeleteOp
)

// Handler 事件处理方法
type Handler func(context.Context, interface{}) error

// Event 事件
type Event struct {
	ID       int64
	Topic    string
	Data     interface{}
	ErrCount int64
}

// EventTask 事件任务
type EventTask struct {
	Event  *Event
	Task   Task
	OpType OpType
}

// Option bus选项
type Option func(*option)

// Bus 总线接口
type Bus interface {
	Pub(string) error
	PubWithData(string, interface{}) error
	Sub(string, Handler)
	Shutdown()
}

type option struct {
	executor  Executor
	storage   Storage
	scheduler Scheduler
}

type bus struct {
	topic2Handler map[string]Handler
	option        *option
	sync.RWMutex
}

// WithStorage 添加消息存储
func WithStorage(storage Storage) Option {
	return func(option *option) {
		option.storage = storage
	}
}

// WithExecutor 添加执行器
func WithExecutor(executor Executor) Option {
	return func(option *option) {
		option.executor = executor
	}
}

// WithScheduler 添加调度器
func WithScheduler(scheduler Scheduler) Option {
	return func(option *option) {
		option.scheduler = scheduler
	}
}

func genOpts(opts ...Option) *option {
	option := &option{}
	for _, opt := range opts {
		opt(option)
	}
	if option.executor == nil {
		option.executor = NewPooledExecutor(defaultPoolSize)
	}
	if option.storage == nil {
		option.storage = newMemStorage()
	}
	if option.scheduler == nil {
		option.scheduler = newScheduler()
	}
	return option
}

func defaultBus() *bus {
	return &bus{
		topic2Handler: make(map[string]Handler),
	}
}

// New 初始化总线实例
func New(opts ...Option) Bus {
	b := defaultBus()
	b.option = genOpts(opts...)
	go b.schedule()
	return b

}

func (b *bus) Pub(topic string) error {
	if err := b.PubWithData(topic, nil); err != nil {
		return err
	}
	return nil
}

func (b *bus) PubWithData(topic string, data interface{}) error {
	event := &Event{Topic: topic, Data: data}
	id, err := b.option.storage.Sink(topic, event)
	if err != nil {
		return err
	}
	event.ID = id
	eventTask := b.genEventTask(event)
	b.option.scheduler.AddEventTask(eventTask)
	return nil
}

func (b *bus) Sub(topic string, handler Handler) {
	b.Lock()
	defer b.Unlock()
	b.topic2Handler[topic] = handler
}

func (b *bus) Shutdown() {
	b.option.scheduler.Close()
	b.option.executor.Shutdown()
}
func (b *bus) genEventTask(event *Event) *EventTask {
	b.RLock()
	defer b.RUnlock()

	handler, exist := b.topic2Handler[event.Topic]
	if !exist {
		handler = func(ctx context.Context, data interface{}) error {
			return nil
		}
	}
	task := func(ctx context.Context) {
		if err := handler(ctx, event.Data); err == nil {
			event.ErrCount = 0
			task := b.genDeleteEventTask(event)
			b.option.scheduler.AddEventTask(task)
			return
		}
		event.ErrCount++
		task := b.genEventTask(event)
		b.option.scheduler.AddEventTask(task)
	}
	return &EventTask{
		Event:  event,
		Task:   task,
		OpType: DoOp,
	}
}

func (b *bus) genDeleteEventTask(event *Event) *EventTask {
	task := func(ctx context.Context) {
		if err := b.option.storage.Delete(event.ID); err != nil {
			event.ErrCount++
			task := b.genDeleteEventTask(event)
			b.option.scheduler.AddEventTask(task)
		}
	}
	return &EventTask{
		Event:  event,
		Task:   task,
		OpType: DeleteOp,
	}

}

func (b *bus) schedule() {
	b.option.scheduler.Schedule(b.option.executor.Execute)
}
