package bus

import (
	"github.com/wwq1988/queue"
)

// Scheduler 调度器
type Scheduler interface {
	AddEventTask(*EventTask)
	Schedule(TaskHandler)
	Close()
}

type scheduler struct {
	queue *queue.Queue
}

func newScheduler() Scheduler {
	return &scheduler{
		queue: queue.New(),
	}
}

func (s *scheduler) AddEventTask(eventTask *EventTask) {
	s.queue.Push(eventTask.Task)
}

func (s *scheduler) Schedule(taskHandler TaskHandler) {
	taskHandlerWrapped := s.taskHandlerWrapper(taskHandler)
	s.queue.BIter(taskHandlerWrapped)
}

func (s *scheduler) taskHandlerWrapper(taskHandler TaskHandler) func(interface{}) {
	return func(task interface{}) {
		taskHandler(task.(Task))
	}
}

func (s *scheduler) Close() {
	s.queue.Close()
}
