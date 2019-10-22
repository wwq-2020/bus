package bus

import (
	"context"
	"testing"
	"time"
)

func testTaskHandler(task Task) {
	task(context.Background())
}

func TestScheduler(t *testing.T) {
	s := newScheduler()
	var result int
	eventTask := &EventTask{Task: func(context.Context) {
		result++
	}}
	go s.Schedule(testTaskHandler)
	s.AddEventTask(eventTask)
	time.Sleep(time.Millisecond * 100)
	s.Close()
	if result != 1 {
		t.Fatalf("expected:%d,got:%d", 1, result)
	}
}
