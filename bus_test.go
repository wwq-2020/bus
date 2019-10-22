package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/wwq1988/bus"
)

type fakeExecutor struct {
	doneCh chan struct{}
}

func (fe fakeExecutor) Execute(task bus.Task) {
	task(context.Background())
}

func (fe fakeExecutor) Shutdown() {
	close(fe.doneCh)
}

func TestBus(t *testing.T) {
	fe := fakeExecutor{doneCh: make(chan struct{})}
	bus := bus.New(bus.WithExecutor(fe))
	expected := "world"
	topic := "hello"
	resultCh := make(chan interface{}, 1)
	bus.Sub(topic, func(ctx context.Context, data interface{}) error {
		resultCh <- data
		return nil
	})
	bus.PubWithData(topic, expected)
	select {
	case <-time.After(time.Second * 1):
		t.Fatal("got no data")
	case got := <-resultCh:
		if got != expected {
			t.Fatalf("expected:%v, got:%+v", expected, got)
		}
	}
	bus.Shutdown()
	select {
	case <-fe.doneCh:
	default:
		t.Fatal("expected shutdown")
	}
	Check(t)

}
