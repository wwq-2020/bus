package bus_test

import (
	"context"
	"fmt"

	"github.com/wwq1988/bus"
)

func ExampleBus() {
	bus := bus.New()
	topic := "hello"
	bus.Sub(topic, func(ctx context.Context, data interface{}) error {
		fmt.Println(data)
		return nil
	})
	bus.Pub(topic)
}
