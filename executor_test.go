package bus_test

import (
	"context"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/wwq1988/bus"
)

var goroutinesToIgnore = []string{
	"testing.Main(",
	"testing.tRunner(",
	"testing.(*M).",
	"runtime.goexit",
	"created by runtime.gc",
	"created by runtime/trace.Start",
	"interestingGoroutines",
	"runtime.MHeap_Scavenger",
	"signal.signal_recv",
	"sigterm.handler",
	"runtime_mcall",
	"(*loggingT).flushDaemon",
	"goroutine in C code",
}

func RegisterIgnoreGoroutine(s string) {
	goroutinesToIgnore = append(goroutinesToIgnore, s)
}

func ignore(g string) bool {
	sl := strings.SplitN(g, "\n", 2)
	if len(sl) != 2 {
		return true
	}
	stack := strings.TrimSpace(sl[1])
	if strings.HasPrefix(stack, "testing.RunTests") {
		return true
	}

	if stack == "" {
		return true
	}

	for _, s := range goroutinesToIgnore {
		if strings.Contains(stack, s) {
			return true
		}
	}

	return false
}

func interestingGoroutines() (gs []string) {
	buf := make([]byte, 2<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		if !ignore(g) {
			gs = append(gs, g)
		}
	}
	sort.Strings(gs)
	return
}

type Errorfer interface {
	Errorf(format string, args ...interface{})
}

func check(efer Errorfer, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	var leaked []string
	for time.Now().Before(deadline) {
		if leaked = interestingGoroutines(); len(leaked) == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, g := range leaked {
		efer.Errorf("Leaked goroutine: %v", g)
	}
}

func Check(efer Errorfer) {
	check(efer, 3*time.Second)
}

func TestExecutor(t *testing.T) {
	pe := bus.NewPooledExecutor(1)
	resultCh := make(chan string)
	expected := "hello"
	task := func(context.Context) {
		resultCh <- expected
	}
	pe.Execute(task)
	got := <-resultCh
	if got != expected {
		t.Fatalf("expected:%v, got:%+v", expected, got)
	}
	pe.Shutdown()
	// Check(t)
}
