package bus

import (
	"reflect"
	"testing"
)

func TestStorage(t *testing.T) {
	s := newMemStorage()
	expected := &Event{Data: "world"}
	id, err := s.Sink("hello", expected)
	if err != nil {
		t.Fatalf("expected err:nil, got:%+v", err)
	}
	inner := s.(*memStorage)
	got := inner.m[id]
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("expected %+v, got:%+v", expected, got)
	}
	if err := s.Delete(id); err != nil {
		t.Fatalf("expected err:nil, got:%+v", err)
	}
	_, exist := inner.m[id]
	if !reflect.DeepEqual(exist, false) {
		t.Fatalf("expected false, got:%+v", exist)
	}
}
