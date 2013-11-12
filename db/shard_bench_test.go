package db

import (
	"strconv"
	"testing"
	"time"

	"github.com/skydb/sky/core"
)

func BenchmarkInsertEvent0001(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsertEvent(b, e, 1, func() {
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkInsertEvent0010(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsertEvent(b, e, 10, func() {
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkInsertEvent0100(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsertEvent(b, e, 100, func() {
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkInsertEvent1000(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsertEvent(b, e, 1000, func() {
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkInsertEvents0001(b *testing.B) {
	benchmarkInsertEvents(b, 1)
}

func BenchmarkInsertEvents0010(b *testing.B) {
	benchmarkInsertEvents(b, 10)
}

func BenchmarkInsertEvents0100(b *testing.B) {
	benchmarkInsertEvents(b, 100)
}

func BenchmarkInsertEvents1000(b *testing.B) {
	benchmarkInsertEvents(b, 1000)
}


func benchmarkInsertEvent(b *testing.B, e *core.Event, eventsPerObject int, fn func()) {
	e.Timestamp = musttime("2000-01-01T00:00:00Z")
	withShard(func(s *shard) {
		b.ResetTimer()
		var objectId, index int
		for i := 0; i < b.N; i++ {
			if index == 0 || index >= eventsPerObject {
				index = 0
				objectId++
			}
			s.InsertEvent("tbl0", strconv.Itoa(objectId), e)
			fn()
			index++
		}
	})
}

func benchmarkInsertEvents(b *testing.B, eventsPerObject int) {
	var timestamp = musttime("2000-01-01T00:00:00Z")
	var events = make([]*core.Event, 0)
	for i:=0; i<eventsPerObject; i++ {
		events = append(events, &core.Event{Timestamp: timestamp.Add(time.Duration(i) * time.Second), Data: map[int64]interface{}{1:100}})
	}

	withShard(func(s *shard) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.InsertEvents("tbl0", strconv.Itoa(i / eventsPerObject), events)
		}
	})
}
