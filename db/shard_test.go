package db

import (
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/skydb/sky/core"
	"github.com/stretchr/testify/assert"
)

func TestShardInsertEvent(t *testing.T) {
	withShard(func(s *shard) {
		s.InsertEvent("foo", "bar", testevent("2000-01-01T00:00:00Z", 1, "john"), false)
		e, err := s.getEvent("foo", "bar", musttime("2000-01-01T00:00:00Z"))
		assert.Nil(t, err, "")
		assert.Equal(t, e.Timestamp, musttime("2000-01-01T00:00:00Z"), "")
		assert.Equal(t, e.Data[1], "john", "")
	})
}

func BenchmarkAppendTransient0001(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{-1:100}}
	benchmarkInsert(b, e, 1, func() {
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkAppendTransient0010(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{-1:100}}
	benchmarkInsert(b, e, 10, func() {
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkAppendTransient0100(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{-1:100}}
	benchmarkInsert(b, e, 100, func() {
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkAppendTransient1000(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{-1:100}}
	benchmarkInsert(b, e, 1000, func() {
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}


func BenchmarkPrependTransient0001(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{-1:100}}
	benchmarkInsert(b, e, 1, func() {
		e.Timestamp = e.Timestamp.Add(-1 * time.Second)
	})
}

func BenchmarkPrependTransient0010(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{-1:100}}
	benchmarkInsert(b, e, 10, func() {
		e.Timestamp = e.Timestamp.Add(-1 * time.Second)
	})
}

func BenchmarkPrependTransient0100(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{-1:100}}
	benchmarkInsert(b, e, 100, func() {
		e.Timestamp = e.Timestamp.Add(-1 * time.Second)
	})
}

func BenchmarkPrependTransient1000(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{-1:100}}
	benchmarkInsert(b, e, 1000, func() {
		e.Timestamp = e.Timestamp.Add(-1 * time.Second)
	})
}



func BenchmarkAppendPermanent0001(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsert(b, e, 1, func() {
		e.Data[1] = e.Data[1].(int) + 1
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkAppendPermanent0010(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsert(b, e, 10, func() {
		e.Data[1] = e.Data[1].(int) + 1
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkAppendPermanent0100(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsert(b, e, 100, func() {
		e.Data[1] = e.Data[1].(int) + 1
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}

func BenchmarkAppendPermanent1000(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsert(b, e, 1000, func() {
		e.Data[1] = e.Data[1].(int) + 1
		e.Timestamp = e.Timestamp.Add(1 * time.Second)
	})
}


func BenchmarkPrependPermanent0001(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsert(b, e, 1, func() {
		e.Data[1] = e.Data[1].(int) + 1
		e.Timestamp = e.Timestamp.Add(-1 * time.Second)
	})
}

func BenchmarkPrependPermanent0010(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsert(b, e, 10, func() {
		e.Data[1] = e.Data[1].(int) + 1
		e.Timestamp = e.Timestamp.Add(-1 * time.Second)
	})
}

func BenchmarkPrependPermanent0100(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsert(b, e, 100, func() {
		e.Data[1] = e.Data[1].(int) + 1
		e.Timestamp = e.Timestamp.Add(-1 * time.Second)
	})
}

func BenchmarkPrependPermanent1000(b *testing.B) {
	e := &core.Event{Data: map[int64]interface{}{1:100}}
	benchmarkInsert(b, e, 1000, func() {
		e.Data[1] = e.Data[1].(int) + 1
		e.Timestamp = e.Timestamp.Add(-1 * time.Second)
	})
}


func benchmarkInsert(b *testing.B, e *core.Event, eventsPerObject int, fn func()) {
	e.Timestamp = musttime("2000-01-01T00:00:00Z")
	withShard(func(s *shard) {
		b.ResetTimer()
		var objectId, index int
		for i := 0; i < b.N; i++ {
			if index == 0 || index >= eventsPerObject {
				index = 0
				objectId++
			}
			s.InsertEvent("tbl0", strconv.Itoa(objectId), e, false)
			fn()
			index++
		}
	})
}

func withShard(f func(*shard)) {
	path, _ := ioutil.TempDir("", "")
	defer os.RemoveAll(path)

	s := newShard(path)
	if err := s.Open(4096, 126, options(false)); err != nil {
		panic(err.Error())
	}
	defer s.Close()

	f(s)
}
