package collections

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type stackTestNode struct {
	value int // exercise a nonzero Link offset
	link  Link[stackTestNode]
	state atomic.Int32
}

func TestTreiberStackDetachAndRepush(t *testing.T) {
	s := NewTreiberStack(func(n *stackTestNode) *Link[stackTestNode] { return &n.link })
	nodes := [3]stackTestNode{{value: 1}, {value: 2}, {value: 3}}
	for i := range nodes {
		s.Push(&nodes[i])
	}
	var first, second []int
	s.ConsumeAll(func(n *stackTestNode) {
		first = append(first, n.value)
		s.Push(n)
	})
	s.ConsumeAll(func(n *stackTestNode) {
		second = append(second, n.value)
		require.Nil(t, n.link.next)
	})
	require.Equal(t, []int{3, 2, 1}, first)
	require.Equal(t, []int{1, 2, 3}, second)
	s.ConsumeAll(func(*stackTestNode) { t.Fatal("stack should be empty") })
	require.Panics(t, func() {
		NewTreiberStack(func(*stackTestNode) *Link[stackTestNode] { return nil })
	})
	external := new(Link[stackTestNode])
	require.Panics(t, func() {
		NewTreiberStack(func(*stackTestNode) *Link[stackTestNode] { return external })
	})
}

func TestTreiberStackConcurrentReuse(t *testing.T) {
	const producers, consumers, rounds = 8, 4, 2000
	s := NewTreiberStack(func(n *stackTestNode) *Link[stackTestNode] { return &n.link })
	var consumed atomic.Int64
	var stop atomic.Bool
	var writers, readers sync.WaitGroup
	for range consumers {
		readers.Go(func() {
			for !stop.Load() {
				s.ConsumeAll(func(n *stackTestNode) {
					if !n.state.CompareAndSwap(1, 2) {
						t.Error("node consumed twice or before publication")
						return
					}
					if n.value <= 0 || n.value > rounds {
						t.Errorf("unpublished payload: %d", n.value)
					}
					consumed.Add(1)
					n.state.Store(0) // return ownership; producer can immediately reuse it
				})
				runtime.Gosched()
			}
		})
	}
	for range producers {
		writers.Go(func() {
			n := new(stackTestNode)
			for i := 1; i <= rounds; i++ {
				if stop.Load() {
					return
				}
				n.value = i
				n.state.Store(1)
				s.Push(n)
				for n.state.Load() != 0 {
					if stop.Load() {
						return
					}
					runtime.Gosched()
				}
			}
		})
	}
	done := make(chan struct{})
	go func() { writers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Error("concurrent reuse stalled")
	}
	stop.Store(true)
	writers.Wait()
	readers.Wait()
	require.EqualValues(t, producers*rounds, consumed.Load())
	require.Nil(t, s.head.Load())
}
