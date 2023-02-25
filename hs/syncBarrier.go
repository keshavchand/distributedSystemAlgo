package main

import (
	"context"
	"sync"
)

// Blocks till SyncBarrier
// reaches target
type SyncBarrier struct {
	cl     *sync.Cond
	count  int64
	target int64

	ctx context.Context
	fn  context.CancelFunc
}

func newSyncBarrier(target int64) SyncBarrier {
	l := sync.Mutex{}
	ctx, fn := context.WithCancel(context.Background())
	return SyncBarrier{
		cl:     sync.NewCond(&l),
		target: target,

		ctx: ctx,
		fn:  fn,
	}
}

func (sg *SyncBarrier) Wait() {
	sg.cl.L.Lock()
	defer sg.cl.L.Unlock()
	sg.count += 1

	if sg.count == sg.target {
		sg.count -= 1
		sg.cl.Broadcast()
		sg.fn()
		return
	}
	sg.cl.Wait()

	sg.count -= 1
}
