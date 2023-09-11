package batch

import (
	"context"
	"math"
	"time"

	"github.com/abevier/tsk/futures"
	"github.com/abevier/tsk/internal/tsk"
	"github.com/abevier/tsk/results"
)

// RunBatchFunction is a function that handles a flushed batch of task items.
// Each time the batch executor flushes a batch, a slice of these task is provided
// to a function with this signature.
//
// This function must return a slice of results.Result of equal size to the number of
// tasks passed in. Each result in the returned slice should correspond to the task with
// the same index i.e. the returned result at index i corresponds to the provided task at index i.
//
// If the RunBatchFunction returns an error every item in the batch will complete with that error.
type RunBatchFunction[T any, R any] func(tasks []T) ([]results.Result[R], error)

type batch[T any, R any] struct {
	tasks   []T
	futures []*futures.Future[R]
}

func (b *batch[T, R]) add(task T, future *futures.Future[R]) int {
	b.tasks = append(b.tasks, task)
	b.futures = append(b.futures, future)
	return len(b.tasks)
}

// Executor batches values of type T submitted via multiple producers and invokes the provided run function
// when a batch flushes either due to size or a timeout.  Results of type R are returned to the caller of Submit
// when the batch finishes execution.
// A Executor must be created by calling New
type Executor[T any, R any] struct {
	maxSize   int
	maxLinger time.Duration
	taskChan  chan tsk.TaskFuture[T, R]
	flushChan chan struct{}
	run       RunBatchFunction[T, R]
}

// New creates a new Executor with the specified options that invokes the provided run function
// when a batch of items flushes due to either size or time.
func New[T any, R any](opts Opts, run RunBatchFunction[T, R]) *Executor[T, R] {
	be := &Executor[T, R]{
		maxSize:   opts.MaxSize,
		maxLinger: opts.MaxLinger,
		taskChan:  make(chan tsk.TaskFuture[T, R]),
		flushChan: make(chan struct{}),
		run:       run,
	}

	be.startWorker()

	return be
}

// Submit adds an item to a batch and then blocks until the batch has been processed and a result
// has been returned.
func (be *Executor[T, R]) Submit(ctx context.Context, task T) (R, error) {
	f := be.SubmitF(task)
	return f.Get(ctx)
}

// SubmitF adds an item to a batch without blocking and then returns a futures.Future that will
// contain the result of the batch computation when it is completed
func (be *Executor[T, R]) SubmitF(task T) *futures.Future[R] {
	future := futures.New[R]()
	be.taskChan <- tsk.TaskFuture[T, R]{Task: task, Future: future}
	return future
}

func (be *Executor[T, R]) startWorker() {
	go func() {
		var currentBatch *batch[T, R]
		t := time.NewTimer(math.MaxInt64)

		for {
			select {
			case <-t.C:
				// batch expired due to time
				if currentBatch != nil {
					go be.runBatch(currentBatch)
					currentBatch = nil
				}
				t.Reset(math.MaxInt64)

			case _, ok := <-be.flushChan:
				if !ok {
					if !t.Stop() {
						<-t.C
					}
					return
				}

				// batch flushed explicitly
				if currentBatch != nil {
					go be.runBatch(currentBatch)
					currentBatch = nil

					if !t.Stop() {
						<-t.C
					}
					t.Reset(math.MaxInt64)
				}

			case ft, ok := <-be.taskChan:
				if !ok {
					if !t.Stop() {
						<-t.C
					}
					return
				}

				if currentBatch == nil {
					// open a new batch since once doesn't exist
					currentBatch = &batch[T, R]{
						tasks: make([]T, 0, be.maxSize),
					}

					if !t.Stop() {
						<-t.C
					}
					t.Reset(be.maxLinger)
				}

				size := currentBatch.add(ft.Task, ft.Future)
				if size >= be.maxSize {
					// flush the batch due to size
					go be.runBatch(currentBatch)
					currentBatch = nil

					if !t.Stop() {
						<-t.C
					}
					t.Reset(math.MaxInt64)
				}
			}
		}
	}()
}

func (be *Executor[T, R]) runBatch(b *batch[T, R]) {
	res, err := be.run(b.tasks)
	if err != nil {
		for _, f := range b.futures {
			f.Fail(err)
		}
		return
	}

	if len(res) != len(b.tasks) {
		for _, f := range b.futures {
			f.Fail(ErrBatchResultMismatch)
		}
		return
	}

	for i, r := range res {
		if r.Err != nil {
			b.futures[i].Fail(r.Err)
		} else {
			b.futures[i].Complete(r.Val)
		}
	}
}

func (be *Executor[T, R]) Flush() {
	be.flushChan <- struct{}{}
}

// Close closes the Executor's underlying channel.  It is the responsibility of the caller to ensure
// that no writers are still calling Submit or SubmitF as this will cause a panic.
func (be *Executor[T, R]) Close() {
	close(be.taskChan)
	close(be.flushChan)
}
