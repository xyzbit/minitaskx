package infomer

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/xyzbit/minitaskx/core/model"
	"github.com/xyzbit/minitaskx/internal/queque"
)

type Infomer struct {
	id      string
	running atomic.Bool

	indexer      *Indexer
	loader       recordLoader
	changeQueque queque.TypedInterface[*model.Change]

	opts *options
}

func New(
	id string,
	indexer *Indexer,
	loader recordLoader,
	opts ...Option,
) *Infomer {
	return &Infomer{
		id:           id,
		indexer:      indexer,
		loader:       loader,
		changeQueque: queque.NewTyped[*model.Change](),
		opts:         newOptions(opts...),
	}
}

func (i *Infomer) Run(ctx context.Context) error {
	swapped := i.running.CompareAndSwap(false, true)
	if !swapped {
		return errors.New("infomer already running")
	}

	// init and monitor indexer
	if err := i.indexer.initAndMonitor(ctx); err != nil {
		return err
	}

	// compare task's change and enqueue.
	go i.enqueueIfTaskChange(ctx)

	// monitor exit signal.
	<-ctx.Done()
	return i.shutdown()
}

// graceful shutdown.
func (i *Infomer) shutdown() error {
	stopCtx := context.Background()
	if i.opts.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		stopCtx, cancel = context.WithTimeout(stopCtx, i.opts.shutdownTimeout)
		defer cancel()
	}

	shutdownCh := make(chan struct{})
	go func() {
		i.changeQueque.ShutDownWithDrain()
		shutdownCh <- struct{}{}
	}()

	select {
	case <-stopCtx.Done():
		if stopCtx.Err() != nil {
			i.opts.logger.Error("[Infomer] shutdown timeout: %v", stopCtx.Err())
		}
		return stopCtx.Err()
	case <-shutdownCh:
		i.opts.logger.Info("[Infomer] shutdown success")
		return nil
	}
}

func (i *Infomer) enqueueIfTaskChange(ctx context.Context) error {
	for {
		// load want and real task status
		wantTaskRuns, err := i.loader.ListRunnableTasks(ctx, i.id)
		if err != nil {
			i.opts.logger.Error("[Infomer] load task failed: %v", err)
			continue
		}
		realTasks := i.indexer.listRealTasks()

		// diff get changes
		changes := diff(wantTaskRuns, realTasks)
		changesRaw, _ := json.Marshal(changes)
		i.opts.logger.Info("[Infomer] 期望任务数(%d), 实际任务数(%d), 任务状态变化事件:%s", len(wantTaskRuns), len(realTasks), string(changesRaw))

		// enqueue
		for _, change := range changes {
			i.changeQueque.Add(change)
		}

		// wait for next
		time.Sleep(i.opts.runInterval)
	}
}

func diff(wants []*model.TaskRun, reals []*model.Task) []*model.Change {
	realMap := lo.KeyBy(reals, func(t *model.Task) string {
		return t.TaskKey
	})
	wantMap := lo.KeyBy(wants, func(t *model.TaskRun) string {
		return t.TaskKey
	})

	var transitions []*model.Change

	// 1. check create or update
	for _, want := range wants {
		real, exists := realMap[want.TaskKey]
		if !exists {
			transitions = append(transitions, &model.Change{
				TaskKey: want.TaskKey,
				Real:    model.TaskStatusNotExist,
				Want:    want.WantRunStatus,
			})
		} else if real.Status != want.WantRunStatus {
			transitions = append(transitions, &model.Change{
				TaskKey: want.TaskKey,
				Real:    real.Status,
				Want:    want.WantRunStatus,
			})
		}
	}

	// 2. check delete
	for _, real := range reals {
		if _, exists := wantMap[real.TaskKey]; !exists {
			transitions = append(transitions, &model.Change{
				TaskKey: real.TaskKey,
				Real:    real.Status,
				Want:    model.TaskStatusNotExist, // 假设有这个状态表示需要删除
			})
		}
	}

	return transitions
}
