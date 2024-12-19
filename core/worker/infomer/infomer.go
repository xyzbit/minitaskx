package infomer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/samber/lo"

	"github.com/xyzbit/minitaskx/core/components/log"
	"github.com/xyzbit/minitaskx/core/model"
	"github.com/xyzbit/minitaskx/internal/queue"
	"github.com/xyzbit/minitaskx/pkg/util/retry"
)

type Infomer struct {
	running atomic.Bool

	indexer      *Indexer
	recorder     recorder
	changeQueque queue.TypedInterface[model.Change]

	logger log.Logger
}

func New(
	indexer *Indexer,
	recorder recorder,
	logger log.Logger,
) *Infomer {
	return &Infomer{
		indexer:      indexer,
		recorder:     recorder,
		changeQueque: queue.NewTyped[model.Change](),
		logger:       logger,
	}
}

func (i *Infomer) Run(ctx context.Context, workerID string, runInterval time.Duration) error {
	swapped := i.running.CompareAndSwap(false, true)
	if !swapped {
		return errors.New("infomer already running")
	}

	var wg sync.WaitGroup

	// monitor change result
	wg.Add(1)
	go func() {
		defer wg.Done()
		i.monitorChangeResult(ctx)
	}()
	// compare task's change and enqueue.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i.enqueueIfTaskChange(ctx, workerID, runInterval)
	}()

	wg.Wait()
	return nil
}

func (i *Infomer) ChangeConsumer() ChangeConsumer {
	return &changeConsumer{i: i}
}

// graceful shutdown.
// Stop sending new events and wait for old events to be consumed.
func (i *Infomer) Shutdown(ctx context.Context) error {
	shutdownCh := make(chan struct{})
	go func() {
		i.changeQueque.ShutDownWithDrain()
		shutdownCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		if ctx.Err() != nil {
			i.logger.Error("[Infomer] shutdown timeout: %v", ctx.Err())
		}
		return ctx.Err()
	case <-shutdownCh:
		i.logger.Info("[Infomer] shutdown success")
		return nil
	}
}

func (i *Infomer) enqueueIfTaskChange(ctx context.Context, workerID string, runInterval time.Duration) {
	t := time.NewTicker(runInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// load want and real task status
			wantTasks, err := i.recorder.ListRunnableTasks(context.Background(), workerID)
			if err != nil {
				i.logger.Error("[Infomer] load task failed: %v", err)
				continue
			}
			realTasks := i.indexer.ListRealTasks()

			// diff get changes
			changes := diff(wantTasks, realTasks)

			// enqueue
			for _, change := range changes {
				if exist := i.changeQueque.Add(change); !exist {
					i.logger.Info("[Infomer] enqueue change: %v", change)
				}
			}
		}
	}
}

func (i *Infomer) monitorChangeResult(ctx context.Context) {
	i.indexer.SetAfterChange(func(t *model.Task) {
		i.logger.Info("[Infomer] monitor task %s status changed: %s", t.TaskKey, t.Status)

		if err := retry.Do(func() error {
			if t.Status.IsFinalStatus() {
				return i.recorder.FinishTaskTX(context.Background(), t)
			}
			return i.recorder.UpdateTask(context.Background(), t)
		}); err != nil {
			i.logger.Error("[Infomer] UpdateTask(%s) failed: %v", t.TaskKey, err)
		}

		i.changeQueque.Done(model.Change{TaskKey: t.TaskKey}) // only need task key to mask.
	})
	// monitor real task status
	i.indexer.Monitor(ctx)
}

func diff(wants []*model.Task, reals map[string]model.TaskStatus) []model.Change {
	realMap := reals
	wantMap := lo.KeyBy(wants, func(t *model.Task) string {
		return t.TaskKey
	})

	var changes []model.Change

	// 1. check create or update
	for _, want := range wants {
		realStatus, exists := realMap[want.TaskKey]
		if !exists {
			realStatus = model.TaskStatusNotExist
		}
		if realStatus == want.Status {
			continue
		}

		changeType, err := model.GetChangeType(realStatus, want.Status)
		if err != nil {
			log.Error("[diff] task key: %s, realStatus: %s, wantStatus: %s, err: %v", want.TaskKey, realStatus, want.Status, err)
			continue
		}
		changes = append(changes, model.Change{
			TaskKey:    want.TaskKey,
			TaskType:   want.Type,
			ChangeType: changeType,
			Task:       want,
		})
	}

	// 2. check delete
	for taskKey := range reals {
		if task, exists := wantMap[taskKey]; !exists {
			changes = append(changes, model.Change{
				TaskKey:    task.TaskKey,
				TaskType:   task.Type,
				ChangeType: model.ChangeDelete,
			})
		}
	}

	return changes
}
