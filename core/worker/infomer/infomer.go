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

	indexer     *Indexer
	recorder    recorder
	changeQueue queue.TypedInterface[model.Change]

	logger log.Logger
}

func New(
	indexer *Indexer,
	recorder recorder,
	logger log.Logger,
) *Infomer {
	return &Infomer{
		indexer:     indexer,
		recorder:    recorder,
		changeQueue: queue.NewTyped[model.Change](),
		logger:      logger,
	}
}

func (i *Infomer) Run(ctx context.Context, workerID string, resync time.Duration) error {
	swapped := i.running.CompareAndSwap(false, true)
	if !swapped {
		return errors.New("infomer already running")
	}
	trigger, err := i.makeTigger(ctx, workerID, resync)
	if err != nil {
		return err
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
		i.enqueueIfTaskChange(ctx, trigger)
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
		i.changeQueue.ShutDownWithDrain()
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

// update recorder want + real cache need atomic.
func (i *Infomer) enqueueIfTaskChange(ctx context.Context, ch <-chan triggerInfo) {
	for {
		select {
		case <-ctx.Done():
			return
		case triggerInfo, ok := <-ch:
			if !ok {
				return
			}
			// load want and real task status
			taskPairs, err := i.loadTaskPairsThreadSafe(ctx, triggerInfo)
			if err != nil {
				i.logger.Error("[Infomer] loadTaskPairs failed: %v", err)
				continue
			}

			// diff to get change
			changes := diff(taskPairs)

			// handle exception change.
			changes = i.handleException(changes)

			// changeQueue can ensure that only one operation of a task is executed at the same time.
			for _, change := range changes {
				if exist := i.changeQueue.Add(change); !exist {
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
				return i.recorder.FinishTask(context.Background(), t)
			}
			return i.recorder.UpdateTask(context.Background(), t)
		}); err != nil {
			i.logger.Error("[Infomer] UpdateTask(%s) failed: %v", t.TaskKey, err)
		}

		// mark change done, other operation of the task can enqueue.
		i.changeQueue.Done(model.Change{TaskKey: t.TaskKey}) // only need task key to mask.
	})
	// monitor real task status
	i.indexer.Monitor(ctx)
}

type taskPair struct {
	want *model.Task
	real *model.Task
}

func (i *Infomer) loadTaskPairsThreadSafe(ctx context.Context, info triggerInfo) ([]taskPair, error) {
	// 1. check processing task, Ensure serial execution of the same task.
	processingKeys := make(map[string]struct{}, len(info.taskKeys))
	for _, key := range info.taskKeys {
		if i.changeQueue.Exist(model.Change{TaskKey: key}) {
			processingKeys[key] = struct{}{}
		}
	}

	// 2. load want and real task.
	wantTaskKeys, realTaskKeys := info.taskKeys, info.taskKeys
	if info.resync {
		realTaskKeys = i.indexer.ListTaskKeys()
	}
	if len(processingKeys) > 0 {
		wtemp, rtemp := make([]string, 0, len(wantTaskKeys)), make([]string, 0, len(realTaskKeys))
		for _, key := range wantTaskKeys {
			if _, exist := processingKeys[key]; !exist {
				wtemp = append(wtemp, key)
			}
		}
		for _, key := range realTaskKeys {
			if _, exist := processingKeys[key]; !exist {
				rtemp = append(rtemp, key)
			}
		}
		wantTaskKeys, realTaskKeys = wtemp, rtemp
	}
	taskPairs, err := i.loadTaskPairs(ctx, wantTaskKeys, realTaskKeys)
	if err != nil {
		return nil, err
	}

	// 3. filter finished task.
	// After the task is completed, the status is modified by the system, which will lead to some abnormal situations.
	// This situation needs to be filtered. For details, please refer to github.com/xyzbit/minitaskx/docs/exception.md.
	ret := make([]taskPair, 0, len(taskPairs))
	for _, pair := range taskPairs {
		if want := pair.want; want != nil {
			if want.Status.IsAutoFinished() {
				continue
			}
		}
		if real := pair.real; real != nil {
			if real.Status.IsAutoFinished() {
				continue
			}
		}
		ret = append(ret, pair)
	}
	return ret, nil
}

func (i *Infomer) loadTaskPairs(ctx context.Context, wantTaskKeys, realTaskKeys []string) ([]taskPair, error) {
	wantTasks, err := i.recorder.BatchGetWantTask(ctx, wantTaskKeys)
	if err != nil {
		return nil, err
	}
	realTasks := i.indexer.ListTasks(realTaskKeys)

	realMap := lo.KeyBy(realTasks, func(t *model.Task) string { return t.TaskKey })
	wantMap := lo.KeyBy(wantTasks, func(t *model.Task) string { return t.TaskKey })

	taskPairs := make([]taskPair, 0, len(wantTasks))
	for _, want := range wantTasks {
		taskPairs = append(taskPairs, taskPair{want: want, real: realMap[want.TaskKey]})
	}
	for _, real := range realTasks {
		_, exists := wantMap[real.TaskKey]
		if !exists {
			taskPairs = append(taskPairs, taskPair{real: real})
		}
	}

	return taskPairs, nil
}

func diff(taskPairs []taskPair) []model.Change {
	var changes []model.Change

	for _, pair := range taskPairs {
		var changeTask *model.Task
		want, real := pair.want, pair.real
		wantStatus, realStatus := model.TaskStatusNotExist, model.TaskStatusNotExist
		if real != nil {
			changeTask = real
			realStatus = real.Status
		}
		if want != nil {
			changeTask = want
			wantStatus = want.Status
		}

		if realStatus == wantStatus {
			continue
		}

		changeType, err := model.GetChangeType(realStatus, wantStatus)
		if err != nil {
			log.Error("[diff] task key: %s, realStatus: %s, wantStatus: %s, err: %v", want.TaskKey, realStatus, wantStatus, err)
			continue
		}
		changes = append(changes, model.Change{
			TaskKey:    changeTask.TaskKey,
			TaskType:   changeTask.Type,
			ChangeType: changeType,
			Task:       changeTask,
		})
	}

	return changes
}

func (i *Infomer) handleException(cs []model.Change) []model.Change {
	normalChanges := make([]model.Change, 0, len(cs))
	for _, c := range cs {
		if !c.IsException() {
			normalChanges = append(normalChanges, c)
			continue
		}

		if c.ChangeType == model.ChangeExceptionFinish {
			if err := i.recorder.FinishTask(context.Background(), &model.Task{
				TaskKey: c.TaskKey,
				Status:  model.TaskStatusStop,
				Msg:     "exception finish",
			}); err != nil {
				log.Error("[Infomer] FinishTask(%s) when handleException: %v", c.TaskKey, err)
			}
		}
	}
	return normalChanges
}
