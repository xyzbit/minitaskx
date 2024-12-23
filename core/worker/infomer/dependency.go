package infomer

import (
	"context"

	"github.com/xyzbit/minitaskx/core/model"
)

// Obtain real execution task's info
type realTaskLoader interface {
	List(ctx context.Context) ([]*model.Task, error)
	ResultChan() <-chan *model.Task
}

type recorder interface {
	UpdateTask(ctx context.Context, task *model.Task) error
	FinishTask(ctx context.Context, task *model.Task) error
	BatchGetWantTask(ctx context.Context, taskKeys []string) ([]*model.Task, error)
	// returns all runnable tasks of the current worker.
	ListRunnableTasks(ctx context.Context, workerID string) (keys []string, err error)
	// watch all runnable tasks change.
	WatchRunnableTasks(ctx context.Context, workerID string) (keys <-chan []string, err error)
}
