package model

import (
	"context"

	"github.com/pkg/errors"
)

var changeFuncsMap = make(map[TaskStatus]map[TaskStatus]ChangeFunc)

type Change struct {
	TaskKey    string
	Real, Want TaskStatus
}

type ChangeFunc func(ctx context.Context, taskKey string) error

func RegisterChangeFunc(real, want TaskStatus, fn ChangeFunc) {
	if _, ok := changeFuncsMap[real]; !ok {
		changeFuncsMap[real] = make(map[TaskStatus]ChangeFunc)
	}

	changeFuncsMap[real][want] = fn
}

func GetChangeFunc(
	realRunStatus, wantRunStatus TaskStatus,
) (ChangeFunc, error) {
	m, ok := changeFuncsMap[realRunStatus]
	if !ok {
		return nil, errors.Errorf("当前状态为[%s], 不支持转换", realRunStatus)
	}
	fid, ok := m[wantRunStatus]
	if !ok {
		return nil, errors.Errorf("当前状态[%s] -> 期望运行状态[%s], 不支持转换", realRunStatus, wantRunStatus)
	}

	return fid, nil
}
