package model

import (
	"strings"
)

type TaskStatus string

const (
	TaskStatusNotExist       TaskStatus = "not_exist" // It is a virtual state used to mark that the task does not exist for deletion processing.
	TaskStatusWaitScheduling TaskStatus = "wait_scheduling"
	TaskStatusWaitRunning    TaskStatus = "wait_running"
	TaskStatusRunning        TaskStatus = "running"
	TaskStatusWaitPaused     TaskStatus = "wait_paused"
	TaskStatusPaused         TaskStatus = "paused"
	TaskStatusWaitStop       TaskStatus = "wait_stopped"
	TaskStatusSuccess        TaskStatus = "success"
	TaskStatusFailed         TaskStatus = "failed"
	TaskStatusStop           TaskStatus = "stop"

	TaskStatusExecptionRun   TaskStatus = "execption_run"
	TaskStatusExecptionPause TaskStatus = "execption_pause"
	TaskStatusExecptionStop  TaskStatus = "execption_stop"
)

func (ts TaskStatus) String() string {
	return string(ts)
}

func (ts TaskStatus) IsWaitStatus() bool {
	return strings.HasPrefix(ts.String(), "wait_")
}

func (ts TaskStatus) IsExecptionStatus() bool {
	return strings.HasPrefix(ts.String(), "execption_")
}

func (ts TaskStatus) IsFinalStatus() bool {
	return ts == TaskStatusSuccess || ts == TaskStatusFailed || ts == TaskStatusStop
}

func (ts TaskStatus) CanTransition(nextStatus TaskStatus) error {
	_, err := GetChangeFunc(ts, nextStatus)
	return err
}

func (ts TaskStatus) PreWaitStatus() TaskStatus {
	if ts.IsFinalStatus() {
		return TaskStatusWaitStop
	}
	if ts == TaskStatusPaused {
		return TaskStatusWaitPaused
	}
	if ts == TaskStatusRunning {
		return TaskStatusWaitRunning
	}
	return ""
}
