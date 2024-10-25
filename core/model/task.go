package model

import "time"

type Task struct {
	ID        int64
	TaskKey   string
	BizID     string
	BizType   string
	Type      string
	Payload   string
	Labels    map[string]string
	Staints   map[string]string
	Extra     map[string]string
	Status    TaskStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

type TaskRun struct {
	ID            int64
	TaskKey       string
	WorkerID      string
	NextRunAt     *time.Time
	WantRunStatus TaskStatus
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
