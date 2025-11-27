package croner

import (
	"time"
)

type JobInfo struct {
	ID          uint64     `gorm:"column:id;type:bigint unsigned;primaryKey;autoIncrement:true" json:"id"`
	TaskUID     string     `gorm:"column:task_uid;type:varchar(64);default:'';not null;comment:任务id" json:"task_uid"`
	Expr        string     `gorm:"column:expr;type:varchar(32);not null;comment:cron表达式" json:"expr"`
	TaskStatus  TaskStatus `gorm:"column:task_status;type:varchar(32);not null;comment:任务状态" json:"task_status"`
	FuncID      string     `gorm:"column:func_id;type:varchar(64);not null;comment:函数Id" json:"func_id"`
	Params      string     `gorm:"column:params;type:varchar(1024);not null;comment:任务参数" json:"params"`
	Description string     `gorm:"column:description;type:varchar(1024);not null;comment:描述" json:"description"`
	InvokeTimes int        `gorm:"column:invoke_times;type:int;default:-1;not null;comment:执行次数" json:"invoke_times"`
	CreatedAt   *time.Time `gorm:"column:created_at;type:datetime;default:CURRENT_TIMESTAMP;comment:创建时间" json:"created_at"`
}

const (
	TaskInvokeUnlimit = -1
)

type TaskStatus string

const (
	TaskStatusEnable  TaskStatus = "enable"
	TaskStatusDisable TaskStatus = "disable"
)

func (t *JobInfo) GetJobUID() string {
	return t.TaskUID
}
func (t *JobInfo) SetJobUID(uid string) {
	t.TaskUID = uid
}
func (t *JobInfo) GetCronExpr() string {
	return t.Expr
}
func (t *JobInfo) IsEnable() bool {
	return t.TaskStatus == TaskStatusEnable
}
func (t *JobInfo) GetJobId() any {
	return t.ID
}
func (t *JobInfo) GetFuncId() string {
	return t.FuncID
}
func (t *JobInfo) GetParams() string {
	return t.Params
}

type JobInMemoryRepo struct {
	jobs []*JobInfo
}

func NewJobInMemoryRepo() *JobInMemoryRepo {
	return &JobInMemoryRepo{
		jobs: make([]*JobInfo, 0),
	}
}

func (t *JobInMemoryRepo) UpsertJob(job Job) error {
	jobInfo := job.(*JobInfo)
	v, err := t.GetJob(jobInfo.ID)
	if err != nil {
		return err
	}
	if v == nil {
		jobInfo.ID = uint64(len(t.jobs) + 1)
		t.jobs = append(t.jobs, jobInfo)
		return nil
	}

	for i, old := range t.jobs {
		if old.ID == jobInfo.ID {
			t.jobs[i] = jobInfo
			return nil
		}
	}
	return nil
}
func (t *JobInMemoryRepo) OnJobRemoved(job Job) error {
	jobInfo := job.(*JobInfo)
	old, err := t.GetJob(jobInfo.ID)
	if err != nil {
		return err
	}
	if old == nil {
		return nil
	}
	old.TaskStatus = TaskStatusDisable
	return nil
}

func (t *JobInMemoryRepo) GetJob(id uint64) (*JobInfo, error) {
	for _, job := range t.jobs {
		if job.ID == id {
			return job, nil
		}
	}
	return nil, nil
}
