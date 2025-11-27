package croner

import (
	"fmt"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	ErrFuncNotFound = fmt.Errorf("func not found")
	ErrFuncExists   = fmt.Errorf("func exists")
	ErrFuncParams   = fmt.Errorf("invalid func params")
)

type JobManager struct {
	cronI          *cron.Cron
	cronExprParser cron.ScheduleParser
	cronParse      *cron.Parser
	jobRepository  JobRepository
	funcRegistry   map[string]JobFuner
	listener       JobListener
}

func NewJobManager(
	cronI *cron.Cron,
	cronExprParser cron.ScheduleParser,
	jobRepository JobRepository,
) *JobManager {
	return &JobManager{
		cronI:          cronI,
		cronExprParser: cronExprParser,
		jobRepository:  jobRepository,
		funcRegistry:   map[string]JobFuner{},
	}
}

type JobListener interface {
	OnStart(job Job)
	OnFail(job Job, err error)
	OnSuccess(job Job)
}

type Job interface {
	GetJobUID() string
	SetJobUID(uid string)
	GetCronExpr() string
	IsEnable() bool
	GetJobId() any
	GetFuncId() string
	GetParams() string
}

type JobRepository interface {
	UpsertJob(job Job) error
	OnJobRemoved(job Job) error
}

func (t *JobManager) Start() {
	t.cronI.Start()
}
func (t *JobManager) NextN(expr string, n int) ([]time.Time, error) {
	schedule, err := t.cronExprParser.Parse(expr)
	if err != nil {
		return nil, err
	}
	nexts := make([]time.Time, 0, n)
	next := time.Now()
	for i := 0; i < n; i++ {
		next = schedule.Next(next)
		nexts = append(nexts, next)
	}
	return nexts, nil
}

func (t *JobManager) RegisterJob(job Job) error {
	var err error
	uid := job.GetJobUID()
	uidV, _ := strconv.ParseInt(uid, 10, 64)

	funcId := job.GetFuncId()
	jobFunc := t.funcRegistry[funcId]
	if jobFunc == nil {
		return fmt.Errorf("%w: %s", ErrFuncNotFound, funcId)
	}
	if v, ok := jobFunc.(CloneableJobFuncer); ok {
		jobFunc = v.CloneJobFuner()
	}
	params, err := jobFunc.UnmarshalParams(job.GetParams())
	if err != nil {
		return fmt.Errorf("%w: %s", ErrFuncParams, funcId)
	}
	jobImpl := &JobImpl{
		Manager: t,
		Job:     job,
		Funer:   jobFunc,
		Params:  params,
	}
	var entryID cron.EntryID
	if job.IsEnable() {
		entryID, err = t.cronI.AddJob(job.GetCronExpr(), jobImpl)
		if err != nil {
			return err
		}
		job.SetJobUID(strconv.FormatInt(int64(entryID), 10))
	} else {
		job.SetJobUID("")
	}
	err = t.jobRepository.UpsertJob(job)
	if err != nil {
		// remove job added before on fail
		if job.IsEnable() {
			t.cronI.Remove(entryID)
		} else {
			// reset uid when disable on fail
			job.SetJobUID(uid)
		}
		return err
	}

	// add job when success
	if uidV != 0 {
		taskEntry := t.cronI.Entry(cron.EntryID(uidV))
		if taskEntry.Valid() {
			t.cronI.Remove(taskEntry.ID)
		}
	}
	return nil
}

func (t *JobManager) RemoveJob(job Job) error {
	uid := job.GetJobUID()
	job.SetJobUID("")
	err := t.jobRepository.OnJobRemoved(job)
	if err != nil {
		return err
	}
	v, _ := strconv.ParseInt(uid, 10, 64)
	if v != 0 {
		t.cronI.Remove(cron.EntryID(v))
	}
	return err
}

func (t *JobManager) SetListener(listener JobListener) {
	t.listener = listener
}

func (t *JobManager) RegisterFuner(funcId string, funer JobFuner) error {
	_, ok := t.funcRegistry[funcId]
	if ok {
		return fmt.Errorf("%w: %s", ErrFuncExists, funcId)
	}
	t.funcRegistry[funcId] = funer
	return nil
}

type JobFuner interface {
	Invoke(ctx *JobContext) error
	UnmarshalParams(paramsStr string) (any, error)
}

type CloneableJobFuncer interface {
	CloneJobFuner() JobFuner
}

type JobContext struct {
	Job    Job
	Params any
}

type JobImpl struct {
	Manager *JobManager
	Job     Job
	Funer   JobFuner
	Params  any
}

func (t *JobImpl) Run() {
	var err error
	if t.Manager.listener != nil {
		t.Manager.listener.OnStart(t.Job)
	}
	ctx := &JobContext{
		Job:    t.Job,
		Params: t.Params,
	}
	err = t.Funer.Invoke(ctx)
	if err != nil {
		if t.Manager.listener != nil {
			t.Manager.listener.OnFail(t.Job, err)
		}
		return
	}
	if t.Manager.listener != nil {
		t.Manager.listener.OnSuccess(t.Job)
	}
}
