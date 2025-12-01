package croner

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/robfig/cron/v3"
)

var (
	ErrFuncNotFound = fmt.Errorf("func not found")
	ErrFuncExists   = fmt.Errorf("func exists")
	ErrFuncParams   = fmt.Errorf("invalid func params")
)

type JobManager struct {
	cronI            *cron.Cron
	cronExprParser   cron.ScheduleParser
	cronParse        *cron.Parser
	jobRepository    JobRepository
	funcRegistry     map[string]JobFunc
	systemJobFuncIds map[string]struct{}
	listener         JobListener
	logger           CronLogger
}

func NewJobManager(
	cronI *cron.Cron,
	cronExprParser cron.ScheduleParser,
	jobRepository JobRepository,
) *JobManager {
	return NewJobManagerWithLogger(
		cronI,
		cronExprParser,
		jobRepository,
		newDefaultLogger(),
	)
}

func NewJobManagerWithLogger(
	cronI *cron.Cron,
	cronExprParser cron.ScheduleParser,
	jobRepository JobRepository,
	logger CronLogger,
) *JobManager {
	return &JobManager{
		cronI:            cronI,
		cronExprParser:   cronExprParser,
		jobRepository:    jobRepository,
		funcRegistry:     map[string]JobFunc{},
		logger:           logger,
		systemJobFuncIds: map[string]struct{}{},
	}
}

type JobListener interface {
	OnStart(startAt time.Time, job Job)
	OnFail(startAt time.Time, endAt time.Time, job Job, err error)
	OnSuccess(startAt time.Time, endAt time.Time, job Job)
}

type Job interface {
	// GetJobID must set after upsert or before remove
	GetJobID() any
	GetCronExpr() string
	GetFuncId() string
	GetParams() string
	SetParams(params string)
	GetTParams() any
	IsEnable() bool
	IsSystemJob() bool
}

type JobRepository interface {
	UpsertJob(job Job) error
	OnJobRemoved(job Job) error
	ListRunnableJobs() ([]Job, error)
	//GetBySysFuncId no job must return (nil,nil)
	GetBySysFuncId(funcId string) (Job, error)
}

func (t *JobManager) SetJobRepository(repo JobRepository) {
	t.jobRepository = repo
}

func (t *JobManager) Start() error {
	t.cronI.Start()
	var err error
	jobs, err := t.jobRepository.ListRunnableJobs()
	if err != nil {
		return err
	}
	validJobs := make([]Job, 0, len(jobs))
	for i, job := range jobs {
		if !job.IsEnable() {
			t.logger.Errorf("start: job is not enable: %d/%d, %v", i+1, len(jobs), job.GetJobID())
			continue
		}
		err = t.ValidExpr(job.GetCronExpr())
		if err != nil {
			t.logger.Errorf("start: invalid cron expr: %d/%d, %v %s: %s", i+1, len(jobs), job.GetJobID(), job.GetCronExpr(), err)
			continue
		}
		if job.IsSystemJob() {
			taskEntry := t.GetByJobId(job.GetJobID())
			if taskEntry.Valid() {
				t.logger.Infof("start: skip for system job %s already exists: %d/%d", job.GetFuncId(), i+1, len(jobs))
				continue
			}
		}
		validJobs = append(validJobs, job)
	}
	for i, job := range validJobs {
		err = t.RegisterJob(job)
		if err != nil {
			return fmt.Errorf("start: register job: %d/%d, %v: %w", i+1, len(jobs), job.GetJobID(), err)
		}
	}
	t.logger.Infof("start: add %d init jobs", len(validJobs))
	return nil
}

func (t *JobManager) IsSystemJob(funcId string) bool {
	_, ok := t.systemJobFuncIds[funcId]
	return ok
}

func (t *JobManager) ValidExpr(expr string) error {
	_, err := t.cronExprParser.Parse(expr)
	if err != nil {
		return err
	}
	return nil
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

// RegisterInitSystemJob call this first when init a system job, otherwise call RegisterJob with new or old job
func (t *JobManager) RegisterInitSystemJob(job Job) error {
	return t.registerJob(job, true)
}
func (t *JobManager) RegisterJob(job Job) error {
	return t.registerJob(job, false)
}
func (t *JobManager) registerJob(job Job, isInitSystemJob bool) error {
	var err error

	funcId := job.GetFuncId()
	jobFunc := t.funcRegistry[funcId]
	if jobFunc == nil {
		return fmt.Errorf("%w: %s", ErrFuncNotFound, funcId)
	}
	if v, ok := jobFunc.(CloneableJobFunc); ok {
		jobFunc = v.CloneJobFunc()
	}

	if isInitSystemJob && job.IsSystemJob() {
		// replace to persistence sys job when init
		pjob, err := t.jobRepository.GetBySysFuncId(job.GetFuncId())
		if err != nil {
			return err
		}
		if pjob != nil {
			job = pjob
		}
	}

	var tParams any
	paramsStr := job.GetParams()
	if paramsStr == "" {
		var hasParams bool
		tParamsV := job.GetTParams()
		if tParamsV == nil {
			hasParams = false
		} else if tParamsValue := reflect.ValueOf(tParamsV); tParamsValue.IsZero() {
			// 空结构体
			hasParams = true
		} else if tParamsValue.Kind() == reflect.Ptr {
			// 判断到非指针是
			v := tParamsValue.Elem()
			for v.Kind() == reflect.Ptr && !v.IsNil() {
				v = tParamsValue.Elem()
			}
			if v.Kind() == reflect.Ptr {
				hasParams = true
			} else {
				hasParams = !v.IsZero()
			}
		}
		if hasParams {
			// 有value，需要序列化保存一下
			tParams = tParamsV
			paramsStr2, err := jobFunc.MarshalParams(tParamsV)
			if err != nil {
				return fmt.Errorf("%w: %s: %w", ErrFuncParams, funcId, err)
			}
			job.SetParams(paramsStr2)
			tParams, err = jobFunc.UnmarshalParams(paramsStr2)
			if err != nil {
				return fmt.Errorf("%w: %s: %w", ErrFuncParams, funcId, err)
			}
		} else {
			// 没有value 函数自己处理空字符串的情况
			tParams, err = jobFunc.UnmarshalParams("")
			if err != nil {
				return fmt.Errorf("%w: %s: %w", ErrFuncParams, funcId, err)
			}
		}
	} else {
		tParams, err = jobFunc.UnmarshalParams(paramsStr)
		if err != nil {
			return fmt.Errorf("%w: %s: %w", ErrFuncParams, funcId, err)
		}
	}

	err = t.jobRepository.UpsertJob(job)
	if err != nil {
		return err
	}

	if job.IsEnable() {
		taskEntry := t.GetByJobId(job.GetJobID())
		if taskEntry.Valid() {
			t.cronI.Remove(taskEntry.ID)
		}
		jobImpl := &JobImpl{
			Manager: t,
			Job:     job,
			Func:    jobFunc,
			Params:  tParams,
		}
		_, err = t.cronI.AddJob(job.GetCronExpr(), jobImpl)
		if err != nil {
			return err
		}
	}

	if job.IsSystemJob() {
		t.systemJobFuncIds[funcId] = struct{}{}
	}
	return nil
}
func (t *JobManager) GetByJobId(jobId any) cron.Entry {
	for _, entry := range t.cronI.Entries() {
		jobImpl, ok := entry.Job.(*JobImpl)
		if !ok {
			continue
		}
		if jobImpl.Job.GetJobID() == jobId {
			return entry
		}
	}
	return cron.Entry{}
}

func (t *JobManager) RemoveJob(job Job) error {
	err := t.jobRepository.OnJobRemoved(job)
	if err != nil {
		return err
	}
	taskEntry := t.GetByJobId(job.GetJobID())
	if taskEntry.Valid() {
		t.cronI.Remove(taskEntry.ID)
	}
	return err
}

func (t *JobManager) SetListener(listener JobListener) {
	t.listener = listener
}

func (t *JobManager) RegisterFunc(funcId string, fun JobFunc) error {
	_, ok := t.funcRegistry[funcId]
	if ok {
		return fmt.Errorf("%w: %s", ErrFuncExists, funcId)
	}
	t.funcRegistry[funcId] = fun
	return nil
}

func (t *JobManager) ListFuncIds() []string {
	ids := make([]string, 0, len(t.funcRegistry))
	for id := range t.funcRegistry {
		ids = append(ids, id)
	}
	return ids
}

type JobFunc interface {
	UnmarshalParams(paramsStr string) (any, error)
	MarshalParams(v any) (string, error)
	Invoke(ctx *JobContext) error
}
type JsonParamsJobFunc struct{}

func (t *JsonParamsJobFunc) MarshalParams(v any) (string, error) {
	bytes, err := json.Marshal(v)
	return string(bytes), err
}

type CloneableJobFunc interface {
	CloneJobFunc() JobFunc
}

type JobContext struct {
	Job    Job
	Params any
}

type JobImpl struct {
	Manager *JobManager
	Job     Job
	Func    JobFunc
	Params  any
}

func (t *JobImpl) Run() {
	var err error
	startAt := time.Now()
	if t.Manager.listener != nil {
		t.Manager.listener.OnStart(startAt, t.Job)
	}
	ctx := &JobContext{
		Job:    t.Job,
		Params: t.Params,
	}
	err = t.Func.Invoke(ctx)
	endAt := time.Now()
	if err != nil {
		if t.Manager.listener != nil {
			t.Manager.listener.OnFail(startAt, endAt, t.Job, err)
		}
		return
	}
	if t.Manager.listener != nil {
		t.Manager.listener.OnSuccess(startAt, endAt, t.Job)
	}
}
