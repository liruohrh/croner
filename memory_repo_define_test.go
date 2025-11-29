package croner

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/samber/lo"
)

var (
	ErrNotFound = errors.New("not found")
)

type JobInfo struct {
	ID                           uint64        `json:"id"`
	JobUID                       string        `json:"job_uid"`
	Expr                         string        `json:"expr"`
	JobStatus                    JobInfoStatus `json:"job_status"`
	FuncID                       string        `son:"func_id"`
	Params                       string        `json:"params"`
	Description                  string        `json:"description"`
	IsSystem                     bool          `json:"is_system"`
	InvokeTimes                  int           `json:"invoke_times"`
	CreatedAt                    *time.Time    `json:"created_at"`
	TParams                      any           `gorm:"-" json:"-"`
	IsDelete                     bool          `gorm:"-" json:"-"`
	IgnorePersistenceOptOnDelete bool          `gorm:"-" json:"-"`
	IsInitSysJob                 bool          `gorm:"-" json:"-"`
}

const (
	JobInfoInvokeUnlimit = -1
)

type JobInfoStatus string

const (
	JonInfoStatusEnable  JobInfoStatus = "Enable"
	JobInfoStatusDisable JobInfoStatus = "Disable"
)

func (t *JobInfo) GetJobID() any {
	return t.ID
}
func (t *JobInfo) GetJobUID() string {
	return t.JobUID
}
func (t *JobInfo) SetJobUID(uid string) {
	t.JobUID = uid
}
func (t *JobInfo) GetCronExpr() string {
	return t.Expr
}
func (t *JobInfo) IsEnable() bool {
	return t.HasInvokeTimes() && t.JobStatus == JonInfoStatusEnable
}
func (t *JobInfo) HasInvokeTimes() bool {
	return t.InvokeTimes == -1 || t.InvokeTimes > 0
}
func (t *JobInfo) GetFuncId() string {
	return t.FuncID
}
func (t *JobInfo) GetParams() string {
	return t.Params
}
func (t *JobInfo) SetParams(params string) {
	t.Params = params
}

func (t *JobInfo) IsSystemJob() bool {
	return t.IsSystem
}
func (t *JobInfo) GetTParams() any {
	return t.TParams
}

type JobLog struct {
	ID           uint64       `son:"id"`
	JobInfoID    uint64       `json:"job_info_id"`
	JobLogStatus JobLogStatus `json:"job_log_status"`
	StartAt      *time.Time   `json:"start_at"`
	EndAt        *time.Time   `json:"end_at"`
	Message      string       `json:"message"`
	CreatedAt    *time.Time   `json:"created_at"`
}

type JobLogStatus string

const (
	JobLogStatusSuccess JobLogStatus = "success"
	JobLogStatusFail    JobLogStatus = "fail"
)

type JobInMemoryRepo struct {
	mu     sync.RWMutex
	jobs   map[uint64]*JobInfo
	nextID uint64

	logs      map[uint64]*JobLog
	nextLogId uint64
	logMu     sync.RWMutex

	jobManager *JobManager
}

func NewJobInMemoryRepo() *JobInMemoryRepo {
	return &JobInMemoryRepo{
		jobs:      make(map[uint64]*JobInfo),
		nextID:    1,
		logs:      make(map[uint64]*JobLog),
		nextLogId: 1,
	}
}

func (t *JobInMemoryRepo) AfterProperties(jobManager *JobManager) error {
	t.jobManager = jobManager
	return nil
}

func (t *JobInMemoryRepo) UpsertJob(job Job) error {
	jobInfo := job.(*JobInfo)
	if jobInfo.IsSystem {
		// 系统任务只能有一个
		sysJob, err := t.getBySysFuncId(jobInfo.FuncID)
		if err == nil {
			if jobInfo.IsInitSysJob {
				// 初始化系统任务时，有持久化数据就不更新
				return nil
			}
			// 系统任务只能修改部分字段
			return t.updateSysJob(sysJob, jobInfo)
		} else if errors.Is(err, ErrNotFound) {
			// 系统任务不存在，创建新的
			return t.create(jobInfo)
		}
		return err
	}

	if jobInfo.ID == 0 {
		return t.create(jobInfo)
	}
	return t.update(jobInfo)
}
func (t *JobInMemoryRepo) OnJobRemoved(job Job) error {
	jobInfo := job.(*JobInfo)
	if jobInfo.IgnorePersistenceOptOnDelete {
		return nil
	}
	if jobInfo.IsDelete && !jobInfo.IsSystem {
		// 系统任务只能禁用
		return t.deleteById(jobInfo.ID)
	}
	return t.disableById(jobInfo.ID, JobInfoStatusDisable)
}

func (t *JobInMemoryRepo) ListRunnableJobs() ([]Job, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	runnableJobs := make([]Job, 0, len(t.jobs))
	for _, job := range t.jobs {
		if job.IsEnable() {
			runnableJobs = append(runnableJobs, job)
		}
	}
	return runnableJobs, nil
}

func (s *JobInMemoryRepo) OnStart(startAt time.Time, job Job) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	jobInfo := job.(*JobInfo)
	jobLog := &JobLog{
		JobInfoID:    jobInfo.ID,
		JobLogStatus: JobLogStatusSuccess,
		StartAt:      &startAt,
		EndAt:        &startAt,
		Message:      "started",
	}
	jobLog.ID = s.nextLogId
	s.nextLogId++
	s.logs[s.nextLogId] = jobLog
}

func (s *JobInMemoryRepo) OnFail(startAt, endAt time.Time, job Job, err error) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	jobInfo := job.(*JobInfo)
	jobLog := &JobLog{
		JobInfoID:    jobInfo.ID,
		JobLogStatus: JobLogStatusFail,
		StartAt:      &startAt,
		EndAt:        &endAt,
		Message:      err.Error(),
	}
	jobLog.ID = s.nextLogId
	s.nextLogId++
	s.logs[s.nextLogId] = jobLog
}

func (s *JobInMemoryRepo) OnSuccess(startAt, endAt time.Time, job Job) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	jobInfo := job.(*JobInfo)

	var err error
	var errs []error
	if jobInfo.InvokeTimes > 0 {
		jobInfo.InvokeTimes--
		if jobInfo.InvokeTimes == 0 {
			err = s.jobManager.RemoveJob(jobInfo)
			if err != nil {
				errs = append(errs, fmt.Errorf("remove no invoke times job: %w", err))
			}
		}
		err = s.updateInvokeById(jobInfo.ID, jobInfo.InvokeTimes)
		if err != nil {
			errs = append(errs, fmt.Errorf("update invoke times: %w", err))
		}
	}
	var msg string
	err = wrapErrs(errs...)
	if err != nil {
		msg = err.Error()
	}
	jobLog := &JobLog{
		JobInfoID:    jobInfo.ID,
		JobLogStatus: JobLogStatusFail,
		StartAt:      &startAt,
		EndAt:        &endAt,
		Message:      msg,
	}
	jobLog.ID = s.nextLogId
	s.nextLogId++
	s.logs[s.nextLogId] = jobLog
}

// service

func (s *JobInMemoryRepo) Create(entity *JobInfo) error {
	if entity.FuncID == "" {
		return fmt.Errorf("require FuncID")
	}
	if s.jobManager.IsSystemJob(entity.FuncID) {
		return fmt.Errorf("can not create system job")
	}
	entity.IsSystem = false
	return s.jobManager.RegisterJob(entity)
}

func (s *JobInMemoryRepo) Update(entity *JobInfo) error {
	old, err := s.GetByID(entity.ID)
	if err != nil {
		return fmt.Errorf("fail to get jobInfo, %s", err)
	}
	if old.IsSystem {
		old.Expr = entity.Expr
		old.JobStatus = entity.JobStatus
		old.Params = entity.Params
		old.Description = entity.Description
	}
	return s.jobManager.RegisterJob(entity)
}

func (s *JobInMemoryRepo) Delete(id uint64) error {
	old, err := s.GetByID(id)
	if err != nil {
		return fmt.Errorf("get jobInfo, %s", err)
	}
	if old.IsSystem {
		return fmt.Errorf("can not delete system job")
	}
	old.IsDelete = true
	return s.jobManager.RemoveJob(old)
}

func (s *JobInMemoryRepo) DeleteBatch(ids []uint64) error {
	jobInfos, err := s.GetByIds(ids)
	if err != nil {
		return fmt.Errorf("fail to get jobInfos, %s", err)
	}
	noSysJobIds := make([]uint64, 0)
	sysJobIds := make([]uint64, 0)
	for _, jobInfo := range jobInfos {
		jobInfo.IgnorePersistenceOptOnDelete = true
		if jobInfo.IsSystem {
			sysJobIds = append(sysJobIds, jobInfo.ID)
		} else {
			noSysJobIds = append(noSysJobIds, jobInfo.ID)
		}
	}
	if len(noSysJobIds) > 0 {
		err = s.DeleteNoSysByIds(noSysJobIds)
		if err != nil {
			return err
		}
	}
	if len(sysJobIds) > 0 {
		err = s.DisableSysByIds(sysJobIds)
		if err != nil {
			return err
		}
	}
	for _, jobInfo := range jobInfos {
		err = s.jobManager.RemoveJob(jobInfo)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *JobInMemoryRepo) GetByID(id uint64) (*JobInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res, ok := s.jobs[id]
	if ok {
		tmp := *res
		return &tmp, nil
	}
	return nil, ErrNotFound
}

func (s *JobInMemoryRepo) EnableJobById(id uint64) error {
	entity, err := s.GetByID(id)
	if err != nil {
		return fmt.Errorf("get jobInfo, %s", err)
	}
	entity.JobStatus = JonInfoStatusEnable
	return s.jobManager.RegisterJob(entity)
}

func (s *JobInMemoryRepo) DisableJobById(id uint64) error {
	entity, err := s.GetByID(id)
	if err != nil {
		return fmt.Errorf("get jobInfo, %s", err)
	}
	return s.jobManager.RemoveJob(entity)
}

func (s *JobInMemoryRepo) ListFuncIds(excludeSys bool) []string {
	funcIds := s.jobManager.ListFuncIds()
	if !excludeSys {
		return funcIds
	}
	return lo.Filter(funcIds, func(item string, index int) bool {
		return !s.jobManager.IsSystemJob(item)
	})
}

// db opt

func (t *JobInMemoryRepo) getBySysFuncId(funcID string) (*JobInfo, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for _, job := range t.jobs {
		if job.IsSystem && job.FuncID == funcID {
			return job, nil
		}
	}
	return nil, ErrNotFound
}

func (t *JobInMemoryRepo) deleteById(id uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.jobs, id)
	return nil
}

func (t *JobInMemoryRepo) disableById(id uint64, status JobInfoStatus) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	job, exists := t.jobs[id]
	if !exists {
		return nil
	}
	job.JobStatus = status
	return nil
}

func (t *JobInMemoryRepo) updateInvokeById(id uint64, invokeTimes int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	job, exists := t.jobs[id]
	if !exists {
		return nil
	}
	job.InvokeTimes = invokeTimes
	return nil
}

func (t *JobInMemoryRepo) GetByIds(ids []uint64) ([]*JobInfo, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	result := make([]*JobInfo, 0, len(ids))
	for _, id := range ids {
		if job, exists := t.jobs[id]; exists {
			result = append(result, job)
		}
	}
	return result, nil
}

func (t *JobInMemoryRepo) DeleteNoSysByIds(ids []uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, id := range ids {
		if job, exists := t.jobs[id]; exists && !job.IsSystem {
			delete(t.jobs, id)
		}
	}
	return nil
}

func (t *JobInMemoryRepo) DisableSysByIds(ids []uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, id := range ids {
		if job, exists := t.jobs[id]; exists && job.IsSystem {
			job.JobStatus = JobInfoStatusDisable
		}
	}
	return nil
}

func (t *JobInMemoryRepo) updateSysJob(sysJob *JobInfo, newJob *JobInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	sysJob.Expr = newJob.Expr
	sysJob.JobStatus = newJob.JobStatus
	sysJob.Params = newJob.Params
	sysJob.Description = newJob.Description
	return nil
}

func (t *JobInMemoryRepo) create(jobInfo *JobInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	jobInfo.ID = t.nextID
	t.nextID++
	t.jobs[jobInfo.ID] = jobInfo
	return nil
}

func (t *JobInMemoryRepo) update(jobInfo *JobInfo) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.jobs[jobInfo.ID] = jobInfo
	return nil
}
