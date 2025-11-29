package croner

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
)

func TestJobManager(t *testing.T) {
	var err error

	manager := createTestManager()

	manager.SetListener(&PrintLogListener{})

	err = manager.RegisterFunc("hello", &HelloJobFunc{})
	require.NoError(t, err)

	now := time.Now()
	jobInfo := &JobInfo{
		Expr:        "0/5 * * * * *",
		JobStatus:   JonInfoStatusEnable,
		FuncID:      "hello",
		Params:      `{"name": "sb"}`,
		Description: "a test job",
		InvokeTimes: JobInfoInvokeUnlimit,
		CreatedAt:   &now,
	}
	err = manager.RegisterJob(jobInfo)
	require.NoError(t, err)
	nexts, err := manager.NextN(jobInfo.Expr, 10)
	require.NoError(t, err)
	for i, next := range nexts {
		t.Logf("%d/%d: %s", i+1, len(nexts), next)
	}

	err = manager.Start()
	require.NoError(t, err)
	time.Sleep(60 * time.Second)
}

func TestRegisterFunc(t *testing.T) {
	manager := createTestManager()

	t.Run("ok", func(t *testing.T) {
		err := manager.RegisterFunc("test_func", &HelloJobFunc{})
		require.NoError(t, err)
		require.NotNil(t, manager.funcRegistry["test_func"])
	})

	t.Run("repeat fail", func(t *testing.T) {
		err := manager.RegisterFunc("test_func", &HelloJobFunc{})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrFuncExists)
	})

	t.Run("register many funcers", func(t *testing.T) {
		err := manager.RegisterFunc("func1", &HelloJobFunc{})
		require.NoError(t, err)
		err = manager.RegisterFunc("func2", &HelloJobFunc{})
		require.NoError(t, err)
		require.Equal(t, 3, len(manager.funcRegistry))
	})
}

func TestRegisterJob(t *testing.T) {
	var err error
	manager := createTestManager()
	err = manager.Start()
	require.NoError(t, err)
	helloJobFunc := &HelloJobFunc{}
	err = manager.RegisterFunc("hello", helloJobFunc)
	require.NoError(t, err)

	t.Run("no exists func", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			JobStatus:   JonInfoStatusEnable,
			FuncID:      "non_exist",
			Params:      `{"name": "test"}`,
			Description: "test job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RegisterJob(job)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrFuncNotFound)
	})

	t.Run("invalid cron", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "invalid cron",
			JobStatus:   JonInfoStatusEnable,
			FuncID:      "hello",
			Params:      `{"name": "test"}`,
			Description: "test job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RegisterJob(job)
		require.Error(t, err)
		t.Log(err)
	})

	t.Run("invalid params", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			JobStatus:   JonInfoStatusEnable,
			FuncID:      "hello",
			Params:      `{invalid json}`,
			Description: "test job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RegisterJob(job)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrFuncParams)
	})

	t.Run("disable job", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			JobStatus:   JobInfoStatusDisable,
			FuncID:      "hello",
			Params:      `{"name": "test"}`,
			Description: "disabled job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RegisterJob(job)
		require.NoError(t, err)
		require.Empty(t, job.GetJobUID())
		time.Sleep(6 * time.Second)
		require.Equal(t, 0, helloJobFunc.runtimes)
	})

	t.Run("enable_update_disable_remove job", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			JobStatus:   JonInfoStatusEnable,
			FuncID:      "hello",
			Params:      `{"name": "original"}`,
			Description: "original job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RegisterJob(job)
		require.NoError(t, err)
		require.NotEmpty(t, job.GetJobUID())
		log.Printf("before 2.4s")
		time.Sleep(2400 * time.Millisecond)
		log.Printf("after 2.4s")
		require.Equal(t, 1, helloJobFunc.runtimes)
		require.Equal(t, "original", helloJobFunc.latestName)

		oldUID := job.GetJobUID()
		job.Expr = "*/3 * * * * *"
		job.Params = `{"name": "updated"}`
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		require.NotEmpty(t, job.GetJobUID())
		require.NotEqual(t, oldUID, job.GetJobUID())
		time.Sleep(3400 * time.Millisecond)
		require.Equal(t, 2, helloJobFunc.runtimes)
		require.Equal(t, "updated", helloJobFunc.latestName)

		job.JobStatus = JobInfoStatusDisable
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		require.Empty(t, job.GetJobUID())
		time.Sleep(3400 * time.Millisecond)
		require.Equal(t, 2, helloJobFunc.runtimes)
		require.Equal(t, "updated", helloJobFunc.latestName)

		job.JobStatus = JonInfoStatusEnable
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		require.NotEmpty(t, job.GetJobUID())
		time.Sleep(3400 * time.Millisecond)
		require.Equal(t, 3, helloJobFunc.runtimes)
		require.Equal(t, "updated", helloJobFunc.latestName)

		err = manager.RemoveJob(job)
		require.NoError(t, err)
		require.Empty(t, job.GetJobUID())
		time.Sleep(3400 * time.Millisecond)
		require.Equal(t, 3, helloJobFunc.runtimes)
	})
}

func TestRemoveJob(t *testing.T) {
	manager := createTestManager()
	_ = manager.RegisterFunc("hello", &HelloJobFunc{})

	t.Run("remove registered", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/5 * * * * *",
			JobStatus:   JonInfoStatusEnable,
			FuncID:      "hello",
			Params:      `{"name": "test"}`,
			Description: "test job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RegisterJob(job)
		require.NoError(t, err)

		err = manager.RemoveJob(job)
		require.NoError(t, err)
	})

	t.Run("remove unregistered", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			ID:          9999,
			JobUID:      "0",
			Expr:        "*/5 * * * * *",
			JobStatus:   JonInfoStatusEnable,
			FuncID:      "hello",
			Params:      `{"name": "test"}`,
			Description: "test job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RemoveJob(job)
		require.NoError(t, err)
	})
}

func TestNextN(t *testing.T) {
	manager := createTestManager()

	nexts, err := manager.NextN("*/5 * * * * *", 5)
	require.NoError(t, err)
	require.Equal(t, 5, len(nexts))
	for i := 1; i < len(nexts); i++ {
		require.Equal(t, nexts[i].Unix(), nexts[i-1].Add(5*time.Second).Unix())
	}
}

func TestJobListener(t *testing.T) {
	t.Run("success job", func(t *testing.T) {
		manager := createTestManager()
		listener := &MockJobListener{}
		manager.SetListener(listener)

		err := manager.RegisterFunc("success", &SuccessJobFuncer{})
		require.NoError(t, err)
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			JobStatus:   JonInfoStatusEnable,
			FuncID:      "success",
			Params:      `{}`,
			Description: "success job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		err = manager.Start()
		require.NoError(t, err)
		time.Sleep(2400 * time.Millisecond)
		err = manager.RemoveJob(job)
		require.NoError(t, err)
		require.Equal(t, int32(1), atomic.LoadInt32(&listener.startCount))
		require.Equal(t, int32(1), atomic.LoadInt32(&listener.successCount))
		require.Equal(t, int32(0), atomic.LoadInt32(&listener.failCount))
	})

	t.Run("fail job", func(t *testing.T) {
		manager := createTestManager()
		listener := &MockJobListener{}
		manager.SetListener(listener)

		err := manager.RegisterFunc("fail", &FailJobFuncer{})
		require.NoError(t, err)
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			JobStatus:   JonInfoStatusEnable,
			FuncID:      "fail",
			Params:      `{}`,
			Description: "fail job",
			InvokeTimes: JobInfoInvokeUnlimit,
			CreatedAt:   &now,
		}
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		err = manager.Start()
		require.NoError(t, err)
		time.Sleep(2400 * time.Millisecond)
		require.Equal(t, int32(1), atomic.LoadInt32(&listener.startCount))
		require.Equal(t, int32(0), atomic.LoadInt32(&listener.successCount))
		require.Equal(t, int32(1), atomic.LoadInt32(&listener.failCount))
	})
}

func TestSystemJob(t *testing.T) {
	var err error
	manager := createTestManager()
	err = manager.Start()
	require.NoError(t, err)

	manager.SetListener(&PrintLogListener{})

	funcID := "demo_system_job"
	demoSystemJobFunc := &DemoSystemJobFunc{}
	err = manager.RegisterFunc(funcID, demoSystemJobFunc)
	require.NoError(t, err)

	now := time.Now()
	jobInfo := &JobInfo{
		IsSystem:    true,
		Expr:        "*/3 * * * * *",
		JobStatus:   JonInfoStatusEnable,
		FuncID:      funcID,
		Params:      `{"name": "demo_system_job"}`,
		Description: "demo system job",
		InvokeTimes: JobInfoInvokeUnlimit,
		CreatedAt:   &now,
	}
	err = manager.RegisterJob(jobInfo)
	require.NoError(t, err)
	time.Sleep(3400 * time.Millisecond)
	require.Equal(t, 1, demoSystemJobFunc.runtimes)

	err = manager.RemoveJob(jobInfo)
	require.NoError(t, err)

	repo := manager.jobRepository.(*JobInMemoryRepo)

	jobInfo, err = repo.GetByID(jobInfo.ID)
	require.NoError(t, err)
	require.Equal(t, JobInfoStatusDisable, jobInfo.JobStatus)

	err = repo.Create(jobInfo)
	require.Error(t, err)
	require.ErrorContains(t, err, "system job")

	jobInfo.JobStatus = JonInfoStatusEnable
	jobInfo.InvokeTimes = 20
	err = repo.Update(jobInfo)
	jobInfo, err = repo.GetByID(jobInfo.ID)
	require.NoError(t, err)
	require.Equal(t, JobInfoInvokeUnlimit, jobInfo.InvokeTimes)
	time.Sleep(3400 * time.Millisecond)
	require.Equal(t, 2, demoSystemJobFunc.runtimes)

	err = repo.Delete(jobInfo.ID)
	require.Error(t, err)
	require.ErrorContains(t, err, "system job")

	err = manager.RegisterJob(jobInfo)
	require.NoError(t, err)
	oldId := jobInfo.ID
	jobInfo, err = repo.GetByID(jobInfo.ID)
	require.NoError(t, err)
	require.Equal(t, oldId, jobInfo.ID)
	err = repo.DeleteBatch([]uint64{jobInfo.ID})
	require.NoError(t, err)
	jobInfo, err = repo.GetByID(jobInfo.ID)
	require.NoError(t, err)
	require.Equal(t, JobInfoStatusDisable, jobInfo.JobStatus)
}

type DemoSystemJobFunc struct {
	JsonParamsJobFunc
	runtimes int
}

func (t *DemoSystemJobFunc) UnmarshalParams(params string) (any, error) {
	return params, nil
}

func (t *DemoSystemJobFunc) Invoke(ctx *JobContext) error {
	t.runtimes++
	log.Printf("demo system job: %s", ctx.Params)
	return nil
}

func createTestManager() *JobManager {
	cronExprParser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronI := cron.New(
		cron.WithParser(cronExprParser),
		cron.WithLogger(cron.VerbosePrintfLogger(log.New(os.Stdout, "[cron] ", log.LstdFlags|log.Lmsgprefix))),
	)
	repo := NewJobInMemoryRepo()
	manager := NewJobManager(cronI, cronExprParser, repo)
	err := repo.AfterProperties(manager)
	if err != nil {
		panic(err)
	}
	return manager
}

type SuccessJobFuncer struct {
	JsonParamsJobFunc
}

func (s *SuccessJobFuncer) Invoke(ctx *JobContext) error {
	return nil
}

func (s *SuccessJobFuncer) UnmarshalParams(_ string) (any, error) {
	return map[string]interface{}{}, nil
}

type FailJobFuncer struct {
	JsonParamsJobFunc
}

func (f *FailJobFuncer) Invoke(ctx *JobContext) error {
	return errors.New("intentional failure")
}

func (f *FailJobFuncer) UnmarshalParams(_ string) (any, error) {
	return map[string]interface{}{}, nil
}

type MockJobListener struct {
	startCount   int32
	failCount    int32
	successCount int32
}

func (m *MockJobListener) OnStart(startAt time.Time, job Job) {
	atomic.AddInt32(&m.startCount, 1)
}

func (m *MockJobListener) OnFail(startAt, endAt time.Time, job Job, err error) {
	atomic.AddInt32(&m.failCount, 1)
}

func (m *MockJobListener) OnSuccess(startAt, endAt time.Time, job Job) {
	atomic.AddInt32(&m.successCount, 1)
}

type HelloJobFunc struct {
	runtimes   int
	latestName string
	funcId     string
	JsonParamsJobFunc
}
type HelloJobFuncParams struct {
	Name string `json:"name"`
}

func (t *HelloJobFunc) Invoke(c *JobContext) error {
	params := c.Params.(*HelloJobFuncParams)
	t.latestName = params.Name
	log.Printf("helloJob: %d: hello croner, hi %s", t.runtimes, params.Name)
	t.runtimes++
	return nil
}
func (t *HelloJobFunc) UnmarshalParams(paramsStr string) (any, error) {
	params := &HelloJobFuncParams{}
	err := json.Unmarshal([]byte(paramsStr), params)
	return params, err
}

type CloneableHelloJobFunc struct {
	runtimes int
	JsonParamsJobFunc
}
type CloneableHelloJobFuncParams struct {
	Name string `json:"name"`
}

func (t *CloneableHelloJobFunc) Invoke(c *JobContext) error {
	params := c.Params.(*CloneableHelloJobFuncParams)
	log.Printf("helloJob: %d: hello croner, hi %s", t.runtimes, params.Name)
	t.runtimes++
	return nil
}
func (t *CloneableHelloJobFunc) UnmarshalParams(paramsStr string) (any, error) {
	params := &CloneableHelloJobFuncParams{}
	err := json.Unmarshal([]byte(paramsStr), params)
	return params, err
}
func (t *CloneableHelloJobFunc) CloneJobFunc() JobFunc {
	funer := &CloneableHelloJobFunc{}
	return funer
}

type PrintLogListener struct {
}

func (t *PrintLogListener) OnStart(startAt time.Time, job Job) {
	jobInfo := job.(*JobInfo)
	log.Printf("job start: %v, startAt: %v\n", jobInfo.ID, startAt)
}
func (t *PrintLogListener) OnFail(startAt, endAt time.Time, job Job, err error) {
	jobInfo := job.(*JobInfo)
	log.Printf("job fail: %v, endAt: %v, err: %s", jobInfo.ID, endAt, err)
}
func (t *PrintLogListener) OnSuccess(startAt, endAt time.Time, job Job) {
	jobInfo := job.(*JobInfo)
	log.Printf("job success: %v, endAt: %v", jobInfo.ID, endAt)
}
