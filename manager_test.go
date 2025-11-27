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

func TestJobRepository(t *testing.T) {
	repo := NewJobInMemoryRepo()
	manager := createTestManagerWithRepo(repo)
	_ = manager.RegisterFuner("hello", &HelloJobFuner{})

	now := time.Now()
	job := &JobInfo{
		Expr:        "*/5 * * * * *",
		TaskStatus:  TaskStatusEnable,
		FuncID:      "hello",
		Params:      `{"name": "test"}`,
		Description: "test job",
		InvokeTimes: TaskInvokeUnlimit,
		CreatedAt:   &now,
	}
	err := manager.RegisterJob(job)
	require.NoError(t, err)

	job2, err := repo.GetJob(job.ID)
	require.NoError(t, err)
	require.NotNil(t, job2)
}

func TestJobManager(t *testing.T) {
	var err error

	cronExprParser := cron.NewParser(
		cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
	)
	cronI := cron.New(
		cron.WithParser(cronExprParser),
		cron.WithLogger(cron.VerbosePrintfLogger(log.New(os.Stdout, "cron: ", log.LstdFlags))),
	)
	repo := NewJobInMemoryRepo()

	manager := NewJobManager(cronI, cronExprParser, repo)

	manager.SetListener(&PrintLogListener{})

	err = manager.RegisterFuner("hello", &HelloJobFuner{})
	require.NoError(t, err)

	now := time.Now()
	jobInfo := &JobInfo{
		Expr:        "0/5 * * * * *",
		TaskStatus:  TaskStatusEnable,
		FuncID:      "hello",
		Params:      `{"name": "sb"}`,
		Description: "a test job",
		InvokeTimes: TaskInvokeUnlimit,
		CreatedAt:   &now,
	}
	err = manager.RegisterJob(jobInfo)
	require.NoError(t, err)
	nexts, err := manager.NextN(jobInfo.Expr, 10)
	require.NoError(t, err)
	for i, next := range nexts {
		t.Logf("%d/%d: %s", i+1, len(nexts), next)
	}

	manager.Start()
	time.Sleep(60 * time.Second)
}

func TestRegisterFuner(t *testing.T) {
	manager := createTestManager()

	t.Run("ok", func(t *testing.T) {
		err := manager.RegisterFuner("test_func", &HelloJobFuner{})
		require.NoError(t, err)
		require.NotNil(t, manager.funcRegistry["test_func"])
	})

	t.Run("repeat fail", func(t *testing.T) {
		err := manager.RegisterFuner("test_func", &HelloJobFuner{})
		require.Error(t, err)
		require.ErrorIs(t, err, ErrFuncExists)
	})

	t.Run("register many funcers", func(t *testing.T) {
		err := manager.RegisterFuner("func1", &HelloJobFuner{})
		require.NoError(t, err)
		err = manager.RegisterFuner("func2", &HelloJobFuner{})
		require.NoError(t, err)
		require.Equal(t, 3, len(manager.funcRegistry))
	})
}

func TestRegisterJob(t *testing.T) {
	var err error
	manager := createTestManager()
	manager.Start()
	helloJobFuner := &HelloJobFuner{}
	err = manager.RegisterFuner("hello", helloJobFuner)
	require.NoError(t, err)

	t.Run("no exists func", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			TaskStatus:  TaskStatusEnable,
			FuncID:      "non_exist",
			Params:      `{"name": "test"}`,
			Description: "test job",
			InvokeTimes: TaskInvokeUnlimit,
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
			TaskStatus:  TaskStatusEnable,
			FuncID:      "hello",
			Params:      `{"name": "test"}`,
			Description: "test job",
			InvokeTimes: TaskInvokeUnlimit,
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
			TaskStatus:  TaskStatusEnable,
			FuncID:      "hello",
			Params:      `{invalid json}`,
			Description: "test job",
			InvokeTimes: TaskInvokeUnlimit,
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
			TaskStatus:  TaskStatusDisable,
			FuncID:      "hello",
			Params:      `{"name": "test"}`,
			Description: "disabled job",
			InvokeTimes: TaskInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RegisterJob(job)
		require.NoError(t, err)
		require.Empty(t, job.GetJobUID())
		time.Sleep(6 * time.Second)
		require.Equal(t, 0, helloJobFuner.runtimes)
	})

	t.Run("enable_update_disable_remove job", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			TaskStatus:  TaskStatusEnable,
			FuncID:      "hello",
			Params:      `{"name": "original"}`,
			Description: "original job",
			InvokeTimes: TaskInvokeUnlimit,
			CreatedAt:   &now,
		}
		err := manager.RegisterJob(job)
		require.NoError(t, err)
		require.NotEmpty(t, job.GetJobUID())
		time.Sleep(2400 * time.Millisecond)
		require.Equal(t, 1, helloJobFuner.runtimes)
		require.Equal(t, "original", helloJobFuner.latestName)

		oldUID := job.GetJobUID()
		job.Expr = "*/3 * * * * *"
		job.Params = `{"name": "updated"}`
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		require.NotEmpty(t, job.GetJobUID())
		require.NotEqual(t, oldUID, job.GetJobUID())
		time.Sleep(3400 * time.Millisecond)
		require.Equal(t, 2, helloJobFuner.runtimes)
		require.Equal(t, "updated", helloJobFuner.latestName)

		job.TaskStatus = TaskStatusDisable
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		require.Empty(t, job.GetJobUID())
		time.Sleep(3400 * time.Millisecond)
		require.Equal(t, 2, helloJobFuner.runtimes)
		require.Equal(t, "updated", helloJobFuner.latestName)

		job.TaskStatus = TaskStatusEnable
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		require.NotEmpty(t, job.GetJobUID())
		time.Sleep(3400 * time.Millisecond)
		require.Equal(t, 3, helloJobFuner.runtimes)
		require.Equal(t, "updated", helloJobFuner.latestName)

		err = manager.RemoveJob(job)
		require.NoError(t, err)
		require.Empty(t, job.GetJobUID())
		time.Sleep(3400 * time.Millisecond)
		require.Equal(t, 3, helloJobFuner.runtimes)
	})
}

func TestRemoveJob(t *testing.T) {
	manager := createTestManager()
	_ = manager.RegisterFuner("hello", &HelloJobFuner{})

	t.Run("remove registered", func(t *testing.T) {
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/5 * * * * *",
			TaskStatus:  TaskStatusEnable,
			FuncID:      "hello",
			Params:      `{"name": "test"}`,
			Description: "test job",
			InvokeTimes: TaskInvokeUnlimit,
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
			TaskUID:     "0",
			Expr:        "*/5 * * * * *",
			TaskStatus:  TaskStatusEnable,
			FuncID:      "hello",
			Params:      `{"name": "test"}`,
			Description: "test job",
			InvokeTimes: TaskInvokeUnlimit,
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

		err := manager.RegisterFuner("success", &SuccessJobFuncer{})
		require.NoError(t, err)
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			TaskStatus:  TaskStatusEnable,
			FuncID:      "success",
			Params:      `{}`,
			Description: "success job",
			InvokeTimes: TaskInvokeUnlimit,
			CreatedAt:   &now,
		}
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		manager.Start()
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

		err := manager.RegisterFuner("fail", &FailJobFuncer{})
		require.NoError(t, err)
		now := time.Now()
		job := &JobInfo{
			Expr:        "*/2 * * * * *",
			TaskStatus:  TaskStatusEnable,
			FuncID:      "fail",
			Params:      `{}`,
			Description: "fail job",
			InvokeTimes: TaskInvokeUnlimit,
			CreatedAt:   &now,
		}
		err = manager.RegisterJob(job)
		require.NoError(t, err)
		manager.Start()
		time.Sleep(2400 * time.Millisecond)
		require.Equal(t, int32(1), atomic.LoadInt32(&listener.startCount))
		require.Equal(t, int32(0), atomic.LoadInt32(&listener.successCount))
		require.Equal(t, int32(1), atomic.LoadInt32(&listener.failCount))
	})
}

func createTestManager() *JobManager {
	cronExprParser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronI := cron.New(cron.WithParser(cronExprParser))
	repo := NewJobInMemoryRepo()
	return NewJobManager(cronI, cronExprParser, repo)
}

func createTestManagerWithRepo(repo JobRepository) *JobManager {
	cronExprParser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	cronI := cron.New(cron.WithParser(cronExprParser))
	return NewJobManager(cronI, cronExprParser, repo)
}

type SuccessJobFuncer struct{}

func (s *SuccessJobFuncer) Invoke(ctx *JobContext) error {
	return nil
}

func (s *SuccessJobFuncer) UnmarshalParams(_ string) (any, error) {
	return map[string]interface{}{}, nil
}

type FailJobFuncer struct{}

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

func (m *MockJobListener) OnStart(job Job) {
	atomic.AddInt32(&m.startCount, 1)
}

func (m *MockJobListener) OnFail(job Job, err error) {
	atomic.AddInt32(&m.failCount, 1)
}

func (m *MockJobListener) OnSuccess(job Job) {
	atomic.AddInt32(&m.successCount, 1)
}

type HelloJobFuner struct {
	runtimes   int
	latestName string
}
type HelloJobFunerParams struct {
	Name string `json:"name"`
}

func (t *HelloJobFuner) Invoke(c *JobContext) error {
	params := c.Params.(*HelloJobFunerParams)
	t.latestName = params.Name
	log.Printf("helloJob: %d: hello croner, hi %s", t.runtimes, params.Name)
	t.runtimes++
	return nil
}
func (t *HelloJobFuner) UnmarshalParams(paramsStr string) (any, error) {
	params := &HelloJobFunerParams{}
	err := json.Unmarshal([]byte(paramsStr), params)
	return params, err
}

type CloneableHelloJobFuner struct {
	runtimes int
}
type CloneableHelloJobFunerParams struct {
	Name string `json:"name"`
}

func (t *CloneableHelloJobFuner) Invoke(c *JobContext) error {
	params := c.Params.(*CloneableHelloJobFunerParams)
	log.Printf("helloJob: %d: hello croner, hi %s", t.runtimes, params.Name)
	t.runtimes++
	return nil
}
func (t *CloneableHelloJobFuner) UnmarshalParams(paramsStr string) (any, error) {
	params := &CloneableHelloJobFunerParams{}
	err := json.Unmarshal([]byte(paramsStr), params)
	return params, err
}
func (t *CloneableHelloJobFuner) CloneJobFuner() JobFuner {
	funer := &CloneableHelloJobFuner{}
	return funer
}

type PrintLogListener struct {
}

func (t *PrintLogListener) OnStart(job Job) {
	log.Printf("job start: %v\n", job.GetJobId())
}
func (t *PrintLogListener) OnFail(job Job, err error) {
	log.Printf("job fail: %v, err: %s", job.GetJobId(), err)
}
func (t *PrintLogListener) OnSuccess(job Job) {
	log.Printf("job success: %v", job.GetJobId())
}
