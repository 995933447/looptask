package looptask

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	uuid "github.com/satori/go.uuid"
	"testing"
	"time"
)

type TestSubTask struct {
	Id            string
	Retry         bool
	PrintBoundary bool
	Attempted     int
}

func (t *TestSubTask) Exec() (nextInterval time.Duration, retry bool) {
	if t.PrintBoundary {
		fmt.Printf("=================\n")
	}
	fmt.Printf("sub task (Id:%s) exec. attempted:%d\n", t.Id, t.Attempted)
	if t.Retry {
		//fmt.Printf("i am ready to retry, %s\n", t.Id)
	}

	t.Attempted++
	return time.Second, true
	//return time.Second * 1, t.Retry
}

var _ SubTask = (*TestSubTask)(nil)

type TestTask struct {
	Id string
}

func (t *TestTask) Ready() bool {
	fmt.Printf("task(id:%s) init.\n", t.Id)
	return true
}

func (t TestTask) GetSubTaskConcur() uint32 {
	//return 3
	return 1
}

func (t TestTask) NextBatchSubTasks() []SubTask {
	return []SubTask{
		//&TestSubTask{
		//	Id:    t.Id + "@subtask:1",
		//	Retry: true,
		//},
		&TestSubTask{
			Id:            t.Id + "@subtask:2",
			PrintBoundary: true,
		},
		&TestSubTask{
			Id:    t.Id + "@subtask:3",
			Retry: true,
		},
		&TestSubTask{
			Id:            t.Id + "@subtask:4",
			PrintBoundary: true,
		},
	}
}

func (t TestTask) OnProceed() {
	fmt.Printf("task(id:%s) on proceed.\n", t.Id)
}

var _ Task = (*TestTask)(nil)

func TestLoopTask(t *testing.T) {
	var refreshTimes int
	sched, err := NewSched(SchedCfg{
		Name:             "loopTask1",
		ConcurTaskMax:    3,
		GrpConcurTaskMax: 1,
		//GrpConcurTaskMax: 2,
		NewTaskHdl: func(taskId, taskGrp string) (Task, error) {
			return &TestTask{
				Id: taskId,
			}, nil
		},
		RefreshTasksHdl: func() (mapGrpToTaskIds map[string][]string) {
			if refreshTimes > 0 {
				return map[string][]string{}
			}
			refreshTimes++
			taskIdMap := map[string][]string{
				//"1": {"@1", "@2", "@3", "@4"},
				//"2": {"@1", "@2", "@3"},
				"3": {"@1", "@2"},
				"4": {"@1"},
			}
			newTaskIdMap := map[string][]string{}
			for grpId, taskIds := range taskIdMap {
				var newTaskIds []string
				for _, taskId := range taskIds {
					newTaskIds = append(newTaskIds, "grp:"+grpId+"@task:"+taskId+"@"+uuid.NewV4().String()+"@refresh:"+fmt.Sprintf("%d", refreshTimes))
				}
				newTaskIdMap[grpId] = newTaskIds
			}
			return newTaskIdMap
		},
		CheckTaskStoppedHdl: func(taskIds []string) (stoppedTaskIds []string) {
			return nil
		},
		RefreshTaskIntervalMs:    1000,
		CheckTasksStopIntervalMs: 1000,
		SubTaskMaxAttempt:        300,
		ElectRedis:               GetRedisPool(),
	})
	if err != nil {
		t.Errorf("New task err: %s", err.Error())
		return
	}
	sched.SetGrpConcur("3", 1)
	sched.Run()
	select {}
}

func GetRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:         2,
		MaxActive:       0, //when zero,there's no limit. https://godoc.org/github.com/garyburd/redigo/redis#Pool
		IdleTimeout:     time.Minute * 3,
		MaxConnLifetime: time.Minute * 10,
		Wait:            true,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", "localhost:6379")
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			return nil
		},
	}
}
