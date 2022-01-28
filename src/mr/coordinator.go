package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int32

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Completed  TaskStatus = 2
)

type Task struct {
	name   []string
	status TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	MapTask    []Task
	ReduceTask []Task
	NReduce    int
}

var lock sync.RWMutex

var lockBool sync.RWMutex
var mapDone bool = false
var reduceDone bool = false

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) HandWorkerReq(args *ReqArgs, reply *ReqReply) error {
	if args.ReqNumber == 1 {
		lockBool.RLock()
		if !mapDone {
			if mapTask, mapStatus := AssignTask(c.MapTask); mapStatus >= 0 {
				// lock.Lock()

				reply.TypeName = "map"
				reply.Content = mapTask.name
				reply.Idx = mapStatus
				reply.NReduce = c.NReduce

				lock.Lock()
				mapTask.status = InProgress
				lock.Unlock()
				// lock.Unlock()

				go CheckStatus(mapTask)

			} else if mapStatus == -1 {
				lockBool.RUnlock()
				lockBool.Lock()
				mapDone = true
				lockBool.Unlock()
				lockBool.RLock()
				// lock.Unlock()
			} else {
				// lock.Lock()
				reply.TypeName = "allinprogress"
				// lock.Unlock()
			}
		} else if !reduceDone {
			if redTask, redStatus := AssignTask(c.ReduceTask); redStatus >= 0 {
				reply.TypeName = "reduce"
				reply.Idx = redStatus
				reply.Content = redTask.name
				reply.NReduce = c.NReduce

				lock.Lock()
				redTask.status = InProgress
				lock.Unlock()

				go CheckStatus(redTask)
			} else if redStatus == -1 {
				lockBool.RUnlock()
				lockBool.Lock()
				reduceDone = true
				lockBool.Unlock()
				lockBool.RLock()
			} else {
				// lock.Lock()
				reply.TypeName = "allinprogress"
				// lock.Unlock()
			}
		} else {
			// lock.Lock()
			reply.TypeName = "finish"
			// lock.Unlock()
		}
		lockBool.RUnlock()
	}
	return nil
}

func CheckStatus(t *Task) {
	time.Sleep(10 * time.Second)

	lock.Lock()
	defer lock.Unlock()

	if t.status != Completed {
		t.status = Idle
	}
}

func (c *Coordinator) HandFinishInfo(args *FinishReq, reply *FinishReply) error {
	idx := args.Idx
	if args.TaskStr == "map" {
		//考虑状态
		//写入reduce任务
		lock.Lock()

		if c.MapTask[idx].status != Completed {
			for i := range c.ReduceTask {
				c.ReduceTask[i].name = append(c.ReduceTask[i].name, args.Ret[i])
			}
			c.MapTask[idx].status = Completed
		}

		lock.Unlock()
	}

	if args.TaskStr == "reduce" {

		lock.Lock()

		if c.ReduceTask[idx].status != Completed {
			c.ReduceTask[idx].status = Completed
		}

		lock.Unlock()
	}
	return nil
}

func AssignTask(tasks []Task) (*Task, int) {
	lock.RLock()
	defer lock.RUnlock()
	for i := range tasks {
		if tasks[i].status == Idle {
			return &tasks[i], i
		}
	}
	if WorkDone(tasks) {
		return nil, -1 //work done
	} else {
		return nil, -2 //all in progress wait
	}
}

func WorkDone(tasks []Task) bool {
	lock.RLock()
	defer lock.RUnlock()
	for i := range tasks {
		if tasks[i].status != Completed {
			return false
		}
	}
	return true
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// lock.RLock()
	// defer lock.RUnlock()
	// if WorkDone(c.MapTask) && WorkDone(c.ReduceTask) {
	// 	ret = true
	// }
	lockBool.RLock()
	defer lockBool.RUnlock()
	if mapDone && reduceDone {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.MapTask = make([]Task, len(files))
	for i := range files {
		c.MapTask[i].name = append(c.MapTask[i].name, files[i])
		c.MapTask[i].status = Idle
	}

	c.ReduceTask = make([]Task, nReduce)
	for i := range c.ReduceTask {
		c.ReduceTask[i].status = Idle
	}

	c.server()
	return &c
}
