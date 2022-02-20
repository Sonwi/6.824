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

type TaskStatus int8

const (
	Idle       TaskStatus = 0
	InProgress TaskStatus = 1
	Completed  TaskStatus = 2
)

type Task struct {
	inputFileName []string   //输入文件
	status        TaskStatus //任务状态
}

type Coordinator struct {
	// Your definitions here.
	MapTask    []Task
	ReduceTask []Task
	NReduce    int //记录reduce worker数目 关系到map输出的文件数
}

var coordinateLock sync.RWMutex //用于控制Coordinate结构内变量的访问
var lockBool sync.RWMutex       //控制mapDone 和 reduceDone

//标识任务完成状态
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
				reply.Content = mapTask.inputFileName
				reply.Idx = mapStatus
				reply.NReduce = c.NReduce

				coordinateLock.Lock()
				mapTask.status = InProgress
				coordinateLock.Unlock()
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
				reply.Content = redTask.inputFileName
				reply.NReduce = c.NReduce

				coordinateLock.Lock()
				redTask.status = InProgress
				coordinateLock.Unlock()

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

//检查任务状态
func CheckStatus(t *Task) {
	time.Sleep(10 * time.Second)

	coordinateLock.Lock()
	defer coordinateLock.Unlock()

	if t.status != Completed {
		t.status = Idle
	}
}

func (c *Coordinator) HandFinishInfo(args *FinishReq, reply *FinishReply) error {
	idx := args.Idx
	if args.TypeName == "map" {
		coordinateLock.Lock()

		if c.MapTask[idx].status != Completed {
			for i := range c.ReduceTask {
				c.ReduceTask[i].inputFileName = append(c.ReduceTask[i].inputFileName, args.Ret[i])
			}
			c.MapTask[idx].status = Completed
		}

		coordinateLock.Unlock()
	}

	if args.TypeName == "reduce" {

		coordinateLock.Lock()

		if c.ReduceTask[idx].status != Completed {
			c.ReduceTask[idx].status = Completed
		}

		coordinateLock.Unlock()
	}
	return nil
}

//读取任务状态 分配任务 返回值中int如果为正数则是worker id, 为-1表示该类任务完成，为-2表示所有任务都在执行
func AssignTask(tasks []Task) (*Task, int) {
	coordinateLock.RLock()
	defer coordinateLock.RUnlock()
	for i := range tasks {
		if tasks[i].status == Idle {
			return &tasks[i], i
		}
	}
	if IsWorkDone(tasks) {
		return nil, -1 //work done
	} else {
		return nil, -2 //all in progress wait
	}
}

//用于判断某一类任务是否完成
func IsWorkDone(tasks []Task) bool {
	coordinateLock.RLock()
	defer coordinateLock.RUnlock()
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
		c.MapTask[i].inputFileName = append(c.MapTask[i].inputFileName, files[i])
		c.MapTask[i].status = Idle
	}

	c.ReduceTask = make([]Task, nReduce)
	for i := range c.ReduceTask {
		c.ReduceTask[i].status = Idle
	}

	c.server()
	return &c
}
