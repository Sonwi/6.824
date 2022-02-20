package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
// 节点请求任务
type ReqArgs struct {
	ReqNumber int8 //占用一个字节表示请求 为1表示申请任务
}

// type TaskType struct {
// 	TypeName string
// 	Idx      int
// }

// Master回应任务内容
type ReqReply struct {
	TypeName string   // map or reduce or allinprogress
	Idx      int      //worker idx
	Content  []string //file names for work content
	NReduce  int      //reduce worker number
}

// 报告任务完成情况
type FinishReq struct {
	TypeName string   //map or reduce
	Ret      []string //output files(map or reduce)
	Idx      int      //worker idx (map or reduce)
}

//Master 回应worker
type FinishReply struct {
	Done bool //for reply
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
