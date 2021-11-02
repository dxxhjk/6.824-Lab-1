package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type TaskType int32

const (
	UNKNOWN		TaskType = -1
	WAITING		TaskType = 0
	MAP_TASK	TaskType = 1
	REDUCE_TASK	TaskType = 2
)

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskReply struct {
	TaskType TaskType
	MapTaskFileName string
	ReduceTaskFileNameList []string
	ReduceTaskId int
	ReduceNum int
}

type TaskFinishedArgs struct {
	TaskType TaskType
	WorkerId int
}

type TaskFinishedReply struct {
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
