package mr

import (
	"log"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.

	mapTaskFileNameList []string
	mapTaskToDo []string
	mapTaskDoing map[int]string
	mapTaskFinished []string
	reduceTaskToDo []int
	reduceTaskDoing map[int]int
	reduceTaskFinished []int
	mapNum int
	reduceNum int
	lock sync.Mutex

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	c.lock.Lock()
	if len(c.mapTaskToDo) != 0 {
		reply.TaskType = MAP_TASK
		reply.MapTaskFileName = c.mapTaskToDo[0]
		reply.ReduceNum = c.reduceNum
		c.mapTaskDoing[args.WorkerId] = c.mapTaskToDo[0]
		c.mapTaskToDo = c.mapTaskToDo[1:]

	} else if len(c.reduceTaskToDo) != 0 {
		reply.TaskType = REDUCE_TASK
		var reduceTaskNameList []string
		for _, mapTaskFileName := range c.mapTaskFileNameList {
			reduceTaskName := "mr-intermediate-" + mapTaskFileName + "-" + strconv.Itoa(c.reduceTaskToDo[0])
			reduceTaskNameList = append(reduceTaskNameList, reduceTaskName)
		}
		reply.ReduceTaskFileNameList = reduceTaskNameList
		reply.ReduceTaskId = c.reduceTaskToDo[0]
		reply.ReduceNum = c.reduceNum
		c.reduceTaskDoing[args.WorkerId] = c.reduceTaskToDo[0]
		c.reduceTaskToDo = c.reduceTaskToDo[1:]

	} else {
		reply.TaskType = WAITING
	}
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) TaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {

	if args.TaskType == MAP_TASK {
		c.mapTaskFinished = append(c.mapTaskFinished, c.mapTaskDoing[args.WorkerId])
		log.Println("Map task finished: ", c.mapTaskDoing[args.WorkerId])
		delete(c.mapTaskDoing, args.WorkerId)

	} else if args.TaskType == REDUCE_TASK {
		c.reduceTaskFinished = append(c.reduceTaskFinished, c.reduceTaskDoing[args.WorkerId])
		log.Println("Reduce task finished: ", c.reduceTaskDoing[args.WorkerId])
		delete(c.mapTaskDoing, args.WorkerId)
	}
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
	ret = len(c.reduceTaskToDo) == 0 && len(c.reduceTaskDoing) == 0

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
	c.mapTaskToDo = []string{}
	c.mapTaskDoing = make(map[int]string)
	c.mapTaskFinished = []string{}
	c.reduceTaskToDo = []int{}
	c.reduceTaskDoing = make(map[int]int)
	c.reduceTaskFinished = []int{}
	c.mapTaskFileNameList = c.mapTaskToDo
	c.reduceNum = nReduce

	for _, file := range files {
		c.mapTaskToDo = append(c.mapTaskToDo, file)
	}
	c.mapNum = len(c.mapTaskToDo)
	for i := 0; i < nReduce; i++ {
		c.reduceTaskToDo = append(c.reduceTaskToDo, i)
	}

	c.server()
	return &c
}
