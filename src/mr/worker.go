package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func iHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	workerId := os.Getpid()
	args, reply := GetTaskArgs{}, GetTaskReply{}
	args.WorkerId = workerId
	rpcSuccess := call("Coordinator.GetTask", &args, &reply)

	for rpcSuccess && reply.TaskType != UNKNOWN {
		if reply.TaskType == MAP_TASK {
			log.Println("Start processing map task: " + reply.MapTaskFileName)
			handleMapTask(mapf, reducef, reply.ReduceNum, reply.MapTaskFileName)

		} else if reply.TaskType == REDUCE_TASK {
			log.Println("Start processing map task: ", reply.ReduceTaskFileNameList)
			handleReduceTask(reducef, reply.ReduceTaskId, reply.ReduceTaskFileNameList)

		} else if reply.TaskType == WAITING {
			log.Println("Waiting for new task")
			time.Sleep(1 * time.Second)
		}
		reply.TaskType = WAITING
		rpcSuccess = call("Coordinator.GetTask", &args, &reply)
	}
}

func handleMapTask(mapf func(string, string) []KeyValue,
		reducef func(string, []string) string, reduceNum int, mapTaskFileName string) {

	// 打开 map 任务文件读数据
	file, err := os.Open(mapTaskFileName)
	if err != nil {
		log.Fatalf("cannot open %v",mapTaskFileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapTaskFileName)
	}
	err = file.Close()
	if err != nil {
		log.Fatal("Close file error: ", err)
		return
	}

	// 进行 map 任务
	kva := mapf(mapTaskFileName, string(content))
	var intermediate []KeyValue
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))

	// 按 reduce 数创建 map 结果的中间文件
	var intermediateFileNameList []string
	var intermediateFileList []*os.File
	for i := 0; i < reduceNum; i++ {
		intermediateFileName := "mr-intermediate-" + mapTaskFileName + "-" + strconv.Itoa(i) + "-tmp"
		intermediateFileNameList = append(intermediateFileNameList, intermediateFileName)
		intermediateFile, _ := os.Create(intermediateFileName)
		intermediateFileList = append(intermediateFileList, intermediateFile)
	}

	// 将 map 结果按照 key 的 Hash 写入临时中间文件
	reduceKVList := make([][]KeyValue, 10)
	var encList []*json.Encoder
	for i := 0; i < len(intermediate); {

		// 用 reduce 函数合并 map 结果中的相同 key
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// 打开 json 临时中间文件，并将 map 结果写入
		var kv KeyValue
		kv.Key = intermediate[i].Key
		kv.Value = output
		reduceKVList[iHash(intermediate[i].Key) % 10] = append(reduceKVList[iHash(intermediate[i].Key) % 10], kv)
		//log.Println(iHash(intermediate[i].Key) % 10)
		i = j
	}
	for i := 0; i < len(reduceKVList); i++ {
		log.Println(reduceKVList[i][0], reduceKVList[i][1])
	}
	for i := 0; i < reduceNum; i++ {
		encList = append(encList, json.NewEncoder(intermediateFileList[i]))
		for _, reduceKV := range reduceKVList {
			err = encList[i].Encode(reduceKV)
			if err != nil {
				log.Fatal("Encode error: ", err, i)
				return
			}
		}
	}

	// 关闭临时文件并更名，去掉 -tmp
	for _, file := range intermediateFileList {
		err = file.Close()
		if err != nil {
			log.Fatal("Close file error: ", err)
			return
		}
	}
	for i := 0; i < reduceNum; i++ {
		intermediateFileName := "mr-intermediate-" + mapTaskFileName + "-" + strconv.Itoa(i)
		err = os.Rename(intermediateFileNameList[i], intermediateFileName)
		if err != nil {
			log.Fatal("Rename file error: ", err)
			return
		}
	}

	// 回调 coordinator 的 taskFinish 通知任务完成
	args, reply := TaskFinishedArgs{}, TaskFinishedReply{}
	args.WorkerId = os.Getpid()
	call("Coordinator.TaskFinished", &args, &reply)
}

func handleReduceTask(reducef func(string, []string) string, reduceTaskId int, reduceTaskFileNameList []string) {

	// 读出所需中间文件所有的数据
	var kva []KeyValue
	for _, reduceTaskFileName := range reduceTaskFileNameList {
		// 等待map任务写入文件完成后尝试打开文件直到成功为止
		file, err := os.Open(reduceTaskFileName)
		for err != nil {
			time.Sleep(400)
			file, err = os.Open(reduceTaskFileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))

	oName := "mr-out-" + strconv.Itoa(reduceTaskId)
	oFile, _ := os.Create(oName)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(oFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	oFile.Close()
	args, reply := TaskFinishedArgs{}, TaskFinishedReply{}
	args.WorkerId = os.Getpid()
	call("Coordinator.TaskFinished", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
