package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


// 进行文件读取，执行具体的Map/Reduce逻辑
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	//读取文件
	fileName := response.Filename
	//fmt.Printf("reply.Task.Filename: %v",reply.Task.Filename)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := os.ReadFile(fileName)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	//执行map函数
	kva := mapf(fileName, string(content))

	//创建中间文件，有多少个reduce任务就有多少个list
	intermediate := make([][]KeyValue, response.ReduceNum)
	for _, kv := range kva {
		reduceTask := ihash(kv.Key) % response.ReduceNum
		intermediate[reduceTask] = append(intermediate[reduceTask], kv) //将hash相同的kv放到同一个list/reduce任务中
	}

	//将中间文件以json格式写入磁盘,命名格式为mr-<mapTaskID>-<reduceTaskID>
	//中间文件的个数为nMap*nReduce
	for i := 0; i < response.ReduceNum; i++ {
		interFileName := fmt.Sprintf("mr-%v-%v", response.TaskId, i) //创建格式化的字符串
		interFile, err := os.Create(interFileName)
		if err != nil {
			log.Fatalf("cannot create %v", interFileName)
		}
		enc := json.NewEncoder(interFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		interFile.Close()
	}
}

func shuffle(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}

func DoReduceTask(reducef func(string, []string) string, response *Task) {
	reduceFileNum := response.TaskId
	intermediate := shuffle(response.FileSlice) //读取kvpair并将其排序后返回
	dir, err := os.Getwd()
	if err!=nil{
		fmt.Printf("获取当前工作目录失败")
	}
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), fn)
}

func GetTask() Task{
	args := Args{}
	reply := Task{}
	ok := call("Coordinator.Request",&args,&reply)
	if ok {
		//fmt.Printf("Get task : %v success\n",reply.TaskId)
	}else{
		fmt.Printf("call failed!\n")
	}
	return reply
}


func callDone(response *Task) Task{
	args :=response
	reply := Task{}

	ok := call("Coordinator.Report", &args, &reply)
	if ok {
		//fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}


// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//向coordinator进行请求，取原数据进行处理

	//轮询coordinator 向其发送请求
	for {
		task := GetTask()
		switch task.WorkType {
			case MapWork: //doMap
				DoMapTask(mapf, &task)
				//fmt.Printf("task: %v complete the map work,will to callDone()",task.TaskId)
				callDone(&task)   //做完了需要告知coordinator
			case ReduceWork: //doReduce
				DoReduceTask(reducef, &task)
				callDone(&task)   //做完了需要告知coordinator
			case Wait: //wait
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(1 * time.Second)
			case ExitWork: //completed the total Job
				fmt.Println("Task is all completed...")
				return
			default:
				fmt.Printf("WorkType Error!\n")
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
