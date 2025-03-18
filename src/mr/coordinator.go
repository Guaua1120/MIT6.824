package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
	"fmt"
	"strconv"
    "strings"
)

var mu sync.Mutex  //全局锁变量

type State int
//任务的状态类型
const (
	Idle      State = 0
	Continued State = 1
	Completed State = 2
)

//协调器的阶段类型
type phase int
const (
	mapPhase       phase = 0
	reducePhase    phase = 1
	completedPhase phase = 2
)

//worker的状态类型
type WorkType int
const (
	MapWork    WorkType = 0
	ReduceWork WorkType = 1 
	Wait       WorkType = 2
	ExitWork  WorkType  = 3 
)

type Task struct {
	Filename  		string
	TaskId        	int
	ReduceNum 		int //传入reducer的数量用于hash
	WorkType    	WorkType
	FileSlice  		[]string
}

// TaskMetaInfo 保存任务的元数据
type TaskMetaInfo struct {
	state     State     // 任务的状态
	TaskAdr   *Task     // 传入任务的指针,为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
	StartTime time.Time // 维护任务的开始时间
}

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 通过下标hash快速定位
}

type Coordinator struct {
	// Your definitions here.
	files     			[]string  //传入的文件数组
	TaskId				int       //初始化时为每个task分配唯一id
	ReduceNum 			int
	WorkPhase 			phase   //目前整个框架应该处于什么样的工作状态
	ReduceTaskChannel 	chan *Task     // reduce阶段的管道
	MapTaskChannel    	chan *Task     // map阶段的管道
	taskMetaHolder    	TaskMetaHolder // 存着task的元信息
}

//分配任务时转换框架的状态
func (c *Coordinator) toNextPhase() {
	if c.WorkPhase == mapPhase {
		c.makeReduceTasks()
		c.WorkPhase = reducePhase
	} else if c.WorkPhase == reducePhase {
		c.WorkPhase = completedPhase
	}
}

// 判断给定任务是否在工作，并修正其目前任务信息状态
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, exist := t.MetaMap[taskId]
	if !exist || taskInfo.state != Idle {
		//不存在或者任务正在做或者已完成
		return false
	}
	taskInfo.state = Continued
	taskInfo.StartTime = time.Now()
	return true
}

// 检查多少个任务做了包括（map、reduce）,
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)

	// 遍历储存task信息的map
	for _, v := range t.MetaMap {
		// 首先判断任务的类型
		if v.TaskAdr.WorkType == MapWork {
			// 判断任务是否完成,下同
			if v.state == Completed {
				mapDoneNum++
			} else {
				//正在做的和没开始做的都算unDone
				mapUnDoneNum++
			}
		} else if v.TaskAdr.WorkType == ReduceWork {
			if v.state == Completed {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}

	}
	// 如果某一个map或者reduce全部做完了，代表需要切换下一阶段，返回true
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return false

}


// Your code here -- RPC handlers for the worker to call.
// coordinator向worker分配任务
func (c *Coordinator) Request(args *Args, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.WorkPhase{
	case mapPhase:
		{
			if len(c.MapTaskChannel)>0{
				*reply = * <-c.MapTaskChannel  //深拷贝，指针之间不能直接赋值
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
				}
			}else{  //任务分配完了但是还没有完全做完
				reply.WorkType = Wait // 如果map任务被分发完了但是又没完成，此时就将任务设为Wait
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
			break
		}
	case reducePhase:
		{
			if len(c.ReduceTaskChannel)>0 {
				*reply = * <-c.ReduceTaskChannel
				if !c.taskMetaHolder.judgeState(reply.TaskId) {
					fmt.Printf("taskid[ %d ] is running\n", reply.TaskId)
				}
			}else{
				reply.WorkType = Wait // 如果reduce任务被分发完了但是又没完成，此时就将任务设为Wait
				if c.taskMetaHolder.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
			break;
		}
	case completedPhase:
		{
			reply.WorkType = ExitWork
			break;
		}
	default:
		{
			panic("The phase undefined ! ! !")
		}
	}
	return nil
}

// worker完成任务后，向coordinator报告
func (c *Coordinator) Report(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.WorkType{
	case MapWork:
		{
			meta, exist := c.taskMetaHolder.MetaMap[args.TaskId]
			if exist&& meta.state == Continued{
				meta.state = Completed
				fmt.Printf("Map task Id[%v] is completed.\n", args.TaskId)
			}else{
				//fmt.Printf("Map task Id[%v] is finished already!\n", args.TaskId)
			}
		}
	case ReduceWork:
		meta, exist := c.taskMetaHolder.MetaMap[args.TaskId]
		//prevent a duplicated work which returned from another worker
		if exist && meta.state == Continued {
			meta.state = Completed
			fmt.Printf("Reduce task Id[%d] is finished.\n", args.TaskId)
		} else {
			//fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
		}
		break
	default:
		panic("The task type undefined ! ")
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	// Your code here.
	return c.WorkPhase == completedPhase
}

func (c *Coordinator) GenerateTaskId() int{
	id := c.TaskId
	c.TaskId++
	return id
}

// 将接受taskMetaInfo储存进MetaHolder里
func (t *TaskMetaHolder) acceptMeta(TaskInfo *TaskMetaInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]
	if meta != nil {
		fmt.Println("meta contains task which id = ", taskId)  //要放置的位置已经有了其他的Task，error
		return false
	} else {
		t.MetaMap[taskId] = TaskInfo
	}
	return true
}


//中间文件的命名格式为：mr-<mapTaskID>-<reduceTaskID>
//将所有reduceTaskID为reduceNum的文件名放在切片中返回
func selectReduceName(reduceNum int) []string {
	var s []string
	path, err := os.Getwd()
	if err!= nil{
		fmt.Printf("无法获取当前工作目录")
	}
	files, err := os.ReadDir(path)
	if err!=nil {
		fmt.Printf("无法读取当前工作目录下的文件")
	}
	for _, fi := range files {
		// 匹配对应的reduce文件
		if strings.HasPrefix(fi.Name(), "mr-") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

func (c *Coordinator) makeMapTasks(files []string){
	for _,value :=range files{
		id := c.GenerateTaskId()
		task := Task{
			Filename  :value,
			TaskId    :id,
			ReduceNum :c.ReduceNum,
			WorkType  :MapWork,
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Idle, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)
		fmt.Printf("make a map task : %v\n", task.TaskId)
		c.MapTaskChannel <- &task  //将map任务放入map管道
	}
}

func (c *Coordinator) makeReduceTasks() {

	for i := 0; i < c.ReduceNum; i++ {
		id := c.GenerateTaskId()
		task := Task{
			TaskId:    id,
			WorkType:  ReduceWork,
			FileSlice: selectReduceName(i), //告诉work该reduceTask对应的有哪些文件应该被处理
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			state:   Idle, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskMetaHolder.acceptMeta(&taskMetaInfo)

		fmt.Println("make a reduce task : %v", task.TaskId)
		c.ReduceTaskChannel <- &task
	}
}

func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.WorkPhase == completedPhase {
			mu.Unlock()
			break
		}

		for _, v := range c.taskMetaHolder.MetaMap {
			if v.state == Continued && time.Since(v.StartTime) > 10*time.Second {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))
				switch v.TaskAdr.WorkType {
				case MapWork:
					c.MapTaskChannel <- v.TaskAdr
					v.state = Idle
				case ReduceWork:
					c.ReduceTaskChannel <- v.TaskAdr
					v.state = Idle
				}
			}
		}
		mu.Unlock()
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReduceNum:         nReduce,
		WorkPhase:         mapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // (map和reduce)任务的总数应该是files + Reducer的数量
		},
	}
	c.makeMapTasks(files)  //将Map任务放到Map管道中，taskMetaInfo放到taskMetaHolder中

	go c.CrashHandler() //开启新协程监控crash

	
	c.server()
	return &c
}
