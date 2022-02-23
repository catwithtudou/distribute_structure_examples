package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"



const(
	MaxTaskCap = 5
	MaxReduceCap = 5
)

type CoordinatorOp interface {
	WorkerHandler(args *Args,reply *Reply)error
	InterHandler(args *InterFile,reply *Reply)error
	GenerateTask()
}

type Coordinator struct {
	MapTaskCh chan string
	ReduceTaskCh chan int

	FilesStatus map[string]TaskStatus
	MapTaskNum int
	NReduce int
	InterFilename [][]string
	DoneMap bool
	DoneReduce bool
	ReduceTaskStatus map[int]TaskStatus
	Lock *sync.RWMutex
}

func NewCoordinator(files []string,nReduce int)CoordinatorOp{
	c:=&Coordinator{}
	c.MapTaskCh = make(chan string,MaxTaskCap)
	c.ReduceTaskCh = make(chan int,MaxReduceCap)
	c.FilesStatus =make(map[string]TaskStatus)
	c.MapTaskNum = 0
	c.NReduce = nReduce
	c.InterFilename = make([][]string,nReduce)
	c.DoneMap = false
	c.DoneReduce = false
	c.ReduceTaskStatus = make(map[int]TaskStatus)
	c.Lock = new(sync.RWMutex)
	for _,v:=range files{
		c.FilesStatus[v] = UnAllocated
	}
	for i:=0;i<nReduce;i++{
		c.ReduceTaskStatus[i] = UnAllocated
	}
	return c
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator)WorkerHandler(args *Args,reply *Reply)error{

	switch args.WorkType {
	case WorkTask:
		select {
		case filename:=<-c.MapTaskCh:
			reply.Filename =filename
			reply.TaskType = MapTask
			reply.MapNumIndex = c.MapTaskNum
			reply.NReduce = c.NReduce

			c.Lock.Lock()
			c.FilesStatus[filename]=Allocated
			c.MapTaskNum++
			c.Lock.Unlock()
			go c.WorkerTimeCheck(MapTask,filename)
			return nil
		case reduceNum:=<-c.ReduceTaskCh:
			reply.TaskType = ReduceTask
			reply.ReduceFileList = c.InterFilename[reduceNum]
			reply.NReduce = c.NReduce
			reply.ReduceNumIndex = reduceNum

			c.Lock.Lock()
			c.ReduceTaskStatus[reduceNum] = Allocated
			c.Lock.Unlock()
			go c.WorkerTimeCheck(ReduceTask,reduceNum)
			return nil
	}
	case DoneMap:
		c.Lock.Lock()
		defer c.Lock.Unlock()
		c.FilesStatus[args.WorkInfo]=Done
	case DoneReduce:
		index,_:=strconv.Atoi(args.WorkInfo)
		c.Lock.Lock()
		defer c.Lock.Unlock()
		c.ReduceTaskStatus[index]=Done
	}
	return nil
}

func (c *Coordinator)WorkerTimeCheck(workType TaskType,workInfo interface{}){
	ticker:=time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for{
		select {
			case <-ticker.C:
				if workType == MapTask{
					filename:=workInfo.(string)
					c.Lock.Lock()
					if c.FilesStatus[filename] == Done{
						c.Lock.Unlock()
						return
					}
					c.FilesStatus[filename] = UnAllocated
					c.Lock.Unlock()
					c.MapTaskCh<-filename
				}else if workType == ReduceTask{
					nReduceIndex := workInfo.(int)
					c.Lock.Lock()
					if c.ReduceTaskStatus[nReduceIndex] == Done{
						c.Lock.Unlock()
						return
					}
					c.ReduceTaskStatus[nReduceIndex] = UnAllocated
					c.Lock.Unlock()
					c.ReduceTaskCh<-nReduceIndex
				}
		}
	}
}


func (c *Coordinator)InterHandler(args *InterFile,reply *Reply)error{
	nReduceType:=args.NReduceType
	filename:=args.WorkInfo
	c.InterFilename[nReduceType] = append(c.InterFilename[nReduceType],filename)
	return nil
}


func (c *Coordinator)GenerateTask() {
	for k,v:=range c.FilesStatus{
		if v==UnAllocated{
			c.MapTaskCh <- k
		}
	}
	ok:=false
	for !ok{
		ok = c.checkAllMapTask()
	}
	c.DoneMap = true

	for k,v:=range c.ReduceTaskStatus{
		if v == UnAllocated{
			c.ReduceTaskCh <- k
		}
	}

	ok = false
	for !ok{
		ok = c.checkAllReduceTask()
	}
	c.DoneReduce = true
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
	ret = c.DoneReduce


	return ret
}


func (c *Coordinator)checkAllMapTask()bool{
	defer c.Lock.RUnlock()
	for _,v:=range c.FilesStatus{
		if v!=Done{
			return false
		}
	}
	return true
}

func (c *Coordinator)checkAllReduceTask()bool{
	c.Lock.RLock()
	defer c.Lock.RUnlock()
	for _,v:=range c.ReduceTaskStatus{
		if v!=Done{
			return false
		}
	}
	return true
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := NewCoordinator(files,nReduce).(*Coordinator)

	go c.GenerateTask()

	c.server()
	return c
}
