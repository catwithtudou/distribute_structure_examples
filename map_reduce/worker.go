package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


type WorkOp interface {
	ReqTask(workType WorkSign,workInfo string)Reply
	mapWorker(reply *Reply,mapFunc func(string,string)[]KeyValue)error
	reduceWorker(reply *Reply,reduceFunc func(string,[]string)string)error
	SendInterFiles(workType WorkSign,workInfo string,nReduceType int)Reply
}

type DefaultWorker struct{}

func NewDefaultWorker()WorkOp{
	return &DefaultWorker{}
}

func (w *DefaultWorker)ReqTask(workType WorkSign,workInfo string)Reply{
	args:=Args{}
	args.WorkType = workType
	args.WorkInfo = workInfo

	reply:=Reply{}

	res:=call("Coordiantor.WorkerHandler",&args,&reply)
	if !res {
		return Reply{}
	}
	return reply
}

func (w *DefaultWorker)mapWorker(reply *Reply,mapFunc func(string,string)[]KeyValue)error{
	tFile,err:=os.Open(reply.Filename)
	if err!=nil{
		return err
	}
	defer tFile.Close()

	value,err:=ioutil.ReadAll(tFile)
	if err!=nil{
		return err
	}

	kvs:=mapFunc(reply.Filename,string(value))

	kvp:=Partition(kvs,reply.NReduce)

	for i:=0;i<reply.NReduce;i++{
		filename,err:=WriteToJSONFile(kvp[i],reply.MapNumIndex,i)
		if err!=nil{
			return err
		}
		_ = w.SendInterFiles(InterTask,filename,i)
	}

	_ = w.ReqTask(DoneMap,reply.Filename)
	return nil
}

func (w *DefaultWorker)reduceWorker(reply *Reply,reduceFunc func(string,[]string)string)error{
	var inter []KeyValue
	for _,v:=range reply.ReduceFileList{
		file,err:= os.Open(v)
		if err!=nil{
			return err
		}
		defer file.Close()
		decoder:=json.NewDecoder(file)
		for{
			var kv KeyValue
			if err:=decoder.Decode(&kv);err!=nil{
				return nil
			}
			inter = append(inter,kv)
		}
	}
	sort.Sort(ByKey(inter))
	oName:="mr-out-"+strconv.Itoa(reply.ReduceNumIndex)
	oFile,err:=os.Create(oName)
	if err!=nil{
		return err
	}

	for i:=0;i<len(inter);{
		j:=i+1
		for j<len(inter)&&inter[j].Key == inter[i].Key{
			j++
		}
		var values []string
		for k:=i;k<j;k++{
			values = append(values,inter[k].Value)
		}
		out:=reduceFunc(inter[i].Key,values)
		fmt.Fprintf(oFile,"%v %v\n",inter[i].Key,out)
		i = j
	}

	_ = w.ReqTask(DoneReduce,strconv.Itoa(reply.ReduceNumIndex))

	return nil
}

func (w *DefaultWorker)SendInterFiles(workType WorkSign,workInfo string,nReduceType int)Reply{
	args:=InterFile{}
	args.WorkType = workType
	args.WorkInfo = workInfo
	args.NReduceType = nReduceType

	reply:=Reply{}

	res:=call("Coordinator.InterHandler",&args,&reply)
	if !res{
		return Reply{}
	}

	return reply
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	worker:=NewDefaultWorker()
	for{
		// call for the task
		reply:=worker.ReqTask(WorkTask,"")
		if reply.TaskType==0{
			continue
		}
		switch reply.TaskType {
		case MapTask:
			err:=worker.mapWorker(&reply,mapf)
			if err!=nil{
				log.Println("map worker error:"+err.Error())
				return
			}
		case ReduceTask:
			err:=worker.reduceWorker(&reply,reducef)
			if err!=nil{
				log.Println("reduce worker error:"+err.Error())
				return
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
