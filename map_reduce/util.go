package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)

type ByKey []KeyValue

func (b ByKey)Len()int{return len(b)}
func (b ByKey)Swap(i,j int) {b[i],b[j]=b[j],b[i]}
func (b ByKey)Less(i,j int)bool { return b[i].Key<b[j].Key}



func WriteToJSONFile(inter []KeyValue,mapTaskIndex,reduceTaskIndex int)(string,error){
	filename:="mr-"+strconv.Itoa(mapTaskIndex)+"-"+strconv.Itoa(reduceTaskIndex)
	tFile,err:=os.Create(filename)
	if err!=nil{
		return "",err
	}
	encoder:=json.NewEncoder(tFile)
	for _,kv:=range inter{
		err:=encoder.Encode(&kv)
		if err!=nil{
			return "",err
		}
	}
	return filename,nil
}

func WriteReduceOut(key,values string,nReduce int)error{
	filename:="mr-out-"+strconv.Itoa(nReduce)
	tFile,err:=os.Open(filename)
	if err!=nil{
		tFile,err=os.Create(filename)
		if err!=nil{
			return err
		}
	}
	fmt.Fprintf(tFile,"%v %v\n",key,values)
	return nil
}


func Partition(kv []KeyValue,nReduce int)[][]KeyValue{
	kvs:=make([][]KeyValue,nReduce)
	for _,v:=range kv{
		index:=ihash(v.Key) % nReduce
		kvs[index] = append(kvs[index],v)
	}
	return kvs
}


