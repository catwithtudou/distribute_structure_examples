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
type WorkSign int

const(
	WorkTask WorkSign = iota+1
	InterTask
	DoneMap
	DoneReduce
)

type InterFile struct{
	WorkType WorkSign
	WorkInfo string
	NReduceType int
}


type Args struct{
	WorkType WorkSign
	WorkInfo string
}

type Reply struct {
	TaskType TaskType
	Filename string
	MapNumIndex int
	NReduce int
	ReduceNumIndex int
	ReduceFileList []string
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
