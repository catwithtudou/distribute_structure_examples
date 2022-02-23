package mr



type TaskType int

const(
	MapTask TaskType = iota+1
	ReduceTask
)


type TaskStatus int

const (
	UnAllocated TaskStatus = iota+1
	Allocated
	Done
)