package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
	TaskWait // worker is sleeping
	TaskExit // task is done
)

// ---- RequestTask ----
type RequestTaskArgs struct {
	WorkerID int
}
type RequestTaskReply struct {
	Type TaskType
	// MAP
	Filename string
	TaskID   int // mapTaskID or reduceID
	NReduce  int
	NMap     int
	// REDUCE
	ReduceID int // double in TaskID for comfort
}

// ---- ReportTask ----
type ReportTaskArgs struct {
	Type    TaskType
	TaskID  int
	Success bool
}
type ReportTaskReply struct{}

type ExampleArgs struct{ X int }
type ExampleReply struct{ Y int }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
