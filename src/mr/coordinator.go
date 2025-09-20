package mr

import (
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Phase int

const (
	PhaseMap Phase = iota
	PhaseReduce
	PhaseDone
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Completed
)

type TaskMeta struct {
	State     TaskState
	StartTime time.Time // for 10s timeout
	Filename  string    // only for MAP
}

type Coordinator struct {
	mu          sync.Mutex
	phase       Phase
	files       []string
	nMap        int
	nReduce     int
	mapTasks    []TaskMeta    // length = nMap
	reduceTasks []TaskMeta    // length = nReduce
	timeout     time.Duration // = 10 * time.Second
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == PhaseMap {
		c.reviveStale(c.mapTasks)
		if id, ok := pickIdle(c.mapTasks); ok {
			startTask(c.mapTasks, id)
			reply.Type = TaskMap
			reply.TaskID = id
			reply.Filename = c.files[id]
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			return nil
		}
		if allCompleted(c.mapTasks) {
			c.phase = PhaseReduce
		} else {
			reply.Type = TaskWait
			return nil
		}
	}

	if c.phase == PhaseReduce {
		c.reviveStale(c.reduceTasks)
		if id, ok := pickIdle(c.reduceTasks); ok {
			startTask(c.reduceTasks, id)
			reply.Type = TaskReduce
			reply.TaskID = id
			reply.ReduceID = id
			reply.NMap = c.nMap
			reply.NReduce = c.nReduce
			return nil
		}
		if allCompleted(c.reduceTasks) {
			c.phase = PhaseDone
		} else {
			reply.Type = TaskWait
			return nil
		}
	}

	reply.Type = TaskExit
	return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case TaskMap:
		if args.TaskID >= 0 && args.TaskID < len(c.mapTasks) {
			if args.Success && c.mapTasks[args.TaskID].State == InProgress {
				c.mapTasks[args.TaskID].State = Completed
			} else if !args.Success {
				c.mapTasks[args.TaskID].State = Idle
			}
		}
	case TaskReduce:
		if args.TaskID >= 0 && args.TaskID < len(c.reduceTasks) {
			if args.Success && c.reduceTasks[args.TaskID].State == InProgress {
				c.reduceTasks[args.TaskID].State = Completed
			} else if !args.Success {
				c.reduceTasks[args.TaskID].State = Idle
			}
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	sock := coordinatorSock()
	_ = os.Remove(sock)
	l, e := net.Listen("unix", sock)
	if e != nil {
		panic(e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == PhaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		files:       files,
		nMap:        len(files),
		nReduce:     nReduce,
		phase:       PhaseMap,
		timeout:     10 * time.Second,
		mapTasks:    make([]TaskMeta, len(files)),
		reduceTasks: make([]TaskMeta, nReduce),
	}
	for i, f := range files {
		c.mapTasks[i].Filename = f
	}
	c.server()
	return c
}

func (c *Coordinator) reviveStale(tasks []TaskMeta) {
	now := time.Now()
	for i := range tasks {
		if tasks[i].State == InProgress && now.Sub(tasks[i].StartTime) > c.timeout {
			tasks[i].State = Idle
		}
	}
}

func pickIdle(tasks []TaskMeta) (int, bool) {
	for i := range tasks {
		if tasks[i].State == Idle {
			return i, true
		}
	}
	return -1, false
}

func startTask(tasks []TaskMeta, id int) {
	tasks[id].State = InProgress
	tasks[id].StartTime = time.Now()
}

func allCompleted(tasks []TaskMeta) bool {
	for i := range tasks {
		if tasks[i].State != Completed {
			return false
		}
	}
	return true
}
