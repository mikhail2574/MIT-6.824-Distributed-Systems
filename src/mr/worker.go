package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		req := RequestTaskArgs{WorkerID: os.Getpid()}
		var rep RequestTaskReply
		ok := call("Coordinator.RequestTask", &req, &rep)
		if !ok {
			return
		}

		switch rep.Type {
		case TaskMap:
			success := doMap(rep.TaskID, rep.Filename, rep.NReduce, mapf)
			_ = call("Coordinator.ReportTask", &ReportTaskArgs{
				Type: TaskMap, TaskID: rep.TaskID, Success: success,
			}, &ReportTaskReply{})

		case TaskReduce:
			success := doReduce(rep.TaskID, rep.NMap, reducef)
			_ = call("Coordinator.ReportTask", &ReportTaskArgs{
				Type: TaskReduce, TaskID: rep.TaskID, Success: success,
			}, &ReportTaskReply{})

		case TaskWait:
			time.Sleep(200 * time.Millisecond)

		case TaskExit:
			return
		}
	}
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

func writeAtomically(final string, write func(f *os.File) error) error {
	tmp, err := os.CreateTemp("", "mr-tmp-")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := write(tmp); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), final)
}

func doMap(mapID int, filename string, nReduce int, mapf func(string, string) []KeyValue) bool {
	data, err := os.ReadFile(filename)
	if err != nil {
		return false
	}

	kvs := mapf(filename, string(data))

	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		r := ihash(kv.Key) % nReduce
		buckets[r] = append(buckets[r], kv)
	}

	for r := 0; r < nReduce; r++ {
		final := intermediateName(mapID, r) // "mr-mapID-reduceID"
		err := writeAtomically(final, func(f *os.File) error {
			enc := json.NewEncoder(f)
			for _, kv := range buckets[r] {
				if err := enc.Encode(&kv); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return false
		}
	}
	return true
}

func intermediateName(mapID, reduceID int) string {
	return fmt.Sprintf("mr-%d-%d", mapID, reduceID)
}

func doReduce(reduceID int, nMap int, reducef func(string, []string) string) bool {
	m := make(map[string][]string)
	for mapID := 0; mapID < nMap; mapID++ {
		name := intermediateName(mapID, reduceID)
		f, err := os.Open(name)
		if err != nil {
			return false
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				f.Close()
				return false
			}
			m[kv.Key] = append(m[kv.Key], kv.Value)
		}
		f.Close()
	}

	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := fmt.Sprintf("mr-out-%d", reduceID)
	err := writeAtomically(out, func(f *os.File) error {
		for _, k := range keys {
			val := reducef(k, m[k])
			if _, err := fmt.Fprintf(f, "%v %v\n", k, val); err != nil {
				return err
			}
		}
		return nil
	})
	return err == nil
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
