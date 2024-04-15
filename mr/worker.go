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

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

/*
	use ihash(key) % NReduce to choose the reduce
	task number for each KeyValue emitted by Map.
*/
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

/* 
	worker execute map task
	map operation on the input file given by the coordinator
*/
func executeMap(mapf func(string, string) []KeyValue, filename string, nReduce int, index int) bool {
	kvall := make([][]KeyValue, nReduce)
	file, err := os.Open(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: can not open %v\n", time.Now().String(), filename)
		return false
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: can not read %v\n", time.Now().String(), filename)
		return false
	}
	file.Close()

	mapRes := mapf(filename, string(content))

	// map result are mapped into `nReduce` bucket
	for _, kv := range mapRes {
		index := ihash(kv.Key) % nReduce
		kvall[index] = append(kvall[index], kv)
	}

	// write key-value to different json files
	for i, kva := range kvall {
		// implement atomical write by two-phase trick: write to a temporary file and rename it
		oldname := fmt.Sprintf("temp_inter_%d_%d.json", index, i)
		tempfile, err := os.OpenFile(oldname, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: map can not open temp file %v\n", time.Now().String(), oldname)
			return false
		}
		defer os.Remove(oldname)

		enc := json.NewEncoder(tempfile)
		for _, kv := range kva {
			if err := enc.Encode(&kv); err != nil {
				fmt.Fprintf(os.Stderr, "%s Worker: map can not write to temp file %v\n", time.Now().String(), oldname)
				return false
			}
		}

		newname := fmt.Sprintf("inter_%d_%d.json", index, i)
		if err := os.Rename(oldname, newname); err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: map can not rename temp file %v\n", time.Now().String(), oldname)
			return false
		}
	}
	return true
}

/*
	worker execute reduce task
	gather all key-value stored in intermidiate files named `inter_*_index`
	and write to a single file `mr-out-index`
*/
func executeReduce(reducef func(string, []string) string, split int, index int) bool {
	var kva []KeyValue
	for i := 0; i < split; i++ {
		filename := fmt.Sprintf("inter_%d_%d.json", i, index)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s Worker: can not read intermidiate file %v\n", time.Now().String(), filename)
			return false
		}

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

	// two-phase trick to implement atomical write
	oldname := fmt.Sprintf("temp-mr-out-%d", index)
	newname := fmt.Sprintf("mr-out-%d", index)

	tempfile, err := os.OpenFile(oldname, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: reduce can not open temp file %v\n", time.Now().String(), oldname)
		return false
	}
	defer os.Remove(oldname)

	// reduce on values that have the same key
	i := 0
	for i < len(kva) {
		j := i + 1
		values := []string{}
		values = append(values, kva[i].Value)
		for j < len(kva) && kva[i].Key == kva[j].Key {
			values = append(values, kva[j].Value)
			j++
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	if err := os.Rename(oldname, newname); err != nil {
		fmt.Fprintf(os.Stderr, "%s Worker: reduce can not rename temp file %v\n", time.Now().String(), oldname)
		return false
	}

	return true
}

/*
	main/mrworker.go calls this function.
	worker polls for new task from coordinator periodically and
	if coordinator crashes we assume that we are done.
*/
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := QueryArgs{}
		reply := QueryReply{}
		// can not connect to the coordinator
		// assume that the coordinator has exited, then exit
		if !(call("Coordinator.HandleQuery", &args, &reply)) {
			fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
			os.Exit(0)
		}
		if reply.Kind == "none" {
			continue
		}

		// execute the task
		responseArgs := ResponseArgs{}
		responseReply := ResponseReply{}
		responseArgs.Kind = reply.Kind
		responseArgs.Index = reply.Index
		if reply.Kind == "map" {
			if executeMap(mapf, reply.File, reply.NReduce, reply.Index) {
				fmt.Fprintf(os.Stderr, "%s Worker: map task performed successfully\n", time.Now().String())
				if !(call("Coordinator.HandleResponse", &responseArgs, &responseReply)) {
					fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
					os.Exit(0)
				}
			} else {
				fmt.Fprintf(os.Stderr, "%s Worker: map task failed\n", time.Now().String())
			}
		} else {
			if executeReduce(reducef, reply.Split, reply.Index) {
				fmt.Fprintf(os.Stderr, "%s Worker: reduce task performed successfully\n", time.Now().String())
				if !(call("Coordinator.HandleResponse", &responseArgs, &responseReply)) {
					fmt.Fprintf(os.Stderr, "%s Worker: exit", time.Now().String())
					os.Exit(0)
				}
			}
		}

		time.Sleep(time.Second)
	}
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