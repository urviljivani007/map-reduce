package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	IDLE        = 0
	IN_PROGRESS = 1
	COMPLETED   = 2
	MAP         = 0
	REDUCE      = 1
	NONE 		= 2
)

type Task struct {
	lock      sync.Mutex
	filename  string
	state     int
	timestamp time.Time
}

type Coordinator struct {
	mu            sync.Mutex
	mapRemain     int
	reduceRemain  int
	mTasks        []*Task
	rTasks        []*Task
}

// Your code here -- RPC handlers for the worker to call.

/* 
	wait keeps a check on a task executed by worker. If worker failes to finish it in 10 seconds, 
	if it fails to execute it in 10 seconds, we mark that task as idle and it will be picked up by a new worker.
*/
func wait(task *Task) {
	time.Sleep(10 * time.Second)

	task.lock.Lock()
	if task.state == COMPLETED {
		fmt.Fprintf(os.Stderr, "%s coordinator: task %s completed\n", time.Now().String(), task.filename)
	} else {
		task.state = IDLE
		fmt.Fprintf(os.Stderr, "%s coordinator: task %s failed, re-allocate to other workers\n", time.Now().String(), task.filename)
	}
	task.lock.Unlock()
}
/* 
	assigns tasks to workers if some tasks are pending or idle.
*/
func (c *Coordinator) HandleQuery(args *QueryArgs, reply *QueryReply) error {
	reply.Kind = "none"
	c.mu.Lock()
	if c.mapRemain != 0 {
		// look for a map task
		for i, task := range c.mTasks {
			task.lock.Lock()
			defer task.lock.Unlock()
			if task.state == IDLE {
				task.state = IN_PROGRESS
				reply.Kind = "map"
				reply.File = task.filename
				reply.NReduce = len(c.rTasks)
				reply.Index = i
				task.timestamp = time.Now()
				go wait(task) // start timer
				break
			}
		}
	} else {
		// look for a reduce task
		for i, task := range c.rTasks {
			task.lock.Lock()
			defer task.lock.Unlock()
			if task.state == IDLE {
				task.state = IN_PROGRESS
				reply.Kind = "reduce"
				reply.Split = len(c.mTasks)
				reply.Index = i
				task.timestamp = time.Now()
				go wait(task) // start timer
				break
			}
		}
	}
	c.mu.Unlock()
	return nil
}

/*
	handles response from workers.
*/
func (c *Coordinator) HandleResponse(args *ResponseArgs, reply *ResponseReply) error {
	now := time.Now()
	var task *Task
	if args.Kind == "map" {
		task = c.mTasks[args.Index]
	} else {
		task = c.rTasks[args.Index]
	}

	if now.Before(task.timestamp.Add(10 * time.Second)) {
		task.lock.Lock()
		task.state = COMPLETED
		task.lock.Unlock()
		// a task is completed, decrease remain count
		c.mu.Lock()
		if args.Kind == "map" {
			c.mapRemain--
		} else {
			c.reduceRemain--
		}
		c.mu.Unlock()
	} else {
		task.lock.Lock()
		task.state = IDLE
		task.lock.Unlock()
	}
	return nil
}

/*
	start a thread that listens for RPCs from worker.go
*/
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	// listening to the socket
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

/*
	main/mrcoordinator.go calls Done() periodically to find out
	if the entire job has finished.
*/
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	if c.reduceRemain == 0 {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

/*
	create a new coordinator.
	main/mrcoordinator.go calls this function.
*/
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	coordinator := Coordinator{}
	coordinator.mTasks = make([]*Task, len(files))
	coordinator.rTasks = make([]*Task, nReduce)
	coordinator.mu = sync.Mutex{}
	coordinator.mapRemain = len(files)
	coordinator.reduceRemain = nReduce

	// initialize coordinator data structure
	for i, file := range files {
		coordinator.mTasks[i] = new(Task)
		coordinator.mTasks[i].lock = sync.Mutex{}
		coordinator.mTasks[i].filename = file
		coordinator.mTasks[i].state = IDLE
	}

	for i := 0; i < nReduce; i++ {
		coordinator.rTasks[i] = new(Task)
		coordinator.rTasks[i].lock = sync.Mutex{}
		coordinator.rTasks[i].state = IDLE
	}

	fmt.Fprintf(os.Stderr, "%s coordinator: initialization completed\n", time.Now().String())

	coordinator.server()
	return &coordinator
}
