package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type JobStatus struct {
	// timeoutStamp record the timestamp for timeout
	timeoutStamp time.Time
	jobId        int
}

type Coordinator struct {
	// Your definitions here.
	reduceCount int
	// jobStatusMap record the timestamp for timeout
	jobStatusMap   map[string]JobStatus
	files          []string
	totalJobs      int
	assignedJobs   int
	unfinishedJobs int
	stage          WorkType
	mutex          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func getIntermediateName(reducerId int) string {
	return "mr-X-" + strconv.Itoa(reducerId)
}

// convertStage convert from Map stage to Reduce stage
func (c *Coordinator) convertStage() {
	c.unfinishedJobs = 0
	c.stage = Reduce
	// the intermediate file name format is `mr-X-Y`, where X is the Map task number, and Y is the reduce task number.
	c.jobStatusMap = make(map[string]JobStatus)
	for i := 0; i < c.reduceCount; i++ {
		c.jobStatusMap["mr-X-"+strconv.Itoa(i)] = JobStatus{}
	}
	c.assignedJobs = 0
	c.totalJobs = c.reduceCount
}

// fillBasicReplyInfo fill basic fields according to coordinator's info
func (c *Coordinator) fillBasicReplyInfo(reply *RequestCoordinatorReply, jobId int) {
	reply.ReplyStatus = Success
	reply.JobType = c.stage
	reply.JobId = jobId
	if c.stage == Map {
		reply.FileCount = c.reduceCount
	} else {
		reply.FileCount = len(c.files)
	}
}

// ServeMapReduce is the coordinator main process logic
func (c *Coordinator) ServeMapReduce(args *RequestWorkerArgs, reply *RequestCoordinatorReply) error {
	if args == nil {
		return fmt.Errorf("[ServeMapReduce] the args is nil")
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// assign the job to worker
	if len(c.jobStatusMap) == 0 && c.stage != Reduce {
		c.convertStage()
	}

	// if no available job now
	if c.assignedJobs == c.totalJobs {
		// if all finished, let the worker stop
		if c.unfinishedJobs == 0 {
			reply.ReplyStatus = Finished
			return nil
		}

		// check whether there is a job timeout, for this lab, use ten sec
		currentTime := time.Now()
		for jobFile, jobStatus := range c.jobStatusMap {
			// assign the timeout job to the current worker
			if jobStatus.jobId != 0 && currentTime.After(jobStatus.timeoutStamp) {
				c.jobStatusMap[jobFile] = JobStatus{
					timeoutStamp: currentTime.Add(10 * time.Second),
					jobId:        jobStatus.jobId,
				}
				// generate the reply
				reply.FileName = jobFile
				c.fillBasicReplyInfo(reply, jobStatus.jobId)
				return nil
			}
		}

		// if no available job now, let the worker wait for other job timeout
		reply.ReplyStatus = Waiting
		return nil
	}

	// still have available jobs
	c.fillBasicReplyInfo(reply, c.assignedJobs)
	c.assignedJobs += 1
	c.unfinishedJobs += 1

	if c.stage == Map {
		reply.FileName = c.files[reply.JobId]
	} else {
		reply.FileName = getIntermediateName(reply.JobId)
	}

	c.jobStatusMap[reply.FileName] = JobStatus{
		timeoutStamp: time.Now().Add(10 * time.Second),
		jobId:        reply.JobId,
	}
	return nil
}

func (c *Coordinator) ServeACK(args *FinishWorkerArgs, reply *FinishCoordinatorReply) error {
	if args == nil || reply == nil {
		return fmt.Errorf("[ServeACK] invalid parameter nil")
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.jobStatusMap, args.FileName)
	c.unfinishedJobs -= 1
	reply.ReplyStatus = Success
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Your code here.
	if c.stage == Reduce && len(c.jobStatusMap) == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.jobStatusMap = make(map[string]JobStatus)
	for _, filename := range files {
		c.jobStatusMap[filename] = JobStatus{}
	}

	c.reduceCount = nReduce
	c.assignedJobs = 0
	c.unfinishedJobs = 0
	c.totalJobs = len(files)
	c.stage = Map
	c.server()
	return &c
}
