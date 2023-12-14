package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type WorkType int
type RequestType int
type ReplyType int

const (
	Map WorkType = iota
	Reduce
)

const (
	Request RequestType = iota
	Finish
)

const (
	Success ReplyType = iota
	Waiting
	Finished
	Failed
)

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

type RequestWorkerArgs struct {
	ArgType RequestType
}

type FinishWorkerArgs struct {
	ArgType  RequestType
	FileName string
}

type RequestCoordinatorReply struct {
	ReplyStatus ReplyType
	JobType     WorkType
	JobId       int
	FileCount   int
	FileName    string
}

type FinishCoordinatorReply struct {
	ReplyStatus ReplyType
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
