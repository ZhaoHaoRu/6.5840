package kvraft

import (
	"fmt"
	"os"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

const (
	ArgEmptyErr   Err = "The args is nil"
	ReplyEmptyErr Err = "The reply is nil"
	NotLeaderErr  Err = "Current raft peer is not a leader"
	TimeoutErr    Err = "Operation timeout"
	UnknownErr    Err = "Unknown error"
	OutOfDateErr  Err = "The request is out of date"
	None          Err = ""
)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId   int64
	SeqNumber int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId   int64
	SeqNumber int
}

type GetReply struct {
	Err   Err
	Value string
}

func debug(message string) {
	pid := os.Getpid()
	logMessage := fmt.Sprintf("[pid %d] %s\n", pid, message)
	fmt.Printf(logMessage)
}
