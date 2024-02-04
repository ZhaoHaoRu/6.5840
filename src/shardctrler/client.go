package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"fmt"
	"os"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId int
	// clerkId the Id for current clerk
	clerkId int64
	// seqNumber for current clerk
	seqNumber int
	// mu is the lock for concurrency
	mu sync.RWMutex
}

func debug(message string) {
	pid := os.Getpid()
	logMessage := fmt.Sprintf("[pid %d] %s\n", pid, message)
	fmt.Printf(logMessage)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.leaderId = -1
	ck.clerkId = nrand()
	ck.seqNumber = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClerkId = ck.clerkId
	args.SeqNumber = ck.seqNumber
	ck.seqNumber += 1

	debug(fmt.Sprintf("[Clerk.Query] args: %+v", args))
	// try the known leader first
	if ck.leaderId != -1 && ck.servers[ck.leaderId] != nil {
		var reply QueryReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.WrongLeader == false && reply.Err != TimeoutErr {
			return reply.Config
		}
	}
	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err != TimeoutErr {
				ck.leaderId = id
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClerkId = ck.clerkId
	args.SeqNumber = ck.seqNumber
	ck.seqNumber += 1

	debug(fmt.Sprintf("[Clerk.Join] args: %+v", args))
	// try the known leader first
	if ck.leaderId != -1 && ck.servers[ck.leaderId] != nil {
		var reply JoinReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.WrongLeader == false && reply.Err != TimeoutErr {
			return
		}
	}
	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err != TimeoutErr {
				ck.leaderId = id
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClerkId = ck.clerkId
	args.SeqNumber = ck.seqNumber
	ck.seqNumber += 1

	debug(fmt.Sprintf("[Clerk.Leave] args: %+v", args))
	// try the known leader first
	if ck.leaderId != -1 && ck.servers[ck.leaderId] != nil {
		var reply LeaveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.WrongLeader == false && reply.Err != TimeoutErr {
			return
		}
	}
	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err != TimeoutErr {
				ck.leaderId = id
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClerkId = ck.clerkId
	args.SeqNumber = ck.seqNumber
	ck.seqNumber += 1

	debug(fmt.Sprintf("[Clerk.Move] args: %+v", args))
	// try the known leader first
	if ck.leaderId != -1 && ck.servers[ck.leaderId] != nil {
		var reply MoveReply
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.WrongLeader == false && reply.Err != TimeoutErr {
			return
		}
	}
	for {
		// try each known server.
		for id, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err != TimeoutErr {
				ck.leaderId = id
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
