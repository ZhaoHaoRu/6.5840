package kvraft

import (
	"6.5840/labrpc"
	"fmt"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	// clerkId the Id for current clerk
	clerkId int64
	// seqNumber for current clerk
	seqNumber int
	// mu is the lock for concurrency
	mu sync.RWMutex
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
	// You'll have to add code here.
	ck.leaderId = -1
	ck.clerkId = nrand()
	ck.seqNumber = 0
	return ck
}

func (ck *Clerk) GetClerkId() int64 {
	return ck.clerkId
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		SeqNumber: ck.seqNumber,
	}
	ck.seqNumber += 1
	ck.mu.Unlock()
	ok := false
	// if clerk knows the leader, directly send to the leader
	if ck.leaderId != -1 && ck.servers[ck.leaderId] != nil {
		reply := GetReply{Err: None}
		ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == None || len(reply.Err) == 0) {
			debug(fmt.Sprintf("[Clerk.Get] case1: receive success, args: %+v, leader: %d, reply: %+v", args, ck.leaderId, reply))
			return reply.Value
		} else {
			// debug(fmt.Sprintf("[Clerk.Get] case1: receive fail, args: %+v, leader: %d, reply: %+v, ok: %+v", args, ck.leaderId, reply, ok))
		}
	}
	// If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver,
	// the Clerk should re-try by sending to a different kvserver.
	for {
		for i, server := range ck.servers {
			reply := GetReply{Err: None}
			ok = server.Call("KVServer.Get", &args, &reply)
			if ok && (reply.Err == None || len(reply.Err) == 0) {
				ck.leaderId = i
				debug(fmt.Sprintf("[Clerk.Get] case2: receive success, args: %+v, leader: %d, reply: %+v", args, i, reply))
				return reply.Value
			} else {
				// debug(fmt.Sprintf("[Clerk.Get] case2: receive fail, args: %+v, leader: %d, reply: %+v, ok: %+v", args, i, reply, ok))
			}
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		SeqNumber: ck.seqNumber,
	}
	ck.seqNumber += 1
	ck.mu.Unlock()
	ok := false
	// if clerk knows the leader, directly send to the leader
	if ck.leaderId != -1 && ck.servers[ck.leaderId] != nil {
		reply := PutAppendReply{Err: None}
		ok = ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)

		if ok && (reply.Err == None || len(reply.Err) == 0) {
			debug(fmt.Sprintf("[Clerk.PutAppend] case1: receive success, args: %+v, leader: %d, reply: %+v", args, ck.leaderId, reply))
			return
		} else {
			// debug(fmt.Sprintf("[Clerk.PutAppend] case1: receive fail, args: %+v, leader: %d, reply: %+v", args, ck.leaderId, reply))
		}
	}
	// If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver,
	// the Clerk should re-try by sending to a different kvserver.
	for {
		for i, server := range ck.servers {
			reply := PutAppendReply{Err: None}
			ok = server.Call("KVServer.PutAppend", &args, &reply)
			if ok && (reply.Err == None || len(reply.Err) == 0) {
				debug(fmt.Sprintf("[Clerk.PutAppend] case2: receive success, args: %+v, leader: %d, reply: %+v", args, i, reply))
				ck.leaderId = i
				return
			} else {
				// debug(fmt.Sprintf("[Clerk.PutAppend] case2: receive fail, args: %+v, leader: %d, reply: %+v", args, ck.leaderId, reply))
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
