package kvraft

import (
	"6.5840/labrpc"
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
	args := GetArgs{
		Key:       key,
		ClerkId:   ck.clerkId,
		SeqNumber: ck.seqNumber,
	}
	ck.seqNumber += 1
	reply := GetReply{Err: None}
	ok := false
	// if clerk knows the leader, directly send to the leader
	if ck.leaderId != -1 && ck.servers[ck.leaderId] != nil {
		ok = ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == None {
			// debug(fmt.Sprintf("[Clerk.Get] send to server success, args: %+v, leader: %d, reply: %+v", args, ck.leaderId, reply))
			return reply.Value
		}
	}
	// If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver,
	// the Clerk should re-try by sending to a different kvserver.
	for {
		for i, server := range ck.servers {
			reply.Err = None
			ok = server.Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == None {
				// debug(fmt.Sprintf("[Clerk.Get] send to server success, args: %+v, leader: %d, reply: %+v", args, i, reply))
				ck.leaderId = i
				return reply.Value
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
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClerkId:   ck.clerkId,
		SeqNumber: ck.seqNumber,
	}
	ck.seqNumber += 1
	reply := PutAppendReply{Err: None}
	ok := false
	// if clerk knows the leader, directly send to the leader
	if ck.leaderId != -1 && ck.servers[ck.leaderId] != nil {
		ok = ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == None {
			// debug(fmt.Sprintf("[Clerk.PutAppend] send to server success, args: %+v, leader: %d, reply: %+v", args, ck.leaderId, reply))
			return
		}
	}
	// If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver,
	// the Clerk should re-try by sending to a different kvserver.
	for {
		for i, server := range ck.servers {
			reply.Err = None
			ok = server.Call("KVServer.PutAppend", &args, &reply)
			if ok && reply.Err == None {
				// debug(fmt.Sprintf("[Clerk.PutAppend] send to server success, args: %+v, leader: %d, reply: %+v", args, i, reply))
				ck.leaderId = i
				return
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
