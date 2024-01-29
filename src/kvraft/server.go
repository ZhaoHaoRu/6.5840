package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int

const (
	GetOp OpType = iota
	PutOp
	AppendOp
	UnKnown
)

const RaftTimeOut = 30 * time.Second

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        OpType
	ClerkId   int64
	SeqNumber int
}

type RaftApplyResult struct {
	Err   Err
	Value string
}

// StateMachine is the backend for fault-tolerant kv
type StateMachine struct {
	storage map[string]string
}

func (sm *StateMachine) get(key string) string {
	result, ok := sm.storage[key]
	if ok {
		return result
	}
	return ""
}

func (sm *StateMachine) put(key string, value string) {
	sm.storage[key] = value
}

func (sm *StateMachine) append(key string, value string) {
	curValue := sm.get(key)
	curValue += value
	sm.put(key, curValue)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	stateMachine *StateMachine
	// sessionMap record clerk's corresponding latest seq number
	sessionMap    map[int64]int
	replyChannels map[int]chan RaftApplyResult
}

func (kv *KVServer) debug(message string) {
	pid := os.Getpid()
	logMessage := fmt.Sprintf("[pid %d] [ServerId %d] %s\n", pid, kv.me, message)
	fmt.Printf(logMessage)
}

// operationHandler handle get/put/append together
func (kv *KVServer) operationHandler(op *Op) RaftApplyResult {
	result := RaftApplyResult{}

	// enter an Op in the Raft log using Start()
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		result.Err = NotLeaderErr
		return result
	}

	// keep reading applyCh while PutAppend() and Get() handlers submit
	// commands to the Raft log using Start()
	kv.mu.Lock()
	resultCh, ok := kv.replyChannels[index]
	if !ok {
		kv.replyChannels[index] = make(chan RaftApplyResult)
		resultCh, ok = kv.replyChannels[index]
		if !ok {
			panic("[operationHandler] create reply channel fail")
		}
	}
	// kv.debug(fmt.Sprintf("[operationHandler] create receive channel success, index: %d", index))
	kv.mu.Unlock()
	select {
	case result = <-resultCh:
		// update session map
		// FIXME(zhr): maybe need check whether current seqNumber is up-to-date
		kv.mu.Lock()
		result.Err = None
		// kv.debug(fmt.Sprintf("[operationHandler] get result success, result: %+v\n", result))
		kv.sessionMap[op.ClerkId] = op.SeqNumber
		kv.mu.Unlock()
		return result
	case <-time.After(RaftTimeOut):
		result.Err = TimeoutErr
	}
	return result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = None
	if kv.killed() {
		return
	}
	if args == nil && reply == nil {
		return
	}
	if args == nil {
		reply.Err = ReplyEmptyErr
		return
	}

	kv.mu.Lock()
	// check duplicated request
	if latestSeqNumber, ok := kv.sessionMap[args.ClerkId]; ok {
		if args.SeqNumber <= latestSeqNumber {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	// generate the Op
	op := Op{
		Key:       args.Key,
		Op:        GetOp,
		ClerkId:   args.ClerkId,
		SeqNumber: args.SeqNumber,
	}
	result := kv.operationHandler(&op)
	//if result.Err == None {
	//	kv.debug(fmt.Sprintf("[Get] get %+v reply success", args))
	//}
	reply.Err = result.Err
	reply.Value = result.Value
}

func getOpType(name string) OpType {
	switch name {
	case "Get":
		return GetOp
	case "Put":
		return PutOp
	case "Append":
		return AppendOp
	default:
		return UnKnown
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = None
	if kv.killed() {
		return
	}
	if args == nil && reply == nil {
		return
	}
	if args == nil {
		reply.Err = ReplyEmptyErr
		return
	}

	kv.mu.Lock()
	// check duplicated request
	if latestSeqNumber, ok := kv.sessionMap[args.ClerkId]; ok {
		if args.SeqNumber <= latestSeqNumber {
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()

	// generate the Op
	op := Op{
		Key:       args.Key,
		Value:     args.Value,
		Op:        getOpType(args.Op),
		ClerkId:   args.ClerkId,
		SeqNumber: args.SeqNumber,
	}
	result := kv.operationHandler(&op)
	reply.Err = result.Err
	//if reply.Err == None {
	//	kv.debug(fmt.Sprintf("[PutAppend] get %+v reply success", args))
	//}
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if kv.killed() {
				break
			}
			applyResult := RaftApplyResult{
				Err: None,
			}
			if msg.SnapshotValid {
				panic("snapshot is not implemented")
			} else if msg.CommandValid {
				if cmd, ok := msg.Command.(Op); ok {
					// apply to the state machine
					kv.mu.Lock()
					switch cmd.Op {
					case GetOp:
						applyResult.Value = kv.stateMachine.get(cmd.Key)
					case PutOp:
						kv.stateMachine.put(cmd.Key, cmd.Value)
					case AppendOp:
						kv.stateMachine.append(cmd.Key, cmd.Value)
					}
					// send result to the operation handler
					// kv.debug(fmt.Sprintf("[applier] get reply channel with id %d", msg.CommandIndex))
					resultCh, ok := kv.replyChannels[msg.CommandIndex]
					kv.mu.Unlock()
					if ok {
						//if applyResult.Err == None {
						//	kv.debug(fmt.Sprintf("[applier] get apply result, apply result: %+v\n", applyResult))
						//} else {
						//	panic(fmt.Sprintf("[applier] the result err field is incorrect, %+v\n", applyResult))
						//}
						resultCh <- applyResult
					}
				}
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.stateMachine = &StateMachine{
		storage: make(map[string]string),
	}
	kv.sessionMap = make(map[int64]int)
	kv.replyChannels = make(map[int]chan RaftApplyResult)
	go kv.applier()
	return kv
}
