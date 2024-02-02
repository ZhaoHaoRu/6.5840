package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
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

const RaftTimeOut = 100 * time.Millisecond

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
	// in case for term mismatch
	Term int
}

// StateMachine is the backend for fault-tolerant kv
type StateMachine struct {
	Storage map[string]string
}

func (sm *StateMachine) get(key string) string {
	result, ok := sm.Storage[key]
	// debug(fmt.Sprintf("[StateMachine.get] Key:%+v, value:%+v", key, result))
	if ok {
		return result
	}
	return ""
}

func (sm *StateMachine) put(key string, value string) {
	sm.Storage[key] = value
	// debug(fmt.Sprintf("[StateMachine.put] Key:%+v, value:%+v", key, value))
}

func (sm *StateMachine) append(key string, value string) {
	curValue, _ := sm.Storage[key]
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
	sessionMap    map[int64]Op
	replyChannels map[int]chan RaftApplyResult
	// lastApplied indicating the last included index for snapshot
	lastApplied int
}

func (kv *KVServer) debug(message string) {
	pid := os.Getpid()
	logMessage := fmt.Sprintf("[pid %d] [ServerId %d] %s\n", pid, kv.me, message)
	fmt.Printf(logMessage)
}

func (kv *KVServer) getReplyChan(index int) chan RaftApplyResult {
	resultCh, ok := kv.replyChannels[index]
	if !ok {
		kv.replyChannels[index] = make(chan RaftApplyResult)
	} else {
		// kv.debug("out of date channel")
		// NOTE(zhr): there might be two requests with the same index, let the preceding one fail
		resultCh <- RaftApplyResult{
			Err: OutOfDateErr,
		}
		kv.replyChannels[index] = make(chan RaftApplyResult)
	}
	resultCh, _ = kv.replyChannels[index]
	return resultCh
}

// operationHandler handle get/put/append together
func (kv *KVServer) operationHandler(op *Op) RaftApplyResult {
	result := RaftApplyResult{Err: None}

	// enter an Op in the Raft log using Start()
	index, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		result.Err = NotLeaderErr
		return result
	}

	// keep reading applyCh while PutAppend() and Get() handlers submit
	// commands to the Raft log using Start()
	kv.mu.Lock()
	resultCh := kv.getReplyChan(index)
	// kv.debug(fmt.Sprintf("[operationHandler] create receive channel success, index: %d", index))
	kv.mu.Unlock()
	select {
	case result = <-resultCh:
		// update session map
		// kv.debug(fmt.Sprintf("[operationHandler] get reply %+v to op: %+v, raft index: %d", result, op, index))
	case <-time.After(RaftTimeOut):
		result.Err = TimeoutErr
	}
	kv.mu.Lock()
	delete(kv.replyChannels, index)
	kv.mu.Unlock()
	return result
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err = None
	if kv.killed() {
		return
	}
	if args == nil && reply == nil {
		reply.Err = ArgEmptyErr
		return
	}
	if args == nil {
		reply.Err = ReplyEmptyErr
		return
	}

	kv.mu.Lock()

	// check duplicated request
	if latestOp, ok := kv.sessionMap[args.ClerkId]; ok {
		if args.SeqNumber < latestOp.SeqNumber {
			kv.mu.Unlock()
			reply.Err = OutOfDateErr
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
	//if result.Err == None {
	//	kv.debug(fmt.Sprintf("[KVServer.Get] get reply %+v to op: %+v", reply, op))
	//}
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
		reply.Err = ArgEmptyErr
		return
	}
	if args == nil {
		reply.Err = ReplyEmptyErr
		return
	}

	kv.mu.Lock()
	// check duplicated request
	if latestOp, ok := kv.sessionMap[args.ClerkId]; ok {
		if args.SeqNumber < latestOp.SeqNumber {
			kv.mu.Unlock()
			reply.Err = OutOfDateErr
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

func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	curSize := kv.rf.GetStateSize()
	upperBound := 0.8 * float64(kv.maxraftstate)
	// kv.debug(fmt.Sprintf("[needSnapshot] cur size: %d, upper bound: %d", curSize, int(upperBound)))
	return curSize >= int(upperBound)
}

func (kv *KVServer) generateSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(*kv.stateMachine)
	_ = e.Encode(kv.sessionMap)
	return w.Bytes()
}

func (kv *KVServer) applySnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var newStateMachine StateMachine
	var newSessionMap map[int64]Op
	if d.Decode(&newStateMachine) != nil || d.Decode(&newSessionMap) != nil {
		return
	}
	kv.stateMachine = &newStateMachine
	kv.sessionMap = newSessionMap
	// kv.debug(fmt.Sprintf("[applySnapshot] apply snapshot, stateMachine storage: %+v, sessionMap: %+v\n", kv.stateMachine.Storage, kv.sessionMap))
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if kv.killed() {
				break
			}
			applyResult := RaftApplyResult{
				Err:  None,
				Term: msg.CommandTerm,
			}
			if msg.SnapshotValid {
				kv.mu.Lock()
				// the snapshot is more up-to-date
				if msg.SnapshotIndex > kv.lastApplied {
					// kv.debug("begin to apply snapshot")
					// apply the snapshot
					kv.applySnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else if msg.CommandValid {
				if cmd, ok := msg.Command.(Op); ok {
					// apply to the state machine
					kv.mu.Lock()
					// check whether the command already been applied
					isValid := true
					isLast := false
					if latestOp, ok := kv.sessionMap[cmd.ClerkId]; ok {
						if cmd.SeqNumber <= latestOp.SeqNumber {
							isValid = false
						}
						if cmd.SeqNumber == latestOp.SeqNumber && cmd.Op == latestOp.Op && cmd.Key == latestOp.Key {
							isLast = true
						}
					}
					switch cmd.Op {
					case GetOp:
						if isValid || isLast {
							applyResult.Value = kv.stateMachine.get(cmd.Key)
						} else {
							applyResult.Err = OutOfDateErr
						}
					case PutOp:
						if isValid {
							kv.stateMachine.put(cmd.Key, cmd.Value)
						} else {
							applyResult.Err = OutOfDateErr
						}
					case AppendOp:
						if isValid {
							kv.stateMachine.append(cmd.Key, cmd.Value)
						} else {
							applyResult.Err = OutOfDateErr
						}
					}

					// update session map
					if isValid {
						kv.sessionMap[cmd.ClerkId] = cmd
					}
					// update last applied
					if msg.CommandIndex > kv.lastApplied {
						kv.lastApplied = msg.CommandIndex
					}

					// send result to the operation handler
					// check whether it is a valid leader server
					term, isLeader := kv.rf.GetState()
					if term == applyResult.Term && isLeader {
						resultCh := kv.replyChannels[msg.CommandIndex]
						kv.mu.Unlock()
						resultCh <- applyResult
						kv.mu.Lock()
					}

					// check whether need to take snapshot
					if kv.needSnapshot() {
						// kv.debug("[applier] need to take snapshot")
						snapshot := kv.generateSnapshot()
						kv.rf.Snapshot(kv.lastApplied, snapshot)
					}
					kv.mu.Unlock()
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
		Storage: make(map[string]string),
	}

	kv.sessionMap = make(map[int64]Op)
	kv.lastApplied = 0

	// When a kvserver server restarts, it should read the snapshot from persister and restore its state from the snapshot.
	if kv.maxraftstate != -1 {
		kv.applySnapshot(kv.rf.GetLastSnapshot())
		kv.lastApplied = kv.rf.GetLastIndex()
	}

	kv.replyChannels = make(map[int]chan RaftApplyResult)
	go kv.applier()
	return kv
}
