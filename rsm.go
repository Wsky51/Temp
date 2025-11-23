package rsm

import (
	"fmt"
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// 
// 客户端请求的正常处理流程如下：
// 客户端向服务领导者发送请求；
// 服务领导者调用rsm.Submit()提交该请求；
// rsm.Submit()调用raft.Start()提交请求，随后进入等待状态；
// Raft 协议提交该请求，并将其发送到所有节点的applyCh通道；
// 每个节点上的rsm读取器协程从applyCh读取请求，并传递给服务的DoOp()方法；
// 领导者节点上的rsm读取器协程将DoOp()的返回值传递给最初提交请求的Submit()协程，Submit()将该值返回给客户端。

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

type StateMachineResult struct {
	Msg raftapi.ApplyMsg
	Res any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine

	// Your definitions here.
	seqNo int // 提交请求的序列号
	smCh  chan StateMachineResult // 读协程从通道中获取消息后反馈给Submit
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		seqNo:        0,
		smCh:		  make(chan StateMachineResult),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	fmt.Println("me:", me)
	go rsm.DealApplyMsg()
	return rsm
}

// 
func (rsm *RSM) DealApplyMsg() {
	for {
		msg := <- rsm.applyCh
		rsm.mu.Lock()
		
		op, ok := msg.Command.(Op)
		DPrintf("DealApplyMsg, msg:%v, ok:%v, op:%v", msg.Command, ok, op)
		var res any
		res = nil
		if msg.CommandValid && ok{
			res = rsm.sm.DoOp(op)
		} 
		rsm.mu.Unlock()
		rsm.smCh <- StateMachineResult{msg, res}
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	op := Op{Me: rsm.me, Id: rsm.seqNo, Req: req}
	rsm.seqNo++

	DPrintf("[Start] me:%d,op:%v", rsm.me, op)

	idx, term, ok := rsm.Raft().Start(op)
	if !ok {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	
	// fmt.Println("try to send me:", rsm.me, ", op: ", op, ",idx", idx, ", term:", term, ", req:", req)
	DPrintf("try to send,me:%d,op:%v, idx:%v, term:%v, ok:%v ", rsm.me, op, idx, term, ok)

	msg, res := <- rsm.smCh
	DPrintf("me:%d, msg:%v, res:%v", rsm.me, msg, res)
	// msg := <- rsm.applyCh
	// if idx == msg.CommandIndex && msg.Command == op{
	// 	fmt.Println("msg success:", msg)
	// 	return rpc.OK ,rsm.sm.DoOp(op.Req)
	// }
	return rpc.ErrWrongLeader, nil
	// your code here
	// return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}
