package rsm

import (
	"fmt"
	"log"
	"sync"
	"time"

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
	seqNo int // 提交请求指令的序列号
	resultMap 	sync.Map // key: 指令ID(string), value: chan bool
	killed		bool 	// kill 状态
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
		killed: 	  false,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	fmt.Println("me:", me)
	go rsm.reader()
	return rsm
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh  {
		op, ok := msg.Command.(Op)
		DPrintf("RSM reader获取到日志[S%d], msg:%v, ok:%v, op:%v", rsm.me, msg.Command, ok, op)
		var res any

		rsm.mu.Lock()
		if msg.CommandValid && ok{
			res = rsm.sm.DoOp(op.Req)
		}else{
			DPrintf("S%d CommandValid %v or ok %v 不对", rsm.me, msg.CommandValid, ok)
			rsm.mu.Unlock()
			continue
		}
		val, ok := rsm.resultMap.Load(op)
		rsm.mu.Unlock()

		if !ok {
			// 没找到，不用通知Start方法
			continue
		}
		smRes := StateMachineResult{Msg: msg, Res:res}
		resultChan, ok := val.(chan StateMachineResult)
		if ok {
			resultChan <- smRes
		}
	}
	rsm.mu.Lock()
	rsm.killed = true
	rsm.mu.Unlock()
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

	// 1. 生成提交指令，查询sync.Map表看是否已经提交过该指令，如果提交过，则直接返回
	rsm.mu.Lock()
	op := Op{Me: rsm.me, Id: rsm.seqNo, Req: req}
	if _, exists := rsm.resultMap.Load(op); exists {
		rsm.mu.Unlock()
		// 该指令已经被提交过，没必要提交第二次了
		DPrintf("S%d 重复提交日志%v，直接退出", rsm.me, op)
		return rpc.ErrMaybe, nil
	}

	if rsm.killed {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	// 2. 提交该指令到Raft，并创建sync.Map的key, value等待reader协程反馈消息
	rsm.seqNo++
	DPrintf("[Start] me:%d,op:%v", rsm.me, op)
	index, term, ok := rsm.Raft().Start(op)
 	
	if !ok {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}
	resultChan := make(chan StateMachineResult, 1)
	rsm.resultMap.Store(op, resultChan)
	defer func() {
		// 函数退出时清理映射和通道，防止泄漏
		rsm.resultMap.Delete(op)
		close(resultChan)
	}()

	rsm.mu.Unlock()

	// 3. 等待协程返回消息，2s超时若还没返回，则直接结束。
	for {
		select {
			case result := <-resultChan:
				DPrintf("[Start] RSM成功返回消息，S%d,op:%v", rsm.me, op)
				return rpc.OK, result.Res
			case <-time.After(time.Second * 2): // 2秒超时
				return rpc.ErrMaybe, nil
			default:
				rsm.mu.Lock()
				if rsm.killed {
					rsm.mu.Unlock()
					return rpc.ErrWrongLeader, nil
				}
				// 检查当前term是否变化（失去leader）
				currentTerm, isCurrentLeader := rsm.rf.GetState()
				rsm.mu.Unlock()
				if currentTerm != term || !isCurrentLeader {
					DPrintf("RSM[%d] lost leader (term %d -> %d), index %d", rsm.me, term, currentTerm, index)
					return rpc.ErrWrongLeader, nil
				}
		}
	}

	// your code here
	// return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}
