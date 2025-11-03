package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
	// "go/printer"
	"math/rand"

	// "sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// 定义用户角色枚举
type Role int

const (
	Follower  Role = iota // 0
	Candidate             // 1
	Leader                // 2
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases	monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	//outside paper
	electionTimeout        int64                 // 选举超时时长
	electionTimeoutBeginTs int64                 // 开始计算选举超时的时间戳
	state                  Role                  // 0: follwer, 1: candidate, 2: leader
	voteCnt                int                   // 获得到的投票数
	applyCh                chan raftapi.ApplyMsg //和客户端的通信信道

	//snapshot
	lastIncludedIndex int // 日志压缩
	lastIncludedTerm int // 日志压缩
	
}

func (rf *Raft) GetRoleStr() string {
	if rf.state == Follower {
		return "Follower"
	}
	if rf.state == Candidate {
		return "Candidate"
	}
	if rf.state == Leader {
		return "Leader"
	}
	return "None"
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.state == Leader
}

func (rf *Raft) persistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

    e.Encode(rf.currentTerm)
    e.Encode(rf.votedFor)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
    e.Encode(rf.log)

    data := w.Bytes()
    return data
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	raftstate := rf.persistState()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&lastIncludedIndex)  != nil || d.Decode(&lastIncludedTerm)  != nil || d.Decode(&logs)  != nil  {
		DebugPretty(dError, "S%d decode error happend", rf.me)
	}else{
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		DebugPretty(dError, "S%d decode success,term:%, votedFor:%d", rf.me, rf.currentTerm, rf.votedFor)
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	DebugPretty(dSnap, "S%d snapshot idx:%d, log:%v, snapshot:%v", rf.me, rf.log, snapshot)
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int //PrevLogIndex 表示前一条日志条目的索引，即当前要发送的日志条目组的前一个位置。
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	ConflictIndex int // optional: fast-backoff hint
    ConflictTerm  int // optional: fast-backoff hint
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DebugPretty(dLog, "S%d(%d) <- S%d(%d) 的ApdEty: %v" ,rf.me, rf.currentTerm, args.LeaderId, args.Term, args)
	// 1. 对于rpc任期小于当前任期的AppendEntries包直接返回, 通知领导者任期过期，促使其回退成为follwer
	if args.Term < rf.currentTerm {
		DebugPretty(dHeart, "S%d(%d) 收到 S%d(%d) 的ApdEty,任期条件不满足", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 对于rpc任期大于当前任期的AppendEntries，直接转为Follow状态, 且更新自己的任期, 不用返回
	if args.Term > rf.currentTerm{
		rf.becomeFollwer(args.Term, true)
	}

	// 重置超时
	rf.resetTimeout()
	reply.Term = args.Term

	// 2. 日志一致性检查，args.prevLogIndex 超出了自己日志的最后一个索引（即自己的日志太短），回复false
	if args.PrevLogIndex > len(rf.log) -1{
		reply.Success = false
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		DebugPretty(dLog, "S%d(%d) <- S%d(%d) 的ApdEty, 日志PrevLogIndex不匹配, reply:%v", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply)
		return
	}

	// 检查prevLogIndex处是否存在日志冲突
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		// 找到冲突任期的第一个索引
        index := args.PrevLogIndex
        // 向前遍历日志，找到 ConflictTerm 这个任期的第一个条目
        for index > 0 && rf.log[index-1].Term == reply.ConflictTerm {
            index--
        }
        reply.ConflictIndex = index // 这是 ConflictTerm 这个任期开始的位置
		DebugPretty(dHeart, "S%d(%d) <- S%d(%d) 的ApdEty, 日志PrevLogTerm不匹配, reply:%v", rf.me, rf.currentTerm, args.LeaderId, args.Term, reply)
		return
	}
	if len(args.Entries) == 0 {
		DebugPretty(dHeart, "S%d(%d) 收到 S%d(%d) 的心跳", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	}
	
	// 3. 到此处，说明PrevLogIndex和PrevLogTerm完全匹配上了
	for i, v := range args.Entries {
        idx := args.PrevLogIndex + 1 + i
		// 跳过那些重复的日志
		if idx <= len(rf.log) - 1 {
			if rf.log[idx].Term != v.Term {
				// 截断本地日志并追加新日志
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
			// term一致则跳过
		} else {
			rf.log = append(rf.log, v)
			rf.persist()
		}
    }

	if len(args.Entries) != 0 {
		DebugPretty(dLog, "S%d(%d) 收到 S%d(%d) 的ApdEty, 匹配上, 目前日志最大为%v(%d) ", rf.me, rf.currentTerm, args.LeaderId, args.Term, rf.log[len(rf.log) - 1].Command, rf.log[len(rf.log) - 1].Index)
	}
	
	// 4. 更新 commitIndex 与应用（apply）
	// if args.LeaderCommit > rf.commitIndex {
	// 	newCommitIndex := min(args.LeaderCommit, len(rf.log) - 1)
	// 	if newCommitIndex > rf.commitIndex {
	// 		rf.commitIndex = newCommitIndex
	// 		DebugPretty(dCommit, "S%d(%d) commitIdx被更新到%d, 此处日志:%v", rf.me, rf.currentTerm, rf.commitIndex, rf.log[rf.commitIndex].Command)
	// 	}
	// }

	lastNewIndex := args.PrevLogIndex + len(args.Entries)
	newCommit := min(args.LeaderCommit, lastNewIndex)
	if newCommit > rf.commitIndex {
		rf.commitIndex = newCommit
		DebugPretty(dCommit, "S%d(%d) commitIdx被更新到%d, 此处日志:%v", rf.me, rf.currentTerm, rf.commitIndex, rf.log[rf.commitIndex].Command)
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 检查日志是否至少和自己一样新, 如果新来的日志比当前节点的日志更新，则返回true
// 比较Candidate的日志是否至少和自己一样新：
// 比较最后一条日志的term，如果Candidate的term更大，则通过
// 如果term相同，则比较index，Candidate的index更大或相等则通过
func (rf *Raft) isLogNewer(lastLogTerm int, lastLogIndex int) bool {
	lastLogEntry := rf.log[len(rf.log)-1]
	curLastLogIndex := lastLogEntry.Index
	curLastLogTerm := lastLogEntry.Term
	if lastLogTerm > curLastLogTerm {
		return true
	}

	if lastLogTerm == curLastLogTerm && lastLogIndex >= curLastLogIndex {
		return true
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DebugPretty(dVote, "S%d(%d) <- rpc S%d(%d) 收到投票请求", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	// 投票规则
	//1 任期检查
	// 如果请求中的term < 自己的当前term，拒绝投票，并直接返回
	// 如果请求中的term > 自己的当前term，更新自己的term并转为Follower
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DebugPretty(dVote, "S%d(%d) <- S%d(%d) 投票请求并拒绝，任期条件不满足, args:%v, (rpc)%v, (cur)%v", rf.me, rf.currentTerm, args.CandidateId, args.Term, args, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollwer(args.Term, false)
	}

	reply.Term = args.Term
	//2 本投票周期内没有投票给其他人，且候选人的日志更加新，则投票给他
	is_lognewer := rf.isLogNewer(args.LastLogTerm, args.LastLogIndex)
	if is_lognewer && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetTimeout()
		rf.persist()
		DebugPretty(dVote, "S%d <- (rpc)S%d 投票请求并赞成，args:%v", rf.me, args.CandidateId, args)
		return
	}
	DebugPretty(dVote, "S%d(%d) <- S%d(%d) 投票请求并拒绝，args:%v, 日志是否更新:%v, votedFor:%v", rf.me, rf.currentTerm, args.CandidateId, args.Term, args, is_lognewer, rf.votedFor)
	reply.VoteGranted = false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) makeAppendEntries(server int) (AppendEntriesArgs, AppendEntriesReply) {
	preLogIdx := rf.nextIndex[server] - 1
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: preLogIdx, PrevLogTerm: rf.log[preLogIdx].Term, Entries: make([]LogEntry, len(rf.log[preLogIdx + 1:])), LeaderCommit: rf.commitIndex}
	copy(args.Entries, rf.log[preLogIdx + 1:])
	return args, AppendEntriesReply{}
}

func (rf *Raft) replicateToPeer(serverId int) {
	rf.mu.Lock()
	if rf.killed() || rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	args, reply := rf.makeAppendEntries(serverId)
	DebugPretty(dLog, "S%d(%d) -> S%d发送日志复制, from %v~%v",rf.me, rf.currentTerm, serverId, args.PrevLogIndex+1, len(rf.log)-1)
	rf.mu.Unlock()
	
	ok := rf.sendAppendEntries(serverId, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		// 1.  reply.Term > currentTerm，说明响应来自一个更新的任期。领导者会立即承认自己过时, 转为follower；如果 reply.Term < currentTerm，这是一个陈旧的响应，可以直接忽略
		if reply.Term > rf.currentTerm { // 如果其他节点的任期更大，则回退成为follwer
			DebugPretty(dLog, "S%d(%d) 任期没 S%d(%d)大，回退成为flw",rf.me, rf.currentTerm, serverId, reply.Term)
			rf.becomeFollwer(reply.Term, true)
			return
		}

		if rf.state != Leader {
			return
		}

		// 2. 处理任期相同的日志复制
		if reply.Success {
			// 2.1 更新 nextIndex 和 matchIndex：这意味着跟随者成功接收了日志条目。
			rf.nextIndex[serverId] = max(args.PrevLogIndex + len(args.Entries) + 1, rf.nextIndex[serverId])
			rf.matchIndex[serverId] = rf.nextIndex[serverId] - 1
			DebugPretty(dLog, "S%d(%d) 已经将日志%v成功复制到%d, rf.nextIdx=%d", rf.me, rf.currentTerm, args, serverId, rf.nextIndex[serverId])

			// 2.2 领导者只能提交当前任期的日志条目，并且是那些已经被大多数节点复制了的条目
			for j := len(rf.log) - 1; j > rf.commitIndex; j-- {
				if rf.log[j].Term != rf.currentTerm { // 只能提交本周期内的日志
					continue
				}
				count := 1
				for k := 0; k < len(rf.matchIndex); k++ {
					if k != rf.me && rf.matchIndex[k] >= j {
						count++
					}
				}
				if count > len(rf.matchIndex)/2{ // 只需找到最大即可
					rf.commitIndex = j
					DebugPretty(dLog, "S%d(%d) 更新commitIdx=%d",rf.me, rf.currentTerm, rf.commitIndex)
					break
				}
			}
		}else{ // 日志不匹配
			oldNextIndex := rf.nextIndex[serverId]
			// rf.nextIndex[serverId] = max(1, rf.nextIndex[serverId] - 1)

			// 使用冲突信息优化 nextIndex
			if reply.ConflictTerm == -1 {
				// 情况1: 跟随者日志太短
				rf.nextIndex[serverId] = reply.ConflictIndex
			} else{
				// 情况2: 任期冲突
				conflictTermIndex := -1
				for i := args.PrevLogIndex; i > 0; i-- {
					if rf.log[i].Term == reply.ConflictTerm {
						conflictTermIndex = i
						break
					}
				}
				if conflictTermIndex != -1 {
					// 领导者也有这个任期的日志
					// 将 nextIndex 设置为此任期的最后一条日志的下一个索引
					// 这样下次 RPC 的 PrevLogIndex 就会指向我们共同拥有的这个任期的最后一条日志
					rf.nextIndex[serverId] = conflictTermIndex + 1
				} else {
					// 领导者根本没有这个任期的日志
					// 这意味着 Follower 的整个 ConflictTerm 任期的日志都是领导者没有的
					// 因此，领导者应该直接回退到 Follower 给出的 ConflictIndex
					// 跳过 Follower 的整个 ConflictTerm 任期
					rf.nextIndex[serverId] = reply.ConflictIndex
				}
				// rf.nextIndex[serverId] = conflictTermIndex
			}
			rf.nextIndex[serverId] = min(rf.nextIndex[serverId], oldNextIndex - 1)
			rf.nextIndex[serverId] = max(1, rf.nextIndex[serverId])

			DebugPretty(dLog, "S%d(%d) 发送日志复制到%d 日志不匹配, 回退nextIdx=%d",rf.me, rf.currentTerm, serverId, rf.nextIndex[serverId])
			go rf.replicateToPeer(serverId)
		}
	}else{
		DebugPretty(dDrop, "S%d(%d) 发送日志复制到%d 消息丢失",rf.me, rf.currentTerm, serverId)
	}
}

func (rf *Raft) replicateToAllPeers() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.replicateToPeer(i)
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	term, isLeader := rf.GetState()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !isLeader {
		return -1, -1, false // 不是leader的话直接返回
	}
	
	logEntry := LogEntry{Index: len(rf.log), Term: term, Command: command}
	DebugPretty(dLog, "S%d(%d) 收到了一条日志: %v(%v)", rf.me, rf.currentTerm, logEntry.Index ,command)
	rf.log = append(rf.log, logEntry)
	rf.persist()
	rf.replicateToAllPeers()
	return logEntry.Index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetTimeout() {
	rf.electionTimeout = 500 + (rand.Int63() % 500) // 重置超时时间
	rf.electionTimeoutBeginTs = time.Now().UnixMilli()
}

func (rf *Raft) becomeFollwer(term int, resetTimeout bool) {
	if resetTimeout {
		rf.resetTimeout()
	}
	rf.state = Follower // Follwer
	rf.voteCnt = 0
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm++ // 当前任期号自增1
	rf.resetTimeout()
	rf.state = Candidate // Candidate
	rf.voteCnt = 1       // 为自己投一票
	rf.votedFor = rf.me
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader // Leader
	rf.votedFor = rf.me
	rf.initNextAndMatchIndex()
	// 成为领导人后立刻发送心跳消息给其他节点
	rf.replicateToAllPeers()
	rf.persist()
}

func (rf *Raft) sendHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				if rf.state != Leader || rf.killed(){
					rf.mu.Unlock()
					return
				}
				args, reply := rf.makeAppendEntries(server)
				rf.mu.Unlock()
				DebugPretty(dHeart, "S%d -> S%d 发心跳", rf.me, server)
				ok := rf.sendAppendEntries(server, &args, &reply)

				rf.mu.Lock()
				if ok && rf.state == Leader  {
					if rf.currentTerm < reply.Term { // 任期小于follwer，回退成为follwer
						rf.becomeFollwer(reply.Term, true)
						DebugPretty(dLeader, "S%d 当前任期%v小于 S%d 的任期%v, 自身回退成为follwer", rf.me, rf.currentTerm, server, reply.Term)
					}
					if !reply.Success {
						// rf.nextIndex[server] = max(1, rf.nextIndex[server] - 1)
						oldNextIndex := rf.nextIndex[server]
						// rf.nextIndex[serverId] = max(1, rf.nextIndex[serverId] - 1)
			
						// 使用冲突信息优化 nextIndex
						if reply.ConflictTerm == -1 {
							// 情况1: 跟随者日志太短
							rf.nextIndex[server] = reply.ConflictIndex
						} else{
							// 情况2: 任期冲突
							conflictTermIndex := -1
							for i := args.PrevLogIndex; i > 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									conflictTermIndex = i
									break
								}
							}
							if conflictTermIndex != -1 {
								// 领导者也有这个任期的日志
								// 将 nextIndex 设置为此任期的最后一条日志的下一个索引
								// 这样下次 RPC 的 PrevLogIndex 就会指向我们共同拥有的这个任期的最后一条日志
								rf.nextIndex[server] = conflictTermIndex + 1
							} else {
								// 领导者根本没有这个任期的日志
								// 这意味着 Follower 的整个 ConflictTerm 任期的日志都是领导者没有的
								// 因此，领导者应该直接回退到 Follower 给出的 ConflictIndex
								// 跳过 Follower 的整个 ConflictTerm 任期
								rf.nextIndex[server] = reply.ConflictIndex
							}
							// rf.nextIndex[serverId] = conflictTermIndex
						}
						rf.nextIndex[server] = min(rf.nextIndex[server], oldNextIndex - 1) // 确保至少自减1
						rf.nextIndex[server] = max(1, rf.nextIndex[server]) // 确保>=1
						DebugPretty(dLog, "S%d(%d) 发送心跳到%d 日志不匹配, 回退nextIdx=%d",rf.me, rf.currentTerm, server, rf.nextIndex[server])
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

// 开始一场选举
func (rf *Raft) startElection() {
	// 向其他节点发送请求投票消息
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				lastLogEntry := rf.log[len(rf.log)-1]
				args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogEntry.Index, LastLogTerm: lastLogEntry.Term}
				DebugPretty(dVote, "S%d(%d) -> %d 请求投票(选举超时),args:%v", rf.me, rf.currentTerm, server, args)
				rf.mu.Unlock()
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)

				// 收集投票情况
				rf.mu.Lock()
				if ok {
					// 1.如果不是候选人状态就没必要处理投票结果了
					if rf.state != Candidate { 
						rf.mu.Unlock()
						return
					}
					// 2.如果响应中的任期号 T_response 大于候选人自己当前的任期号, 说明候选人已经过时了, 回退为跟随者，并更新自己的currentTerm
					if reply.Term > rf.currentTerm {
						rf.becomeFollwer(reply.Term, true)
						DebugPretty(dLeader, "S%d 本次选举失败,因为有更大任期,回退为Flw", rf.me)
						rf.mu.Unlock()
						return
					}

					//3. 如果响应中的任期号小于候选人当前的, 则直接忽略这个响应
					if reply.Term < rf.currentTerm {
						rf.mu.Unlock()
						return
					}

					//4. 处理有效任期下的投票结果
					if reply.VoteGranted {
						rf.voteCnt++
						if rf.voteCnt > len(rf.peers)/2 {
							DebugPretty(dLeader, "S%d 成为领导者(%d)", rf.me, rf.currentTerm)
							rf.becomeLeader()
						}
					}
				}
				rf.mu.Unlock()
			}(i)
		}
	}
}

func (rf *Raft) applyMsg() {
	for {
		rf.mu.Lock()
		if !rf.killed() {
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				msg := raftapi.ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
				DebugPretty(dCommit, "S%d 提交日志%v(%d)到通道", rf.me, rf.log[rf.lastApplied].Command, rf.lastApplied)
				rf.applyCh <- msg
			}
		}
		rf.mu.Unlock()
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) ticker() {

	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		now_ms := time.Now().UnixMilli() // 获取当前时间
		rf.mu.Lock()
		if rf.state != Leader && now_ms > rf.electionTimeoutBeginTs + rf.electionTimeout { // 发生选举超时
			DebugPretty(dTimer, "S%d 开始发起选举", rf.me)
			rf.becomeCandidate()
			rf.startElection() // 开启新选举
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			rf.replicateToAllPeers()
		}
		rf.mu.Unlock()
		
	}
}

func (rf *Raft) initNextAndMatchIndex() {
	rf.nextIndex = make([]int, len(rf.peers)) // 初始化最后一个日志的index + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log) // 初始化都是1
	}

	rf.matchIndex = make([]int, len(rf.peers)) // 初始化为0
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.dead = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{}
	rf.log = append(rf.log, LogEntry{0, 0, nil})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.initNextAndMatchIndex()

	rf.electionTimeout = 500 + (rand.Int63() % 500) // 500 ~ 1000ms
	rf.electionTimeoutBeginTs = time.Now().UnixMilli()
	rf.state = Follower
	rf.voteCnt = 0
	rf.applyCh = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()

	return rf
}
