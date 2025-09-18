package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	term = int(rf.currentTerm)
	isleader = rf.state == Leader

	// Your code here (3A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DebugPretty(dHeart, "AppendEntries args:%v", args)

	// 对于rpc任期小于当前任期的AppendEntries包直接返回, 通知领导者任期过期，促使其回退成为follwer
	if args.Term < rf.currentTerm {
		DebugPretty(dHeart, "S%d(%d) 收到 S%d(%d) 的ApdEty,任期条件不满足", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	// 对于rpc任期大于当前任期的AppendEntries，直接转为Follow状态, 且更新自己的任期, 不用返回
	if args.Term > rf.currentTerm{
		rf.becomeFollwer()
		rf.currentTerm = args.Term
	}

	// 重置超时
	rf.resetTimeout()
	reply.Term = args.Term

	// 2. 日志一致性检查，args.prevLogIndex 超出了自己日志的最后一个索引（即自己的日志太短），回复false
	if args.PrevLogIndex > len(rf.log) -1{
		DebugPretty(dHeart, "S%d(%d) 收到 S%d(%d) 的ApdEty, 日志log idx不匹配", rf.me, len(rf.log) -1, args.LeaderId, args.PrevLogIndex)
		reply.Success = false
		return
	}

	// 检查prevLogIndex处是否存在日志冲突
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DebugPretty(dHeart, "S%d(%d) 收到 S%d(%d) 的ApdEty, 日志log term不匹配", rf.me, rf.currentTerm, args.LeaderId, args.Term)
		reply.Success = false
		return
	}
	
	// 如果是心跳包，直接返回
	if len(args.Entries) == 0 {
		DebugPretty(dHeart, "S%d -> S%d 心跳成功", rf.me, args.LeaderId)
		reply.Success = true
		return
	}

	// if len(args.Entries) == 0 {
	// 	reply.Success = true
	// 	if args.LeaderCommit > rf.commitIndex{
	// 		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
	// 	}
	// 	return
	// }else{
	// 如果是日志复制包
	// PrevLogIndex日志匹配上了
	if len(rf.log) > args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm { // PrevLogIndex有日志，且term相同, 说明完全匹配上了
		// 是老的日志复制消息，直接修改对应下标
		if args.PrevLogIndex+len(args.Entries) < len(rf.log) {
			for index, value := range args.Entries {
				rf.log[args.PrevLogIndex+index+1] = value
			}
		} else {
			rf.log = rf.log[:args.PrevLogIndex+1]
			rf.log = append(rf.log, args.Entries...)
		}
		reply.Success = true
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex)
		}
	} else {
		// 如果日志匹配失败，删除多余的日志
		if args.PrevLogIndex >= 0 && args.PrevLogIndex <= len(rf.log) {
			rf.log = rf.log[:args.PrevLogIndex]
		}
		DebugPretty(dHeart, "S%d <- (rpc)S%d AppendEntries，日志匹配失败，回退日志", rf.me, args.LeaderId)
		reply.Success = false
	}
	// }
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
	// 投票规则
	//1 任期检查
	// 如果请求中的term < 自己的当前term，拒绝投票，并直接返回
	// 如果请求中的term > 自己的当前term，更新自己的term并转为Follower
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DebugPretty(dVote, "S%d <- S%d 投票请求并拒绝，任期条件不满足, args:%v, (rpc)%v, (cur)%v", rf.me, args.CandidateId, args, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.becomeFollwer()
		rf.votedFor = -1
	}

	reply.Term = args.Term
	//2 本投票周期内没有投票给其他人，且候选人的日志更加新，则投票给他
	is_lognewer := rf.isLogNewer(args.LastLogTerm, args.LastLogIndex)
	if is_lognewer && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetTimeout()
		DebugPretty(dVote, "S%d <- (rpc)S%d 投票请求并赞成，args:%v", rf.me, args.CandidateId, args)
		return
	}
	DebugPretty(dVote, "S%d <- S%d 投票请求并拒绝，args:%v, 日志是否更新:%v, votedFor:%v", rf.me, args.CandidateId, args, is_lognewer, rf.votedFor)
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
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[server] - 1, PrevLogTerm: rf.nextIndex[server] - 1, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	}
	return args, AppendEntriesReply{}
}

// 避免不可靠网络的影响，不断发送AppendEntries直到成功
func (rf *Raft) sendAppendEntriesUntilOk(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for {
		if rf.killed() || rf.state != Leader {
			return
		}
		ok := rf.sendAppendEntries(server, args, reply)
		if ok {
			if reply.Success { // 日志复制成功
				rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1

				// 判断是否需要更新commit
				numsCopy := make([]int, len(rf.matchIndex))
				copy(numsCopy, rf.matchIndex[:])
				sort.Ints(numsCopy)
				rf.commitIndex = numsCopy[(len(numsCopy)+1)/2]

				return
			} else {
				if reply.Term > rf.currentTerm { // 日志复制失败的原因是任期落后
					rf.becomeFollwer()
					return
				} else { // 日志复制失败的原因是日志冲突, 则不断降低PrevLogIndex, PrevLogTerm
					rf.mu.Lock()
					rf.nextIndex[server]--
					rf.mu.Unlock()
					*args, *reply = rf.makeAppendEntries(server)
					args.Entries = rf.log[args.PrevLogIndex+1:]
					continue
				}
			}
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	term, isLeader = rf.GetState()
	if !isLeader {
		return index, term, isLeader // 不是leader的话直接返回
	}

	rf.mu.Lock()
	logEntry := LogEntry{Index: len(rf.log), Term: term, Command: command}
	rf.log = append(rf.log, logEntry)
	rf.mu.Unlock()

	// 立即向所有节点发送AppendEntries消息
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				args, reply := rf.makeAppendEntries(server)
				// args.Entries = [] LogEntry{logEntry}
				args.Entries = rf.log[args.PrevLogIndex+1:]
				if rf.killed() || rf.state != Leader {
					return
				}
				rf.sendAppendEntriesUntilOk(server, &args, &reply)
			}(i)
		}
	}
	term, isLeader = rf.GetState()
	return logEntry.Index, term, isLeader
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

func (rf *Raft) becomeFollwer() {
	rf.resetTimeout()
	rf.state = Follower // Follwer
	rf.voteCnt = 0
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm++ // 当前任期号自增1
	rf.resetTimeout()
	rf.state = Candidate // Candidate
	rf.voteCnt = 1       // 为自己投一票
	rf.votedFor = rf.me
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader // Leader
	rf.votedFor = rf.me
	rf.initNextAndMatchIndex()
	// 成为领导人后立刻发送心跳消息给其他节点
	rf.sendHeartBeat()
}

func (rf *Raft) sendHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				rf.mu.Lock()
				if rf.state != Leader || rf.killed(){{
					rf.mu.Unlock()
					return
				}}
				args, reply := rf.makeAppendEntries(server)
				rf.mu.Unlock()
				DebugPretty(dHeart, "S%d -> S%d 发心跳", rf.me, server)
				ok := rf.sendAppendEntries(server, &args, &reply)

				rf.mu.Lock()
				if ok && !reply.Success && rf.state == Leader  {
					if rf.currentTerm < reply.Term { // 任期小于follwer，回退成为follwer
						rf.becomeFollwer()
						DebugPretty(dLeader, "S%d 当前任期%v小于 S%d 的任期%v, 自身回退成为follwer", rf.me, rf.currentTerm, server, reply.Term)
						rf.currentTerm = reply.Term
					} else { // PrevLogIndex和PrevLogTerm不匹配
						rf.nextIndex[server]--
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
						rf.becomeFollwer()
						rf.currentTerm = reply.Term
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
			if rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				msg := raftapi.ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
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
			rf.sendHeartBeat()
		}
		rf.mu.Unlock()
		
	}
}

func (rf *Raft) initNextAndMatchIndex() {
	rf.nextIndex = make([]int, len(rf.peers)) // 初始化最后一个日志的index + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = len(rf.log)
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()

	return rf
}
