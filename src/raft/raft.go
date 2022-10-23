package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sort"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有servers的持久化变量
	currentTerm int // 2A
	votedFor    int // 2A
	logs        []*LogEntry

	// 所有servers的可不持久化变量
	commitIndex int // 状态机中已知的被提交的日志条目的索引值(初始化为-1，持续递增）
	lastApplied int // 最后一个被追加到状态机日志的索引值 初始化为-1

	// leader的可不持久化变量
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	state             NodeState // 节点状态
	lastReceived      time.Time // 选举超时计时器
	lastBroadcastTime time.Time // leader上次的广播时间
}

// Make the service or tester wants to create a Raft server. the ports 服务或测试人员想要创建Raft服务器。
// of all the Raft servers (including this one) are in peers[]. this// 所有Raft服务器（包括此服务器）中的端口位于对等服务器中[]。这
// server's port is peers[me]. all the servers' peers[] arrays// 服务器的端口是peers[me]。所有服务器的对等[]阵列
// have the same order. persister is a place for this server to// 有相同的顺序。persister是此服务器用来
// save its persistent state, and also initially holds the most// 保存其持久状态，并且最初还保存
// recent saved state, if any. applyCh is a channel on which the// 最近保存的状态（如果有）。applyCh是一个chan
// tester or service expects Raft to send ApplyMsg messages.// 测试人员或服务人员希望Raft发送ApplyMsg消息。
// Make() must return quickly, so it should start goroutines// Make（）必须快速返回，因此它应该启动goroutines
// for any long-running work.// 对于任何长期运行的工作。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.logs = make([]*LogEntry, 0)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.state = Follower
	rf.lastReceived = time.Now()
	rf.lastBroadcastTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	//go rf.ticker()

	go rf.leaderElection()
	go rf.appendEntriesLoop()
	go rf.applyLogLoop(applyCh)

	DPrintf("RaftNode[%d]启动\n", me)
	return rf
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		var appliedMsg = make([]ApplyMsg, 0)
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				DPrintf("apply a Log, rf.lastApplied=[%d]", rf.lastApplied)
				appliedMsg = append(appliedMsg, ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  rf.logs[rf.lastApplied].Term,
				})

			}
		}()
		for _, msg := range appliedMsg {
			applyCh <- msg
		}
	}
}

func (rf *Raft) leaderElection() {
	for !rf.killed() {
		electionTimeout := 150 + rand.Intn(200)
		startTime := time.Now()
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		// 在sleep一段时间后，lastReceived仍未更新（在startTime之前），说明未发送心跳，要开始选举。
		if rf.lastReceived.Before(startTime) {

			// debug找出来的问题2，这样写导致，经过一轮选举没有选出leader，就无法再进行下轮选举
			//if rf.state == Follower {
			if rf.state != Leader {

				rf.lastReceived = time.Now()
				rf.state = Candidate

				DPrintf("RaftNode[%d] Follower -> Candidate\n", rf.me)

				rf.currentTerm += 1
				rf.votedFor = rf.me

				args := RequestVoteArgs{
					rf.currentTerm,
					rf.me,
					len(rf.logs),
					0,
				}

				if len(rf.logs) != 0 {
					args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
				}

				voteCount := 1
				isLeader := false
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go func(p int) {
							reply := RequestVoteReply{}
							DPrintf("RaftNode[%d] RequestVote starts, "+
								"Term[%d] LastLogIndex[%d] LastLogTerm[%d] 向[%d]要票\n",
								rf.me, args.Term, args.LastLogIndex, args.LastLogTerm, p)

							ok := rf.sendRequestVote(p, &args, &reply)
							if !ok {
								DPrintf("RaftNode[%d] RequestVote [%d]no reply ! Term[%d] voteCnt[%d]\n",
									rf.me, p, args.Term, voteCount)
								return
							}

							//DPrintf("RaftNode[%d] RequestVoting 正在进行！ "+
							//	"Term[%d] LastLogIndex[%d] LastLogTerm[%d] 取得[%d]的票 reply.Term[%d]\n",
							//	rf.me, args.Term, args.LastLogIndex, args.LastLogTerm, p, reply.Term)

							rf.mu.Lock()
							defer rf.mu.Unlock()
							if !reply.VoteGranted {
								if reply.Term > rf.currentTerm {
									rf.currentTerm = reply.Term
									rf.state = Follower
								}
								return
							}
							// 获得选票
							voteCount++
							if voteCount > len(rf.peers)/2 {
								rf.state = Leader
								for i := 0; i < len(rf.peers); i++ {
									rf.nextIndex[i] = len(rf.logs)
								}
								for i := 0; i < len(rf.peers); i++ {
									rf.matchIndex[i] = 0
								}
								// 防止发多次日志
								if isLeader == false {
									isLeader = true
									go rf.appendEntriesLoop()
								}
								DPrintf("RaftNode[%d] RequestVote ends, voteCount[%d] Role[%d] currentTerm[%d]\n",
									rf.me, voteCount, rf.state, rf.currentTerm)
							}
						}(i)
					}
				}

			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader {
				return
			}

			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now()

			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					go func(id int) {
						rf.mu.Lock()
						args := AppendEntriesArgs{
							Term:         rf.currentTerm,
							LeaderId:     rf.me,
							PrevLogIndex: rf.nextIndex[id] - 1,
							Entries:      make([]*LogEntry, 0),
							LeaderCommit: rf.commitIndex,
						}
						args.Entries = append(args.Entries, rf.logs[rf.nextIndex[id]:]...)
						if args.PrevLogIndex >= 0 {
							//fmt.Printf("%d %d\n",args.PrevLogIndex,len(rf.logs))
							args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
						}
						rf.mu.Unlock()

						DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] sendLogTo[%d]  nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
							rf.me, rf.currentTerm, id, rf.nextIndex[id], rf.matchIndex[id], len(args.Entries), rf.commitIndex)

						if rf.state != Leader {
							return
						}

						reply := AppendEntriesReply{}
						if ok := rf.sendAppendEntries(id, &args, &reply); ok {
							rf.mu.Lock()
							defer rf.mu.Unlock()

							defer func() {
								DPrintf("RaftNode[%d] appendEntries ends, currentTerm[%d]  peer[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
									rf.me, rf.currentTerm, id, rf.nextIndex[id], rf.matchIndex[id], rf.commitIndex)
							}()

							// 发送日志时，leader死了
							//if rf.currentTerm != args.Term {
							//	return
							//}

							if reply.Term > rf.currentTerm {
								rf.state = Follower
								rf.currentTerm = reply.Term
								rf.votedFor = -1
								//rf.persist()
								return
							}
							if reply.Success {
								if len(args.Entries) == 0 {
									return
								}
								rf.nextIndex[id] += len(args.Entries)
								rf.matchIndex[id] = rf.nextIndex[id] - 1

								sortedMatchIndex := make([]int, 0)
								sortedMatchIndex = append(sortedMatchIndex, len(rf.logs))
								for i := 0; i < len(rf.peers); i++ {
									if i == rf.me {
										continue
									}
									sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
								}
								sort.Ints(sortedMatchIndex)
								newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
								DPrintf("RaftNode[%d] before updateCommitIndex, newCommitIndex[%d] matchIndex[%v]"+
									" rf.logs[newCommitIndex].Term[%d] rf.currentTerm[%d]",
									rf.me, newCommitIndex, sortedMatchIndex, rf.logs[newCommitIndex].Term, rf.currentTerm)

								if newCommitIndex > rf.commitIndex && rf.logs[newCommitIndex].Term == rf.currentTerm {
									rf.commitIndex = newCommitIndex
								}
								DPrintf("RaftNode[%d] updateCommitIndex, commitIndex[%d] matchIndex[%v]",
									rf.me, rf.commitIndex, sortedMatchIndex)

							} else {
								rf.nextIndex[id] -= 1
								if rf.nextIndex[id] < 0 {
									rf.nextIndex[id] = 0
								}
							}
						} else {
							DPrintf("RaftNode[%d] appendEntries send fail ! rf.killed()=[%v]\n",
								rf.me, rf.killed())
						}
					}(i)
				}
			}

		}()
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
// 返回当前任期以及此server是否相信这是领导者。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isLeader bool
	// Your code here (2A).
	term = rf.currentTerm
	isLeader = rf.state == Leader

	return term, isLeader
}

// example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]\n",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] VoteGranted[%v] \n", rf.me, args.CandidateId, reply.VoteGranted)
	}()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		// debug找出来的问题1
		rf.votedFor = -1
		// 继续向下走进行投票
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// candidate的日志必须比我的新
		// 1 日志长度相同，最后一条日志term大，更新
		// 2 更长的日志，更新
		lastLogTerm := 0
		if len(rf.logs) != 0 {
			lastLogTerm = rf.logs[len(rf.logs)-1].Term
		}
		if args.LastLogTerm < lastLogTerm || args.LastLogIndex < len(rf.logs) {
			return
		}
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastReceived = time.Now()
	}
	//rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("RaftNode[%d] Handle AppendEntries enter func() ! "+
	//	"LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%d]\n",
	//	rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.state)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%v]  prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
		rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.state, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)

	defer func() {
		DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v]",
			rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.state, args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, len(rf.logs))
	}()

	rf.lastReceived = time.Now()

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大term，转为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		//rf.persist()
	}

	// follower日志长度小于leader，返回false
	if len(rf.logs) < args.PrevLogIndex {
		return
	}
	// follower和leader的日志在相同PrevLogIndex下的Term不同，返回false
	if args.PrevLogIndex > -1 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index > len(rf.logs)-1 {
			// 超出部分追加
			rf.logs = append(rf.logs, logEntry)
		} else {
			if rf.logs[index-1].Term != logEntry.Term {
				rf.logs = rf.logs[:index-1]
				rf.logs = append(rf.logs, logEntry)
			}
		}
	}
	//rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if len(rf.logs) < rf.commitIndex {
			rf.commitIndex = len(rf.logs)
		}
	}

	reply.Success = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// had more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Start the service using Raft (e.g. a k/v server) wants to start
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		return -1, -1, false
	}

	term = rf.currentTerm
	logEntry := LogEntry{
		Command: command,
		Term:    term,
	}
	rf.logs = append(rf.logs, &logEntry)
	index = len(rf.logs) - 1

	DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
	return index, term, isLeader
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
