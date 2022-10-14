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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
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
	currentTerm int
	votedFor    int
	// logs      []LogEntry

	// 所有servers的可不持久化变量
	commitIndex int // 状态机中已知的被提交的日志条目的索引值(初始化为0，持续递增）
	lastApplied int // 最后一个被追加到状态机日志的索引值

	// leader的可不持久化变量
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	state    State // 节点
	overtime time.Duration
	timer    time.Ticker
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	//Entries  []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// return currentTerm and whether this server
// believes it is the leader.
// 返回当前任期以及此server是否相信这是领导者。

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm

	isleader = rf.state == Leader

	return term, isleader
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC handler.

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

}

// example code to send a RequestVote RPC to a server.// 向服务器发送RequestVote RPC的示例代码。
// server is the index of the target server in rf.peers[].// server是rf.peers[]中目标服务器的索引。
// expects RPC arguments in args.// 参数中应包含RPC参数。
// fills in *reply with RPC reply, so caller should// 用RPC回复填写*reply，因此调用方应该
// pass &reply.// 通过并回复。
// the types of the args and reply passed to Call() must be// 传递给Call（）的参数和应答的类型必须是
// the same as the types of the arguments declared in the// 与中声明的参数类型相同
// handler function (including whether they are pointers).// 处理程序函数（包括它们是否为指针）。
//
// The labrpc package simulates a lossy network, in which servers// labrpc包模拟有损网络，其中服务器
// may be unreachable, and in which requests and replies may be lost.// 可能无法访问，并且请求和回复可能会丢失。
// Call() sends a request and waits for a reply. If a reply arrives// Call（）发送请求并等待回复。如果收到回复
// within a timeout interval, Call() returns true; otherwise// 在超时间隔内，Call（）返回true；否则
// Call() returns false. Thus Call() may not return for a while.// Call（）返回false。因此，Call（）在一段时间内可能不会返回。
// A false return can be caused by a dead server, a live server that// 错误返回可能是由死机服务器、
// can't be reached, a lost request, or a lost reply.// 无法联系、请求丢失或回复丢失。
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the// 服务器端的处理程序函数不返回。因此，这里
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().// 不需要围绕Call（）实现自己的超时。
//
// look at the comments in ../labrpc/labrpc.go for more details.// 请查看../labrpc/labrpc中的注释。了解更多细节。
//
// if you're having trouble getting RPC to work, check that you've// 如果在让RPC工作时遇到问题，请检查
// capitalized all field names in structs passed over RPC, and// 通过RPC传递的结构中的所有字段名都大写，并且
// that the caller passes the address of the reply struct with &, not// 调用者传递应答结构的地址时使用&，而不是
// the struct itself.// 结构本身。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	return ok
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

	// Your code here (2B).

	return index, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		for rf.killed() == false {
			select {

			case <-rf.timer.C:
				if rf.killed() {
					return
				}
				rf.mu.Lock()
				switch rf.state {
				case Follower:
					rf.state = Candidate
					fallthrough
				case Candidate:
					rf.currentTerm++
					rf.votedFor = rf.me

				}

			}
		}

	}
}

// the service or tester wants to create a Raft server. the ports 服务或测试人员想要创建Raft服务器。
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
