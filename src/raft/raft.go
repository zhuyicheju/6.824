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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term int32
	Log  interface{}
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	currentTerm int32
	votedFor    int

	log []LogEntry // need to be implented

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	//自己添加

	heartbeat_timestamp int64

	state int32

	median_tracker *MedianTracker
}

type RequestVoteArgs struct {
	Term         int32
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int32
}

type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int32
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int32

	Entries []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Me         int
	Term       int32
	Success    bool
	Append_num int
	Next_index int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.state == LEADER
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	reply.Me = rf.me

	if args.Term < currentTerm {
		//任期过低
		reply.Success = false
		return
	}

	if args.PrevLogIndex > len(rf.log)-1 || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		// logPrintf("%v %v %v\n", rf.me, args.PrevLogIndex, len(rf.log)-1)
		//日志不匹配
		//如果只是简单的对NextIndex逐步减1，则这该测试点很可能不通过。(leader backs up quickly over incorrect follower logs)
		//两种情况，prevlogindex可能在当前log右边，或者在左边和中间
		if len(rf.log) != 1 && args.PrevLogIndex <= len(rf.log)-1 {
			rf.log = rf.log[:args.PrevLogIndex-1]
		}
		reply.Next_index = min(args.PrevLogIndex, len(rf.log))
		reply.Success = false
		return
	}

	if args.LeaderCommit > rf.commitIndex {
		if rf.commitIndex == 0 {
			rf.commitIndex++
		}
		for i := rf.commitIndex; i <= min(args.LeaderCommit, len(rf.log)-1); i++ {
			// logPrintf("Server %v: 提交日志 %v", rf.me, i)
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Log, CommandIndex: i}
		}
		rf.commitIndex = args.LeaderCommit
	}
	rf.log = append(rf.log, args.Entries...)
	reply.Append_num = len(args.Entries)
	reply.Success = true
	rf.heartbeat_timestamp = time.Now().UnixMilli()
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	// logPrintf("Serve %v: AppendEntries log %v commit %v\n", rf.me, len(rf.log)-1, rf.commitIndex)

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	if rf.killed() {
		return
	}

	rf.mu.Lock()

	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	if args.Term < currentTerm || (args.Term == currentTerm && rf.votedFor != -1) ||
		args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
		args.LastLogIndex < len(rf.log)-1 {
		// logPrintf("Serve %v: RequestVote %v %v %v %v %v\n", rf.me, args.CandidatedId, args.Term, rf.currentTerm, reply.VoteGranted, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	rf.votedFor = args.CandidatedId
	reply.VoteGranted = true
	rf.heartbeat_timestamp = time.Now().UnixMilli()
	rf.currentTerm = args.Term
	rf.state = FOLLOWER
	// logPrintf("Serve %v: RequestVote %v %v %v %v %v\n", rf.me, args.CandidatedId, args.Term, rf.currentTerm, reply.VoteGranted, rf.votedFor)

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's // log if this
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := int(rf.currentTerm)
	isLeader := (rf.state == LEADER)

	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Log: command})
		rf.median_tracker.Add(rf.me, len(rf.log)-1)
		// logPrintf("Server %v: Start 是否leader %v, Trf.logerm %v, Index %v\n", rf.me, rf.state == LEADER, rf.currentTerm, len(rf.log))
	}

	return index, term, isLeader
}

func Leader(rf *Raft) {
	//进入时持有锁
	rf.state = LEADER
	peers_num := len(rf.peers)

	for i := range rf.matchIndex {
		//初始化每个数组的匹配日志
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = len(rf.log)
	}

	for rf.killed() == false {
		var rw sync.RWMutex
		done := false
		replyCh := make(chan *AppendEntriesReply, peers_num-1)

		for i := 0; i < peers_num; i++ {
			if i == rf.me {
				continue
			}

			//func是按引用捕获
			go func(server int) {
				entries := make([]LogEntry, 0)
				if rf.matchIndex[server] != -1 && len(rf.log)-1 >= rf.nextIndex[server] {
					entries = append(entries, rf.log[rf.nextIndex[server]:]...)
				}
				// logPrintf("Server %v: 向%v发送心跳 prev %v\n", rf.me, server, rf.nextIndex[server]-1)
				args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me,
					PrevLogIndex: rf.nextIndex[server] - 1, PrevLogTerm: rf.log[rf.nextIndex[server]-1].Term,
					LeaderCommit: rf.commitIndex,
					Entries:      entries}
				reply := AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

				var msg *AppendEntriesReply
				if ok {
					msg = &reply
				} else {
					msg = nil
				}

				rw.RLock()
				if !done {
					replyCh <- msg
				}
				rw.RUnlock()
			}(i)
		}

		rf.mu.Unlock()

		ms := 100
		//每秒<=10次beat
		time.Sleep(time.Duration(ms) * time.Millisecond) //选举超时时间

		rw.Lock()
		done = true
		close(replyCh)
		rw.Unlock()

		rf.mu.Lock()

		if rf.state == FOLLOWER {
			//因接到更高term的rpc
			//持有锁退出
			return
		}

		for i := 0; i < peers_num-1; i++ {
			reply, ok := <-replyCh
			if ok && reply != nil {

				//reply类型 success term
				//正常成功   true    ==
				//任期过期   false   >
				//日志冲突   false   ==  term相同，日志不一致，prevLogIndex 或 prevLogTerm 处与 leader 不匹配。
				//日志太短   false   ==  leader 给出的 prevLogIndex 超过了跟随者日志的末尾。

				if rf.currentTerm < reply.Term {
					//任期过期
					rf.currentTerm = reply.Term
					rf.state = FOLLOWER
					rf.votedFor = -1
					//持有锁退出
					return
				}

				if !reply.Success {
					//日志冲突&日志太短

					//寻找leader和follower一致日志
					rf.nextIndex[reply.Me] = reply.Next_index
				} else {
					//正常成功
					rf.nextIndex[reply.Me] += reply.Append_num
					rf.matchIndex[reply.Me] = rf.nextIndex[reply.Me] - 1
					if reply.Append_num != 0 {
						rf.median_tracker.Add(reply.Me, rf.matchIndex[reply.Me])
						median := rf.median_tracker.GetMedian()
						if rf.log[median].Term == rf.currentTerm && median > rf.commitIndex {
							if rf.commitIndex == 0 {
								rf.commitIndex++
							}
							for i := rf.commitIndex; i <= median; i++ {
								// logPrintf("Server %v: 提交日志 %v", rf.me, i)
								rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Log, CommandIndex: i}
							}

							rf.commitIndex = median
						}
					}

					//更新matchindex
				}
			}
		}
	}
}

func Candidate(rf *Raft) {
	//进入时持有锁
	rf.state = CANDIDATE
	peers_num := len(rf.peers)
	for rf.killed() == false {
		granted_cnt := 1
		rf.currentTerm++
		rf.votedFor = rf.me
		args := RequestVoteArgs{Term: rf.currentTerm, CandidatedId: rf.me,
			LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}

		var rw sync.RWMutex
		done := false
		replyCh := make(chan *RequestVoteReply, peers_num-1)

		for i := 0; i < peers_num; i++ {
			if i == rf.me {
				continue
			}

			// logPrintf("Server %v: 向%v发送投票\n", rf.me, i)
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)

				var msg *RequestVoteReply
				if ok {
					msg = &reply
				} else {
					msg = nil
				}

				rw.RLock()
				if !done {
					replyCh <- msg
				}
				rw.RUnlock()

			}(i)
		}

		rf.mu.Unlock()

		ms := 300 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond) //选举超时时间

		rw.Lock()
		done = true
		close(replyCh)
		rw.Unlock()

		rf.mu.Lock()

		if rf.state == FOLLOWER {
			//因接到更高term的rpc
			return
		}

		for i := 0; i < peers_num-1; i++ {
			reply, ok := <-replyCh
			if ok && reply != nil {
				if reply.VoteGranted {
					granted_cnt++
					if granted_cnt >= (peers_num)/2+1 {
						// logPrintf("Server %v: 选举成功，进入leader\n", rf.me)
						Leader(rf)
						return //若是leader被打为follower直接return到tick
					}
				} else {
					//还有对方任期小，但已投过, 情况

					if reply.Term > rf.currentTerm {
						//对方任期大
						rf.currentTerm = reply.Term
						rf.state = FOLLOWER
						rf.votedFor = -1
						return //降为follower
					}
				}
			}
		}

	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 300 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if time.Since(time.UnixMilli(rf.heartbeat_timestamp)).Milliseconds() > ms {
			// logPrintf("Serve %v: 等待超时，开始选举\n", rf.me)
			//超时
			//自下向上转换不需要手动添加状态转换
			Candidate(rf)
		}
		rf.mu.Unlock()
	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.median_tracker = NewMedianTracker(make([]int, len(rf.peers)))

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartbeat_timestamp = time.Now().UnixMilli()
	rf.state = FOLLOWER

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
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
