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

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
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
	Term int
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

	currentTerm int
	votedFor    int

	log []LogEntry // need to be implented

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	//自己添加

	heartbeat_timestamp int64

	state int

	median_tracker *MedianTracker

	done bool
	rw   sync.RWMutex
}

type RequestVoteArgs struct {
	Term         int
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Me            int
	Term          int
	Success       bool
	Append_num    int
	ConflictIndex int
	ConflictTerm  int
	PrevLogIndex  int
	LeaderTerm    int
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
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
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("持久化状态解码错误")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func ParallelCommit(applyCh chan ApplyMsg, index int, log []LogEntry) {
	for i := range log {
		applyCh <- ApplyMsg{CommandValid: true, Command: log[i].Log, CommandIndex: i + index}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("Serve %v: AppendEntries log %v commit %v\n", rf.me, len(rf.log)-1, rf.commitIndex)
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	reply.Me = rf.me
	reply.PrevLogIndex = args.PrevLogIndex
	reply.LeaderTerm = args.Term

	if args.Term < currentTerm {
		//任期过低
		reply.Success = false
		return
	}
	rf.heartbeat_timestamp = time.Now().UnixMilli()
	if args.Term > rf.currentTerm {
		rf.ChangeState(args.Term, -1, FOLLOWER)
	}

	if args.PrevLogIndex > len(rf.log)-1 || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		// log.Printf("%v %v %v\n", rf.me, args.PrevLogIndex, len(rf.log)-1)
		//日志不匹配
		//如果只是简单的对NextIndex逐步减1，则这该测试点很可能不通过。(leader backs up quickly over incorrect follower logs)
		//两种情况，prevlogindex可能在当前log右边，或者在左边和中间

		if len(rf.log) != 1 && args.PrevLogIndex <= len(rf.log)-1 {
			rf.log = rf.log[:args.PrevLogIndex]
		}
		// log.Printf("Server %v: 日志不匹配 %v", rf.me, args.PrevLogIndex)
		reply.ConflictIndex = min(args.PrevLogIndex, len(rf.log))
		if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictIndex = len(rf.log)
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogTerm; ; i-- {
				if i == 0 || rf.log[i].Term != rf.log[args.PrevLogIndex].Term {
					reply.ConflictIndex = i + 1
					break
				}
			}
		}
		reply.Success = false
		return
	}

	for i := range args.Entries {
		if args.PrevLogIndex+i+1 > len(rf.log)-1 {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
		if rf.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
			rf.log = rf.log[:args.PrevLogIndex+i+1]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	reply.Append_num = len(args.Entries)
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		// for i := rf.commitIndex + 1; i <= min(args.LeaderCommit, len(rf.log)-1); i++ {
		// 	rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Log, CommandIndex: i}
		// }
		commitlog := rf.log[rf.commitIndex+1 : min(args.LeaderCommit, len(rf.log)-1)+1]
		go ParallelCommit(rf.applyCh, rf.commitIndex+1, commitlog)
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}

}

func (rf *Raft) PreRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()

	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	if args.Term < currentTerm || (args.Term == currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidatedId) ||
		args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1) {
		// log.Printf("Serve %v: RequestVote %v %v %v %v %v\n", rf.me, args.CandidatedId, args.Term, rf.currentTerm, reply.VoteGranted, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	reply.VoteGranted = true
	// log.Printf("Serve %v: RequestVote %v %v %v %v %v\n", rf.me, args.CandidatedId, args.Term, rf.currentTerm, reply.VoteGranted, rf.votedFor)

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	if rf.killed() {
		return
	}

	rf.mu.Lock()

	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.ChangeState(args.Term, -1, FOLLOWER)
		rf.heartbeat_timestamp = time.Now().UnixMilli()
	}

	if args.Term < rf.currentTerm ||
		(rf.votedFor != -1 && rf.votedFor != args.CandidatedId) ||
		args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
		(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1) {

		// log.Printf("Serve %v: RequestVote %v %v %v %v %v\n", rf.me, args.CandidatedId, args.Term, rf.currentTerm, reply.VoteGranted, rf.votedFor)
		reply.VoteGranted = false
		return
	}

	rf.heartbeat_timestamp = time.Now().UnixMilli()
	reply.VoteGranted = true
	rf.ChangeState(rf.currentTerm, args.CandidatedId, FOLLOWER)
	// log.Printf("Serve %v: RequestVote %v %v %v %v %v\n", rf.me, args.CandidatedId, args.Term, rf.currentTerm, reply.VoteGranted, rf.votedFor)
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

func (rf *Raft) ReceiveReply(stop chan struct{}, replyCh chan *AppendEntriesReply, done *bool) {
	for {
		select {
		case <-stop:
			rf.rw.Lock()
			*done = true
			close(replyCh)
			rf.rw.Unlock()
			return
		case reply := <-replyCh:
			if reply == nil {
				break
			}
			rf.mu.Lock()

			//reply类型 success term
			//正常成功   true    ==
			//任期过期   false   >
			//日志冲突   false   ==  term相同，日志不一致，prevLogIndex 或 prevLogTerm 处与 leader 不匹配。
			//日志太短   false   ==  leader 给出的 prevLogIndex 超过了跟随者日志的末尾。
			if reply.LeaderTerm != rf.currentTerm {
				rf.mu.Unlock()
				break
			}

			if rf.state == FOLLOWER {
				//因接到更高term的rpc
				rf.mu.Unlock()
				return
			}
			if rf.currentTerm < reply.Term {
				//任期过期
				rf.ChangeState(reply.Term, -1, FOLLOWER)
				//持有锁退出
				rf.mu.Unlock()
				return
			}

			if !reply.Success {
				//日志冲突&日志太短

				//寻找leader和follower一致日志
				if reply.ConflictTerm == 0 {
					rf.nextIndex[reply.Me] = reply.ConflictIndex
				} else {
					l := 0
					r := len(rf.log)
					for l+1 < r {
						mid := (l + r) / 2
						if rf.log[mid].Term <= reply.ConflictTerm {
							l = mid
						} else {
							r = mid
						}
					}
					i := l // 最后一个<=term的下标
					if rf.log[i].Term == reply.ConflictTerm {
						rf.nextIndex[reply.Me] = i + 1
					} else {
						rf.nextIndex[reply.Me] = reply.ConflictIndex
					}
				}
			} else {
				//正常成功
				rf.nextIndex[reply.Me] = reply.PrevLogIndex + reply.Append_num + 1
				rf.matchIndex[reply.Me] = rf.nextIndex[reply.Me] - 1
				if reply.Append_num != 0 {
					rf.median_tracker.Add(reply.Me, rf.matchIndex[reply.Me])
					median := rf.median_tracker.GetMedian()
					if rf.log[median].Term == rf.currentTerm && median > rf.commitIndex {
						// for i := rf.commitIndex + 1; i <= median; i++ {
						// // log.Printf("Server %v: 提交日志", rf.me)
						// 	rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[i].Log, CommandIndex: i}
						// }
						commitLog := rf.log[rf.commitIndex+1 : median+1]
						go ParallelCommit(rf.applyCh, rf.commitIndex+1, commitLog)

						rf.commitIndex = median
					}
				}

				//更新matchindex
			}

			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := int(rf.currentTerm)
	isLeader := (rf.state == LEADER)

	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Log: command})
		rf.persist()
		rf.median_tracker.Add(rf.me, len(rf.log)-1)
	}
	// log.Printf("Server %v: Start 是否leader %v, Term %v, Index %v\n", rf.me, rf.state == LEADER, rf.currentTerm, len(rf.log))

	return index, term, isLeader
}

func Leader(rf *Raft) {
	//进入时持有锁
	rf.state = LEADER

	//no-op机制
	// rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Log: nil})
	// rf.persist()
	// rf.median_tracker.Add(rf.me, len(rf.log)-1)

	peers_num := len(rf.peers)

	rf.median_tracker.Add(rf.me, len(rf.log)-1)

	for i := range rf.matchIndex {
		//初始化每个数组的匹配日志
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = len(rf.log)
	}

	done := false
	replyCh := make(chan *AppendEntriesReply, 2*peers_num)
	stopCh := make(chan struct{})
	go rf.ReceiveReply(stopCh, replyCh, &done)

	for !rf.killed() {

		for i := 0; i < peers_num; i++ {
			if i == rf.me {
				continue
			}

			//func是按引用捕获

			prevlogindex := rf.nextIndex[i] - 1
			term := rf.currentTerm
			prevlogterm := rf.log[rf.nextIndex[i]-1].Term
			leadercommit := rf.commitIndex
			entries := make([]LogEntry, 0)
			if len(rf.log)-1 >= rf.nextIndex[i] {
				entries = append(entries, rf.log[rf.nextIndex[i]:]...)
			}

			go func(server int) {
				// log.Printf("Server %v: 向%v发送心跳 prev %v\n", rf.me, server, rf.nextIndex[server]-1)
				args := AppendEntriesArgs{Term: term, LeaderId: rf.me,
					PrevLogIndex: prevlogindex, PrevLogTerm: prevlogterm,
					LeaderCommit: leadercommit,
					Entries:      entries}
				reply := AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

				var msg *AppendEntriesReply
				if ok {
					msg = &reply
				} else {
					msg = nil
				}

				rf.rw.RLock()
				if !done {
					replyCh <- msg
				}
				rf.rw.RUnlock()
			}(i)
		}

		rf.mu.Unlock()

		ms := 100
		//每秒<=10次beat
		time.Sleep(time.Duration(ms) * time.Millisecond) //选举超时时间

		rf.mu.Lock()

		if rf.state == FOLLOWER {
			//因接到更高term的rpc
			//持有锁退出
			close(stopCh)
			return
		}

	}
}

func Prevote(rf *Raft) bool {
	peers_num := len(rf.peers)
	granted_cnt := 1
	args := RequestVoteArgs{Term: rf.currentTerm + 1, CandidatedId: rf.me,
		LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}

	var rw sync.RWMutex
	done := false
	replyCh := make(chan *RequestVoteReply, peers_num-1)

	for i := 0; i < peers_num; i++ {
		if i == rf.me {
			continue
		}

		// log.Printf("Server %v: 向%v发送投票\n", rf.me, i)
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
		return false
	}

	for i := 0; i < peers_num-1; i++ {
		reply, ok := <-replyCh
		if ok && reply != nil {
			if reply.VoteGranted {
				granted_cnt++
				if granted_cnt >= (peers_num)/2+1 {
					return true
				}
			} else {
				//还有对方任期小，但已投过, 情况

				if reply.Term > rf.currentTerm {
					return false
				}
			}
		}
	}
	return false
}

func Candidate(rf *Raft) {
	//进入时持有锁

	//PreVote机制
	rf.state = CANDIDATE
	if !Prevote(rf) {
		rf.state = FOLLOWER
		return
	}

	peers_num := len(rf.peers)
	for !rf.killed() {
		granted_cnt := 1
		rf.ChangeState(rf.currentTerm+1, rf.me, rf.state)
		args := RequestVoteArgs{Term: rf.currentTerm, CandidatedId: rf.me,
			LastLogIndex: len(rf.log) - 1, LastLogTerm: rf.log[len(rf.log)-1].Term}

		var rw sync.RWMutex
		done := false
		replyCh := make(chan *RequestVoteReply, peers_num-1)

		for i := 0; i < peers_num; i++ {
			if i == rf.me {
				continue
			}

			// log.Printf("Server %v: 向%v发送投票\n", rf.me, i)
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
						// log.Printf("Server %v: 选举成功，进入leader\n", rf.me)
						Leader(rf)
						return //若是leader被打为follower直接return到tick
					}
				} else {
					//还有对方任期小，但已投过, 情况

					if reply.Term > rf.currentTerm {
						//对方任期大
						rf.ChangeState(reply.Term, -1, FOLLOWER)
						return //降为follower
					}
				}
			}
		}

	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 300 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if time.Since(time.UnixMilli(rf.heartbeat_timestamp)).Milliseconds() > ms {
			// log.Printf("Serve %v: 等待超时，开始选举\n", rf.me)
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

	rf.heartbeat_timestamp = time.Now().UnixMilli()

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

func (rf *Raft) ChangeState(term int, votefor int, state int) {
	rf.currentTerm = term
	rf.votedFor = votefor
	rf.persist()
	rf.state = state
}
