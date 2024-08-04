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

	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	// "6.824/labgob"
	"6.824/labrpc"
)

const showlog bool = false

func plog(a ...interface{}) {
	if showlog {
		time := time.Now().Format("15:04:05.00000  ")
		fmt.Print(time)
		fmt.Println(a...)
	}
}

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

type LogEntry struct {
	Term         int
	Command      interface{}
	CommandIndex int
}

type RoleType int

const (
	Follower RoleType = iota
	Candidate
	Leader
)

const heartbeatInterval = 100 * time.Millisecond
const electionTimeout = 300 * time.Millisecond

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
	// 2A
	currentTerm        int
	leaderId           int
	role               RoleType
	heartbeatChannel   chan *AppendEntriesArgs
	winElectionChannel chan bool
	votedFor           int
	voteCount          int

	// 2B
	log         []LogEntry
	commitIndex int   // all servers
	lastApplied int   // all servers
	nextIndex   []int // leader
	matchIndex  []int // leader
	applyCh     chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	rf.mu.Unlock()
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
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
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()

	plog(rf.role, rf.me, " receive request from", args.CandidateId, "term", args.Term, rf.currentTerm)
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.role = Follower
		rf.voteCount = 0
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].CommandIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}

	} else if args.Term == rf.currentTerm {
		if rf.votedFor != -1 && rf.votedFor != rf.me {
			reply.VoteGranted = false
		} else if args.CandidateId == rf.votedFor {
			reply.VoteGranted = true
		} else {
			if args.LastLogTerm > rf.log[len(rf.log)-1].Term || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].CommandIndex) {
				rf.role = Follower
				rf.voteCount = 0
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			}
		}
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()

	rf.heartbeatChannel <- &AppendEntriesArgs{}
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
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.voteCount = 1
	rf.currentTerm++
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go func(x int) {
			reply := &RequestVoteReply{}
			plog("candidate ", rf.me, " send request vote to ", x)
			ok := rf.sendRequestVote(x, args, reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.votedFor = -1
				rf.voteCount = 0
				rf.role = Follower
			} else if reply.VoteGranted && ok && rf.role == Candidate {

				rf.voteCount++
				plog("candidate ", rf.me, " get vote from ", x, "votecount:", rf.voteCount)
				if rf.voteCount > len(rf.peers)/2 {
					rf.role = Leader

					rf.leaderId = rf.me
					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log) - 1
						rf.matchIndex[i] = 0
						if rf.nextIndex[i] == 0 {
							rf.nextIndex[i] = 1
						}
						if rf.me == i {
							rf.matchIndex[i] = len(rf.log) - 1
						}
					}
					plog("candidate:", rf.me, " win election")

					rf.winElectionChannel <- true
				}
			}

			rf.mu.Unlock()
		}(i)
	}

}

// check heart beat
func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	plog("follower:", rf.me, " receive heartbeat", rf.currentTerm, "leader commit:", args.LeaderCommit, " pre index:", args.PrevLogIndex)
	if len(args.Entries) == 0 {
		if args.Term < rf.currentTerm {

			reply.Success = false
			reply.Term = rf.currentTerm
		} else {
			reply.Term = rf.currentTerm
			reply.Success = true
			rf.role = Follower
			rf.votedFor = -1
			rf.voteCount = 0
			rf.leaderId = args.LeaderId
			rf.currentTerm = args.Term

		}
		tmp := min(len(rf.log)-1, args.LeaderCommit)
		rf.commitIndex = tmp
		rf.mu.Unlock()
		rf.heartbeatChannel <- args

		if tmp > 0 && rf.commitIndex > rf.lastApplied {

			plog("follower:", rf.me, " commit log", tmp)

			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[tmp].Command,
				CommandIndex: rf.log[tmp].CommandIndex,
			}
			rf.lastApplied = tmp
		}
		return
	}

	if args.Term < rf.currentTerm || args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex < len(rf.log) && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
			rf.heartbeatChannel <- args
		}
		return
	} else {

		rf.log = rf.log[:args.PrevLogIndex+1]
		plog("follower:", rf.me, " origin log", rf.log)
		rf.log = append(rf.log, args.Entries...)
		plog("follower:", rf.me, " receive log", rf.log)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	rf.votedFor = -1
	rf.voteCount = 0
	rf.leaderId = args.LeaderId
	rf.role = Follower
	rf.currentTerm = args.Term

	tmp := min(len(rf.log)-1, args.LeaderCommit)
	rf.commitIndex = tmp
	rf.mu.Unlock()

	rf.heartbeatChannel <- args
	if tmp > 0 {
		plog("follower:", rf.me, " commit log", tmp)
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[tmp].Command,
			CommandIndex: rf.log[tmp].CommandIndex,
		}
	}

}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

// heart beat
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
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
	rf.mu.Lock()

	if rf.role != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}

	index = len(rf.log)
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{CommandIndex: index, Term: term, Command: command})
	plog("leader:", rf.me, " append log ", index)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	rf.mu.Unlock()

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
// heartsbeats recently.1
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()

		switch role {
		case Follower:

			select {
			case <-rf.heartbeatChannel:
				continue

			case <-time.After(electionTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				rf.mu.Lock()

				rf.role = Candidate
				rf.votedFor = -1
				rf.voteCount = 0

				rf.mu.Unlock()
				continue
			}
		case Candidate:
			plog("candidate:", rf.me, "start election")
			go rf.startElection()
			select {
			case <-rf.winElectionChannel:
				continue
			case <-time.After(electionTimeout + time.Duration(rand.Int31()%300)*time.Millisecond):
				rf.mu.Lock()
				rf.votedFor = -1
				rf.voteCount = 0

				rf.mu.Unlock()
				continue
			}

		case Leader:

			for i := 0; i < len(rf.peers); i++ {
				if rf.me == i {
					continue
				}
				go func(x int) {
					args := &AppendEntriesArgs{}
					reply := &AppendEntriesReply{}

					rf.mu.Lock()
					if rf.role != Leader {
						rf.mu.Unlock()
						return
					}
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = 0
					args.PrevLogTerm = 0
					if len(rf.log) > rf.nextIndex[x] {

						args.PrevLogIndex = rf.nextIndex[x] - 1
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term

						args.Entries = rf.log[args.PrevLogIndex+1:]
					}

					rf.mu.Unlock()

					plog("leader:", rf.me, " sendAppendEntries to", x)
					ok := rf.sendAppendEntries(x, args, reply)
					if ok && reply.Success == false {
						rf.mu.Lock()
						if reply.Term > args.Term {

							plog("leader:", rf.me, " sendAppendEntries to", x, "fail")
							plog("leader:", rf.me, "term:", rf.currentTerm, "change to", reply.Term)
							rf.role = Follower
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.voteCount = 0

						} else {
							rf.nextIndex[x]--
						}
						rf.mu.Unlock()

					} else if ok {
						if len(args.Entries) > 0 {
							rf.mu.Lock()
							rf.nextIndex[x] = args.Entries[len(args.Entries)-1].CommandIndex + 1
							rf.matchIndex[x] = args.Entries[len(args.Entries)-1].CommandIndex
							tmp := make([]int, len(rf.matchIndex))
							copy(tmp, rf.matchIndex)
							sort.Ints(tmp)
							if tmp[len(tmp)/2] > rf.commitIndex {
								rf.commitIndex = tmp[len(tmp)/2]
								go func(i int, cmd interface{}) {
									rf.applyCh <- ApplyMsg{
										CommandValid: true,
										Command:      cmd,
										CommandIndex: i,
									}
								}(rf.commitIndex, rf.log[rf.commitIndex].Command)
							}

							rf.mu.Unlock()
						}

					}

				}(i)

			}

			time.Sleep(heartbeatInterval)
		}
		// fmt.Println(rf.me," sleep:", sleeptime)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 1
	rf.votedFor = -1
	rf.voteCount = 0
	rf.role = Follower
	rf.heartbeatChannel = make(chan *AppendEntriesArgs)
	rf.winElectionChannel = make(chan bool)

	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers), len(peers))
	for i := range peers {
		rf.nextIndex[i] = 1
	}
	rf.matchIndex = make([]int, len(peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
