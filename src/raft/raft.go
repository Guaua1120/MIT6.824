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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"

)

// 日志结构
type Log struct{
	Term	int
	Command	interface{}
}
const NULL int = -1

//需要持久化的数据结构(字段首字母需大写)
type PersistStruct struct{
	CurrentTerm		int
	VotedFor 		int
	Logs	 		[]Log
}

//定义全局心跳间隔时间，必须保证 心跳时间 << 选举超时时间 <<平均故障时间MTBF
const HeartBeat	 		= 50 * time.Millisecond	
//定义选举超时时间，在一个选举周期内未选出leader，重新进行选举
const ElectionTimeout  	= 500 * time.Millisecond

// 设置心跳超时，表示经过了这么长的时间没有收到心跳，应该开始选举，推举自己成为候选者，每个server每次都应该是在一个范围内的随机数
func getRandomTime() time.Duration{
	return time.Duration(200+rand.Intn(200)) * time.Millisecond  // 随机产生200-400ms
}


type state int
const(
	Follower 	state=0
	Candidate	state=1
	Leader		state=2
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state		state			//当前server的状态，是leader还是candidate还是follower
	currentTerm int 			//当前的任期
	votedFor  	int				//作为follower时投票给谁
	logs        []Log			//日志项列表
	commitIndex int 			//已提交的日志项的最大索引
	lastApplied	int				//已经在状态机中执行的目录项的最大索引(先commit-->再apply)
	nextIndex   []int 			//作为leader时，保存要发送给每一个server的日志项的索引
	matchIndex  []int			//作为leader时，和其他server的日志项相同的最大日志项索引

	applyCh		chan ApplyMsg	//所有的server共用的消息管道，用于发送日志请求

	votedNums	int				//server作为candidate时得到的票数

	timer		*time.Ticker	//每个节点的计时器
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state==Leader{
		isLeader = true
	}else{
		isLeader = false
	}
	return term, isLeader
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
	// persistent state: currentTerm, votedFor, logs[]

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	ps := PersistStruct{
		CurrentTerm:	rf.currentTerm,
		VotedFor:		rf.votedFor,
	}
	ps.Logs = make([]Log,len(rf.logs))
	copy(ps.Logs,rf.logs)

	e.Encode(ps)
	raftstate := w.Bytes()
	//save的两个参数，一个是persistent状态，另一个是快照
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var ps PersistStruct
	if d.Decode(&ps) != nil{
	  	panic("persisten error")
	} else {
	  	rf.currentTerm = ps.CurrentTerm
	  	rf.votedFor = ps.VotedFor
		rf.logs = make([]Log,len(ps.Logs))		//深拷贝
		copy(rf.logs,ps.Logs)
	}
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term			int 	//候选者的任期
	CandidateId		int		//候选者的id
	LastLogIndex 	int 	//候选者的最后一条日志索引号
	LastLogTerm 	int 	//候选者的最后一条日志的任期号，用于判断是否给候选者投票
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term		int 	// currentTerm for candidate to update itself
	VoteGranted	bool	// true 表示赞成/给他投票；false表示否决/不给他投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A, 3B).
	
	//DPrintf("id:%v(currentTerm=%v) 收到了 id:%v(currentTerm=%v) 的投票请求",rf.me,rf.currentTerm,args.CandidateId,args.Term)

	// 当前接收方server宕机
	if rf.killed() {
		reply.Term = -1
		reply.VoteGranted = false
		return
	}

	//出现网络分区，candidate已经过时
	if rf.currentTerm>args.Term{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//投票者的任期比candidate要小
	//是否给candidate投票取决于日志的新旧状态
	if rf.currentTerm < args.Term{
		rf.votedFor = NULL				//新的一轮开启，先重置投票状态
		rf.currentTerm = args.Term  	//更新投票者的term
		rf.persist()					//持久化保存
	}

	if rf.votedFor==NULL{

		currentLogIndex := len(rf.logs)-1 	//投票者当前的最大日志索引项
		currentLogTerm := rf.logs[currentLogIndex].Term

		//选举限制：投票者的日志比选举者的日志更新（不包括一样新），则拒绝投票
		// If votedFor is null or candidateId, 
		// and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

		// 比较最后一条日志的Term或者索引
		// 更新的定义：投票者和candidate的日志，最后一条日志的任期号大的更新，如果任期号相同，则索引号更大的更新

		if 	(args.LastLogTerm < currentLogTerm || ((args.LastLogTerm == currentLogTerm) && args.LastLogIndex < currentLogIndex))  {
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

		//candidate日志更新
		rf.votedFor = args.CandidateId
		rf.persist()						//持久化保存
		rf.state = Follower					//由leader变回follower
		rf.timer.Reset(getRandomTime())		//重置定时器
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		//DPrintf("id:%v 投票给了 id: %v ",rf.me,args.CandidateId)
		//DPrintf("id:%v 的状态为：%v (0=follower,1=candidate,2=leader) ",rf.me,rf.state)
		
	}else{
		//任期相同的情况下，看投票者投的是谁
		//如果不是candidate说明已经投过票了
		if rf.votedFor!= args.CandidateId{
			reply.VoteGranted=false
			return
		}else{
			rf.state = Follower
		}
		
	}
	rf.timer.Reset(getRandomTime())
	
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool{

	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//DPrintf("id: %v 向 id: %v 调用RPC Raft.RequestVote",rf.me,server)
	for !ok {
		// 失败重传
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		//DPrintf("id: %v 向 id: %v 调用RPC Raft.RequestVote",rf.me,server)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//处理reply的不同情况

	//投票者crash了
	if reply.Term==-1{
		return false
	}
	
	//candidate 没有获得选票
	if reply.VoteGranted==false{
		//candidate 任期比投票者小，更新candidate的任期并变回follower
		if(reply.Term>rf.currentTerm){
			rf.state = Follower
			rf.votedNums = 0
			rf.currentTerm = reply.Term		
			rf.votedFor = -1
			rf.timer.Reset(getRandomTime())	
			rf.persist()		//持久化保存
		}
		return ok
		
	}

	//candidate获得了选票
	if reply.VoteGranted==true && reply.Term == rf.currentTerm && rf.votedNums <= (len(rf.peers)/2) {
		rf.votedNums++
		//DPrintf("id:%v 获得了id: %v 的选票，目前得到了：%v 张票",rf.me,server,rf.votedNums)
	}

	//统计票数，如果票数过一半，选举变成leader
	if rf.votedNums > (len(rf.peers)/2){
		DPrintf("id:%v 变成了leader,当前Term: %v ",rf.me,rf.currentTerm)
		rf.votedNums = 0	 		//防止重复变成leader执行
		rf.state = Leader

		//初始化一些变量
		rf.nextIndex = make([]int, len(rf.peers))
		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.logs) 		//nextIndex表示leader下一条要发送给其他server的日志index，初始化为leader的最后一条日志索引index+1(无哨兵的情况下)，有哨兵就初始化为leader的日志长度
		}
		rf.matchIndex[rf.me] = len(rf.logs)-1
		rf.timer.Reset(HeartBeat)		//初始化心跳间隔
	}
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

	// Your code here (3B).

	rf.mu.Lock()
	defer rf.mu.Unlock();

	//server crash的情况
	if rf.killed(){
		return index,term,false
	}
	
	//不是leader直接返回
	if rf.state!=Leader{
		return index,term,false
	}

	//是leader的话就将日志条目初始化并添加到leader的日志列表中并返回
	DPrintf("执行了一次start,往leader：%v 里面新增了一个日志条目",rf.me)
	isLeader = true
	newEntry := Log{
		Term: rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs,newEntry)
	rf.persist()					//持久化保存
	index = len(rf.logs)-1
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index

	return index,rf.currentTerm,isLeader

}

type AppendEntriesArgs struct{
	Term			int 	//leader的任期
	LeaderId		int		//leader的id，使得follower可以将请求转发给leader
	PrevLogIndex	int		//预计要从哪里追加日志项的index（发送的新日志条目的前一个位置的索引）
	PrevLogTerm		int		//对应PrevLogIndex的任期号
	Entries			[]Log	//发送给server的存储的日志，为空时代表心跳连接
	LeaderCommit	int		//leader已提交的日志项的最大索引
}

type AppendEntriesReply struct{
	Term			int		//心跳接收者当前的任期
	Success			bool	//是否更新日志成功
	//当日志发生冲突时，交给leader的信息 (优化项)
	XTerm			int 	//follower中冲突的日志项的Term
	XIndex			int  	//follower中与冲突项的Term相同的第一个日志项的index
	XLen			int		//follower的日志长度

}

// send the AppendEntries RPC call
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs,reply *AppendEntriesReply) bool {

	if rf.killed() {
		return false
	}

	// 如果append失败应该不断的retries ,直到这个log成功的被store
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		DPrintf("leader: %v 向 follower：%v 发送追加日志请求",rf.me,server)
		DPrintf("follower:%v 的心跳参数PrevLogIndex：%v,发送的日志项的长度为:%v",server,args.PrevLogIndex,len(args.Entries))
	}else{
		return false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//接收端宕机，直接返回false
	if reply.Term==-1{
		return false
	}

	//请求过期
	//网络出现分区，follower的任期比leader要大,leader重新变回follower，并重置超时时间
	if args.Term<reply.Term {
		rf.state = Follower  				//leader重新变回follower
		rf.votedFor =NULL					//重置投票状态
		rf.currentTerm = reply.Term 		//更新自己的任期
		rf.timer.Reset(getRandomTime())  	//重新设置超时时间
		rf.persist()						//持久化保存
		DPrintf("leader：%v out of date ，id:%v 拒绝追加日志",rf.me,server)
		return false
	}

	//追加日志成功有两种情况
	//1、心跳的success (return ok就行)
	//2、follower成功copy，更新nextIndex和matchIndex 
	if reply.Success {
		if len(args.Entries)!=0{
			rf.nextIndex[server] += len(args.Entries) 				//	nextIndex更新为leader的最后一条日志index+1
			rf.matchIndex[server] = rf.nextIndex[server] -1			//	matchIndex更新为leader的最后一条日志的index
			DPrintf("leader:%v 更新了follower:%v的nextIndex,现在的值为：%v ",rf.me,server,rf.nextIndex[server])
			DPrintf("id:%v 追加日志成功",server)
		}
	}else{
		//追加日志失败有3种情况
		//1、出现日志冲突(inconsistency)的情况：
		//	1.1、follower在args.preLogIndex处还没有下标（args.preLogIndex大于follower的最后一条日志的下标），说明follower前面还有没收到的下标，因此拒绝该日志
		//	1.2、如果follwer的preLogIndex处的日志的任期和args.preLogTerm不相等，那么说明日志存在conflict,拒绝附加日志	
		//2、接收端宕机（上面已处理）
		//3、出现网络分区，leader已经OutOfDate（上面已处理）

		DPrintf("leader:%v 日志出现冲突，id:%v 追加日志失败",rf.me,server)
		DPrintf("args的参数：args.Term = %v,args.LeaderId = %v,args.PrevLogIndex = %v ",args.Term,args.LeaderId,args.PrevLogIndex)

		//对nextIndex的更新需要进行优化，加快执行速度
		//	 Case 1: leader doesn't have XTerm:
    	//		nextIndex = XIndex
		//	Case 2: leader has XTerm:
	  	//		nextIndex = leader's last entry for XTerm
		//	Case 3: follower's log is too short:
	  	//		nextIndex = XLen

		if reply.XTerm == -1 {
			// 日志过短，回退到 XLen
			rf.nextIndex[server] = reply.XLen
		} else {
			// 在 leader 的日志里查找 reply.XTerm
			lastIndex := -1
			for i := len(rf.logs) - 1; i >= 0; i-- {
				if rf.logs[i].Term == reply.XTerm {
					lastIndex = i
					break
				}
			}
			if lastIndex != -1 {
				rf.nextIndex[server] = lastIndex
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}

		DPrintf("reply的参数：reply.XTerm = %v,reply.XIndex = %v ,reply.XLen = %v",reply.XTerm,reply.XIndex,reply.XLen)
		DPrintf("leader: %v,rf.nextIndex[%v] = %v",rf.me,server,rf.nextIndex[server])

	}
	return ok
}

//利用matchIndex数组判断是否有超过半数replicated ，有则提交，无则退出
func (rf *Raft) tryCommit(){

	DPrintf("leader:%v 执行tryCommit协程",rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//新任期的leader 刚开始选举的心跳的情况
	if len(rf.logs) == 1 || rf.logs[len(rf.logs)-1].Term != rf.currentTerm {
		DPrintf("leader:%v 新任期刚开始，没有需要提交的日志",rf.me)
		return 
	}

	//根据leader中的最后一条log的index从后向前遍历
	//找到最近一条已提交的当前任期的log的index。因为后面committed表明前面也一定committed了
	lastLogIndex := len(rf.logs)-1
	lastCommitIndex := rf.commitIndex
	DPrintf("leader:%v 的commitIndex=:%v", rf.me,rf.commitIndex)
	DPrintf("leader:%v 的最后一条日志的index=%v", rf.me,lastLogIndex)
	for index := lastLogIndex ; index>rf.commitIndex ; index--{
		
		//leader只会通过计算副本的方式来commit当前任期内的日志项，而之前任期内的会通过日志匹配间接commit
		if rf.logs[index].Term != rf.currentTerm{
			DPrintf("leader:%v 的index= %v 的日志不是当前任期内的日志，直接跳过", rf.me, index)
			continue;
		}
		count := 0
		for _ , value := range rf.matchIndex{
			if value >=	index {
				count++
				DPrintf("leader: %v 的索引为index= %v 的日志replicated+1，现在count= %v",rf.me,index,count)
			}
		}
		if count >= len(rf.peers)/2+1{
			lastCommitIndex = index
			DPrintf("leader:%v 最后一条将要提交的日志索引为: %v ",rf.me,lastCommitIndex)
			break
		}
	}	

	// 对处于[rf.commitIndex+1,lastCommitIndex]之间的log进行提交
	for index := rf.commitIndex+1 ; index<=lastCommitIndex ; index++{
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.applyCh <- applyMsg
		rf.commitIndex = rf.lastApplied
		DPrintf("leader:%v 索引index=%v 的日志已提交",rf.me,index)
	}

	return

}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	DPrintf("follower:%v 处理leader:%v 的追加日志请求",rf.me,args.LeaderId)
	
	reply.XLen = len(rf.logs)

	// 当前接收端server crash
	if rf.killed() {
		reply.Term = -1
		reply.Success = false
		return
	}


	// 出现网络分区，leader的任期比当前raft的任期还小，说明leader已经OutOfDate
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm  //传递用于更新过时leader的任期
		DPrintf("leader:%v out of date ,拒绝追加日志请求",args.LeaderId)
		return
	}

	//当前是分区的leader，收到了来自其他分区且任期更大的leader心跳
	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = args.LeaderId
		rf.persist()					//持久化保存
		DPrintf("id:%v 重置心跳超时时间定时器",rf.me)
		rf.timer.Reset(getRandomTime())
	}

	// 日志正常无冲突收到了心跳,更新自身的信息
	rf.state = Follower				//防止candidate再次进行选举
	rf.currentTerm = args.Term		
	rf.votedFor = args.LeaderId
	rf.persist()					//持久化保存
	DPrintf("id:%v 重置心跳超时时间定时器",rf.me)
	rf.timer.Reset(getRandomTime())
	reply.Success = true
	reply.Term = rf.currentTerm

	// 网络没有问题，判断日志是否出现冲突
	// Reply false if log doesn’t contain an entry at prevLogIndex,whose term matches prevLogTerm 
	// 出现冲突:在follower的日志的args.preLogIndex处的日志的Term与args.preLogTerm不相等
	// 具体来说有2种可能的情况
	// 1、follower在args.preLogIndex处还没有下标（args.preLogIndex大于follower的最后一条日志的下标），说明follower前面还有没收到的下标，因此拒绝该日志
	// 2、如果follwer的preLogIndex处的日志的任期和args.preLogTerm不相等，那么说明日志存在conflict,拒绝附加日志

	if len(rf.logs)-1 < args.PrevLogIndex {
		// Follower 日志长度不够
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = -1 // 标记 leader 需要回退
		reply.XLen = len(rf.logs)
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// 找到冲突的 XTerm
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.XTerm = rf.logs[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.logs[i].Term != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
		}
		return
	}

	// 不是心跳的话就在follower中追加日志(深拷贝解引用)
	if len(args.Entries)!=0 {
		rf.logs = append([]Log(nil), rf.logs[:args.PrevLogIndex + 1]...)		//将prevLogIndex之后的日志删除,左闭右开
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()															//持久化保存
		DPrintf("follower:%v 成功handler日志追加,目前最后一条日志的索引为:%v",rf.me,len(rf.logs)-1)
	}

	// 根据args里面的LeaderCommit将（上一步的RPC请求的）日志提交至与Leader相同
	for rf.lastApplied < args.LeaderCommit && rf.lastApplied < len(rf.logs) {
		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.logs[rf.lastApplied].Command,
		}
		rf.applyCh <- applyMsg
		rf.commitIndex = rf.lastApplied
	}
	return
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
	rf.mu.Lock()
	rf.timer.Stop()  		//kill之后要将计时器暂停
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//ticker 同时具备着选举超时、心跳间隔发送和心跳超时开始选举的任务
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		select{
		case <- rf.timer.C:   		//follower没有收到心跳，于是开始选举
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.state{
			case Follower:
				rf.state = Candidate
				fallthrough    			//强制执行下一case
			case Candidate:
				// 初始化自身的任期、并把票投给自己
				rf.currentTerm += 1
				rf.votedFor = rf.me
				rf.persist()					//持久化保存
				rf.votedNums = 1 				// 统计自身的票数
				DPrintf("candidate:%v 给自己投票，票数为： %v",rf.me,rf.votedNums)

				// 	每轮选举开始时，重新设置选举超时
				//	如果使用固定时间ElectionTimeout，
				// 	小概率可能出现在同一个分区中的server都是candidate，而且选举超时相近，
				// 	他们都只给自己投票而拒绝给其他的server投票导致选举一直超时失败
				rf.timer.Reset(getRandomTime())
				// 对自身以外的节点进行选举
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					rf.mu.Lock()
					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logs) - 1,
						LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
					}
					rf.mu.Unlock()

					voteReply := RequestVoteReply{}
					go rf.sendRequestVote(i, &voteArgs, &voteReply)
				}
				
			case Leader:
				// 进行心跳/日志同步
				rf.timer.Reset(HeartBeat)
				DPrintf("leader:%v 的最后一条日志索引为:%v",rf.me,len(rf.logs)-1)
				rf.mu.Unlock()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					rf.mu.Lock()
					appendEntriesArgs := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i]-1,		//leader向follower追加的新日志条目前一个位置的索引
						PrevLogTerm:  0,						//对应PrevLogIndex日志项的Term
						Entries:      nil,
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()

					appendEntriesReply := AppendEntriesReply{}

					rf.mu.Lock()
					// 如果leader的最后一条日志索引大于等于follower的nextIndex[i],则说明需要进行日志更新
					DPrintf("将要发送请求的nextIndex[%v]为: %v",i,rf.nextIndex[i])
					if len(rf.logs)-1 >= rf.nextIndex[i] && rf.nextIndex[i]>=1 {
						//深拷贝解引用
						appendEntriesArgs.Entries= make([]Log, len(rf.logs[rf.nextIndex[i]:]))
						copy(appendEntriesArgs.Entries, rf.logs[rf.nextIndex[i]:])
						
					}
					
					if appendEntriesArgs.PrevLogIndex < len(rf.logs) && appendEntriesArgs.PrevLogIndex>=0 {
						appendEntriesArgs.PrevLogTerm = rf.logs[appendEntriesArgs.PrevLogIndex].Term
					}

					rf.mu.Unlock()	
					go rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply)
				}
				go rf.tryCommit()
			}
			
		}
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
	rf := &Raft{
		peers:				peers,
		persister:			persister,
		me:					me,
		state:				Follower,
		currentTerm: 		0,
		votedFor:			NULL,
		logs:				make([]Log,0),			//日志索引的下标从1开始，意味着index=0是一个有效位（哨兵）
		commitIndex:		0,
		lastApplied:		0,
		nextIndex:			make([]int,len(peers)),
		matchIndex:			make([]int,len(peers)),			
		applyCh:			applyCh,	
		votedNums:			0,
	}

	rf.timer = time.NewTicker(getRandomTime())		//初始化计时器
	rf.logs = append(rf.logs,Log{})					//在日志项前面新增加一个哨兵项
	// Your initialization code here (3A, 3B, 3C).

	//DPrintf("初始化结束，Server id:%v ,状态为：%v ",rf.me,rf.state)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

