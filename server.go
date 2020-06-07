package myraft

import (
	"log"
	"math/rand"
	"myraft/config"
	"myraft/member"
	"myraft/transport"
	"sync"
)

// 代表着 raft 中的peer node 可以代表是leader 也可能是 follower 或者 candidate

const (
	FOLLOWER  = iota //O
	CANDIDATE        //1
	LEADER           //2
)

type Raft struct {
	//所有机器都有的
	state       int           //状态
	currentTerm uint64        //当前任期号 启动时为0
	rwLock      *sync.RWMutex //读写锁 用于 term 增加时使用
	voteFor     uint64        //给哪一个节点投票
	commitIndex uint64        //已提交的日志index
	lastApplied int           // 应用到状态机中的最新的日志index
	id          uint64        //机器唯一标识
	//leader 所有
	nextIndex        map[uint64]uint64 //对于每台服务器，要发送到该服务器的下一个日志条目的索引(初始化为leader lastlog索引index +1)
	matchIndex       map[uint64]uint64 //对于每个服务器，在服务器上复制已知的最高日志条目的索引(初始化为0,单调递增)
	electionTimeout  int64             // election timeout选举超时是指followers跟随者成为candidates候选者之前所等待的时间 默认是在150 毫秒到300 毫秒之间
	heartBeatTimeout int64             //心跳间隔
	peers            []uint64
	localAddr        string
	pr               *ProcessHandler
	transport        *transport.Transport
	lead             uint64
}

func NewServer(c *config.RaftConfig) *Raft {

	r := &Raft{
		state:       FOLLOWER,
		currentTerm: 0, //todo 从持久化信息读取
		rwLock:      &sync.RWMutex{},
		voteFor:     0, //默认为空
		commitIndex: 0, //todo 从持久化信息读取
		lastApplied: 0, //todo 从持久化信息读取
		nextIndex:   nil,
		matchIndex:  nil,
	}
	r.electionTimeout = RandInt64(150, 300)
	//生成 transport
	rc := member.NewRaftCluster(c.LocalAddr, c.ClusterAddr)
	r.transport = transport.NewTransport(rc)
	r.id = uint64(rc.LocalID())
	r.peers = rc.Peers()
	r.pr = MakeProcessHandler(r.peers)
	r.localAddr = c.LocalAddr
	return r
}

func RandInt64(min, max int64) int64 {
	if min >= max || min == 0 || max == 0 {
		return max
	}
	return rand.Int63n(max-min) + min
}

func (r *Raft) Transport() *transport.Transport {
	return r.transport
}

func (r *Raft) handle(rm transport.RaftMessage) error {

	switch rm.Type {
	case transport.MsgVote:
		voteOrNo := (rm.Term >= r.currentTerm) &&
			((r.voteFor == 0 || r.voteFor == rm.From) && rm.LogIndex >= r.commitIndex)
		if voteOrNo {
			r.voteFor = rm.From
			r.transport.SendMessage(transport.RaftMessage{To: rm.From, From: r.id, Success: true, Type: transport.MsgVoteResp})
			log.Printf("id:%d vote for id:%d ,its term:%d and lastIndex:%d", r.id, rm.From, rm.Term, rm.LogIndex)
			return nil
		} else {
			log.Printf("id:%d reject vote from id:%d,its term:%d and lastIndex:%d", r.id, rm.From, rm.Term, rm.LogIndex)
			r.transport.SendMessage(transport.RaftMessage{To: rm.From, From: r.id, Success: false, Type: transport.MsgVote})
			return nil
		}
	case transport.MsgVoteResp:
		//todo 投票响应消息
		r.pr.recordVote(rm.From, rm.Success)
		granted, rejected, re := r.pr.countVotes()
		switch re {
		case VoteWon:
			//选举成功
			log.Printf("id:%d receive  %d granted votes will become leader,current term:%d,current commit index:%d",
				r.id, granted, r.currentTerm, r.commitIndex)
			r.becomeLeader()
			r.broadcastAppend()
			return nil
		case VoteLost:
			log.Printf("id:%d receive %d rejected votes will become follower,current term:%d,current commit index:%d",
				r.id, rejected, r.currentTerm, r.commitIndex)
			r.becomeFollower(r.currentTerm, 0)
			return nil
		}
	case transport.MsgHeartbeat:
		//todo 接收心跳请求信息
		switch r.state {
		case LEADER:
			//判断term 大小
			if rm.Term > r.currentTerm {
				//接收
				r.becomeFollower(rm.Term, rm.From)
				r.transport.SendMessage(transport.RaftMessage{From: r.id, To: rm.From, Success: true, Type: transport.MsgHeartBeatResp})
				return nil
			} else {
				//拒绝接收
				r.transport.SendMessage(transport.RaftMessage{To: rm.From, From: r.id, Success: false,
					Type: transport.MsgHeartBeatResp})
				return nil
			}
		case CANDIDATE:
			r.becomeFollower(rm.Term, rm.From)
			return nil
		case FOLLOWER:
			//重置选举定时器
			return nil
		}
		return nil

	case transport.MsgHeartBeatResp:
		//todo 处理心跳响应信息

		return nil
	}
	return nil
}

func (r *Raft) becomeLeader() {
	r.rwLock.Lock()
	r.rwLock.Unlock()
	r.state = LEADER
	r.voteFor = 0
	r.pr.ResetVotes()
}

func (r *Raft) broadcastAppend() {

}

func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.rwLock.Lock()
	defer r.rwLock.Unlock()
	r.currentTerm = term
	r.voteFor = 0
	r.lead = lead
	r.state = FOLLOWER
	r.electionTimeout = RandInt64(150, 300)
	r.pr.ResetVotes()
}
