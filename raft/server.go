package raft

import (
	"log"
	"math/rand"
	"myraft/config"
	"myraft/member"
	"myraft/raft/transport"
	"sync"
	"time"
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
	nextIndex      map[uint64]uint64 //对于每台服务器，要发送到该服务器的下一个日志条目的索引(初始化为leader lastlog索引index +1)
	matchIndex     map[uint64]uint64 //对于每个服务器，在服务器上复制已知的最高日志条目的索引(初始化为0,单调递增)
	electionTimer  *time.Timer       // election timeout选举超时是指followers跟随者成为candidates候选者之前所等待的时间 默认是在150 毫秒到300 毫秒之间
	heartBeatTimer *time.Timer       //心跳间隔
	peers          []uint64
	localAddr      string
	pr             *ProcessHandler
	transport      *transport.Transport
	lead           uint64
	electionTick   uint32 //接收到一次心跳设置为1 应付重置选举定时器 时还接收到 定时器中channel 的数据
}

func NewServer(c *config.RaftConfig) *Raft {

	r := &Raft{
		state:        FOLLOWER,
		currentTerm:  0, //todo 从持久化信息读取
		rwLock:       &sync.RWMutex{},
		voteFor:      0, //默认为空
		commitIndex:  0, //todo 从持久化信息读取
		lastApplied:  0, //todo 从持久化信息读取
		nextIndex:    nil,
		matchIndex:   nil,
		electionTick: 0,
	}
	r.electionTimer = time.NewTimer(time.Millisecond * time.Duration(RandInt64(150, 300)))
	r.heartBeatTimer = time.NewTimer(time.Millisecond * time.Duration(RandInt64(100, 150)))
	//生成 raft
	rc := member.NewRaftCluster(c.LocalAddr, c.ClusterAddr)
	r.transport = transport.NewTransport(rc)
	r.id = uint64(rc.LocalID())
	r.peers = rc.Peers()
	r.pr = MakeProcessHandler(r.peers)
	r.localAddr = c.LocalAddr
	go r.run()
	return r
}

func RandInt64(min, max int64) int64 {
	if min >= max || min == 0 || max == 0 {
		return max
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Int63n(max-min) + min
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
			r.electionTimer.Reset(time.Millisecond * time.Duration(RandInt64(150, 300)))
			return nil
		} else {
			log.Printf("id:%d reject vote from id:%d,its term:%d and lastIndex:%d", r.id, rm.From, rm.Term, rm.LogIndex)
			r.transport.SendMessage(transport.RaftMessage{To: rm.From, From: r.id, Success: false, Type: transport.MsgVoteResp})
			return nil
		}

	case transport.MsgVoteResp:
		if r.state == LEADER || r.state == FOLLOWER {
			log.Printf("id:%d,current state:%d,will not handle msg vote resp", r.id, r.state)
			return nil
		}
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
			//判断term 大小

			if rm.Term >= r.currentTerm {

				//接受
				if rm.Term > r.currentTerm {
					log.Printf("change term ,receive heart from id:%d,its term:%d > current term:%d,current id:%d ",
						rm.From, rm.Term, r.currentTerm, r.id)
					r.currentTerm = rm.Term

				}
				log.Printf("accept success,receive heartbeat from id:%d,its term:%d,current term:%d,id:%d",
					rm.From, rm.Term, r.currentTerm, r.id)
				//r.electionTimer.Reset(time.Millisecond * time.Duration(RandInt64(150, 300)))
				r.electionTick += 1

				r.transport.SendMessage(transport.RaftMessage{From: r.id, To: rm.From, Success: true, Type: transport.MsgHeartBeatResp})
			} else {
				//不接受
				log.Printf("accept fail,,receive heartbeat from id:%d,its term:%d,current term:%d,id:%d",
					rm.From, rm.Term, r.currentTerm, r.id)
				r.transport.SendMessage(transport.RaftMessage{To: rm.From, From: r.id, Success: false, Term: r.currentTerm, Type: transport.MsgHeartBeatResp})
			}
			return nil
		}
		return nil

	case transport.MsgHeartBeatResp:
		//todo 处理心跳响应信息
		if rm.Success {
			log.Printf("accept heat beat success from %d,current id:%d", rm.From, r.id)
		} else {
			log.Printf("accept heart beat fail,from id:%d, its term:%d,current term:%d id:%d",
				rm.From, rm.Term, r.currentTerm, r.id)
		}
		return nil
	}
	return nil
}

func (r *Raft) becomeLeader() {
	r.rwLock.Lock()
	r.rwLock.Unlock()
	r.state = LEADER
	r.voteFor = 0
	r.heartBeatTimer.Reset(time.Millisecond * time.Duration(RandInt64(100, 150)))
	r.electionTimer.Stop()
	r.pr.ResetVotes()
}

func (r *Raft) broadcastAppend() {
	r.rwLock.RLock()
	defer r.rwLock.RUnlock()
	for _, peerId := range r.peers {
		if r.id == peerId {
			continue
		}
		r.transport.SendMessage(transport.RaftMessage{From: r.id, To: peerId,
			Term: r.currentTerm, Type: transport.MsgHeartbeat})
	}
}

func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.rwLock.Lock()
	defer r.rwLock.Unlock()
	r.currentTerm = term
	r.voteFor = 0
	r.lead = lead
	r.state = FOLLOWER
	r.electionTimer.Reset(time.Millisecond * time.Duration(RandInt64(150, 300)))
	r.heartBeatTimer.Stop()
	r.pr.ResetVotes()
}
func (r *Raft) run() {
	for {
		select {
		case <-r.electionTimer.C:
			if r.electionTick > 0 {
				r.electionTimer.Reset(time.Millisecond * time.Duration(RandInt64(150, 300)))

				log.Printf("id:%d have receive heart beat so do not become candidate", r.id)
				r.rwLock.Lock()
				r.electionTick = 0
				r.rwLock.Unlock()
				continue
			}
			if r.state == LEADER || r.state == CANDIDATE || r.voteFor > 0 {
				r.electionTimer.Reset(time.Millisecond * time.Duration(RandInt64(150, 300)))
				//todo 当为candidate时则说明 遇到网络延迟没有接收到大多数票
				continue
			}
			log.Printf("id:%d will become candidate ", r.id)
			r.becomeCandidate()
			r.broadcastVote()
			r.electionTimer.Reset(time.Millisecond * time.Duration(RandInt64(150, 300)))

		case <-r.heartBeatTimer.C:
			if r.state == LEADER {
				r.broadcastAppend()
				r.heartBeatTimer.Reset(time.Millisecond * time.Duration(RandInt64(100, 150)))
			} else {
				r.heartBeatTimer.Reset(time.Millisecond * time.Duration(RandInt64(100, 150)))
				continue
			}
			r.heartBeatTimer.Reset(time.Millisecond * time.Duration(RandInt64(100, 150)))
		case receiveMessage := <-r.transport.MsgWaitToHandle:
			go func() {
				err := r.handle(receiveMessage)
				if err != nil {
					log.Printf("id:%d handle mesage error,message:%+v,error:%v",
						r.id, receiveMessage, err)
				}
			}()
		}
	}
}

func (r *Raft) becomeCandidate() {
	r.rwLock.Lock()
	r.currentTerm += 1
	r.state = CANDIDATE
	r.lead = 0
	r.voteFor = r.id
	r.pr.recordVote(r.id, true)
	r.electionTimer.Reset(time.Millisecond * time.Duration(RandInt64(150, 300)))
	r.rwLock.Unlock()
}

func (r *Raft) broadcastVote() {
	r.rwLock.RLock()
	defer r.rwLock.RUnlock()
	log.Printf("id:%d start to send vote current term:%d  ", r.id, r.currentTerm)
	for _, peerId := range r.peers {
		if r.id == peerId {
			continue
		}
		r.transport.SendMessage(transport.RaftMessage{From: r.id, To: peerId, Term: r.currentTerm,
			LogIndex: r.commitIndex, Type: transport.MsgVote})
	}
}

func (r *Raft) ID() uint64 {
	return r.id
}
