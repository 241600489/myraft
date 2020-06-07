package myraft

import (
	"math/rand"
	"myraft/config"
	"myraft/member"
	"myraft/tranport"
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
	currentTerm int32         //当前任期号 启动时为0
	rwLock      *sync.RWMutex //读写锁 用于 term 增加时使用
	voteFor     uint64        //给哪一个节点投票
	commitIndex int64         //已提交的日志index
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
	transport        *tranport.Transport
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
	r.transport = tranport.NewTransport(rc)
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

func (r *Raft) Transport() *tranport.Transport {
	return r.transport
}

func (r *Raft) handle(rm tranport.RaftMessage) {

	switch rm.Type {
	case tranport.MsgVote:
		//todo 处理投票请求信息
	case tranport.MsgVoteResp:
		//todo 投票响应消息
	case tranport.MsgHeartbeat:
		//todo 接收心跳请求信息
	case tranport.MsgHeartBeatResp:
		//todo 处理心跳响应信息
	}
}
