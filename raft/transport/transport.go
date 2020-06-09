package transport

import (
	"log"
	"myraft/member"
	"myraft/types"
	"net/rpc"
	"strconv"
)

type MessageType uint8

const (
	MsgVote MessageType = 1 + iota
	MsgVoteResp
	MsgHeartbeat
	MsgHeartBeatResp
)

type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}
type RaftMessage struct {
	From     uint64 //从哪里来
	To       uint64 //发送给谁
	Type     MessageType
	Success  bool //是否成功
	Term     uint64
	LogIndex uint64
	Entries  []Entry
}
type Transport struct {
	rc              *member.RaftCluster
	msgWait         chan RaftMessage
	MsgWaitToHandle chan RaftMessage
}

//处理rpc
func (tr *Transport) HandleMessage(in RaftMessage, resp *RaftMessage) error {
	//将rpc 消息发送给 到通道里由 raft 实例 处理
	if in.To == uint64(tr.rc.LocalID()) {
		log.Printf("id:%d,receive message:%+v", tr.rc.LocalID(), in)
		tr.MsgWaitToHandle <- in
	}
	return nil
}

func (tr *Transport) SendMessage(in RaftMessage) {
	//直接放到通道里
	m := tr.rc.Member(types.ID(in.To))
	if m == nil {
		log.Printf("can not find member by id:%d", in.To)
		return
	}
	//获取
	client, err := rpc.Dial("tcp", m.PeerAddr())
	if err != nil {
		log.Printf("get rpc client fail,its address:%s and its id:%d,current id:%d",
			m.PeerAddr(), m.ID, in.From)
		return
	}
	go func() {
		var resp RaftMessage

		callErr := client.Call("Transport"+strconv.FormatUint(in.To, 10)+".HandleMessage",
			in, &resp)
		if callErr != nil {
			log.Printf("call Transport.HandleMessage error,args:%v,error:%v", in, callErr)
		}
		closeErr := client.Close()
		if closeErr != nil {
			log.Printf("close client fail,current id:%d,error:%v", in.From, closeErr)
		}
	}()

}
func NewTransport(rc *member.RaftCluster) *Transport {
	tr := &Transport{
		rc:              rc,
		msgWait:         make(chan RaftMessage, 50),
		MsgWaitToHandle: make(chan RaftMessage, 50),
	}
	return tr
}
