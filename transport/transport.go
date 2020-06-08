package transport

import "myraft/member"

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
	rc      *member.RaftCluster
	msgWait chan RaftMessage
}

//处理rpc 消息
func (tr *Transport) HandleMessage(in *RaftMessage, resp *RaftMessage) error {
	//将rpc 消息发送给 到通道里由 raft 实例 处理

	return nil
}

func (tr *Transport) SendMessage(in RaftMessage) {
	//直接放到通道里
	tr.msgWait <- in
}

func NewTransport(rc *member.RaftCluster) *Transport {
	tr := &Transport{
		rc:      rc,
		msgWait: make(chan RaftMessage, 50),
	}
	return tr
}
