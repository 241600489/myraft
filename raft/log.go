package raft

//操作日志条目
type Log struct {
	stableStore   Storage
	unStableStore Storage
}
