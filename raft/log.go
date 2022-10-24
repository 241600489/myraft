package raft

import "myraft/storage"

//操作日志条目
type Log struct {
	stableStore   storage.Storage
	unStableStore storage.Storage
}
