package raft

import (
	"log"
	"sync"
)

type VoteResult uint8

const (
	//选举等待中 投票和否决投票还没有达到 大多数节点
	VoteWaiting VoteResult = 1 + iota
	VoteLost
	VoteWon
)

type ProcessHandler struct {
	Votes map[uint64]bool //选举票分布
	peers []uint64        //peer 列表
	rw    *sync.RWMutex
}

func MakeProcessHandler(peers []uint64) *ProcessHandler {
	return &ProcessHandler{
		Votes: make(map[uint64]bool),
		peers: peers,
		rw:    &sync.RWMutex{},
	}
}

func (ph *ProcessHandler) recordVote(id uint64, v bool) {
	ph.rw.Lock()
	defer ph.rw.Unlock()
	_, ok := ph.Votes[id]
	if !ok {
		ph.Votes[id] = v
	}
}

func (ph *ProcessHandler) countVotes() (granted int, rejected int, re VoteResult) {
	ph.rw.RLock()
	defer ph.rw.RUnlock()
	var missing int
	for _, id := range ph.peers {
		v, voted := ph.Votes[id]
		if !voted {
			missing++
			continue
		}
		if v {
			granted++
		} else {
			rejected++
		}
	}
	majorPeer := len(ph.peers)/2 + 1
	log.Printf("major peer :%d,granted:%d,rejected:%d", majorPeer, granted, rejected)
	if granted >= majorPeer {
		re = VoteWon
	} else if granted+missing >= majorPeer {
		re = VoteWaiting
	} else {
		re = VoteLost
	}
	return granted, rejected, re
}

func (ph *ProcessHandler) ResetVotes() {
	ph.Votes = make(map[uint64]bool)
}
