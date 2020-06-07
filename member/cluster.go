package member

import (
	"myraft/types"
	"strings"
)

type Cluster interface {
	// ID returns the cluster ID
	ID() types.ID
	// Members returns a slice of members sorted by their ID
	Members() []*Member
	// Member retrieves a particular member based on ID, or nil if the
	// member does not exist in the cluster
	Member(id types.ID) *Member
}

type RaftCluster struct {
	localID types.ID             //当前节点唯一标识
	cid     types.ID             //当前集群唯一标识
	members map[types.ID]*Member //raft 集群成员

}

//clusterAddr like 127.0.0.1:9009,127.0.0.1:9010,127.0.0.1:9011
func NewRaftCluster(localAddr string, clusterAddr string) *RaftCluster {
	rc := &RaftCluster{
		members: make(map[types.ID]*Member),
	}
	rc.localID = types.GenerateID(localAddr)
	clusterAddrArrs := strings.Split(clusterAddr, ",")
	for _, peerAddr := range clusterAddrArrs {
		m := NewMember(peerAddr)
		rc.members[m.ID] = m
	}
	rc.cid = types.GenerateID(clusterAddr)
	return rc
}
func (rc *RaftCluster) LocalID() types.ID {
	return rc.localID

}
func (rc *RaftCluster) ID() types.ID {
	return rc.cid
}
func (rc *RaftCluster) Members() []*Member {
	var memberSlice []*Member
	for _, v := range rc.members {
		memberSlice = append(memberSlice, v)
	}
	return memberSlice
}
func (rc *RaftCluster) Peers() []uint64 {
	var peers []uint64
	for _, v := range rc.members {
		peers = append(peers, uint64(v.ID))

	}
	return peers

}
func (rc *RaftCluster) Member(id types.ID) *Member {
	return rc.members[id].Clone()
}
