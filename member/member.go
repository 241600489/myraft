package member

import (
	"myraft/types"
)

type Member struct {
	ID       types.ID `json:"id"`
	peerAddr string   //ip:port eg:127.0.0.1:8080
}

func (m *Member) Clone() *Member {
	return &Member{
		ID:       m.ID,
		peerAddr: m.peerAddr,
	}
}

func NewMember(peerAddr string) *Member {
	member := &Member{
		peerAddr: peerAddr,
	}
	//generate ID
	member.ID = types.GenerateID(peerAddr)
	return member

}
