package types

import (
	"crypto/sha1"
	"encoding/binary"
)

type ID uint64

func GenerateID(addr string) ID {
	var b []byte
	b = append(b, []byte(addr)...)
	hash := sha1.Sum(b)

	return ID(binary.BigEndian.Uint64(hash[:8]))
}
