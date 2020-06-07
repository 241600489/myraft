package types

import (
	"fmt"
	"testing"
)

func TestGenerateID(t *testing.T) {
	id := GenerateID("127.0.0.1:9080")
	fmt.Println(id)
	fmt.Println(uint64(id))
}
