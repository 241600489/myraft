package storage

import (
	"os"
	"testing"
)

func TestNewWriter(t *testing.T) {
	writableFile := NewWritableFile("./test1.log")
	writer := NewWriter(writableFile, 0)
	a := "niahojgfaga"
	b := []byte(a)
	writer.AddRecord(b)
	writableFile.Close()
}

func TestWriter_AddRecord(t *testing.T) {
	writableFile := NewWritableFile("./test1.log")
	writer := NewWriter(writableFile, 0)
	dataFile, _ := os.Open("./data.txt")
	dataSta, _ := dataFile.Stat()
	readBuf := make([]byte, dataSta.Size())
	dataFile.Read(readBuf)
	for i := 0; i < 64; i++ {
		writer.AddRecord(readBuf)
	}

	writableFile.Close()
}
