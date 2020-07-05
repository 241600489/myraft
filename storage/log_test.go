package storage

import (
	"fmt"
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

func TestNewReaderAndReadRecord(t *testing.T) {
	test1 := NewWritableFile("./test1.log")
	test1.Append([]byte("fdasfsafa"))
	test1.Close()
	readableFile := NewSequentialFile("./test1.log")
	reader := NewReader(readableFile, 0)
	for {
		ok, result := reader.ReadRecord()
		if ok {
			fmt.Println(string(result))
			continue
		}
		break
	}
	readableFile.Close()
}
func TestReader_SkipInitialBlock(t *testing.T) {
	readableFile := NewSequentialFile("./test1.log")
	reader := NewReader(readableFile, 15)
	for {
		ok, result := reader.ReadRecord()
		if ok {
			fmt.Println(string(result))
			continue
		}
		break
	}
	readableFile.Close()
}
