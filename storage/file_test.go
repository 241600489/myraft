package storage

import (
	"log"
	"testing"
)

func TestNewWritableFile(t *testing.T) {
	fileName := "./test.log"
	writableFile := NewWritableFile(fileName)
	log.Printf("raw fileName:%s,dest fileName:%s", writableFile.fileName, fileName)
	writableFile.Close()
}
func TestWritableFile_Append(t *testing.T) {
	fileName := "./test.log"
	writableFile := NewWritableFile(fileName)
	a := "123456"
	data := []byte(a)
	writableFile.Append(data)
	writableFile.Close()
}

func TestWritableFile_Flush(t *testing.T) {

}
