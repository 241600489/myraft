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

func TestNewSequentialFile(t *testing.T) {
	sequentialFile := NewSequentialFile("./test1.log")
	if sequentialFile.file == nil {
		t.Error("new SequentialFile fail,the real file can not be created")
		return
	}
	sequentialFile.Close()
}
func TestSequentialFile_Read(t *testing.T) {
	sequentialFile := NewSequentialFile("./test1.log")
	defer sequentialFile.Close()
	resultWrap := sequentialFile.Read(32 * 1024)
	result := resultWrap.Data()
	v := string(result[7:resultWrap.Size()])
	log.Printf("read result,len %d,cap:%d", len(result), cap(result))
	log.Printf("the result that read:%s", v)
}
func TestSequentialFile_Skip(t *testing.T) {
	sequentialFile := NewSequentialFile("./test1.log")
	defer sequentialFile.Close()
	skipOk := sequentialFile.Skip(7)
	if !skipOk {
		t.Errorf("skip file:%s %d bytes fail", sequentialFile.fileName, 7)
	}
	resultWrap := sequentialFile.Read(32 * 1024)
	result := resultWrap.Data()
	v := string(result[:resultWrap.Size()])
	log.Printf("read result,len:%d,cap:%d", len(result), cap(result))
	log.Printf("the result that read:%s", v)
}
