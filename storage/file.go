package storage

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"myraft/utils"
	"os"
)

const (
	//65536* 2
	ZMaxByteBufferSize int = 65536
)

type WritableFile struct {
	buf         *bytes.Buffer //缓冲器
	bufArr      []byte
	fileName    string   // 文件名称
	dest        *os.File //实际的文件名称
	writeOffset int
}

func NewWritableFile(fileName string) *WritableFile {
	intervalBuf := make([]byte, 0, ZMaxByteBufferSize)
	destFile, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		log.Panic("create ")
	}

	return &WritableFile{
		buf:         bytes.NewBuffer(intervalBuf),
		fileName:    fileName,
		dest:        destFile,
		bufArr:      make([]byte, ZMaxByteBufferSize, ZMaxByteBufferSize),
		writeOffset: 0,
	}
}

func (wf *WritableFile) Append(data []byte) bool {
	//装一部分到 buf 中 直到它满
	writeSize := len(data)
	copyCount := copy(wf.bufArr[wf.writeOffset:], data)
	wf.writeOffset = wf.writeOffset + copyCount

	if copyCount == writeSize {
		//如果 writeCount 为0 则说明 全写到缓冲池里了
		return true
	}
	writeSize = writeSize - copyCount

	//刷新缓存到文件里
	wf.Flush()
	//
	if writeSize < (ZMaxByteBufferSize - wf.writeOffset) {
		copyCount = copy(wf.bufArr[wf.writeOffset:], data[copyCount:])
		wf.writeOffset = wf.writeOffset + copyCount
		return true
	}
	return wf.writeDirectly(data, copyCount, len(data))

}

func (wf *WritableFile) writeDirectly(data []byte, start int, end int) bool {
	writeCount, err := wf.dest.Write(data[start:end])

	if err != nil {
		log.Printf("write to file directly fail:%+v", err)
		return false
	}
	return writeCount == (len(data) - 1 - start)
}

func (wf *WritableFile) Flush() bool {
	defer func() {
		wf.writeOffset = 0
	}()
	return wf.writeDirectly(wf.bufArr, 0, wf.writeOffset)

}

func (wf *WritableFile) Close() {
	if wf.writeOffset > 0 {
		wf.Flush()
	}
	wf.buf.Reset()
	wf.buf = nil
	err := wf.dest.Close()
	if err != nil {
		fmt.Printf("close file:%s error:%v", wf.fileName, err)
	}
}

type SequentialFile struct {
	file     *os.File
	fileName string
}

func (sf *SequentialFile) Read(size uint64) *utils.SliceWrap {
	for {
		scratch := make([]byte, size)
		count, err := sf.file.Read(scratch)
		if err != nil {
			log.Printf("read from file:%s fail,err:%v", sf.fileName, err)
			continue
		}
		return utils.NewSliceWrap(scratch, uint64(count))
	}
}
func (sf *SequentialFile) Skip(n uint64) bool {
	_, err := sf.file.Seek(int64(n), io.SeekCurrent)
	if err != nil {
		log.Printf("skip %d count byte fail,err:%v", n, err)
		return false
	}
	return false
}

func (sf *SequentialFile) Close() {
	err := sf.file.Close()
	if err != nil {
		log.Printf("close file fail,fileName:%s,err:%v", sf.fileName, err)
	}
}
func NewSequentialFile(fileName string) *SequentialFile {
	dest, err := os.Open(fileName)
	if err != nil {
		log.Printf("open file fail,file name:%s,err:%v", fileName, err)
	}
	return &SequentialFile{
		file:     dest,
		fileName: fileName,
	}
}
