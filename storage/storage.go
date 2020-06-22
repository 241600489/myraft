package storage

import (
	"encoding/binary"
	"log"
	"myraft/raft/transport"
	"os"
)

// 1个g 字节
const MAX_DATA_FILE_SIZE = 1073741824
const UINT64_BYTE_COUNT = 8

type Storage interface {
	//添加日志条目
	AppendEntries(ens []transport.Entry) bool

	// 返回一个位于[lo,hi) 之间的日志条目分片 `maxSize` 限制了返回日志条目的总大小
	// 但是至少返回一个条目
	Entries(lo, hi, maxSize uint64) ([]transport.Entry, error)

	// 返回给定日志索引的任期号，
	Term(i uint64) (uint64, error)
	// 返回最后一个日志条目的索引
	LastIndex() (uint64, error)

	//返回第一个日志条目的索引
	FirstIndex() (uint64, error)
}

type PersistStorage struct {
	//文件存储目录
	storeDir string
	//index 文件对象
	indexFile *os.File
	// 数据 文件对象
	dataFile *os.File
}

func (ps *PersistStorage) AppendEntries(ens []transport.Entry) bool {
	stat, err := ps.dataFile.Stat()
	if err != nil {
		log.Fatal("get data file stat fail,%+v", err)
	}
	//获取data的文件长度 如果超过 1g 则创建一个新的文件
	currentDataSize := stat.Size()
	//每个日志条目采用head-body,head=日志条目长度，body=实际的日志条目
	var byteToWrite []byte

	for _, en := range ens {
		termBytes := make([]byte, UINT64_BYTE_COUNT)
		binary.BigEndian.PutUint64(termBytes, en.Term)
		indexBytes := make([]byte, UINT64_BYTE_COUNT)
		binary.BigEndian.PutUint64(indexBytes, en.Index)
		dataOfLen := len(termBytes) + len(indexBytes) + len(en.Data)
		headBytes := make([]byte, UINT64_BYTE_COUNT)
		binary.BigEndian.PutUint64(headBytes, uint64(dataOfLen))
		byteToWrite = append(byteToWrite, headBytes...)
		byteToWrite = append(byteToWrite, termBytes...)
		byteToWrite = append(byteToWrite, indexBytes...)
		byteToWrite = append(byteToWrite, en.Data...)
	}
	lengthToWrite := int64(len(byteToWrite))
	if (lengthToWrite + currentDataSize) > MAX_DATA_FILE_SIZE {
		//如果加上新增的日志大于 最大限制则新建一个文件
		//关闭旧文件
		err := ps.dataFile.Close()
		if err != nil {
			log.Panicf("close data file %s fail,err:%+v", ps.dataFile.Name(), err)
		}
		//
		//ps.storeDir+"/"
		//os.Create("")
	}
}
