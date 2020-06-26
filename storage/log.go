package storage

import (
	"encoding/binary"
	"hash/crc32"
	"log"
)

type RecordType uint8

const (
	//Zero is reserved for preallocated files
	RZeroType RecordType = iota
	RFullType
	// 用户记录的片段
	RFirstType
	RMiddleType
	RLastType
)
const (
	ZBlockSize      uint64 = 32768
	ZRecordHeadSize uint64 = 7
)

type Writer struct {
	dest        *WritableFile
	blockOffset uint64 //
}

func NewWriter(dest *WritableFile, offset uint64) *Writer {
	return &Writer{
		dest:        dest,
		blockOffset: offset,
	}
}

func (wr *Writer) AddRecord(data []byte) bool {
	residue := uint64(len(data))
	var dataOff uint64 = 0
	var ok bool
	begin := true
	for {
		if residue <= 0 {
			log.Printf("have already write %d bytes", len(data))
			return ok
		}
		//计算
		leftOver := ZBlockSize - wr.blockOffset

		if leftOver < ZRecordHeadSize {
			//如果剩余小于7 则填上\x00\x00\x00\x00\x00\x00 并切换到新的block
			if leftOver > 0 {
				wr.dest.Append(make([]byte, leftOver))
			}
			wr.blockOffset = 0
		}

		avail := ZBlockSize - wr.blockOffset - ZRecordHeadSize
		var fragmentLength uint64
		if uint64(residue) < avail {
			fragmentLength = uint64(residue)
		} else {
			fragmentLength = avail
		}
		end := residue == fragmentLength
		var rType RecordType

		if begin && end {
			rType = RFullType
		} else if end {
			rType = RLastType
		} else if begin {
			rType = RFirstType
		} else {
			rType = RMiddleType
		}

		addPhysicalRecordOk := wr.addPhysicalRecord(rType, data, dataOff, fragmentLength)
		if !addPhysicalRecordOk {
			return false
		}
		begin = false
		residue = residue - fragmentLength
		wr.blockOffset += fragmentLength + ZRecordHeadSize
	}

}

func (wr *Writer) addPhysicalRecord(rType RecordType, data []byte, off uint64, length uint64) bool {

	//构建 record 头部 4 个字节的校验和 2 个字节的length 1 个字节的type
	checkSumForData := crc32.ChecksumIEEE(data[off : off+length])
	headBuf := make([]byte, ZRecordHeadSize)
	binary.BigEndian.PutUint32(headBuf, checkSumForData)
	headBuf[4] = byte(length >> 8)
	headBuf[5] = byte(length)
	headBuf[6] = byte(rType)

	isOk := false
	writeHeadOk := wr.dest.Append(headBuf)
	if writeHeadOk {
		writeBodyOk := wr.dest.Append(data[off : off+length])
		if writeBodyOk {
			wr.dest.Flush()
			isOk = true
		} else {
			log.Printf("write data fail,data length:%d,off:%d,totalDataLength:%d", length, off, len(data))
		}
	} else {
		log.Printf("write head  fail,head:%+v", headBuf)
	}
	return isOk
}
