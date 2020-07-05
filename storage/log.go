package storage

import (
	"encoding/binary"
	"hash/crc32"
	"log"
	"myraft/utils"
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

	REof

	RBadRecord
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
		if residue < avail {
			fragmentLength = residue
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
	binary.BigEndian.PutUint16(headBuf[4:6], uint16(length))
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

type Reader struct {
	dest             *SequentialFile
	initialOffset    uint64
	buf              []byte
	bufWrap          *utils.SliceWrap
	bufEndOff        uint64
	lastRecordOffset uint64
	reSync           bool
	eof              bool //是否已关闭
}

func NewReader(dest *SequentialFile, skipOffset uint64) *Reader {
	return &Reader{
		dest:             dest,
		initialOffset:    skipOffset,
		bufWrap:          utils.NewSliceWrap(make([]byte, 0), 0),
		bufEndOff:        0,
		lastRecordOffset: 0,
		reSync:           false,
		eof:              false,
	}
}
func (r *Reader) SkipInitialBlock() bool {
	if r.initialOffset <= 0 {
		return true
	}
	offsetInBlock := r.initialOffset % ZBlockSize
	skipStartOff := r.initialOffset - offsetInBlock
	if ZBlockSize-offsetInBlock < ZRecordHeadSize {
		skipStartOff += ZBlockSize
	}
	if skipStartOff == 0 {
		return true
	}
	skipOk := r.dest.Skip(skipStartOff)
	if skipOk {
		r.bufEndOff += skipStartOff
		return true
	} else {
		log.Printf("skip %d byte fail", skipStartOff)
		return false
	}
}

func (r *Reader) ReadRecord() (ok bool, result []byte) {
	if r.lastRecordOffset < r.initialOffset {
		if !r.SkipInitialBlock() {
			return false, nil
		}
	}
	//当前读取的是否在某个record 片段中
	inFragmentRecord := false

	//我们正在读取的逻辑记录的记录偏移量。
	//0是一个伪值，用来让编译器满意。
	prospectiveRecordOffset := uint64(0)

	for {
		currentFragment, recordType := r.readPhysicalRecord()
		var fragmentSize uint64
		if currentFragment == nil {
			fragmentSize = 0
		} else {
			fragmentSize = currentFragment.Size()
		}
		physicalRecordOffset := r.bufEndOff - r.bufWrap.Size() - ZRecordHeadSize - fragmentSize

		//由于 需要跳过 initialOffset个字节则可能会将一个完整 用户记录 拦腰剪短所以这里为了跳过 不完整的用户记录
		if r.reSync {
			if recordType == RMiddleType {
				continue
			} else if recordType == RLastType {
				r.reSync = false
				continue
			} else {
				r.reSync = false
			}
		}

		switch recordType {
		case RBadRecord:
			if inFragmentRecord {
				log.Println("error of middle record")
				inFragmentRecord = false
				//清空已添加到result 中的 数据
				result = make([]byte, 0)
			}
			break
		case RFullType:
			if inFragmentRecord {
				log.Panicln("missing start of fragmented")
			}
			result = append(result, currentFragment.Data()...)
			prospectiveRecordOffset = physicalRecordOffset
			r.lastRecordOffset = prospectiveRecordOffset
			return true, result
		case RLastType:
			if !inFragmentRecord {
				log.Panicln("missing start of fragmented")
			}
			//装填结果
			result = append(result, currentFragment.Data()...)
			r.lastRecordOffset = prospectiveRecordOffset
			return true, result
			//设置 lastRecordOffset
		case RMiddleType:
			//装填结果
			if !inFragmentRecord {
				log.Panicln("missing start of fragmented record(1)")
			} else {
				result = append(result, currentFragment.Data()...)
			}
			break
		case RFirstType:
			if inFragmentRecord && len(result) != 0 {
				log.Panicln("partitial record without end")
			}
			inFragmentRecord = true
			prospectiveRecordOffset = physicalRecordOffset
			result = append(result, currentFragment.Data()...)
			break
		case REof:
			if inFragmentRecord {
				result = make([]byte, 0)
			}
			return false, nil
		}
	}
}
func (r *Reader) readPhysicalRecord() (*utils.SliceWrap, RecordType) {
	for {

		if r.bufWrap.Size() < ZRecordHeadSize {
			if r.eof {
				r.bufWrap.Clear()
				return nil, REof
			}
			r.bufWrap.Clear()
			sliceWrap := r.dest.Read(ZBlockSize)
			r.bufEndOff += ZBlockSize
			r.bufWrap = sliceWrap
			if r.bufWrap.Size() < ZBlockSize {
				r.eof = true
			}
			continue
		}
		//解析record的头部
		data := r.bufWrap.Data()
		recordLength := binary.BigEndian.Uint16(data[4:6]) //record 的 长度
		recordType := RecordType(data[6])                  //record 的类型
		checkSum := binary.BigEndian.Uint32(data[0:4])     //record 的类型
		if (uint64(recordLength) + ZRecordHeadSize) > r.bufWrap.Size() {
			log.Printf("bad record length:%d", r.bufWrap.Size())
			r.bufWrap.Clear()
			if r.eof {
				return nil, REof
			}
			return nil, RBadRecord
		}
		//校验验证码
		checkSumFromFile := crc32.ChecksumIEEE(data[7 : uint64(recordLength)+ZRecordHeadSize])
		if checkSumFromFile != checkSum {
			log.Printf("checksum mismatch drop %d byte", r.bufWrap.Size())
			r.bufWrap.Clear()
			return nil, RBadRecord
		}
		r.bufWrap.RemovePrefix(uint64(recordLength) + ZRecordHeadSize)
		if r.bufEndOff-r.bufWrap.Size()-uint64(recordLength)-ZRecordHeadSize < r.initialOffset {
			return nil, RBadRecord
		}
		currentFragment := make([]byte, uint64(recordLength))

		copy(currentFragment, data[7:ZRecordHeadSize+uint64(recordLength)])

		return utils.NewSliceWrap(currentFragment, uint64(recordLength)), recordType

	}

}
