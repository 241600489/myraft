package utils

import "log"

type SliceWrap struct {
	data []byte
	size uint64
}

func (sw *SliceWrap) Data() []byte {
	return sw.data
}
func (sw *SliceWrap) Size() uint64 {
	return sw.size
}

func NewSliceWrap(data []byte, size uint64) *SliceWrap {
	return &SliceWrap{
		data: data,
		size: size,
	}
}

func (sw *SliceWrap) SetSize(size uint64) {
	sw.size = size
}

func (sw *SliceWrap) Clear() {
	sw.data = make([]byte, 0)

	sw.size = 0

}

func (sw *SliceWrap) RemovePrefix(n uint64) {
	if n > sw.size {
		log.Panicf("the parameter n:%d should less than or equals SliceWrap'size:%d", n, sw.size)
	}
	sw.data = sw.data[n:sw.size]
	sw.size -= n

}

func (sw *SliceWrap) Append(data []byte) {
	sw.data = append(sw.data, data...)
	sw.size = uint64(len(sw.data))
}
