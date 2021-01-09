package cache

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"sync/atomic"

	"github.com/shirou/gopsutil/mem"
)

const (
	MiB                    = 1 << 20
	memCacheWriteBlockSize = 256 * MiB
)

var (
	ErrInvalidCapSize      = errors.New("cap must be larger than size")
	ErrMemoryNotEnough     = errors.New("memory is not enough")
	ErrCancelWriteToWriter = errors.New("cancel write to writer")
	ErrConcurrentAccess    = errors.New("cache is being used")
)

var varCache *MemCache

func init() {
	var err error
	varCache, err = NewMemCache(0, 0)
	if err != nil {
		panic(err)
	}
}

// MemCache runs without mutex for better performance
type MemCache struct {
	using int32 // atomic
	size  uint64
	cap   uint64
	data  []byte
}

func NewMemCache(size, cap uint64) (cache *MemCache, err error) {
	if cap < size {
		return nil, ErrInvalidCapSize
	}
	available, err := GetAvailableMem()
	if err != nil {
		return nil, err
	}
	if available < cap {
		return nil, ErrMemoryNotEnough
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	data := make([]byte, cap, cap)
	// prevent virtual memory
	for i := range data {
		data[i] = 0
	}
	cache = &MemCache{
		size: size,
		cap:  cap,
		data: data,
	}
	return
}

func (cache *MemCache) lock() error {
	if !atomic.CompareAndSwapInt32(&cache.using, 0, 1) {
		return ErrConcurrentAccess
	}
	return nil
}

func (cache *MemCache) unlock() {
	atomic.StoreInt32(&cache.using, 0)
}

func (cache *MemCache) Size() uint64 {
	return cache.size
}

func Size() uint64 {
	return varCache.Size()
}

func (cache *MemCache) Cap() uint64 {
	return cache.cap
}

func Cap() uint64 {
	return varCache.Cap()
}

func (cache *MemCache) Slice() []byte {
	return cache.data[:cache.size]
}

func Slice() []byte {
	return varCache.Slice()
}

func (cache *MemCache) Clear() (err error) {
	if err = cache.lock(); err != nil {
		return err
	}
	defer cache.unlock()
	for i := range cache.data {
		cache.data[i] = 0
	}
	return nil
}

func Clear() (err error) {
	return varCache.Clear()
}

func (cache *MemCache) Reset(size uint64) (err error) {
	if err = cache.lock(); err != nil {
		return err
	}
	defer cache.unlock()
	return cache.resize(size, false)
}

func Reset(size uint64) (err error) {
	return varCache.Reset(size)
}

func (cache *MemCache) ResetAvailable(size uint64) (allocated uint64, err error) {
	if err = cache.lock(); err != nil {
		return 0, err
	}
	defer cache.unlock()
	err = cache.resize(size, true)
	return cache.size, err
}

func ResetAvailable(size uint64) (allocated uint64, err error) {
	return varCache.ResetAvailable(size)
}

func (cache *MemCache) resize(size uint64, try bool) (err error) {
	if size <= cache.cap {
		cache.size = size
		for i := range cache.data {
			cache.data[i] = 0
		}
	} else {
		err = cache.recap(size, try)
		cache.size = cache.cap
	}
	return err
}

func (cache *MemCache) recap(cap uint64, try bool) (err error) {
	available, err := GetAvailableMem()
	if err != nil {
		return err
	}
	if available < cap {
		if !try {
			return ErrMemoryNotEnough
		}
		cap = available
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
			cache.cap = uint64(len(cache.data))
		}
	}()
	// collect old memory first
	cache.data = nil
	runtime.GC()
	// allocate new memory
	data := make([]byte, cap, cap)
	// prevent virtual memory
	for i := range data {
		data[i] = 0
	}
	cache.data = data
	cache.cap = cap
	return
}

func (cache *MemCache) Release() (err error) {
	if err = cache.lock(); err != nil {
		return err
	}
	defer cache.unlock()

	cache.size = 0
	cache.cap = 0
	cache.data = nil
	runtime.GC() // perform garbage collection
	return
}

func Release() (err error) {
	return varCache.Release()
}

func (cache *MemCache) WriteAt(buf []byte, offset int64) (n int, err error) {
	if err = cache.lock(); err != nil {
		return
	}
	defer cache.unlock()

	if uint64(offset)+uint64(len(buf)) > cache.size {
		err = io.EOF
	}
	if uint64(offset) >= cache.size {
		return
	}
	n = copy(cache.data[offset:], buf)
	return
}

func WriteAt(buf []byte, offset int64) (n int, err error) {
	return varCache.WriteAt(buf, offset)
}

func (cache *MemCache) ReadAt(buf []byte, offset int64) (n int, err error) {
	if err = cache.lock(); err != nil {
		return
	}
	defer cache.unlock()

	if uint64(offset)+uint64(len(buf)) > cache.size {
		err = io.EOF
	}
	if uint64(offset) >= cache.size {
		return
	}
	n = copy(buf, cache.data[offset:])
	return
}

func ReadAt(buf []byte, offset int64) (n int, err error) {
	return varCache.ReadAt(buf, offset)
}

func (cache *MemCache) WriteToWriter(quit chan struct{}, w io.WriterAt, srcStart, dstStart, len int64) (n int, err error) {
	if err = cache.lock(); err != nil {
		return
	}
	defer cache.unlock()

	if uint64(srcStart+len) > cache.size || uint64(srcStart) >= cache.size {
		err = io.EOF
		return
	}

	var count int64
	var bufSize int64

	for count < len {
		if quit != nil {
			select {
			case <-quit:
				return int(count), ErrCancelWriteToWriter
			default:
			}
		}

		if remain := len - count; remain > memCacheWriteBlockSize {
			bufSize = memCacheWriteBlockSize
		} else {
			bufSize = remain
		}
		if n, err = w.WriteAt(cache.data[srcStart+count:srcStart+count+bufSize], dstStart+count); err != nil {
			return int(count) + n, err
		}
		count += int64(n)
	}

	return int(count), err
}

func WriteToWriter(quit chan struct{}, w io.WriterAt, srcStart, dstStart, len int64) (n int, err error) {
	return varCache.WriteToWriter(quit, w, srcStart, dstStart, len)
}

func GetAvailableMem() (uint64, error) {
	stat, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	var available = stat.Available - stat.SwapFree
	if stat.Available < stat.SwapFree {
		available = 0
	}
	return uint64(float64(available) * 0.90), nil
}
