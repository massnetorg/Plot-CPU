package cache_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/massnetorg/Plot-CPU/massdb.cpu/cache"
	"github.com/shirou/gopsutil/mem"
	"massnet.org/mass/testutil"
)

func TestFreeMemory(t *testing.T) {
	testutil.SkipCI(t)

	var requiredMem = 4 * 1024 * cache.MiB
	var round = 10

	for i := 0; i < round; i++ {
		mockCache(requiredMem, t)
	}
}

func mockCache(requiredMem int, t *testing.T) {
	if m, err := mem.VirtualMemory(); err != nil {
		t.Fatal(err)
	} else if uint64(requiredMem) > m.Available {
		t.Skip("memory is not enough for test", requiredMem, m.Available)
	}

	printMemory(t)

	if err := cache.Reset(uint64(requiredMem)); err != nil {
		t.Fatal(err)
	}
	if err := fillCache(); err != nil {
		t.Fatal(err)
	}

	printMemory(t)

	cache.Release()

	printMemory(t)
}

func fillCache() error {
	var buf [cache.MiB]byte

	for i := uint64(0); i < cache.Size(); i += uint64(len(buf)) {
		if _, err := rand.Read(buf[:]); err != nil {
			return err
		}
		cache.WriteAt(buf[:], int64(i))
	}
	return nil
}

func printMemory(t *testing.T) {
	stat := &runtime.MemStats{}
	runtime.ReadMemStats(stat)
	if str, err := json.Marshal(stat); err != nil {
		t.Fatal(err)
	} else {
		fmt.Println(string(str))
	}

	stat2, err := mem.VirtualMemory()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("\n", stat2.String())
}

func TestOverSizeMemory(t *testing.T) {
	testutil.SkipCI(t)
	err := cache.Reset(cache.MiB << 40)
	fmt.Println("reset:", err, cache.Size(), cache.Cap())
	if err == nil {
		fmt.Println("release:", cache.Release(), cache.Size(), cache.Cap())
	}
}

func TestSwap(t *testing.T) {
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		t.Fatal(err)
	}
	swapStat, err := mem.SwapMemory()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("free: %d, available: %d, used: %d, swap_total: %d, swap_free: %d, swap_used: %d\n", vmStat.Free, vmStat.Available, vmStat.Used, swapStat.Total, swapStat.Free, swapStat.Used)
}
