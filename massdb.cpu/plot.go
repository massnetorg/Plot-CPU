package massdb_cpu

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/massnetorg/Plot-CPU/massdb.cpu/cache"
	"github.com/shirou/gopsutil/disk"
	"massnet.org/mass/logging"
	"massnet.org/mass/poc"
	"massnet.org/mass/poc/pocutil"
)

// Plotting Strategy is decided by the available memory.
//
// Here defines:
//   FREE_MEM  = max available free memory,
//   MAP_A_MEM = required memory for HashMapA,
//   MAP_B_MEM = required memory for HashMapB.
//   Notice: MAP_B_MEM = MAP_A_MEM * 2
//
// Given FREE_MEM, MAP_A_MEM and MAP_B_MEM, the plotting strategy is:
//
// (1) MAP_A_MEM + MAP_B_MEM <= FREE_MEM:
//     Calculate and store HashmapA in memory =>
//     Calculate and store HashmapB in memory =>
//     Write HashmapB to file. (Done)
//
// (2) MAP_B_MEM <= FREE_MEM < MAP_A_MEM + MAP_B_MEM:
//     Calculate and store HashmapA in memory =>
//     Write HashmapA to file ($MASS_TMPDIR) =>
//     Calculate and store HashmapB in memory =>
//     Write HashmapB to file =>
//     Remove HashmapA file. (Done)
//
// (3) MAP_A_MEM <= FREE_MEM < MAP_B_MEM:
//     Calculate and store HashmapA in memory =>
//     Write HashmapA to file ($MASS_TMPDIR) =>
//     Calculate and flush HashmapB window to file (2 rounds) =>
//     Remove HashmapA file. (Done)
//
// (4) FREE_MEM < MAP_A_MEM:
//     Use calcWorker and rwWorker to generate HashmapA file ($MASS_TMPDIR) =>
//     Remove temp files ($MASS_TMPDIR) from HashMapA =>
//     Use calcWorker and rwWorker to generate HashmapB file =>
//     Remove HashmapA file =>
//     Remove temp files ($MASS_TMPDIR) from HashMapB. (Done)
//
// The disk usages for those strategies are:
//   |   Strategy   |     1     |     2     |     3     |       4       |
//   | $MASS_TMPDIR |     0     | MAP_A_MEM | MAP_A_MEM | MAP_B_MEM * 2 |
//   |   DATA_DIR   | MAP_B_MEM | MAP_B_MEM | MAP_B_MEM |   MAP_B_MEM   |
//

const (
	plotStrategy1 = iota
	plotStrategy2
	plotStrategy3
	plotStrategy4
)

var plotStrategies = []func(mdb *MassDBPC) (err error){
	execPlotStrategy1,
	execPlotStrategy2,
	execPlotStrategy3,
	execPlotStrategy4,
}

const (
	maxPlotGoRoutine     = 16                  // max plot go routines
	defaultMaxPlotMem    = 48 * 1024 * poc.MiB // 48 GiB, max memory used when generating HashMapB
	minPlotMem           = 256 * poc.MiB       // 256 MiB, min memory used when generating HashMapB
	minMapABufMem        = 64 * poc.MiB        // 64 MiB, min memory used for reading MapA buffer when generating HashMapB
	waitQuitPlotInterval = 1 << 20             // wait interval on quit plot signal
	EnvPlotTempDir       = "MASS_TMPDIR"       // environment variable for temp plot dir
	EnvMaxPlotMem        = "MASS_MAX_PLOT_MEM" // environment variable for max usable, in MiB
	PlotTempFileSuffix   = ".massdb.cache"  // represents massdb cache
)

var (
	plotTempDir string // path
	maxPlotMem  uint64 // bytes
)

func init() {
	loadPlotTempDir()
	loadMaxPlotMem()
}

func AllocateMemory(bl int) (allocated uint64, err error) {
	freeMem, err := cache.GetAvailableMem()
	if err != nil {
		return 0, err
	}
	if freeMem > maxPlotMem {
		freeMem = maxPlotMem
	}
	mapAMem := uint64((1 << bl) * pocutil.RecordSize(bl))
	mapBMem := mapAMem * 2
	var requiredMem uint64
	if mapAMem+mapBMem <= freeMem {
		requiredMem = mapAMem + mapBMem
	} else if mapBMem <= freeMem {
		requiredMem = mapBMem
	} else if mapAMem <= freeMem {
		requiredMem = mapAMem
	} else {
		requiredMem = mapAMem
	}
	if requiredMem < minPlotMem {
		requiredMem = minPlotMem
	}
	return cache.ResetAvailable(requiredMem)
}

func ReleaseMemory() error {
	return cache.Release()
}

func (mdb *MassDBPC) executePlot(result chan error) {
	var err error
	defer func() {
		if result != nil {
			result <- err
			close(result)
		}
		atomic.StoreInt32(&mdb.plotting, 0)
		mdb.wg.Done()
	}()

	if _, plotted, _ := mdb.Progress(); plotted {
		logging.CPrint(logging.INFO, "massdb already plotted", logging.LogFormat{
			"bit_length": mdb.bl,
			"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		})
		return
	}

	// decide plot strategy
	plotStrategy, err := decidePlotStrategy(mdb.bl, filepath.Dir(mdb.filePathB))
	if err != nil {
		logging.CPrint(logging.ERROR, "fail to decide plot strategy", logging.LogFormat{
			"bl":      mdb.bl,
			"err":     err,
			"pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"tmp_dir": plotTempDir,
			"db_dir":  filepath.Dir(mdb.filePathB),
		})
		return
	}

	// exec plotting
	logging.CPrint(logging.INFO, "start plotting", logging.LogFormat{
		"bit_length": mdb.bl,
		"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"strategy":   plotStrategy,
	})
	start := time.Now()
	if err = plotStrategies[plotStrategy](mdb); err != nil {
		logging.CPrint(logging.ERROR, "fail on plotting", logging.LogFormat{
			"err":        err,
			"bit_length": mdb.bl,
			"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"strategy":   plotStrategy,
		})
		return
	}
	elapsed := time.Since(start)
	if _, plotted, _ := mdb.Progress(); plotted {
		logging.CPrint(logging.INFO, "plot finished", logging.LogFormat{
			"bit_length": mdb.bl,
			"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"elapsed":    elapsed.Seconds(),
		})
	} else {
		logging.CPrint(logging.WARN, "plot is not finished", logging.LogFormat{
			"bit_length": mdb.bl,
			"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"elapsed":    elapsed.Seconds(),
		})
	}
	return
}

func decidePlotStrategy(bl int, dbDir string) (strategy int, err error) {
	strategy = -1

	// get FREE_MEM, MAP_A_MEM and MAP_B_MEM
	freeMem := cache.Cap()
	mapAMem := uint64((1 << bl) * pocutil.RecordSize(bl))
	mapBMem := mapAMem * 2

	// get disk usage of $MASS_TMPDIR and DB_DIR
	tmpDiskStat, err := disk.Usage(plotTempDir)
	if err != nil {
		return
	}
	tmpDirFree := tmpDiskStat.Free
	dbDiskStat, err := disk.Usage(dbDir)
	if err != nil {
		return
	}
	dbDirFree := dbDiskStat.Free

	// decide strategy
	if mapAMem+mapBMem <= freeMem {
		if dbDirFree < mapBMem {
			return -1, ErrDbDiskSizeNotEnough
		}
		strategy = plotStrategy1
	} else if mapBMem <= freeMem {
		if tmpDirFree < mapAMem {
			return -1, ErrTmpDiskSizeNotEnough
		}
		if dbDirFree < mapBMem {
			return -1, ErrDbDiskSizeNotEnough
		}
		strategy = plotStrategy2
	} else if mapAMem <= freeMem {
		if tmpDirFree < mapAMem {
			return -1, ErrTmpDiskSizeNotEnough
		}
		if dbDirFree < mapBMem {
			return -1, ErrDbDiskSizeNotEnough
		}
		strategy = plotStrategy3
	} else {
		if tmpDirFree < mapBMem*2 {
			return -1, ErrTmpDiskSizeNotEnough
		}
		if dbDirFree < mapBMem {
			return -1, ErrDbDiskSizeNotEnough
		}
		strategy = plotStrategy4
	}
	return
}

func execPlotStrategy1(mdb *MassDBPC) (err error) {
	var hmB = mdb.HashMapB
	var bl = mdb.bl
	var volume = hmB.volume
	var half = volume / 2
	var recordSize = pocutil.PoCValue(hmB.recordSize)
	var pkHash = mdb.pubKeyHash
	var b8 [8]byte
	var wg sync.WaitGroup
	var workerNum = getEvenNumCPU()

	// allocate memory
	mapAMemSize := uint64((1 << bl) * pocutil.RecordSize(bl))
	mapBMemSize := mapAMemSize * 2
	if err = makeRequiredMemory(mapAMemSize + mapBMemSize); err != nil {
		logging.CPrint(logging.ERROR, "fail on makeRequiredMemory", logging.LogFormat{
			"bit_length":      bl,
			"pub_key":         hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"required_memory": mapAMemSize + mapBMemSize,
		})
		return err
	}
	memA, memB := cache.Slice()[:mapAMemSize], cache.Slice()[mapAMemSize:]

	// execute pre plot
	logging.CPrint(logging.INFO, "execute pre plot", logging.LogFormat{
		"bit_length": bl,
		"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"worker_num": workerNum,
	})
	var prePlotWorker = func(worker, total int) {
		defer wg.Done()
		var logCheckpointInterval = volume / (50 * pocutil.PoCValue(total))
		var logCheckpoint int
		startPoint := volume / pocutil.PoCValue(total) * pocutil.PoCValue(worker)
		endPoint := volume/pocutil.PoCValue(total) + startPoint
		for x := startPoint; x < endPoint; x++ {
			// calc and write the cache
			y := pocutil.P(x, bl, pkHash)
			if y < half {
				y = y * 2
			} else {
				y = pocutil.FlipValue(y, bl)*2 + 1
			}

			// copy to memA
			target := y * recordSize
			binary.LittleEndian.PutUint64(b8[:], uint64(x))
			copy(memA[target:], b8[:recordSize])

			// log and respond to quit signal
			if x%waitQuitPlotInterval == 0 {
				select {
				case <-mdb.stopPlotCh:
					logging.CPrint(logging.INFO, "pre plot aborted",
						logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": worker})
					return
				default:
				}
			}
			if worker == 0 && x%logCheckpointInterval == 0 {
				logging.CPrint(logging.DEBUG, fmt.Sprintf("pre plot current round %d/%d (%d), total progress %f",
					logCheckpoint, 50, x*pocutil.PoCValue(total), float64(logCheckpoint)/50))
				logCheckpoint++
			}
		}
	}
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go prePlotWorker(i, workerNum)
	}
	wg.Wait()

	// receive quit signal
	select {
	case <-mdb.stopPlotCh:
		return
	default:
	}

	// execute plot
	logging.CPrint(logging.INFO, "execute plot", logging.LogFormat{
		"bit_length": bl,
		"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"worker_num": workerNum,
	})
	var bytesEqualZero = func(bs []byte) bool {
		for i := pocutil.PoCValue(0); i < recordSize; i++ {
			if bs[i] != 0 {
				return false
			}
		}
		return true
	}
	var plotWorker = func(worker, total int) {
		defer wg.Done()
		var logCheckpointInterval = volume / (50 * pocutil.PoCValue(total))
		var logCheckpoint int
		startPoint := volume / pocutil.PoCValue(total) * pocutil.PoCValue(worker)
		endPoint := volume/pocutil.PoCValue(total) + startPoint
		cursor := startPoint * recordSize * 2
		for y := startPoint; y < endPoint; y++ {
			// calc and write the cache
			x, xp := memA[cursor:cursor+recordSize], memA[cursor+recordSize:cursor+recordSize*2]
			cursor += recordSize * 2
			if !bytesEqualZero(x) && !bytesEqualZero(xp) {
				// copy to memB
				z := pocutil.FB(x, xp, bl, pkHash)
				target := z * recordSize * 2
				copy(memB[target:], x)
				copy(memB[target+recordSize:], xp)
				zp := pocutil.FB(xp, x, bl, pkHash)
				target = zp * recordSize * 2
				copy(memB[target:], xp)
				copy(memB[target+recordSize:], x)
			}

			// log and respond to quit signal
			if y%waitQuitPlotInterval == 0 {
				select {
				case <-mdb.stopPlotCh:
					logging.CPrint(logging.INFO, "pre plot aborted",
						logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": worker})
					return
				default:
				}
			}
			if worker == 0 && y%logCheckpointInterval == 0 {
				logging.CPrint(logging.DEBUG, fmt.Sprintf("plot current round %d/%d (%d), total progress %f",
					logCheckpoint, 50, y*pocutil.PoCValue(total), float64(logCheckpoint)/50))
				logCheckpoint++
			}
		}
	}
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go plotWorker(i, workerNum)
	}
	wg.Wait()

	// write memB to file
	logging.CPrint(logging.DEBUG, "writing hashmapB to file", logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed())})
	_, err = cache.WriteToWriter(mdb.stopPlotCh, hmB.data, int64(mapAMemSize), PosProofData, int64(mapBMemSize))
	if err != nil {
		if err == cache.ErrCancelWriteToWriter {
			return nil
		}
		logging.CPrint(logging.ERROR, "fail on WriteToWriter", logging.LogFormat{
			"bit_length": bl,
			"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"err":        err,
		})
		return err
	}

	hmB.checkpoint = half
	hmB.UpdateCheckpoint()
	return hmB.data.Sync()
}

func execPlotStrategy2(mdb *MassDBPC) (err error) {
	var hmB = mdb.HashMapB
	var bl = mdb.bl
	var volume = hmB.volume
	var half = volume / 2
	var recordSize = pocutil.PoCValue(hmB.recordSize)
	var pkHash = mdb.pubKeyHash
	var b8 [8]byte
	var wg sync.WaitGroup
	var workerNum = getEvenNumCPU()

	// allocate memory
	mapAMemSize := uint64((1 << bl) * pocutil.RecordSize(bl))
	mapBMemSize := mapAMemSize * 2
	if err = makeRequiredMemory(mapBMemSize); err != nil {
		logging.CPrint(logging.ERROR, "fail on makeRequiredMemory before pre plot", logging.LogFormat{
			"bit_length":      bl,
			"pub_key":         hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"required_memory": mapBMemSize,
		})
		return err
	}
	memA := cache.Slice()[:mapAMemSize]

	// execute pre plot
	logging.CPrint(logging.INFO, "execute pre plot", logging.LogFormat{
		"bit_length": bl,
		"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"worker_num": workerNum,
	})
	var prePlotWorker = func(worker, total int) {
		defer wg.Done()
		var logCheckpointInterval = volume / (50 * pocutil.PoCValue(total))
		var logCheckpoint int
		startPoint := volume / pocutil.PoCValue(total) * pocutil.PoCValue(worker)
		endPoint := volume/pocutil.PoCValue(total) + startPoint
		for x := startPoint; x < endPoint; x++ {
			// calc and write the cache
			y := pocutil.P(x, bl, pkHash)
			if y < half {
				y = y * 2
			} else {
				y = pocutil.FlipValue(y, bl)*2 + 1
			}

			// copy to memA
			target := y * recordSize
			binary.LittleEndian.PutUint64(b8[:], uint64(x))
			copy(memA[target:], b8[:recordSize])

			// log and respond to quit signal
			if x%waitQuitPlotInterval == 0 {
				select {
				case <-mdb.stopPlotCh:
					logging.CPrint(logging.INFO, "pre plot aborted",
						logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": worker})
					return
				default:
				}
			}
			if worker == 0 && x%logCheckpointInterval == 0 {
				logging.CPrint(logging.DEBUG, fmt.Sprintf("pre plot current round %d/%d (%d), total progress %f",
					logCheckpoint, 50, x*pocutil.PoCValue(total), float64(logCheckpoint)/50))
				logCheckpoint++
			}
		}
	}
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go prePlotWorker(i, workerNum)
	}
	wg.Wait()

	// receive quit signal
	select {
	case <-mdb.stopPlotCh:
		return
	default:
	}

	// write memA to file
	hmA, pathA, err := NewHashMapA(plotTempDir, bl, mdb.pubKey)
	if err != nil {
		return err
	}
	defer func() {
		hmA.Close()
		os.Remove(pathA)
	}()
	logging.CPrint(logging.DEBUG, "writing hashmapA to file", logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed())})
	_, err = cache.WriteToWriter(mdb.stopPlotCh, hmA.data, 0, PosProofData, int64(mapAMemSize))
	if err != nil {
		if err == cache.ErrCancelWriteToWriter {
			return nil
		}
		logging.CPrint(logging.ERROR, "fail on WriteToWriter", logging.LogFormat{
			"bit_length": bl,
			"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"err":        err,
		})
		return err
	}
	hmA.checkpoint = hmA.volume
	hmA.UpdateCheckpoint()
	if err = hmA.data.Sync(); err != nil {
		return err
	}

	// receive quit signal
	select {
	case <-mdb.stopPlotCh:
		return
	default:
	}

	// allocate memory
	if err = makeRequiredMemory(mapBMemSize); err != nil {
		logging.CPrint(logging.ERROR, "fail on makeRequiredMemory before plot", logging.LogFormat{
			"bit_length":      bl,
			"pub_key":         hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"required_memory": mapBMemSize,
		})
		return err
	}
	memB := cache.Slice()

	// execute plot
	logging.CPrint(logging.INFO, "execute plot", logging.LogFormat{
		"bit_length": bl,
		"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"worker_num": workerNum,
	})
	var bytesEqualZero = func(bs []byte) bool {
		for i := pocutil.PoCValue(0); i < recordSize; i++ {
			if bs[i] != 0 {
				return false
			}
		}
		return true
	}
	var plotWorker = func(worker, total int) {
		defer wg.Done()
		var printErr = func(err error) {
			logging.CPrint(logging.ERROR, "unexpected error from plot worker", logging.LogFormat{
				"bit_length": bl,
				"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
				"worker":     worker,
				"err":        err,
			})
		}
		// worker window
		var logCheckpointInterval = volume / (50 * pocutil.PoCValue(total))
		startPoint := volume / pocutil.PoCValue(total) * pocutil.PoCValue(worker)
		endPoint := volume/pocutil.PoCValue(total) + startPoint
		// prepare mapA file reader
		fileA, err := os.Open(pathA)
		if err != nil {
			printErr(err)
			return
		}
		defer fileA.Close()
		if _, err = fileA.Seek(int64(startPoint*recordSize+PosProofData), 0); err != nil {
			printErr(err)
			return
		}
		rdA := bufio.NewReaderSize(fileA, minMapABufMem/total)
		bs := make([]byte, recordSize*2)
		for y := startPoint; y < endPoint; y += 2 {
			// calc and write the cache
			if _, err = rdA.Read(bs); err != nil {
				printErr(err)
				return
			}
			x, xp := bs[:recordSize], bs[recordSize:]
			if !bytesEqualZero(x) && !bytesEqualZero(xp) {
				// copy to memB
				z := pocutil.FB(x, xp, bl, pkHash)
				target := z * recordSize * 2
				copy(memB[target:], x)
				copy(memB[target+recordSize:], xp)
				zp := pocutil.FB(xp, x, bl, pkHash)
				target = zp * recordSize * 2
				copy(memB[target:], xp)
				copy(memB[target+recordSize:], x)
			}

			// log and respond to quit signal
			if y%waitQuitPlotInterval == 0 {
				select {
				case <-mdb.stopPlotCh:
					logging.CPrint(logging.INFO, "pre plot aborted",
						logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": worker})
					return
				default:
				}
			}
			if worker == 0 && y%logCheckpointInterval == 0 {
				progress := float64(y-startPoint) / float64(endPoint-startPoint)
				logging.CPrint(logging.DEBUG, fmt.Sprintf("plot current round %d/%d (%d), total progress %f",
					int(50*progress), 50, y*pocutil.PoCValue(total), progress))
			}
		}
	}
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go plotWorker(i, workerNum)
	}
	wg.Wait()

	// write memB to file
	logging.CPrint(logging.DEBUG, "writing hashmapB to file", logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed())})
	_, err = cache.WriteToWriter(mdb.stopPlotCh, hmB.data, 0, PosProofData, int64(mapBMemSize))
	if err != nil {
		if err == cache.ErrCancelWriteToWriter {
			return nil
		}
		logging.CPrint(logging.ERROR, "fail on WriteToWriter", logging.LogFormat{
			"bit_length": bl,
			"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"err":        err,
		})
		return err
	}

	hmB.checkpoint = half
	hmB.UpdateCheckpoint()
	return hmB.data.Sync()
}

func execPlotStrategy3(mdb *MassDBPC) (err error) {
	var hmB = mdb.HashMapB
	var bl = mdb.bl
	var volume = hmB.volume
	var half = volume / 2
	var recordSize = pocutil.PoCValue(hmB.recordSize)
	var pkHash = mdb.pubKeyHash
	var b8 [8]byte
	var wg sync.WaitGroup
	var workerNum = getEvenNumCPU()

	// allocate memory
	mapAMemSize := uint64((1 << bl) * pocutil.RecordSize(bl))
	mapBMemSize := mapAMemSize * 2
	if err = makeRequiredMemory(mapAMemSize); err != nil {
		logging.CPrint(logging.ERROR, "fail on makeRequiredMemory before pre plot", logging.LogFormat{
			"bit_length":      bl,
			"pub_key":         hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"required_memory": mapAMemSize,
		})
		return err
	}
	memA := cache.Slice()

	// execute pre plot
	logging.CPrint(logging.INFO, "execute pre plot", logging.LogFormat{
		"bit_length": bl,
		"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"worker_num": workerNum,
	})
	var prePlotWorker = func(worker, total int) {
		defer wg.Done()
		var logCheckpointInterval = volume / (50 * pocutil.PoCValue(total))
		var logCheckpoint int
		startPoint := volume / pocutil.PoCValue(total) * pocutil.PoCValue(worker)
		endPoint := volume/pocutil.PoCValue(total) + startPoint
		for x := startPoint; x < endPoint; x++ {
			// calc and write the cache
			y := pocutil.P(x, bl, pkHash)
			if y < half {
				y = y * 2
			} else {
				y = pocutil.FlipValue(y, bl)*2 + 1
			}

			// copy to memA
			target := y * recordSize
			binary.LittleEndian.PutUint64(b8[:], uint64(x))
			copy(memA[target:], b8[:recordSize])

			// log and respond to quit signal
			if x%waitQuitPlotInterval == 0 {
				select {
				case <-mdb.stopPlotCh:
					logging.CPrint(logging.INFO, "pre plot aborted",
						logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": worker})
					return
				default:
				}
			}
			if worker == 0 && x%logCheckpointInterval == 0 {
				logging.CPrint(logging.DEBUG, fmt.Sprintf("pre plot current round %d/%d (%d), total progress %f",
					logCheckpoint, 50, x*pocutil.PoCValue(total), float64(logCheckpoint)/50))
				logCheckpoint++
			}
		}
	}
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go prePlotWorker(i, workerNum)
	}
	wg.Wait()

	// receive quit signal
	select {
	case <-mdb.stopPlotCh:
		return
	default:
	}

	// write memA to file
	hmA, pathA, err := NewHashMapA(plotTempDir, bl, mdb.pubKey)
	if err != nil {
		return err
	}
	defer func() {
		hmA.Close()
		os.Remove(pathA)
	}()
	logging.CPrint(logging.DEBUG, "writing hashmapA to file", logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed())})
	_, err = cache.WriteToWriter(mdb.stopPlotCh, hmA.data, 0, PosProofData, int64(mapAMemSize))
	if err != nil {
		if err == cache.ErrCancelWriteToWriter {
			return nil
		}
		logging.CPrint(logging.ERROR, "fail on WriteToWriter", logging.LogFormat{
			"bit_length": bl,
			"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"err":        err,
		})
		return err
	}
	hmA.checkpoint = hmA.volume
	hmA.UpdateCheckpoint()
	if err = hmA.data.Sync(); err != nil {
		return err
	}

	// allocate memory
	memB := cache.Slice()
	var round = (mapBMemSize + cache.Size() - 1) / cache.Size()

	// execute plot
	logging.CPrint(logging.INFO, "execute plot", logging.LogFormat{
		"bit_length": bl,
		"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"worker_num": workerNum,
		"round":      round,
	})
	var bytesEqualZero = func(bs []byte) bool {
		for i := pocutil.PoCValue(0); i < recordSize; i++ {
			if bs[i] != 0 {
				return false
			}
		}
		return true
	}
	var plotWorker = func(worker, total int, windowLeft, windowRight pocutil.PoCValue) {
		defer wg.Done()
		var printErr = func(err error) {
			logging.CPrint(logging.ERROR, "unexpected error from plot worker", logging.LogFormat{
				"bit_length": bl,
				"pub_key":    hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
				"worker":     worker,
				"err":        err,
			})
		}
		// worker window
		var logCheckpointInterval = volume / (50 * pocutil.PoCValue(total))
		startPoint := volume / pocutil.PoCValue(total) * pocutil.PoCValue(worker)
		endPoint := volume/pocutil.PoCValue(total) + startPoint
		// prepare mapA file reader
		fileA, err := os.Open(pathA)
		if err != nil {
			printErr(err)
			return
		}
		defer fileA.Close()
		if _, err = fileA.Seek(int64(startPoint*recordSize+PosProofData), 0); err != nil {
			printErr(err)
			return
		}
		rdA := bufio.NewReaderSize(fileA, minMapABufMem/total)
		bs := make([]byte, recordSize*2)
		for y := startPoint; y < endPoint; y += 2 {
			// calc and write the cache
			if _, err = rdA.Read(bs); err != nil {
				printErr(err)
				return
			}
			x, xp := bs[:recordSize], bs[recordSize:]
			if !bytesEqualZero(x) && !bytesEqualZero(xp) {
				// copy to memB
				z := pocutil.FB(x, xp, bl, pkHash)
				if windowLeft <= z && z < windowRight {
					target := (z - windowLeft) * recordSize * 2
					copy(memB[target:], x)
					copy(memB[target+recordSize:], xp)
				}
				zp := pocutil.FB(xp, x, bl, pkHash)
				if windowLeft <= zp && zp < windowRight {
					target := (zp - windowLeft) * recordSize * 2
					copy(memB[target:], xp)
					copy(memB[target+recordSize:], x)
				}
			}

			// log and respond to quit signal
			if y%waitQuitPlotInterval == 0 {
				select {
				case <-mdb.stopPlotCh:
					logging.CPrint(logging.INFO, "pre plot aborted",
						logging.LogFormat{"bit_length": bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": worker})
					return
				default:
				}
			}
			if worker == 0 && y%logCheckpointInterval == 0 {
				progress := float64(y-startPoint) / float64(endPoint-startPoint)
				logging.CPrint(logging.DEBUG, fmt.Sprintf("plot current round %d/%d (%d), total progress %f",
					int(50*progress), 50, y*pocutil.PoCValue(total), progress))
			}
		}
	}
	for r := uint64(0); r < round; r++ {
		// calc memB with window
		windowLeft := pocutil.PoCValue(r) * volume / pocutil.PoCValue(round)
		windowRight := pocutil.PoCValue(r+1) * volume / pocutil.PoCValue(round)
		if windowRight > volume {
			windowRight = volume
		}
		if err = cache.Clear(); err != nil {
			return err
		}
		for i := 0; i < workerNum; i++ {
			wg.Add(1)
			go plotWorker(i, workerNum, windowLeft, windowRight)
		}
		wg.Wait()

		// receive quit signal
		select {
		case <-mdb.stopPlotCh:
			return
		default:
		}

		// write memB to file
		logging.CPrint(logging.DEBUG, "writing hashmapB to file", logging.LogFormat{
			"bit_length":  bl,
			"pub_key":     hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
			"round":       r,
			"total_round": round,
		})
		_, err = cache.WriteToWriter(mdb.stopPlotCh, hmB.data, 0, PosProofData+int64(windowLeft*recordSize*2), int64(cache.Size()))
		if err != nil {
			if err == cache.ErrCancelWriteToWriter {
				return nil
			}
			logging.CPrint(logging.ERROR, "fail on WriteToWriter", logging.LogFormat{
				"bit_length":  bl,
				"pub_key":     hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
				"round":       r,
				"total_round": round,
				"err":         err,
			})
			return err
		}
		// flush disk
		if err = hmB.data.Sync(); err != nil {
			return err
		}
	}

	hmB.checkpoint = half
	hmB.UpdateCheckpoint()
	return hmB.data.Sync()
}

func execPlotStrategy4(mdb *MassDBPC) (err error) {
	var hmB = mdb.HashMapB
	var bl = mdb.bl
	var volume = hmB.volume
	var half = volume / 2
	var recordSize = pocutil.PoCValue(hmB.recordSize)
	var wg sync.WaitGroup
	var workerNum = getEvenNumCPU()

	// allocate memory
	mapAMemSize := uint64((1 << bl) * pocutil.RecordSize(bl))
	var roundA = (mapAMemSize + cache.Cap() - 1) / cache.Cap()

	// create hashMapA in $MASS_TMPDIR
	hmA, pathA, err := NewHashMapA(plotTempDir, bl, mdb.pubKey)
	if err != nil {
		return err
	}
	defer func() {
		hmA.Close()
		os.Remove(pathA)
	}()

	// execute pre plot
	logging.CPrint(logging.INFO, "execute pre plot", logging.LogFormat{
		"bit_length":  bl,
		"pub_key":     hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"worker_num":  workerNum,
		"total_round": roundA,
	})
	// create cache A
	cacheFilePathA := make([]string, workerNum)
	cacheFileA := make([]*os.File, workerNum)
	for i := range cacheFilePathA {
		fp := filepath.Join(plotTempDir, uuid.New().String()+PlotTempFileSuffix)
		f, err := os.Create(fp)
		if err != nil {
			// clean previously created caches
			for j := 0; j < i; j++ {
				cacheFileA[j].Close()
				os.Remove(cacheFilePathA[j])
			}
			return err
		}
		cacheFilePathA[i] = fp
		cacheFileA[i] = f
	}
	defer func() {
		for i := range cacheFileA {
			cacheFileA[i].Close()
			os.Remove(cacheFilePathA[i])
		}
	}()
	// calc cacheA, use prePlotCalcWorker
	logging.CPrint(logging.DEBUG, "assign hashMapA calculation work", logging.LogFormat{"worker_count": workerNum})
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		w := bufio.NewWriterSize(cacheFileA[i], minMapABufMem/workerNum)
		go prePlotCalcWorker(&wg, mdb, hmA, w, pocutil.PoCValue(workerNum), pocutil.PoCValue(i))
	}
	wg.Wait()
	// receive quit signal
	select {
	case <-mdb.stopPlotCh:
		return
	default:
	}
	// calc cacheA, use prePlotRWWorker
	var ensureCacheMemoryA = func(startPoint pocutil.PoCValue) error {
		return makeAvailableMemory(uint64(volume-startPoint)*uint64(recordSize), mapAMemSize, minPlotMem)
	}
	var calcWindowSizeA = func() pocutil.PoCValue {
		cacheSize := pocutil.PoCValue(cache.Size())
		rem := (cacheSize / recordSize) & 1
		return cacheSize/recordSize - rem
	}
	for windowLeft := pocutil.PoCValue(0); windowLeft < volume; {
		if err := ensureCacheMemoryA(windowLeft); err != nil {
			return err
		}
		windowRight := windowLeft + calcWindowSizeA() // slide windows defined by [start, end)
		logging.CPrint(logging.DEBUG, fmt.Sprintf("assign pre plot rw worker (%f ~ %f)", float64(windowLeft)/float64(hmA.volume), float64(windowRight)/float64(hmA.volume)),
			logging.LogFormat{"window_left": windowLeft, "window_right": windowRight})
		for i, f := range cacheFileA {
			ff := f
			if _, err := ff.Seek(0, 0); err != nil {
				logging.CPrint(logging.ERROR, "fail to seek mapA cache file", logging.LogFormat{"err": err, "worker": i})
				return err
			}
			r := bufio.NewReaderSize(ff, minMapABufMem/workerNum)
			wg.Add(1)
			go prePlotRWWorker(&wg, mdb, hmA, r, cache.Slice(), windowLeft, windowRight, pocutil.PoCValue(workerNum), pocutil.PoCValue(i))
		}
		wg.Wait()
		// receive quit signal
		select {
		case <-mdb.stopPlotCh:
			return
		default:
		}
		// write pre-plot data
		if n, err := cache.WriteToWriter(mdb.stopPlotCh, hmA.data, 0, int64(hmA.offset)+int64(windowLeft)*int64(recordSize), int64(cache.Size())); err != nil {
			if err == cache.ErrCancelWriteToWriter {
				return nil
			}
			logging.CPrint(logging.ERROR, "fail on writing cache to file", logging.LogFormat{"err": err, "n": n})
			return err
		}
		if err = hmA.data.Sync(); err != nil {
			logging.CPrint(logging.ERROR, "fail on syncing mapA file", logging.LogFormat{"err": err})
			return err
		}
		windowLeft = windowRight
	}
	hmA.checkpoint = hmA.volume
	hmA.UpdateCheckpoint()
	if err = hmA.data.Sync(); err != nil {
		return err
	}

	// remove cache A to save disks
	for i := range cacheFileA {
		cacheFileA[i].Close()
		os.Remove(cacheFilePathA[i])
	}

	// allocate memory
	mapBMemSize := mapAMemSize * 2
	var roundB = (mapBMemSize + cache.Cap() - 1) / cache.Cap()

	// execute plot
	logging.CPrint(logging.INFO, "execute plot", logging.LogFormat{
		"bit_length":  bl,
		"pub_key":     hex.EncodeToString(mdb.pubKey.SerializeCompressed()),
		"worker_num":  workerNum,
		"total_round": roundB,
	})
	// create cache B
	cacheFilePathB := make([]string, workerNum)
	cacheFileB := make([]*os.File, workerNum)
	for i := range cacheFilePathB {
		fp := filepath.Join(plotTempDir, uuid.New().String()+PlotTempFileSuffix)
		f, err := os.Create(fp)
		if err != nil {
			// clean previously created caches
			for j := 0; j < i; j++ {
				cacheFileB[j].Close()
				os.Remove(cacheFilePathB[j])
			}
			return err
		}
		cacheFilePathB[i] = fp
		cacheFileB[i] = f
	}
	defer func() {
		for i := range cacheFileB {
			cacheFileB[i].Close()
			os.Remove(cacheFilePathB[i])
		}
	}()
	// calc cacheB, use plotCalcWorker
	logging.CPrint(logging.DEBUG, "assign hashMapB calculation work", logging.LogFormat{"worker_count": workerNum})
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		w := bufio.NewWriterSize(cacheFileB[i], minMapABufMem/workerNum)
		go plotCalcWorker(&wg, mdb, hmA, pathA, w, pocutil.PoCValue(workerNum), pocutil.PoCValue(i))
	}
	wg.Wait()
	// receive quit signal
	select {
	case <-mdb.stopPlotCh:
		return
	default:
	}
	// calc cacheB, use plotRWWorker
	var ensureCacheMemoryB = func(startPoint pocutil.PoCValue) error {
		return makeAvailableMemory(uint64(half-startPoint)*uint64(recordSize)<<2, mapBMemSize, minPlotMem)
	}
	var calcWindowSizeB = func() pocutil.PoCValue {
		return pocutil.PoCValue((cache.Size() / uint64(recordSize)) >> 2)
	}
	for windowLeft := pocutil.PoCValue(0); windowLeft < half; {
		if err := ensureCacheMemoryB(windowLeft); err != nil {
			return err
		}
		windowRight := windowLeft + calcWindowSizeB() // slide windows defined by [start, end)
		doubleWindowLeft, doubleWindowRight := windowLeft<<1, windowRight<<1
		logging.CPrint(logging.DEBUG, fmt.Sprintf("assign plot rw worker (%f ~ %f)", float64(doubleWindowLeft)/float64(volume), float64(doubleWindowRight)/float64(volume)),
			logging.LogFormat{"double_window_left": doubleWindowLeft, "double_window_right": doubleWindowRight})
		for i, f := range cacheFileB {
			ff := f
			if _, err := ff.Seek(0, 0); err != nil {
				logging.CPrint(logging.ERROR, "fail to seek mapB cache file", logging.LogFormat{"err": err, "worker": i})
			}
			r := bufio.NewReaderSize(ff, minMapABufMem/workerNum)
			wg.Add(1)
			go plotRWWorker(&wg, mdb, hmA, pathA, r, cache.Slice(), windowLeft, windowRight, pocutil.PoCValue(workerNum), pocutil.PoCValue(i))
		}
		wg.Wait()
		// receive quit signal
		select {
		case <-mdb.stopPlotCh:
			return
		default:
		}
		// write plot data
		if n, err := cache.WriteToWriter(mdb.stopPlotCh, hmB.data, 0, int64(hmB.offset)+int64(windowLeft)*int64(recordSize)*4, int64(cache.Size())); err != nil {
			if err == cache.ErrCancelWriteToWriter {
				return nil
			}
			logging.CPrint(logging.ERROR, "fail on writing cache to file", logging.LogFormat{"err": err, "n": n})
			return err
		}
		if err = hmB.data.Sync(); err != nil {
			logging.CPrint(logging.ERROR, "fail on syncing mapB file", logging.LogFormat{"err": err})
			return err
		}
		windowLeft = windowRight
	}

	hmB.checkpoint = half
	hmB.UpdateCheckpoint()
	return hmB.data.Sync()
}

func prePlotCalcWorker(wg *sync.WaitGroup, mdb *MassDBPC, hmA *HashMapA, w *bufio.Writer, workerCount, ordinal pocutil.PoCValue) {
	defer wg.Done()

	var bl = hmA.bl
	var logCheckpointInterval = hmA.volume / (50 * workerCount)
	var xStart = hmA.volume / workerCount * ordinal
	var xEnd = xStart + hmA.volume/workerCount

	var b8 [8]byte
	var err error
	for x := xStart; x < xEnd; x++ {
		y := pocutil.P(x, bl, hmA.pkHash)
		binary.LittleEndian.PutUint64(b8[:], uint64(y))
		if _, err = w.Write(b8[:hmA.recordSize]); err != nil {
			logging.CPrint(logging.ERROR, "[CALC] pre plot writer error", logging.LogFormat{"worker": ordinal, "err": err})
			return
		}

		// log and respond to quit signal
		if x%waitQuitPlotInterval < workerCount {
			select {
			case <-mdb.stopPlotCh:
				logging.CPrint(logging.INFO, "[CALC] pre plot aborted",
					logging.LogFormat{"bit_length": mdb.bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": ordinal})
				return
			default:
			}
		}
		if x%logCheckpointInterval == 0 && ordinal == 0 {
			logging.CPrint(logging.DEBUG, fmt.Sprintf("[CALC] pre plot current round %d/%d (%d)", x/logCheckpointInterval, 50, x))
		}
	}
	if err = w.Flush(); err != nil {
		logging.CPrint(logging.ERROR, "[CALC] pre plot writer flush error", logging.LogFormat{"worker": ordinal, "err": err})
	}
}

func prePlotRWWorker(wg *sync.WaitGroup, mdb *MassDBPC, hmA *HashMapA, r *bufio.Reader, memA []byte, windowLeft, windowRight, workerCount, ordinal pocutil.PoCValue) {
	defer wg.Done()

	var bl, half = hmA.bl, hmA.volume / 2
	var logCheckpointInterval = hmA.volume / (50 * workerCount)
	var xStart = hmA.volume / workerCount * ordinal
	var xEnd = xStart + hmA.volume/workerCount

	bs := make([]byte, hmA.recordSize)
	var err error
	var n int
	var b8 [8]byte
	for x := xStart; x < xEnd; x++ {
		if n, err = r.Read(bs); err != nil && n != hmA.recordSize {
			logging.CPrint(logging.ERROR, "[RW] pre plot reader error", logging.LogFormat{"worker": ordinal, "err": err})
			return
		}
		// write the cache
		y := pocutil.Bytes2PoCValue(bs, bl)
		if y < half {
			y = y * 2
		} else {
			y = pocutil.FlipValue(y, bl)*2 + 1
		}

		// write data within the window defined by [start, end)
		if windowLeft <= y && y < windowRight {
			target := int(y-windowLeft) * hmA.recordSize
			binary.LittleEndian.PutUint64(b8[:], uint64(x))
			copy(memA[target:], b8[:hmA.recordSize])
		}

		// log and respond to quit signal
		if x%waitQuitPlotInterval == 0 {
			select {
			case <-mdb.stopPlotCh:
				logging.CPrint(logging.INFO, "[RW] pre plot aborted",
					logging.LogFormat{"bit_length": mdb.bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": ordinal})
				return
			default:
			}
		}
		if x%logCheckpointInterval == 0 && ordinal == 0 {
			totalProgress := float64(windowLeft)/float64(hmA.volume) + float64(windowRight-windowLeft)/float64(hmA.volume)*(float64(x/logCheckpointInterval)/50)
			logging.CPrint(logging.DEBUG, fmt.Sprintf("[RW] pre plot current round %d/%d, total progress %f", x/logCheckpointInterval, 50, totalProgress))
		}
	}
}

func plotCalcWorker(wg *sync.WaitGroup, mdb *MassDBPC, hmA *HashMapA, pathA string, w *bufio.Writer, workerCount, ordinal pocutil.PoCValue) {
	defer wg.Done()

	var hmB = mdb.HashMapB
	var half, bl, pkHash = hmB.volume / 2, hmB.bl, hmA.pkHash
	var recordSize = pocutil.RecordSize(bl)
	var logCheckpointInterval = hmB.volume / (50 * 2 * workerCount)
	var yStart = half / workerCount * ordinal
	var yEnd = yStart + half/workerCount

	fA, err := os.Open(pathA)
	if err != nil {
		logging.CPrint(logging.ERROR, "[CALC] fail to open duplicate mapA reader", logging.LogFormat{"err": err, "filepath": pathA})
		return
	}
	defer fA.Close()
	if _, err := fA.Seek(int64(hmA.offset+int(yStart)*recordSize*2), 0); err != nil {
		logging.CPrint(logging.ERROR, "[CALC] fail to seek duplicate mapA", logging.LogFormat{"err": err, "filepath": pathA})
		return
	}
	bufRdA := bufio.NewReaderSize(fA, minMapABufMem/int(workerCount))

	var bytesEqualZero = func(bs []byte) bool {
		for i := 0; i < recordSize; i++ {
			if bs[i] != 0 {
				return false
			}
		}
		return true
	}

	var bs = make([]byte, recordSize*2)
	var b8 [8]byte
	for y := yStart; y < yEnd; y++ {
		if _, err := bufRdA.Read(bs); err != nil {
			logging.CPrint(logging.ERROR, "[CALC] fail on mapA buf reader", logging.LogFormat{"err": err})
			return
		}
		x, xp := bs[:recordSize], bs[recordSize:]
		var z, zp pocutil.PoCValue
		if !bytesEqualZero(x) && !bytesEqualZero(xp) {
			z = pocutil.FB(x, xp, bl, pkHash)
			zp = pocutil.FB(xp, x, bl, pkHash)
		}
		binary.LittleEndian.PutUint64(b8[:], uint64(z))
		if _, err = w.Write(b8[:recordSize]); err != nil {
			logging.CPrint(logging.ERROR, "[CALC] plot writer error", logging.LogFormat{"worker": ordinal, "err": err})
			return
		}
		binary.LittleEndian.PutUint64(b8[:], uint64(zp))
		if _, err = w.Write(b8[:recordSize]); err != nil {
			logging.CPrint(logging.ERROR, "[CALC] plot writer error", logging.LogFormat{"worker": ordinal, "err": err})
			return
		}

		if y%waitQuitPlotInterval == 0 {
			select {
			case <-mdb.stopPlotCh:
				logging.CPrint(logging.INFO, "[CALC] plot aborted",
					logging.LogFormat{"bit_length": mdb.bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed()), "worker": ordinal})
				return
			default:
			}
		}
		if y%logCheckpointInterval == 0 && ordinal == 0 {
			logging.CPrint(logging.DEBUG, fmt.Sprintf("[CALC] plot current round %d/%d (%d)", y/logCheckpointInterval, 50, y))
		}
	}
	if err = w.Flush(); err != nil {
		logging.CPrint(logging.ERROR, "[CALC] plot writer flush error", logging.LogFormat{"worker": ordinal, "err": err})
	}
}

func plotRWWorker(wg *sync.WaitGroup, mdb *MassDBPC, hmA *HashMapA, pathA string, r *bufio.Reader, memB []byte, windowLeft, windowRight, workerCount, ordinal pocutil.PoCValue) {
	defer wg.Done()

	var hmB = mdb.HashMapB
	var half, bl = hmB.volume / 2, hmB.bl
	var recordSize = pocutil.RecordSize(bl)
	var logCheckpointInterval = hmB.volume / (50 * 2 * workerCount)
	var yStart = half / workerCount * ordinal
	var yEnd = yStart + half/workerCount
	var doubleWindowLeft, doubleWindowRight = windowLeft << 1, windowRight << 1

	fA, err := os.Open(pathA)
	if err != nil {
		logging.CPrint(logging.ERROR, "[RW] fail to open duplicate mapA reader", logging.LogFormat{"err": err, "filepath": pathA})
		return
	}
	defer fA.Close()
	if _, err := fA.Seek(int64(hmA.offset+int(yStart)*recordSize*2), 0); err != nil {
		logging.CPrint(logging.ERROR, "[RW] fail to seek duplicate mapA", logging.LogFormat{"err": err, "filepath": pathA})
		return
	}
	bufRdA := bufio.NewReaderSize(fA, minMapABufMem/int(workerCount))

	var bytesEqualZero = func(bs []byte) bool {
		for i := 0; i < recordSize; i++ {
			if bs[i] != 0 {
				return false
			}
		}
		return true
	}

	var bsX = make([]byte, recordSize*2)
	var bsZ = make([]byte, recordSize*2)
	for y := yStart; y < yEnd; y++ {
		if _, err := bufRdA.Read(bsX); err != nil {
			logging.CPrint(logging.ERROR, "[RW] fail on mapA buf reader", logging.LogFormat{"err": err})
			return
		}
		x, xp := bsX[:recordSize], bsX[recordSize:]
		if _, err := r.Read(bsZ); err != nil {
			logging.CPrint(logging.ERROR, "[RW] fail on mapB buf reader", logging.LogFormat{"err": err})
			return
		}
		z, zp := pocutil.Bytes2PoCValue(bsZ[:recordSize], bl), pocutil.Bytes2PoCValue(bsZ[recordSize:], bl)
		if !bytesEqualZero(x) && !bytesEqualZero(xp) {
			if doubleWindowLeft <= z && z < doubleWindowRight {
				target := int(z-doubleWindowLeft) * recordSize * 2
				copy(memB[target:], bsX)
			}
			if doubleWindowLeft <= zp && zp < doubleWindowRight {
				target := int(zp-doubleWindowLeft) * recordSize * 2
				copy(memB[target:], xp)
				copy(memB[target+recordSize:], x)
			}
		}

		if y%waitQuitPlotInterval == 0 {
			select {
			case <-mdb.stopPlotCh:
				logging.CPrint(logging.INFO, "[RW] plot aborted",
					logging.LogFormat{"bit_length": mdb.bl, "pub_key": hex.EncodeToString(mdb.pubKey.SerializeCompressed())})
				return
			default:
			}
		}
		if y%logCheckpointInterval == 0 && ordinal == 0 {
			totalProgress := float64(doubleWindowLeft)/float64(hmA.volume) + float64(doubleWindowRight-doubleWindowLeft)/float64(hmA.volume)*(float64(y/logCheckpointInterval)/50)
			logging.CPrint(logging.DEBUG, fmt.Sprintf("[RW] plot current round %d/%d, total progress %f", y/logCheckpointInterval, 50, totalProgress))
		}
	}
}

func makeRequiredMemory(requiredMem uint64) error {
	return cache.Reset(requiredMem)
}

func makeAvailableMemory(requiredMem, maxMem, minMem uint64) error {
	capacity := cache.Cap()
	if capacity < minMem {
		return ErrMemoryNotEnough
	}
	if requiredMem > maxMem {
		requiredMem = maxMem
	}
	if requiredMem > capacity {
		requiredMem = capacity
	}
	return cache.Reset(requiredMem)
}

func getEvenNumCPU() int {
	numCPU := runtime.NumCPU()
	for n := maxPlotGoRoutine; n > 0; n /= 2 {
		if n < numCPU {
			return n
		}
	}
	return 1
}

func GetPlotTempDir() string {
	if plotTempDir == "" {
		loadPlotTempDir()
	}
	return plotTempDir
}

func loadPlotTempDir() {
	plotTempDir = os.TempDir()
	dirStr := os.Getenv(EnvPlotTempDir)
	if dirStr == "" {
		return
	}
	if absPath, err := filepath.Abs(dirStr); err == nil {
		if fi, err := os.Stat(absPath); err != nil {
			if os.IsNotExist(err) && os.MkdirAll(absPath, os.ModePerm) == nil {
				plotTempDir = absPath
			}
		} else if fi.IsDir() {
			plotTempDir = absPath
		}
	}
}

func GetMaxPlotMem() uint64 {
	if maxPlotMem == 0 {
		loadMaxPlotMem()
	}
	return maxPlotMem
}

func loadMaxPlotMem() {
	maxPlotMem = defaultMaxPlotMem
	max, err := strconv.Atoi(os.Getenv(EnvMaxPlotMem))
	if err != nil {
		return
	}
	max = max * poc.MiB // max represents MiB
	if max < minPlotMem {
		max = minPlotMem
	}
	maxPlotMem = uint64(max)
}
