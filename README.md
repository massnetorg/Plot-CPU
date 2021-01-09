# Plot-CPU

Plot-CPU is a tool utilizing multi-core CPUs to boost MassDB plotting process.

## Usage

### Build and Run

**Build**

Make sure your Golang version is newer than `go1.13.3` and `GO111MODULE` is enabled.

`go build -o bin/plot-cpu main.go` to build an executable.

**Run**

`./plot-cpu <output_dir> <ordinal> <public_key> <bit_length>` to plot a MassDB.

For example: `./plot-cpu /path/to/root/dir 0 0372a265421441050884d204292775565b9e7d16dd574a47e64cefff0ec1829ad3 24`.

### Import Package

You can also import `massdb.cpu` into your own project:

```go
import (
	"github.com/massnetorg/Plot-CPU/massdb.cpu"
	"massnet.org/mass/poc/engine/massdb"
)

// It is your responsible to allocate enough memory before plotting
if allocated, err := massdb_cpu.AllocateMemory(bl); err != nil {
    panic(err)
}
defer massdb_cpu.ReleaseMemory()

mdb, err := massdb.CreateDB(massdb_cpu.TypeMassDBCPU, outputDir, ordinal, publicKey, bitLength)
if err != nil {
    panic(err)
}

result := mdb.Plot()
if err := <-result; err != nil {
    panic(err)
}
```

### Environment Variables

- `MASS_MAX_PLOT_MEM` specify the max usable memory for plotting (in MiB).
- `MASS_TMPDIR` specify the directory storing temp files while plotting (should be a SSD with at least 64 GiB free space).

## Plotting Strategies

This implementation provides four different plotting strategies for different size of Memory and SSD.

Plotting strategies are mainly decided by the available(free) memory. 

Here defines:

```
BL          = bit lenght of a massdb,
FREE_MEM    = max available free memory,
MASS_TMPDIR = fast SSD directory to store temp files while plotting,
MAP_A_MEM   = required memory for HashMapA = (2 ^ BL) * [(BL + 7) / 8]
MAP_B_MEM   = required memory for HashMapB = MAP_A_MEM * 2
```

Given the parameters above, select the proper plotting strategy:

1. **MAP_A_MEM + MAP_B_MEM <= FREE_MEM:**
    ```
    Calculate and store HashmapA in memory =>
    Calculate and store HashmapB in memory =>
    Write HashmapB to file. (Done)
    ```

2. **MAP_B_MEM <= FREE_MEM < MAP_A_MEM + MAP_B_MEM:**
    ```
    Calculate and store HashmapA in memory =>
    Write HashmapA to file ($MASS_TMPDIR) =>
    Calculate and store HashmapB in memory =>
    Write HashmapB to file =>
    Remove HashmapA file. (Done)
    ```

3. **MAP_A_MEM <= FREE_MEM < MAP_B_MEM:**
    ```
    Calculate and store HashmapA in memory =>
    Write HashmapA to file ($MASS_TMPDIR) =>
    Calculate and flush HashmapB window to file (2 rounds) =>
    Remove HashmapA file. (Done)
    ```

4. **FREE_MEM < MAP_A_MEM:**
    ```
    Use calcWorker and rwWorker to generate HashmapA file (MASS_TMPDIR) =>
    Remove temp files (MASS_TMPDIR) from HashMapA =>
    Use calcWorker and rwWorker to generate HashmapB file =>
    Remove HashmapA file =>
    Remove temp files (MASS_TMPDIR) from HashMapB. (Done)
    ```

Disk requirements (in Bytes) for these strategies are:

```
|   Strategy   |     1     |     2     |     3     |       4       |
|  MASS_TMPDIR |     0     | MAP_A_MEM | MAP_A_MEM | MAP_B_MEM * 2 |
|  OUTPUT_DIR  | MAP_B_MEM | MAP_B_MEM | MAP_B_MEM |   MAP_B_MEM   |
```

For more details, see `massdb.cpu/plot.go`.

## License

`Plot-CPU` is licensed under the terms of the MIT license. See LICENSE for more information or see https://opensource.org/licenses/MIT.
