package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/massnetorg/Plot-CPU/massdb.cpu"
	"massnet.org/mass/logging"
	"massnet.org/mass/poc"
	"massnet.org/mass/poc/engine/massdb"
	"massnet.org/mass/poc/pocutil"
	"massnet.org/mass/pocec"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	logging.Init(os.TempDir(), "tmp-mass.log", logging.DebugLevel, 1, false)
}

// Arguments: <output_dir> <ordinal> <public_key> <bit_length>
func main() {
	outputDir, ordinal, pk, bl, err := parseArgs(os.Args[1:])
	if err != nil {
		logging.CPrint(logging.FATAL, "fail to parse arguments", logging.LogFormat{"err": err})
		os.Exit(1)
	}

	if allocated, err := massdb_cpu.AllocateMemory(bl); err != nil {
		logging.CPrint(logging.FATAL, "fail to allocate memory for plotting", logging.LogFormat{"err": err, "bl": bl})
		os.Exit(1)
	} else {
		logging.CPrint(logging.INFO, "allocated memory for plotting", logging.LogFormat{"allocated": allocated})
	}
	defer massdb_cpu.ReleaseMemory()

	mdb, err := massdb.CreateDB(massdb_cpu.TypeMassDBCPU, outputDir, ordinal, pk, bl)
	if err != nil {
		logging.CPrint(logging.FATAL, "fail to create massdb", logging.LogFormat{"err": err})
		os.Exit(1)
	}

	start := time.Now()
	result := mdb.Plot()
	if err := <-result; err != nil {
		log.Fatal(err)
	}
	logging.CPrint(logging.INFO, "plot time usage in seconds", logging.LogFormat{"elapsed": time.Since(start).Seconds()})

	logging.CPrint(logging.INFO, "verifying plot validity")
	var validCount = 0
	var testRound = 10000
	challenge := pocutil.SHA256([]byte(strconv.Itoa(rand.Int())))
	for i := 0; i < testRound; i++ {
		challenge = pocutil.SHA256(challenge[:])
		if _, err = mdb.GetProof(challenge); err == nil {
			validCount++
		}
	}
	logging.CPrint(logging.INFO, fmt.Sprintf("valid rate %d/%d (%f)", validCount, testRound, float64(validCount)/float64(testRound)))
}

func parseArgs(args []string) (outputDir string, ordinal int64, pk *pocec.PublicKey, bl int, err error) {
	if len(args) != 4 {
		err = errors.New("count of args should be 4")
		return
	}
	// parse output dir
	if outputDir, err = filepath.Abs(args[0]); err != nil {
		return
	}
	var fi os.FileInfo
	if fi, err = os.Stat(outputDir); err != nil {
		return
	} else if !fi.IsDir() {
		err = fmt.Errorf("not a directory: %s", outputDir)
		return
	}
	// parse ordinal
	if ordinal, err = strconv.ParseInt(args[1], 10, 64); err != nil {
		return
	}
	// parse public key
	var pkBytes []byte
	if pkBytes, err = hex.DecodeString(args[2]); err != nil {
		return
	}
	if pk, err = pocec.ParsePubKey(pkBytes, pocec.S256()); err != nil {
		return
	}
	// parse bit length
	if bl, err = strconv.Atoi(args[3]); err != nil {
		return
	}
	if !poc.EnsureBitLength(bl) {
		err = fmt.Errorf("invalid bitlength: %d", bl)
		return
	}
	return
}
