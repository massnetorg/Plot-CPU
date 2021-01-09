package massdb_cpu_test

import (
	"encoding/hex"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/massnetorg/Plot-CPU/massdb.cpu"
	"massnet.org/mass/logging"
	"massnet.org/mass/poc"
	"massnet.org/mass/poc/engine"
	"massnet.org/mass/poc/pocutil"
	"massnet.org/mass/pocec"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	logging.Init(os.TempDir(), "tmp-mass.log", logging.DebugLevel, 1, false)
}

func TestPlot(t *testing.T) {
	var bl = 24
	sk, err := pocec.NewPrivateKey(pocec.S256())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	pk := sk.PubKey()

	t.Log(hex.EncodeToString(pk.SerializeCompressed()))

	mdb, err := massdb_cpu.NewMassDBPCForTest("testdata", engine.UnknownOrdinal, pk, bl)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	result := mdb.Plot()
	if err = <-result; err != nil {
		t.Error(err)
	}
}

func TestStopPlot(t *testing.T) {
	var bl = 24
	sk, err := pocec.NewPrivateKey(pocec.S256())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	pk := sk.PubKey()

	t.Log(hex.EncodeToString(pk.SerializeCompressed()))

	mdb, err := massdb_cpu.NewMassDBPCForTest("testdata", engine.UnknownOrdinal, pk, bl)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	result := mdb.Plot()
	result2 := mdb.Plot()

	time.Sleep(3 * time.Second)
	result3 := mdb.StopPlot()

	e := <-result
	e2 := <-result2
	e3 := <-result3

	t.Log("e", e)
	t.Log("e2", e2)
	t.Log("e3", e3)
}

func TestValid(t *testing.T) {
	var bl = 26

	if allocated, err := massdb_cpu.AllocateMemory(bl); err != nil {
		t.Fatal(err)
	} else {
		logging.CPrint(logging.INFO, "allocated memory for plotting", logging.LogFormat{"allocated": allocated})
	}
	defer massdb_cpu.ReleaseMemory()

	pkByte, _ := hex.DecodeString("0372a265421441050884d204292775565b9e7d16dd574a47e64cefff0ec1829ad3")
	pk, _ := pocec.ParsePubKey(pkByte, pocec.S256())

	t.Log(hex.EncodeToString(pk.SerializeCompressed()))

	mdb, err := massdb_cpu.NewMassDBPCForTest("testdata", engine.UnknownOrdinal, pk, bl)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	start := time.Now()
	result := mdb.Plot()
	if err := <-result; err != nil {
		t.Error(err)
		t.FailNow()
	}
	t.Log("plot time used:", time.Since(start), time.Since(start).Seconds())

	var validCount = 0
	var testRound = 100000
	challenge := pocutil.SHA256([]byte(strconv.Itoa(rand.Int())))
	pkHash := mdb.PubKeyHash()
	for i := 0; i < testRound; i++ {
		challenge = pocutil.SHA256(challenge[:])
		if _, err := getProof(mdb, challenge, pkHash); err == nil {
			validCount++
		}
	}
	t.Logf("valid count %d/%d (%f)\n", validCount, testRound, float64(validCount)/float64(testRound))
}

func getProof(mdb *massdb_cpu.MassDBPC, challenge pocutil.Hash, pkHash pocutil.Hash) (proof *poc.Proof, err error) {
	cShort := pocutil.CutHash(challenge, mdb.BitLength())
	proof = &poc.Proof{BitLength: mdb.BitLength()}
	proof.X, proof.XPrime, err = mdb.HashMapB.Get(cShort)
	if err != nil {
		return
	}
	if err = poc.VerifyProof(proof, pkHash, challenge); err != nil {
		return nil, err
	}
	return proof, nil
}
