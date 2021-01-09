package massdb_cpu

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"massnet.org/mass/poc"
	"massnet.org/mass/poc/engine/massdb"
	"massnet.org/mass/poc/pocutil"
	"massnet.org/mass/pocec"
)

type MapType uint8

const (
	TypeMassDBCPU           = "massdb.cpu"
	MapTypeHashMapA MapType = iota
	MapTypeHashMapB
)

type MassDBPC struct {
	HashMapB   *HashMapB
	filePathB  string
	bl         int
	pubKey     *pocec.PublicKey
	pubKeyHash pocutil.Hash
	plotting   int32 // atomic
	stopPlotCh chan struct{}
	wg         sync.WaitGroup
}

func (mdb *MassDBPC) Type() string {
	return TypeMassDBCPU
}

func (mdb *MassDBPC) Close() error {
	<-mdb.StopPlot()
	if mdb.HashMapB != nil {
		mdb.HashMapB.Close()
	}
	return nil
}

// Plot is concurrent safe, it starts the plotting work,
// running actual plot func as a thread
func (mdb *MassDBPC) Plot() chan error {
	result := make(chan error, 1)

	if !atomic.CompareAndSwapInt32(&mdb.plotting, 0, 1) {
		result <- ErrAlreadyPlotting
		return result
	}

	mdb.stopPlotCh = make(chan struct{})
	mdb.wg.Add(1)
	go mdb.executePlot(result)

	return result
}

// StopPlot stops plot process
func (mdb *MassDBPC) StopPlot() chan error {
	result := make(chan error, 1)

	var sendResult = func(err error) {
		result <- err
		close(result)
	}

	if atomic.LoadInt32(&mdb.plotting) == 0 {
		sendResult(nil)
		return result
	}

	go func() {
		close(mdb.stopPlotCh)
		mdb.wg.Wait()
		sendResult(nil)
	}()
	return result
}

func (mdb *MassDBPC) Ready() bool {
	plotted, _ := mdb.HashMapB.Progress()
	return plotted
}

func (mdb *MassDBPC) BitLength() int {
	return mdb.bl
}

func (mdb *MassDBPC) PubKeyHash() pocutil.Hash {
	return mdb.pubKeyHash
}

func (mdb *MassDBPC) PubKey() *pocec.PublicKey {
	return mdb.pubKey
}

func (mdb *MassDBPC) Get(z pocutil.PoCValue) (x, xp pocutil.PoCValue, err error) {
	var bl = mdb.bl
	xb, xpb, err := mdb.HashMapB.Get(z)
	if err != nil {
		return 0, 0, err
	}
	return pocutil.Bytes2PoCValue(xb, bl), pocutil.Bytes2PoCValue(xpb, bl), nil
}

func (mdb *MassDBPC) GetProof(challenge pocutil.Hash) (*poc.Proof, error) {
	var bl = mdb.bl
	x, xp, err := mdb.HashMapB.Get(pocutil.CutHash(challenge, bl))
	if err != nil {
		return nil, err
	}
	proof := &poc.Proof{
		X:         x,
		XPrime:    xp,
		BitLength: bl,
	}
	err = poc.VerifyProof(proof, mdb.pubKeyHash, challenge)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

func (mdb *MassDBPC) Progress() (prePlotted, plotted bool, progress float64) {
	if plotted, progB := mdb.HashMapB.Progress(); plotted {
		return true, true, 100
	} else {
		progress = float64(progB*2*100) / float64(mdb.HashMapB.volume)
		return plotted, plotted, progress
	}
}

func (mdb *MassDBPC) Delete() chan error {
	result := make(chan error, 1)

	var sendResult = func(err error) {
		result <- err
		close(result)
	}

	if atomic.LoadInt32(&mdb.plotting) != 0 {
		sendResult(ErrAlreadyPlotting)
		return result
	}

	mdb.HashMapB.Close()

	go func() {
		err := os.Remove(mdb.filePathB)
		sendResult(err)
	}()
	return result
}

func OpenDB(args ...interface{}) (massdb.MassDB, error) {
	dbPath, ordinal, pubKey, bitLength, err := parseArgs(args...)
	if err != nil {
		return nil, err
	}

	pathB := getPath(dbPath, int(ordinal), pubKey, bitLength)
	hmBi, err := LoadHashMap(pathB)
	if err != nil {
		return nil, err
	}
	hmB, ok := hmBi.(*HashMapB)
	if !ok {
		return nil, ErrDBWrongType
	}

	return &MassDBPC{
		HashMapB:   hmB,
		filePathB:  pathB,
		bl:         bitLength,
		pubKey:     pubKey,
		pubKeyHash: pocutil.PubKeyHash(pubKey),
	}, nil
}

func CreateDB(args ...interface{}) (massdb.MassDB, error) {
	dbPath, ordinal, pubKey, bitLength, err := parseArgs(args...)
	if err != nil {
		return nil, err
	}

	pathB := getPath(dbPath, int(ordinal), pubKey, bitLength)
	if err := CreateHashMap(pathB, MapTypeHashMapB, bitLength, pubKey); err != nil && err != massdb.ErrDBAlreadyExists {
		return nil, err
	}
	hmBi, err := LoadHashMap(pathB)
	if err != nil {
		return nil, err
	}
	hmB, ok := hmBi.(*HashMapB)
	if !ok {
		return nil, ErrDBWrongType
	}

	return &MassDBPC{
		HashMapB:   hmB,
		filePathB:  pathB,
		bl:         bitLength,
		pubKey:     pubKey,
		pubKeyHash: pocutil.PubKeyHash(pubKey),
	}, nil
}

func getPath(rootPath string, ordinal int, pubKey *pocec.PublicKey, bitLength int) (pathB string) {
	pubKeyString := hex.EncodeToString(pubKey.SerializeCompressed())
	pathB = strings.Join([]string{strconv.Itoa(ordinal), pubKeyString, strconv.Itoa(bitLength)}, "_") + ".massdb"
	return filepath.Join(rootPath, pathB)
}

func parseArgs(args ...interface{}) (string, int64, *pocec.PublicKey, int, error) {
	if len(args) != 4 {
		return "", 0, nil, 0, massdb.ErrInvalidDBArgs
	}
	dbPath, ok := args[0].(string)
	if !ok {
		return "", 0, nil, 0, massdb.ErrInvalidDBArgs
	}
	ordinal, ok := args[1].(int64)
	if !ok {
		return "", 0, nil, 0, massdb.ErrInvalidDBArgs
	}
	pubKey, ok := args[2].(*pocec.PublicKey)
	if !ok {
		return "", 0, nil, 0, massdb.ErrInvalidDBArgs
	}
	bitLength, ok := args[3].(int)
	if !ok {
		return "", 0, nil, 0, massdb.ErrInvalidDBArgs
	}

	return dbPath, ordinal, pubKey, bitLength, nil
}

func init() {
	massdb.AddDBBackend(massdb.DBBackend{
		Typ:      TypeMassDBCPU,
		OpenDB:   OpenDB,
		CreateDB: CreateDB,
	})
}

func NewMassDBPCForTest(rootPath string, ordinal int64, pubKey *pocec.PublicKey, bitLength int) (*MassDBPC, error) {
	mdb, err := OpenDB(rootPath, ordinal, pubKey, bitLength)
	if err != nil {
		if err != massdb.ErrDBDoesNotExist {
			return nil, err
		}
		mdb, err = CreateDB(rootPath, ordinal, pubKey, bitLength)
		if err != nil {
			return nil, err
		}
	}

	return mdb.(*MassDBPC), nil
}
