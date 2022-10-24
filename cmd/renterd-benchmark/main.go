package main

import (
	"errors"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"go.sia.tech/renterd/api"
)

type (
	flags struct {
		contracts        int
		dataDir          string
		files            int
		minShards        uint
		numShards        uint
		renterdAddr      string
		renterdPassword  string
		renterdRenterKey api.PrivateKey
	}
)

func main() {
	params, newKey := parseFlags()

	printHeader("renterd benchmark tool")
	if newKey {
		log.Println("- generated random key file")
	}
	log.Printf("- min shards %v", params.minShards)
	log.Printf("- num shards %v", params.numShards)
	log.Printf("- min contracts %v", params.contracts)
	log.Printf("- files %v", params.files)

	printHeader("client")
	c := api.NewClient(params.renterdAddr, params.renterdPassword)

	// ensure we have money
	bal, err := c.WalletBalance()
	checkFatal("failed fetching wallet balance", err)
	checkFatalB("no funds in wallet", bal.IsZero())
	log.Println("- wallet balance", bal.HumanString())

	// print the wallet outputs
	utxos, err := c.WalletOutputs()
	checkFatal("failed fetching wallet outputs", err)
	log.Println("- wallet outputs", len(utxos))

	// ensure we have contracts
	printHeader("contracts")
	contracts, err := ensureContracts(c, params.renterdRenterKey, params.contracts)
	checkFatal("failed ensuring contracts", err)
	log.Printf("- found %v usable contracts\n", len(contracts))

	// ensure we have test files
	printHeader("dataset")
	dataset, err := ensureDataset(c, contracts, params.renterdRenterKey, params.dataDir, params.files, uint8(params.minShards), uint8(params.numShards))
	checkFatal("failed ensuring data set", err)
	log.Printf("- found %v usable files\n", len(dataset))

	// run the benchmark
	printHeader("benchmark")
	for _, tc := range []uint8{1, 4, 16, 64} {
		timings, _ := downloadDataset(c, contracts, dataset, tc)
		log.Println(getPercentilesString(timings))
	}
}

func parseFlags() (flags, bool) {
	log.SetFlags(0)
	contracts := flag.Int("contracts", 20, "number of contracts")
	dataDir := flag.String("data-dir", "data", "data directory")
	files := flag.Int("files", 50, "number of files in dataset")
	minShards := flag.Uint("min-shards", 2, "number of min shards")
	numShards := flag.Uint("num-shards", 5, "number of shards")
	renterdAddr := flag.String("addr", "http://127.0.0.1:9980/api", "renterd address")
	renterdPassword := flag.String("password", "test", "renterd API password")
	flag.Parse()

	// parse data dir
	if !filepath.IsAbs(*dataDir) {
		curr, err := os.Getwd()
		if err != nil {
			log.Fatal("failed to get current working directory, err: ", err)
		}
		*dataDir = filepath.Join(curr, *dataDir)
	}

	// ensure it exists
	err := os.MkdirAll(*dataDir, 0777)
	if err != nil {
		log.Fatal("failed to ensure data directory exists, err: ", err)
	}

	// load renter key
	renterKey, generated, err := loadRenterKey(*dataDir)
	if err != nil {
		log.Fatal("failed to load key, err: ", err)
	}

	return flags{
		*contracts,
		*dataDir,
		*files,
		*minShards,
		*numShards,
		*renterdAddr,
		*renterdPassword,
		*renterKey,
	}, generated
}

func loadRenterKey(dataDir string) (*api.PrivateKey, bool, error) {
	path := filepath.Join(dataDir, "renter.key")

	// generate random key
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		key, err := randomRenterKey(path)
		return key, true, err
	} else if err != nil {
		return nil, false, err
	}
	defer f.Close()

	// read bytes
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return nil, false, err
	}

	// unmarshal into key
	var key api.PrivateKey
	err = key.UnmarshalText(b)
	return &key, false, err
}
