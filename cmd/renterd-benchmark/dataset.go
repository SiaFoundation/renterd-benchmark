package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/slab"
)

const (
	FILESIZE_64KB     = 64e3
	FILESIZE_64KB_STR = "64kb"
)

type (
	dataset []fileMeta

	fileMeta struct {
		FileSize    uint64      `json:"filesize"`
		MinShards   uint8       `json:"minshards"`
		TotalShards uint8       `json:"totalshards"`
		Slabs       []slab.Slab `json:"slabs"`
	}
)

func downloadDataset(c *api.Client, contracts []api.Contract, d dataset, threadCount uint8) ([]float64, error) {
	log.Printf("\n- thread count %v...\n", threadCount)

	// randomize the files and limit
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(d), func(i, j int) { d[i], d[j] = d[j], d[i] })

	// create a list of timings
	now := time.Now()
	timings := make([]float64, len(d))

	// create a thread pool
	threadPool := make(chan struct{}, threadCount)
	for i := uint8(0); i < threadCount; i++ {
		threadPool <- struct{}{}
	}

	var atomicDownloadsFinished uint64
	var atomicDownloadErrors uint64
	var wg sync.WaitGroup
	for i := 0; i < len(d); i++ {
		// block
		<-threadPool

		wg.Add(1)
		go func(i int, launched time.Time, meta fileMeta) {
			// Make room for the next thread.
			defer func() {
				threadPool <- struct{}{}
			}()
			defer wg.Done()

			// Keep track of the elapsed time
			start := time.Now()

			var slabs []slab.Slice
			for _, s := range meta.Slabs {
				slabs = append(slabs, slab.Slice{
					Slab:   s,
					Offset: 0,
					Length: uint32(meta.FileSize),
				})
			}

			err := c.DownloadSlabs(ioutil.Discard, slabs, 0, int64(meta.FileSize), contracts)
			if err != nil {
				atomic.AddUint64(&atomicDownloadErrors, 1)
				return
			}

			timings[i] = float64(time.Since(start).Milliseconds())

			numFinished := atomic.AddUint64(&atomicDownloadsFinished, 1)
			numTwentyPct := uint64(math.Ceil(float64(len(d)) / 5))
			if numFinished%numTwentyPct == 0 {
				log.Printf("%v%% finished after %vms\n", (numFinished/numTwentyPct)*20, time.Since(launched).Milliseconds())
			}
		}(i, now, d[i])
	}
	wg.Wait()

	if atomicDownloadErrors != 0 {
		return timings, fmt.Errorf("there were %v errors while downloading", atomicDownloadErrors)
	}
	return timings, nil
}

func ensureDataset(c *api.Client, contracts []api.Contract, renterKey api.PrivateKey, dataDir string, required int, m, n uint8) (dataset, error) {
	log.Println("- ensuring dataset...")

	// ensure the path exists
	path := filepath.Join(dataDir, FILESIZE_64KB_STR)
	_ = os.MkdirAll(path, 0777) // TODO: don't ignore the error here

	// read data dir
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, errors.AddContext(err, "failed reading data dir")
	}

	// filter hidden files
	var i int
	for _, f := range files {
		if !isHidden(f) {
			files[i] = f
			i++
		}
	}
	files = files[:i]

	// read metadatas
	var ds []fileMeta
	for _, f := range files {
		filepath := filepath.Join(path, f.Name())
		content, err := ioutil.ReadFile(filepath)
		if err != nil {
			return nil, errors.AddContext(err, fmt.Sprintf("failed reading file at %v", filepath))
		}

		var meta fileMeta
		err = json.Unmarshal(content, &meta)
		if err != nil {
			return nil, errors.AddContext(err, fmt.Sprintf("failed unmarshaling file contents for file at %v", filepath))
		}

		// skip files that were uploaded with lower min shards
		if meta.MinShards < m {
			continue
		}

		ds = append(ds, meta)
		if len(ds) == required {
			break
		}
	}

	// dataset is present
	missingFiles := required - len(ds)
	if missingFiles == 0 {
		return ds, nil
	}
	log.Printf("- %v files missing\n", missingFiles)

	consecutiveErrors := 3

	// upload missing files
	var timings []float64
	for missingFiles > 0 {
		tip, err := c.ConsensusTip()
		if err != nil {
			return nil, fmt.Errorf("failed contacting renterd API, err: %v", err)
		}

		start := time.Now()
		meta, err := uploadFile(c, contracts, path, tip.Height, FILESIZE_64KB, m, n)
		if err != nil {
			consecutiveErrors++
			if consecutiveErrors < 5 {
				continue
			}
			fmt.Println("upload err", err)
			return nil, err
		}
		timings = append(timings, float64(time.Since(start).Milliseconds()))
		ds = append(ds, meta)
		missingFiles--
		consecutiveErrors = 0
	}

	log.Println("\n- upload stats: \n", getPercentilesString(timings))
	return ds, nil
}

func uploadFile(c *api.Client, contracts []api.Contract, dataDir string, currentHeight, fileSize uint64, minShards, totalShards uint8) (data fileMeta, err error) {
	defer func() {
		if err == nil {
			fmt.Print(".")
		} else {
			fmt.Print("x")
		}
	}()

	// randomize the hosts slice to ensure we upload to random hosts
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(contracts), func(i, j int) { contracts[i], contracts[j] = contracts[j], contracts[i] })

	// create random file
	var slabs []slab.Slab
	buf := bytes.NewReader(fastrand.Bytes(int(fileSize)))
	slabs, err = c.UploadSlabs(buf, minShards, totalShards, currentHeight, contracts)
	if err != nil {
		return fileMeta{}, fmt.Errorf("failed to upload slabs: %w", err)
	}

	// build the metadata
	data = fileMeta{
		Slabs:       slabs,
		MinShards:   minShards,
		TotalShards: totalShards,
		FileSize:    fileSize,
	}

	// write the output file
	json, _ := json.MarshalIndent(data, "", "    ")
	slabFileName := filepath.Join(dataDir, randomSlabFileName())
	err = ioutil.WriteFile(slabFileName, json, 0644)
	if err != nil {
		return fileMeta{}, fmt.Errorf("failed to create slab file: %w", err)
	}

	return data, nil
}
