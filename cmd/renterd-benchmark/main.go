package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/montanaflynn/stats"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
	"lukechampine.com/frand"
)

type (
	flags struct {
		id          string
		numFiles    int
		filesize    int64
		minShards   int
		totalShards int
		dlThreads   []int
		ulThreads   int
		bgUploads   bool

		workerAddresses []string
		workerPasswords []string

		busAddress  string
		busPassword string
	}
)

func parseFlags() flags {
	log.SetFlags(0)

	// benchmark settings
	id := flag.String("id", "renterd-benchmark", "benchmark identifier")
	fileSize := flag.Int("size", 40<<20, "size of files to upload")
	numFiles := flag.Int("files", 100, "number of files in the dataset")
	dlThreadsStr := flag.String("threads", "1,4,16", "comma separated list of thread counts")
	ulThreads := flag.Int("ul-threads", 1, "number of threads the uploader uses")
	bgUploads := flag.Bool("ul-background", false, "upload small files in the background while performing download benchmarks")
	minShards := flag.Int("min-shards", api.DefaultRedundancySettings.MinShards, "number of min shards")
	totalShards := flag.Int("total-shards", api.DefaultRedundancySettings.TotalShards, "number of shards")

	// node settings
	busAddr := flag.String("busAddr", "http://127.0.0.1:9980/api/bus", "bus address")
	busPassw := flag.String("busPassw", "test", "bus password")
	workerAddrs := flag.String("workerAddrs", "http://127.0.0.1:9980/api/worker", "comma separated list of worker addresses")
	workerPasswords := flag.String("workerPassws", "test", "comma separated list of worker passwords")

	flag.Parse()

	var threads []int
	for _, t := range strings.Split(*dlThreadsStr, ",") {
		if tt, err := strconv.Atoi(t); err != nil {
			log.Fatalf("invalid thread count: %v", t)
		} else {
			threads = append(threads, tt)
		}
	}

	return flags{
		id:          *id,
		filesize:    int64(*fileSize),
		numFiles:    *numFiles,
		minShards:   *minShards,
		totalShards: *totalShards,
		dlThreads:   threads,
		ulThreads:   *ulThreads,
		bgUploads:   *bgUploads,

		workerAddresses: strings.Split(*workerAddrs, ","),
		workerPasswords: strings.Split(*workerPasswords, ","),

		busAddress:  *busAddr,
		busPassword: *busPassw,
	}
}

func main() {
	flags := parseFlags()

	// validate flags
	if strings.HasSuffix(flags.id, "/") {
		log.Fatal("benchmark id cannot end with '/'")
	}
	if flags.busAddress == "" || flags.busPassword == "" {
		log.Fatal("no bus credentials provided")
	}
	if len(flags.workerAddresses) == 0 {
		log.Fatal("no workers provided")
	}
	if len(flags.workerAddresses) != len(flags.workerPasswords) {
		log.Fatal("number of worker addresses does not match number of worker passwords")
	}

	stdout := os.Stdout
	stderr := os.Stderr
	defer func() {
		os.Stdout = stdout
		os.Stderr = stderr
		log.SetOutput(os.Stderr)
	}()

	// create multiwriter to capture output
	var buf bytes.Buffer
	mw := io.MultiWriter(stdout, &buf)

	// create a pipe and replace stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	os.Stderr = w
	log.SetOutput(mw)

	// create channel to control when we can return (after copying is finished)
	exit := make(chan bool)
	go func() {
		_, _ = io.Copy(mw, r)
		fmt.Println("DONE")
		exit <- true
	}()

	printHeader("RENTERD benchmarking tool")

	// print configuration
	log.Println("- benchmark: ", flags.id)
	log.Println("- filesize: ", humanReadableByteCount(flags.filesize))
	log.Println("- num files: ", flags.numFiles)
	log.Println("- num dl threads: ", fmt.Sprint(flags.dlThreads))
	log.Println("- num ul threads: ", fmt.Sprint(flags.ulThreads))
	log.Println("- min shards: ", flags.minShards)
	log.Println("- tot shards: ", flags.totalShards)
	log.Println()
	log.Println("- workers: ", strings.Join(flags.workerAddresses, ","))
	log.Println("- bus: ", flags.busAddress)
	log.Println()

	// create bus client
	bc := bus.NewClient(flags.busAddress, flags.busPassword)

	// create worker clients
	wcs := make([]*worker.Client, len(flags.workerAddresses))
	for i, addr := range flags.workerAddresses {
		wcs[i] = worker.NewClient(addr, flags.workerPasswords[i])
	}
	wc := wcs[0]

	// create redundancy settings
	rs := api.RedundancySettings{MinShards: flags.minShards, TotalShards: flags.totalShards}
	if err := rs.Validate(); err != nil {
		log.Fatal("invalid redundancy settings: ", err)
	}

	prefix := fmt.Sprintf("/%s/file_%v_", flags.id, strings.ReplaceAll(humanReadableByteCount(flags.filesize), " ", ""))
	twentyPct := uint64(math.Ceil(float64(flags.numFiles) / 5))

	checkConfig(bc, rs, wcs)
	checkDataset(wc, flags.id, prefix, flags.ulThreads, flags.numFiles, flags.filesize, rs)

	printHeader("running benchmark")

	// run the benchmark for every thread count
	for _, threadCount := range flags.dlThreads {
		printSubHeader(fmt.Sprintf("thread count %d", threadCount))

		// create a list of timings
		timings := make([]float64, flags.numFiles)

		// create a thread pool
		threadPool := make(chan struct{}, threadCount)
		for i := 0; i < threadCount; i++ {
			threadPool <- struct{}{}
		}

		batchStart := time.Now()
		var atomicDownloadsFinished uint64
		var atomicDownloadErrors uint64
		var wg sync.WaitGroup
		for i := 0; i < flags.numFiles; i++ {
			<-threadPool

			wg.Add(1)
			go func(i int) {
				defer func() {
					threadPool <- struct{}{}
				}()
				defer wg.Done()

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				defer cancel()

				start := time.Now()
				err := wc.DownloadObject(ctx, io.Discard, strings.TrimPrefix(prefix+fmt.Sprint(i), "/"))
				if err != nil {
					log.Print(err)
					atomic.AddUint64(&atomicDownloadErrors, 1)
					return
				}
				timings[i] = float64(time.Since(start).Milliseconds())

				numFinished := atomic.AddUint64(&atomicDownloadsFinished, 1)
				if numFinished%twentyPct == 0 {
					log.Printf("%v%% finished after %vms (%v mpbs)\n", (numFinished/twentyPct)*20, time.Since(batchStart).Milliseconds(), mbps(int64(numFinished)*flags.filesize, time.Since(batchStart).Seconds()))
				}
			}(i)
		}
		wg.Wait()

		if atomicDownloadErrors != 0 {
			log.Printf("there were %v errors while downloading\n", atomicDownloadErrors)
		}
		log.Println(percentiles(timings))

		elapsed := time.Since(batchStart)
		totalsize := int64(flags.numFiles) * flags.filesize
		log.Printf("downloaded %v in %v (%v mpbs)\n\n", humanReadableByteCount(totalsize), elapsed.Round(time.Millisecond), mbps(totalsize, elapsed.Seconds()))
	}

	_ = w.Close()
	<-exit

	printHeader("generating report")

	filename := fmt.Sprintf("benchmark_%s_%s.txt", flags.id, time.Now().Format("2006-01-02_15-04-05"))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("ERR: failed to open output to file", err)
		return
	}

	_, err = file.Write(buf.Bytes())
	if err != nil {
		log.Println("ERR: failed to write output to file", err)
		return
	}
	file.Sync()
	file.Close()
	log.Println("report written to", filename)
}

func checkConfig(bc *bus.Client, rs api.RedundancySettings, wcs []*worker.Client) {
	printHeader("checking config")
	defer log.Println("OK")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// check downloads
	if dp, err := bc.DownloadParams(ctx); err != nil {
		log.Fatal("failed to get dl params from the bus", err)
	} else if dp.ContractSet == "" {
		log.Fatal("bus does not have a contract set configured")
	} else if cs, err := bc.Contracts(ctx, dp.ContractSet); err != nil {
		log.Fatal("failed to fetch contracts for download contract set", err)
	} else if len(cs) < rs.MinShards {
		log.Fatal("not enough contracts to satisfy the redundancy settings")
	}

	// check uploads
	if up, err := bc.UploadParams(ctx); err != nil {
		log.Fatal("failed to get dl params from the bus", err)
	} else if up.ContractSet == "" {
		log.Fatal("bus does not have a contract set configured")
	} else if cs, err := bc.Contracts(ctx, up.ContractSet); err != nil {
		log.Fatal("failed to fetch contracts for download contract set", err)
	} else if len(cs) < rs.TotalShards {
		log.Fatal("not enough contracts to satisfy the redundancy settings")
	}

	// check accounts
	for _, wc := range wcs {
		if workerID, err := wc.ID(ctx); err != nil {
			log.Fatal("failed to get worker id", err)
		} else if accounts, err := bc.Accounts(ctx, workerID); err != nil {
			log.Fatalf("failed to get account for worker %v, err: %v", workerID, err)
		} else if len(accounts) < rs.MinShards {
			log.Fatalf("worker %v does not have enough accounts to satisfy the redundancy settings", workerID)
		}
	}
}

func checkDataset(wc *worker.Client, path, prefix string, threads, required int, filesize int64, rs api.RedundancySettings) {
	printHeader("checking dataset")
	defer log.Println("OK")

	filter := func(entries []string) []string {
		filtered := entries[:0]
		for _, entry := range entries {
			if strings.HasPrefix(entry, prefix) {
				filtered = append(filtered, entry)
			}
		}
		return filtered
	}

	// check dataset
	if entries, err := wc.ObjectEntries(context.Background(), path+"/"); err != nil {
		log.Fatal("failed to fetch entries for path: '", path, "/', err: ", err)
	} else if len(filter(entries)) < required {
		// build a map of existing files
		exists := make(map[string]bool)
		for _, entry := range filter(entries) {
			exists[entry] = true
		}

		// build a list of missing indices
		missing := make([]int, 0)
		for i := 0; i < required; i++ {
			if !exists[prefix+fmt.Sprint(i)] {
				missing = append(missing, i)
			}
		}
		log.Printf("uploading %d missing files\n\n", len(missing))

		// create a thread pool
		threadPool := make(chan struct{}, threads)
		for i := 0; i < threads; i++ {
			threadPool <- struct{}{}
		}

		// upload missing files
		var wg sync.WaitGroup
		for _, i := range missing {
			<-threadPool

			wg.Add(1)
			go func(index int) {
				defer func() {
					threadPool <- struct{}{}
				}()
				defer wg.Done()

				start := time.Now()
				path := fmt.Sprintf("%s?minshards=%v&totalshards=%v", strings.TrimPrefix(prefix+fmt.Sprint(index), "/"), rs.MinShards, rs.TotalShards)
				if err := wc.UploadObject(context.Background(), randomFile(filesize), path); err != nil {
					log.Fatal("failed to upload object, err: ", err)
				}

				elapsed := time.Since(start)
				totalSize := int64(float64(filesize) * rs.Redundancy())
				mbps := mbps(totalSize, float64(elapsed.Seconds()))
				log.Printf("upload finished in %v (%v mbps)\n", elapsed.Round(time.Millisecond), mbps)
			}(i)
		}
		wg.Wait()
	}
}

func randomFile(filesize int64) io.Reader {
	data := make([]byte, filesize)
	if _, err := frand.Read(data); err != nil {
		log.Fatal(err)
	}
	return bytes.NewReader(data)
}

func mbps(b int64, s float64) float64 {
	bps := float64(b) / s
	return math.Round(bps*0.000008*100) / 100
}

func humanReadableByteCount(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

func percentiles(timings stats.Float64Data) string {
	if len(timings) == 0 {
		panic("developer error, no timings")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\npercentile stats (%v timings):\n\n", len(timings)))
	for _, p := range []float64{50, 80, 90, 99, 99.9} {
		percentile, err := timings.Percentile(p)
		if err != nil {
			return err.Error()
		}
		sb.WriteString(fmt.Sprintf("%vp: %vms\n", p, percentile))
	}

	return sb.String()
}

func printHeader(h string) {
	log.Printf("\n============================\n")
	log.Println(" " + h)
	log.Printf("============================\n\n")
}

func printSubHeader(h string) {
	log.Printf("- - - - - - - - - - - - - - -\n")
	log.Println(" " + h)
	log.Printf("- - - - - - - - - - - - - - -\n\n")
}
