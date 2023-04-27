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
		ulBgThreads int

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
	ulBgThreads := flag.Int("ul-bg-threads", 0, "number of background upload threads to run while performing download benchmarks")
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
		if tt, err := strconv.Atoi(strings.TrimSpace(t)); err != nil {
			log.Fatalf("invalid thread count: %v", t)
		} else if tt > 0 {
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
		ulBgThreads: *ulBgThreads,

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

	// run the benchmark
	filename := fmt.Sprintf("benchmark_%s_%s.txt", flags.id, time.Now().Format("2006-01-02_15-04-05"))
	report(filename, capture(func() { benchmark(flags) }))
}

func benchmark(params flags) {
	printHeader("RENTERD benchmarking tool")

	// print configuration
	log.Println("- benchmark: ", params.id)
	log.Println("- filesize: ", humanReadableByteCount(params.filesize))
	log.Println("- num files: ", params.numFiles)
	log.Println("- num dl threads: ", fmt.Sprint(params.dlThreads))
	log.Println("- num ul threads: ", fmt.Sprint(params.ulThreads))
	log.Println("- min shards: ", params.minShards)
	log.Println("- tot shards: ", params.totalShards)
	log.Println()
	log.Println("- workers: ", strings.Join(params.workerAddresses, ","))
	log.Println("- bus: ", params.busAddress)
	log.Println()

	// create bus client
	bc := bus.NewClient(params.busAddress, params.busPassword)

	// create worker clients
	wcs := make([]*worker.Client, len(params.workerAddresses))
	for i, addr := range params.workerAddresses {
		wcs[i] = worker.NewClient(addr, params.workerPasswords[i])
	}
	wc := wcs[0]

	// create redundancy settings
	rs := api.RedundancySettings{MinShards: params.minShards, TotalShards: params.totalShards}
	if err := rs.Validate(); err != nil {
		log.Fatal("invalid redundancy settings: ", err)
	}

	prefix := fmt.Sprintf("/%s/file_%v_", params.id, strings.ReplaceAll(humanReadableByteCount(params.filesize), " ", ""))

	checkConfig(bc, rs, wcs)
	checkDataset(wc, params.id, prefix, params.ulThreads, params.numFiles, params.filesize, rs)

	printHeader("running benchmark")

	// start uploading in the background if requested
	if params.ulBgThreads > 0 {
		printSubHeader(fmt.Sprintf("start %d background upload threads", params.ulBgThreads))
		doneChan := make(chan struct{})
		defer close(doneChan)
		for i := 0; i < params.ulBgThreads; i++ {
			go uploadRandomData(wcs, 4<<20, doneChan)
		}
	}

	// run the benchmark for every thread count
	for _, threadCount := range params.dlThreads {
		printSubHeader(fmt.Sprintf("thread count %d", threadCount))

		jobFn := func(i int) error {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			wc := wcs[0]
			for _, wc = range wcs {
				break
			}

			return wc.DownloadObject(ctx, io.Discard, strings.TrimPrefix(prefix+fmt.Sprint(i), "/"))
		}
		progressFn := func(progress float64, elapsed time.Duration, success, _ uint64) {
			log.Printf("%v%% finished after %v (%v mpbs)\n", progress*100, elapsed, mbps(int64(success)*params.filesize, elapsed.Seconds()))
		}

		timings, elapsed, _, failures := parallelize(threadCount, params.numFiles, jobFn, .2, progressFn)
		if failures != 0 {
			log.Printf("there were %v errors while downloading\n", failures)
		}
		log.Println(percentiles(timings))

		downloaded := int64(params.numFiles) * params.filesize
		log.Printf("downloaded %v in %v (%v mpbs)\n\n", humanReadableByteCount(downloaded), elapsed.Round(time.Millisecond), mbps(downloaded, elapsed.Seconds()))
	}
}

func parallelize(threads, n int, jobFn func(i int) error, progress float64, progressFn func(progress float64, elapsed time.Duration, success, failures uint64)) ([]float64, time.Duration, uint64, uint64) {
	start := time.Now()
	timings := make([]float64, n)
	cutoff := uint64(math.Ceil(float64(n) * progress))

	// create a thread pool
	threadPool := make(chan struct{}, threads)
	for i := 0; i < threads; i++ {
		threadPool <- struct{}{}
	}

	var success uint64
	var failure uint64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		<-threadPool

		wg.Add(1)
		go func(i int) {
			defer func() {
				threadPool <- struct{}{}
			}()
			defer wg.Done()

			jobStart := time.Now()
			if err := jobFn(i); err != nil {
				log.Println(err)
				atomic.AddUint64(&failure, 1)
			} else {
				atomic.AddUint64(&success, 1)
				timings[i] = float64(time.Since(jobStart).Milliseconds())
			}

			success := atomic.LoadUint64(&success)
			failure := atomic.LoadUint64(&failure)
			if processed := success + failure; processed%cutoff == 0 {
				progress := float64(processed) / float64(n)
				progressFn(progress, time.Since(start), success, failure)
			}
		}(i)
	}
	wg.Wait()

	return timings, time.Since(start), success, failure
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
	if accounts, err := bc.Accounts(ctx); err != nil {
		log.Fatalf("failed to get accounts, err: %v", err)
	} else if len(accounts) < rs.MinShards {
		log.Fatalf("not enough accounts to satisfy the redundancy settings")
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

	flatten := func(entries []api.ObjectMetadata) []string {
		flattened := make([]string, len(entries))
		for i, entry := range entries {
			flattened[i] = entry.Name
		}
		return flattened
	}

	// check dataset
	if entries, err := wc.ObjectEntries(context.Background(), path+"/"); err != nil {
		log.Fatal("failed to fetch entries for path: '", path, "/', err: ", err)
	} else if len(filter(flatten(entries))) < required {
		// build a map of existing files
		exists := make(map[string]bool)
		for _, entry := range filter(flatten(entries)) {
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

func capture(fn func()) string {
	// backup stdout and stderr
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
		exit <- true
	}()

	fn()

	_ = w.Close()
	<-exit

	return buf.String()
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

func mbps(b int64, s float64) float64 {
	bps := float64(b) / s
	return math.Round(bps*0.000008*100) / 100
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

func randomFile(filesize int64) io.Reader {
	data := make([]byte, filesize)
	if _, err := frand.Read(data); err != nil {
		log.Fatal(err)
	}
	return bytes.NewReader(data)
}

func randomPath() string {
	b := make([]byte, 16)
	frand.Read(b)
	return fmt.Sprintf("%x", b)
}

func report(filename, output string) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("ERR: failed to open output to file", err)
		return
	}

	_, err = file.WriteString(output)
	if err != nil {
		log.Fatal("ERR: failed to write output to file", err)
		return
	}
	file.Sync()
	file.Close()

	log.Println("report written to", filename)
}

func uploadRandomData(wcs []*worker.Client, filesize int64, stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			return
		default:
		}

		wc := wcs[0]
		for _, wc = range wcs {
			break
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		if err := wc.UploadObject(ctx, randomFile(filesize), randomPath()); err != nil {
			log.Println("ERR: background upload failed, err: ", err)
		}
		cancel()
	}
}
