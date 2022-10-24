package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"math/rand"
	"os"
	"strings"

	"github.com/montanaflynn/stats"
	"go.sia.tech/renterd/api"
	"lukechampine.com/frand"
)

func checkFatal(msg string, err error) {
	if err != nil {
		log.Fatalf("%s, err: %v\n", msg, err)
	}
}

func checkFatalB(msg string, cond bool) {
	if cond {
		log.Fatalf("%s\n", msg)
	}
}

func getPercentilesString(timings stats.Float64Data) string {
	if len(timings) == 0 {
		panic("developer error, no timings")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\nPercentile stats (%v timings):\n", len(timings)))
	for _, p := range []float64{50, 80, 90, 99, 99.9} {
		percentile, err := timings.Percentile(p)
		if err != nil {
			return err.Error()
		}
		sb.WriteString(fmt.Sprintf("%vp: %vms\n", p, percentile))
	}

	return sb.String()
}

func isHidden(f fs.FileInfo) bool {
	return strings.HasPrefix(f.Name(), ".")
}

func printHeader(h string) {
	log.Printf("\n============================\n")
	log.Println(" " + h)
	log.Printf("============================\n")
}

func randomSlabFileName() string {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	return hex.EncodeToString(randBytes) + ".slab.json"
}

func randomRenterKey(path string) (*api.PrivateKey, error) {
	key := api.PrivateKey(ed25519.NewKeyFromSeed(frand.Bytes(32)))

	// create keyfile
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// marshal key
	b, err := key.MarshalText()
	if err != nil {
		return nil, err
	}

	// write key to file
	_, err = f.Write(b)
	if err != nil {
		_ = os.Remove(path) // cleanup
		return nil, err
	}

	return &key, nil
}
