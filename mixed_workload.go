package main

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
)

type s3op struct {
	Event  string `json:"op"`
	Size   uint64 `json:"size"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type workerChan struct {
	workChan chan s3op
	wg       *sync.WaitGroup
}

var hasher = fnv.New64a()
var operations = map[string]bool{
	"put":        true,
	"get":        true,
	"head":       true,
	"updatemeta": true,
	"delete":     true,
}

type workloadParams struct {
	// hashKeys keeps track of keys that have already been hashed to a specific worker
	hashKeys map[string]uint64
	// workersChanSlice is an s3 operation channel for each worker
	workersChanSlice []*workerChan
	concurrency      int
}

func setupWorkloadParams(workerChans []*workerChan, concurrency int) *workloadParams {
	keys := make(map[string]uint64)
	return &workloadParams{hashKeys: keys, workersChanSlice: workerChans, concurrency: concurrency}
}

func closeAllWorkerChannels(workChanSlice []*workerChan) {
	for i := 0; i < len(workChanSlice); i++ {
		close(workChanSlice[i].workChan)
	}
}

// SetupOps reads in json file to a struct to determine which s3 operations to generate and then execute
func SetupOps(args *Parameters, workerChans []*workerChan, decoder *json.Decoder) error {
	if _, err := decoder.Token(); err != nil {
		return err
	}

	workType, err := decoder.Token()
	if err != nil {
		return err
	} else if workType != "mixedWorkload" {
		return errors.New("Incorrect workload type specified, must be 'mixedWorkload'")
	}

	workloadParams := setupWorkloadParams(workerChans, args.Concurrency)
	MixedWorkload(args, workloadParams, decoder)
	return nil
}

type opTrack struct {
	ops       float64
	sent      int64
	Operation string `json:"operationType"`
	Ratio     int    `json:"ratio"`
}

// MixedWorkload is the main mixedWorkload function, creates a struct to track relative ratios
func MixedWorkload(args *Parameters, workloadParams *workloadParams, decoder *json.Decoder) {
	ratios := parseFileMixed(args, decoder)
	generateRequests(args, ratios, workloadParams)
}

// Parses mixed workload file into a struct
func parseFileMixed(args *Parameters, decoder *json.Decoder) []opTrack {
	ratios := []opTrack{}
	for decoder.More() {
		if err := decoder.Decode(&ratios); err != nil {
			log.Fatal(err)
		}
	}
	totalPercent := 0
	for _, v := range ratios {
		if _, ok := operations[v.Operation]; !ok {
			log.Fatalf("Mixed workload operation types must be one of {'put','get','delete','updatemeta','head'}, but got %v", v.Operation)
		}
		v.ops = ((float64(args.Requests) * float64(v.Ratio)) / float64(100))
		totalPercent += v.Ratio
	}
	if totalPercent != 100 {
		log.Fatal("Percentage of operations does not sum to 100")
	}
	return ratios
}

// Generates s3ops based on mixedWorkload
// Generates a workload 100 mixed operations at a time. For instance, if 50% Put and 50% Get specified,
// It will generate 50 Puts and send them. Then it will generate 50 Gets and
// send them. It will repeat this until the number of requests specified is reached.
func generateRequests(args *Parameters, ratios []opTrack, workload *workloadParams) {
	sent := 0
	totalOps := args.Requests
	for j := 0; j < int(math.Ceil(float64(totalOps)/100.0)); j++ {
		// Send in batches of 100, However if leftover is < 100
		// adjust the operation's ratio accordingly
		leftover := math.Min(100.0, float64(totalOps-sent))
		for _, v := range ratios {
			for i := 0; i < int(math.Floor((float64(v.Ratio)/100.0)*leftover)); i++ {
				op := s3op{Event: v.Operation, Size: uint64(args.Size), Bucket: args.Bucket, Key: args.Prefix + "-" + strconv.FormatInt(v.sent, 10)}
				sent++
				v.sent++
				sendS3op(op, workload)
			}
		}
	}
}

// Sends s3op to appropriate worker for mixedWorkload
func sendS3op(op s3op, params *workloadParams) {
	workerNum := getHashKey(params.hashKeys, op.Key+op.Bucket, params.concurrency)
	params.workersChanSlice[workerNum].workChan <- op
}

// allocates an individual channel for each worker
func createChannels(concurrency int, workerWG *sync.WaitGroup) []*workerChan {
	workerChanMap := make([]*workerChan, concurrency)
	for i := 0; i < concurrency; i++ {
		s3opChan := make(chan s3op, 100)
		workerChanMap[i] = &workerChan{workChan: s3opChan, wg: workerWG}
	}
	return workerChanMap
}

// Returns the value for a key if it has already been hashed; otherwise generates one
func getHashKey(hashKeyNames map[string]uint64, keyname string, concurrency int) uint64 {
	if v, ok := hashKeyNames[keyname]; ok {
		return v
	}
	return generateHashKey(hashKeyNames, keyname, concurrency)
}

// generates the hashKey for the worker
func generateHashKey(hashKeyNames map[string]uint64, keyname string, concurrency int) uint64 {
	hasher.Write([]byte(keyname))
	n := hasher.Sum64() % uint64(concurrency)
	if len(hashKeyNames) >= 100000 {
		for k := range hashKeyNames {
			delete(hashKeyNames, k)
			break
		}
	}
	// reset so next write for hashing is always on a clean buffer
	hasher.Reset()
	hashKeyNames[keyname] = n
	return n
}

// mocks up metadata for metadata update
func metadataValue(size int) string {
	size = size / 2
	return strings.Repeat("k", size) + "=" + strings.Repeat("v", size)
}
