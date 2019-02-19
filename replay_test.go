package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
)

// global variables for testing
var s1 = s3op{Event: "put", Size: 10000, Bucket: "not", Key: "testobject-0"}
var s2 = s3op{Event: "get", Size: 10000, Bucket: "not", Key: "testobject-0"}
var s3s = s3op{Event: "put", Size: 10000, Bucket: "not", Key: "testobject-1"}
var s4 = s3op{Event: "put", Size: 10000, Bucket: "not", Key: "testobject-2"}
var s5 = s3op{Event: "get", Size: 10000, Bucket: "not", Key: "testobject-0"}
var s6 = s3op{Event: "get", Size: 10000, Bucket: "not", Key: "testobject-1"}
var s7 = s3op{Event: "get", Size: 10000, Bucket: "not", Key: "testobject-2"}

func argGenerator() (args parameters) {
	nrequests := intFlag{value: 1, set: true}
	args = parseAndValidate([]string{})
	args.concurrency = 1
	args.osize = 20
	args.bucketname = "not"
	args.objectprefix = "testobject"
	args.nrequests = &nrequests
	return
}

func TestParseReplayFile(t *testing.T) {
	sampleS3RQ := `{"replay":[
    [
    {"key":"testobject-0", "bucket":"not","op":"put","size":10000},
    {"key":"testobject-0", "bucket":"not","op":"get","size":10000}
    ],
    [
    {"key":"testobject-1", "bucket":"not","op":"put","size":10000},
    {"key":"testobject-1", "bucket":"not","op":"get","size":10000}
    ]
    ]}`
	opsChan := make(chan []s3op, 2)
	args := argGenerator()
	args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleS3RQ))
	// Get opening bracket and type
	_, err := args.jsonDecoder.Token()

	if err != nil {
		t.Fatalf("Should be able to get opening bracket")
	}
	_, err = args.jsonDecoder.Token()

	if err != nil {
		t.Fatalf("Should be able to get opening type")
	}
	expected := [][]s3op{{s1, s2}, {s3s, s6}}
	parseFileReplay(&args, opsChan)
	close(opsChan)
	i := 0
	for ops := range opsChan {
		for j := 0; j < len(ops); j++ {
			if ops[j] != expected[i][j] {
				t.Fatalf("Incorrect S3OP slice parsed: expected %v, but got %v", expected[i][j], ops[j])
			}
		}
		i++
	}
}

func TestParseMixedFile(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":50},
											  {"operationType":"get","ratio":50}]}`
	args := argGenerator()
	args.nrequests = &intFlag{value: 4, set: true}
	args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleMixedWorkload))

	_, err := args.jsonDecoder.Token()

	if err != nil {
		t.Fatalf("Should be able to get opening bracket")
	}
	_, err = args.jsonDecoder.Token()

	if err != nil {
		t.Fatalf("Should be able to get opening type")
	}
	actualRatios := parseFileMixed(&args)
	expectedRatios := []opTrack{
		opTrack{Optype: "put", Ratio: 50, sent: 0, ops: 2},
		opTrack{Optype: "get", Ratio: 50, sent: 0, ops: 2}}

	if len(actualRatios) != 2 {
		t.Fatalf("expected 2 opTrack, but got %v", len(actualRatios))
	}

	for k, v := range expectedRatios {
		if reflect.DeepEqual(v, actualRatios[k]) {
			t.Fatalf("expected %v, but got %v", expectedRatios, actualRatios)
		}
	}
}

func TestGenerateRequestsPutGet(t *testing.T) {
	args := argGenerator()
	args.nrequests = &intFlag{value: 4, set: true}
	args.osize = 10000

	mapRatios := []opTrack{
		opTrack{Optype: "put", Ratio: 50, sent: 0, ops: 2},
		opTrack{Optype: "get", Ratio: 50, sent: 0, ops: 2}}

	keys := map[string]uint64{"nottestobject-0": 0, "nottestobject-1": 1}
	buckets := map[string]bool{"nots3tester": true}
	var wg *sync.WaitGroup
	workers := createChannels(2, wg)
	params := &workloadParams{hashKeys: keys, bucketMap: buckets, workersChanSlice: workers, concurrency: len(workers)}

	opsReceived := make([][]s3op, 2)

	generateRequests(&args, mapRatios, params)

	closeAllWorkerChannels(workers)

	expected := []([]s3op){{s1, s2}, {s3s, s6}}

	for i := 0; i < 2; i++ {
		var workersOpReceived []s3op
		for op := range workers[i].workChan {
			workersOpReceived = append(workersOpReceived, op)
		}
		opsReceived[i] = workersOpReceived
	}
	for j := 0; j < len(opsReceived); j++ {
		for k := 0; k < len(opsReceived[j]); k++ {
			if expected[j][k] != opsReceived[j][k] {
				t.Fatalf("Incorrect S3OP slice parsed: expected %v, but got %v", expected[j][k], opsReceived[j][k])
			}
		}
	}
}

func TestGenerateRequestsPutGetDelete(t *testing.T) {
	args := argGenerator()
	args.nrequests = &intFlag{value: 140, set: true}
	ratios := []opTrack{
		opTrack{Optype: "put", Ratio: 25, sent: 0},
		opTrack{Optype: "get", Ratio: 25, sent: 0},
		opTrack{Optype: "head", Ratio: 25, sent: 0},
		opTrack{Optype: "delete", Ratio: 25, sent: 0},
	}

	keys := map[string]uint64{}
	buckets := map[string]bool{"nots3tester": true}
	var wg *sync.WaitGroup
	workers := createChannels(4, wg)
	params := &workloadParams{hashKeys: keys, bucketMap: buckets, workersChanSlice: workers, concurrency: len(workers)}

	generateRequests(&args, ratios, params)
	closeAllWorkerChannels(workers)
	totalget := 0
	totalput := 0
	totaldelete := 0
	totalhead := 0

	for i := 0; i < 4; i++ {
		for op := range workers[i].workChan {
			switch op.Event {
			case "put":
				totalput += 1
			case "get":
				totalget += 1
			case "delete":
				totaldelete += 1
			case "head":
				totalhead += 1
			default:
				t.Fatalf("Not a valid operation type expected one of {delete,head,get,put},but got %v", op.Event)
			}
		}
	}
	if totalput != 35 {
		t.Fatalf("There should be 25 put requests generated, but %v requests generated", totalput)
	}
	if totalget != 35 {
		t.Fatalf("There should be 25 put requests generated, but %v requests generated", totalget)
	}
	if totaldelete != 35 {
		t.Fatalf("There should be 25 put requests generated, but %v requests generated", totaldelete)
	}
	if totalhead != 35 {
		t.Fatalf("There should be 25 put requests generated, but %v requests generated", totalhead)
	}
}

func TestSplitS3ops(t *testing.T) {
	keys := map[string]uint64{"nottestobject-0": 3, "nottestobject-1": 2, "nottestobject-2": 1}
	buckets := map[string]bool{"nots3tester": true}
	var wg *sync.WaitGroup
	workers := createChannels(4, wg)
	ops := []s3op{s1, s2, s3s, s4, s5, s6, s7}
	params := &workloadParams{hashKeys: keys, bucketMap: buckets, workersChanSlice: workers, concurrency: len(workers)}
	splitS3ops(params, ops, "", "us-east-1")
	opsRecieved := make([][]s3op, 4)
	closeAllWorkerChannels(workers)
	var expected = []([]s3op){{}, {s4, s7}, {s3s, s6}, {s1, s2, s5}}

	for i := 0; i < 4; i++ {
		var workersOpRecieved []s3op
		for op := range workers[i].workChan {
			workersOpRecieved = append(workersOpRecieved, op)
		}
		opsRecieved[i] = workersOpRecieved
	}
	for j := 0; j < 4; j++ {
		for k := 0; k < len(expected[j]); k++ {
			if expected[j][k] != opsRecieved[j][k] {
				t.Fatalf("Incorrect S3OP slice parsed: expected %v, but got %v", expected[j][k], opsRecieved[j][k])
			}
		}
	}
}

func TestGetHashKey(t *testing.T) {
	haskKeys := map[string]uint64{"nottestobject-0": 3, "nottestobject-1": 2, "nottestobject-2": 1}
	n := getHashKey(haskKeys, s1.Bucket+s1.Key, 4)
	if n != 3 {
		t.Fatalf("Should return correct worker, returned %v instead of 3", n)
	}
}

func TestGetHashKeyNotCalculatedYet(t *testing.T) {
	hashKeys := map[string]uint64{}
	n := generateHashKey(hashKeys, s1.Key+s1.Bucket, 10)
	if n != hashKeys[s1.Key+s1.Bucket] {
		t.Fatalf("generateHashKey not storing hashKeys for new key names")
	}
	hashKeys2 := map[string]uint64{}
	if n != generateHashKey(hashKeys2, s1.Key+s1.Bucket, 10) {
		t.Fatalf("generateHashKey same key for the same name")
	}
}

func TestMetadataValue(t *testing.T) {
	m := metadataValue(400)
	keyValue := strings.Split(m, "=")
	if keyValue[0] != strings.Repeat("k", 200) {
		t.Fatalf("Wrong key generated for mock value")
	}
	if keyValue[1] != strings.Repeat("v", 200) {
		t.Fatalf("Wrong key generated for mock value")
	}
}
