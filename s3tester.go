package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/time/rate"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/codahale/hdrhistogram"
)

const (
	// VERSION is displayed with help, bump when updating
	VERSION = "2.1.0"
	// for identifying s3tester requests in the user-agent header
	userAgentString = "s3tester/"
)

type results struct {
	CummulativeResult result    `json:"cummulativeResult"`
	PerEndpointResult []*result `json:"endpointResult,omitempty"`
}

// result holds the performance metrics for a single goroutine that are later aggregated.
type result struct {
	Category   string `json:"category,omitempty"`
	UniqueName string `json:"uniqueName,omitempty"`

	Endpoint    string `json:"endpoint,omitempty"`
	Operation   string `json:"operation,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	UniqObjNum  int    `json:"totalUniqueObjects"`
	Count       int    `json:"totalRequests"`
	Failcount   int    `json:"failedRequests"`
	Fanout      int    `json:"fanout"`

	TotalElapsedTime   float64 `json:"totalElapsedTime (ms)"`
	AverageRequestTime float64 `json:"averageRequestTime (ms)"`
	MinimumRequestTime float64 `json:"minimumRequestTime (ms)"`
	MaximumRequestTime float64 `json:"maximumRequestTime (ms)"`

	NominalRequestsPerSec float64 `json:"nominalRequestsPerSec"`
	ActualRequestsPerSec  float64 `json:"actualRequestsPerSec"`
	ContentThroughput     float64 `json:"contentThroughput (MB/s)"`
	AverageObjectSize     float64 `json:"averageObjectSize"`

	Percentiles map[string]float64 `json:"responseTimePercentiles(ms)"`

	sumObjSize  int64
	elapsedSum  time.Duration
	data        []detail
	latencies   *hdrhistogram.Histogram
	startTime   time.Time
	elapsedTime time.Duration
}

func NewResult() result {
	// Tracking values between 1 hundredth of a millisecond up to (10 hours in hundredths of milliseconds) to 4 significant digits.
	// At the maximum value of 10 hours the resolution will be 3.6 seconds or better.
	var tensOfMicrosecondsPerSecond int64 = 1e5

	// An hdrhistogram has a fixed sized based on its precision and value range. This configuration uses 2.375 MiB per client (concurrency parameter).
	h := hdrhistogram.New(1, 10*3600*tensOfMicrosecondsPerSecond, 4)
	return result{latencies: h}
}

func (this *result) RecordLatency(l time.Duration) {
	// Record latency as hundredths of milliseconds.
	this.latencies.RecordValue(l.Nanoseconds() / 1e4)
}

// detail holds metrics for individual S3 requests.
type detail struct {
	ts      time.Time
	elapsed time.Duration
}

func IsErrorRetryable(err error) bool {
	if err != nil {
		if err, ok := err.(awserr.Error); ok {
			// inconsistency can lead to a multipart complete not seeing a part just uploaded.
			return err.Code() == "InvalidPart"
		}
	}
	return false
}

type CustomRetryer struct {
	maxRetries     int
	defaultRetryer request.Retryer
}

func (d CustomRetryer) ShouldRetry(r *request.Request) bool {
	if IsErrorRetryable(r.Error) {
		return true
	}
	return d.defaultRetryer.ShouldRetry(r)
}

func (d CustomRetryer) RetryRules(r *request.Request) time.Duration {
	return d.defaultRetryer.RetryRules(r)
}

func (d CustomRetryer) MaxRetries() int {
	return d.maxRetries
}

func NewCustomRetryer(retries int) *CustomRetryer {
	return &CustomRetryer{maxRetries: retries, defaultRetryer: client.DefaultRetryer{NumMaxRetries: retries}}
}

type RetryerWithSleep struct {
	base               *CustomRetryer
	retrySleepPeriodMs int
}

func (d RetryerWithSleep) RetryRules(r *request.Request) time.Duration {
	return time.Millisecond * time.Duration(d.retrySleepPeriodMs)
}

func (d RetryerWithSleep) ShouldRetry(r *request.Request) bool {
	return d.base.ShouldRetry(r)
}

func (d RetryerWithSleep) MaxRetries() int {
	return d.base.MaxRetries()
}

func NewRetryerWithSleep(retries int, sleepPeriodMs int) *RetryerWithSleep {
	return &RetryerWithSleep{retrySleepPeriodMs: sleepPeriodMs, base: NewCustomRetryer(retries)}
}

func randMinMax(source *rand.Rand, min int64, max int64) int64 {
	return min + source.Int63n(max-min+1)
}

type durationSetting struct {
	applicable bool
	runstart   time.Time
	maxRunTime time.Duration
}

func NewDurationSetting(duration *intFlag, runStart time.Time) *durationSetting {
	if duration.set {
		maxRunTime := time.Duration(duration.value * 1e9)
		return &durationSetting{applicable: true, runstart: runStart, maxRunTime: maxRunTime}
	} else {
		return &durationSetting{applicable: false}
	}
}

func (ds *durationSetting) enabled() bool {
	if ds.applicable {
		return time.Since(ds.runstart) >= ds.maxRunTime
	}
	return false
}

var detailed []detail

func runtest(args parameters) (float64, results) {
	c := make(chan result, args.concurrency)
	startTime := time.Now()
	startTestWorker(c, args)
	testResult := collectWorkerResult(c, args, startTime)

	if args.optype != "validate" {
		processTestResult(&testResult, args)
		printTestResult(&testResult, args.isJson)
	}
	return float64(testResult.CummulativeResult.Count) / testResult.CummulativeResult.elapsedTime.Seconds(), testResult
}

func startTestWorker(c chan<- result, args parameters) {
	credential, err := loadCredentialProfile(args.profile, args.nosign)
	if err != nil {
		fmt.Println("Failed loading credentials.\nPlease specify env variable AWS_SHARED_CREDENTIALS_FILE if you put credential file other than AWS CLI configuration directory.")
		log.Fatal(err)
	}
	limiter := rate.NewLimiter(args.ratePerSecond, 1)
	workersPerEndpoint := args.concurrency / len(args.endpoints)
	var workerChans []*workerChan
	var workersWG sync.WaitGroup

	if args.jsonDecoder != nil {
		workerChans = createChannels(args.concurrency, &workersWG)

		go func() {
			SetupOps(&args, workerChans, credential)
			closeAllWorkerChannels(workerChans)
		}()
	}

	for i, endpoint := range args.endpoints {
		endpointStartTime := time.Now()
		for currEndpointWorkerId := 0; currEndpointWorkerId < workersPerEndpoint; currEndpointWorkerId++ {
			workerId := i*workersPerEndpoint + currEndpointWorkerId
			var workChan *workerChan
			// if replay or a mixed workload setup a channel for each worker
			if args.jsonDecoder != nil {
				workChan = workerChans[workerId]
				workChan.wg.Add(1)
			}
			go worker(c, args, credential, workerId, endpoint, endpointStartTime, limiter, workChan)
		}
	}
	if args.jsonDecoder != nil {
		workersWG.Wait()
	}
}

func loadCredentialProfile(profile string, nosign bool) (*credentials.Credentials, error) {
	if nosign {
		return credentials.AnonymousCredentials, nil
	}
	providers := make([]credentials.Provider, 0)
	if profile == "" {
		providers = append(providers, &credentials.EnvProvider{})
	}
	providers = append(providers, &credentials.SharedCredentialsProvider{Profile: profile})
	credential := credentials.NewChainCredentials(providers)
	_, err := credential.Get() // invoke it here as a validation of loading credentials
	return credential, err
}

func ReceiveS3Op(svc *s3.S3, httpClient *http.Client, args *parameters, durationLimit *durationSetting, limiter *rate.Limiter, workersChan *workerChan, r *result) {
	for op := range workersChan.workChan {
		args.osize = int64(op.Size)
		args.bucketname = op.Bucket + "s3tester"
		// need to mock up garbage metadata if it is a SUPD S3 event
		if op.Event == "updatemeta" {
			args.metadata = metadataValue(int(op.Size))
		}
		sendRequest(svc, httpClient, op.Event, op.Key, args, r, limiter)
		if durationLimit.enabled() {
			return
		}
	}
	workersChan.wg.Done()
}

func sendRequest(svc *s3.S3, httpClient *http.Client, optype string, keyName string, args *parameters, r *result, limiter *rate.Limiter) {
	r.Count++
	start := time.Now()
	err := DispatchOperation(svc, httpClient, optype, keyName, args, r, int64(args.nrequests.value))
	elapsed := time.Since(start)
	r.RecordLatency(elapsed)

	if err != nil {
		r.Failcount++
		log.Printf("Failed %s on object '%s/%s': %v", args.optype, args.bucketname, keyName, err)
	}
	r.elapsedSum += elapsed

	if args.logging {
		r.data = append(r.data, detail{start, elapsed})
	}

	if limiter.Limit() != rate.Inf {
		limiter.Wait(context.Background())
	}
}

func worker(results chan<- result, args parameters, credentials *credentials.Credentials, id int, endpoint string, runstart time.Time, limiter *rate.Limiter, workerChan *workerChan) {
	httpClient := MakeHTTPClient()
	svc := MakeS3Service(httpClient, args.retrySleep, args.retries, endpoint, args.region, args.consistencyControl, credentials)

	if args.fanout.set {
		if args.optype == "put" {
			svc.Client.Handlers.Send.PushFront(func(r *request.Request) {
				r.HTTPRequest.Header.Add("X-Fanout-Copy-Count", fmt.Sprintf("%d", args.fanout.value))
			})
		} else if args.optype == "get" {
			svc.Client.Handlers.Send.PushFront(func(r *request.Request) {
				r.HTTPRequest.Header.Add("X-Fanout-Copy-Index", fmt.Sprintf("%d", args.fanout.value))
			})
		}
	}

	var source *rand.Rand

	r := NewResult()
	r.Endpoint = endpoint
	r.startTime = runstart

	if args.logging {
		r.data = make([]detail, 0, args.nrequests.value/args.concurrency*args.attempts)
	}

	if args.min != 0 && args.max != 0 {
		source = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	durationLimit := NewDurationSetting(args.duration, runstart)

	if workerChan != nil {
		ReceiveS3Op(svc, httpClient, &args, durationLimit, limiter, workerChan, &r)
	} else {
		maxRequestsPerWorker := int64(args.nrequests.value / args.concurrency)
		if args.duration.set && args.optype != "get" {
			maxRequestsPerWorker = math.MaxInt64 / int64(args.concurrency)
		}
		for j := int64(0); j < maxRequestsPerWorker; j++ {
			var keyName string
			switch args.overwrite {
			case 1:
				keyName = args.objectprefix
			case 2:
				keyName = args.objectprefix + "-" + strconv.FormatInt(j, 10)
			default:
				keyName = args.objectprefix + "-" + strconv.FormatInt(int64(id)*maxRequestsPerWorker+j, 10)
			}

			r.incrementUniqObjNumCount(args.duration.set)

			for repcount := 0; repcount < args.attempts; repcount++ {
				if source != nil {
					//size command line arg usually sets the size for each request we need to overwrite
					// with new random size per request
					newSize := randMinMax(source, args.min, args.max)
					args.osize = newSize
				}

				sendRequest(svc, httpClient, args.optype, keyName, &args, &r, limiter)

				if durationLimit.enabled() {
					results <- r
					return
				}
			}
		}
	}
	results <- r
}

func (this *result) incrementUniqObjNumCount(isDurationSet bool) {
	// This feature is very much tied to the args.attempts option and doesn't work when using duration to get the same values over and over again.
	if this.Operation != "options" && !(isDurationSet && (this.Operation == "get" || this.Operation == "randget")) {
		this.UniqObjNum++
	}
}

func collectWorkerResult(c <-chan result, args parameters, startTime time.Time) results {
	workersPerEndpoint := args.concurrency / len(args.endpoints)
	workerWorkload := args.nrequests.value / args.concurrency
	endpointResultMap := make(map[string]*result)
	if args.logging {
		detailed = make([]detail, 0)
	}

	for i := 0; i < args.concurrency; i++ {
		r := <-c
		// merge results
		if _, hasKey := endpointResultMap[r.Endpoint]; !hasKey {
			// init the endpointResultMap, Concurrency is the counter of the workers that have been collected
			r.Concurrency = 1
			endpointResultMap[r.Endpoint] = &r
		} else {
			mergeResult(endpointResultMap[r.Endpoint], &r)
			endpointResultMap[r.Endpoint].Concurrency++
		}

		if endpointResultMap[r.Endpoint].Concurrency == workersPerEndpoint {
			finishEndpointResultCollection(endpointResultMap[r.Endpoint], r.startTime, args.attempts, args.overwrite, workerWorkload)
		}

		if args.logging {
			detailed = append(detailed, r.data...)
		}
	}
	testResult := processEndpointResults(endpointResultMap, args.endpoints)
	testResult.CummulativeResult.elapsedTime = time.Since(startTime)
	return testResult
}

func finishEndpointResultCollection(endpointResult *result, startTime time.Time, repeat, overwrite, workload int) {
	endpointResult.elapsedTime = time.Since(startTime)
	// TODO: not sure if we should also handle the case where Failcount > UniqObjNum
	// if that won't happend we can simply remove the if statement
	if endpointResult.Failcount > 0 && endpointResult.UniqObjNum >= endpointResult.Failcount {
		endpointResult.UniqObjNum -= endpointResult.Failcount
	}
	endpointResult.correctEndpointUniqObjCountWithOverwriteSetting(overwrite, workload)
}

func mergeResult(aggregateResults, r *result) {
	aggregateResults.latencies.Merge(r.latencies)
	aggregateResults.sumObjSize += r.sumObjSize
	aggregateResults.UniqObjNum += r.UniqObjNum
	aggregateResults.Count += r.Count
	aggregateResults.Failcount += r.Failcount
	aggregateResults.elapsedSum += r.elapsedSum
}

func (r *result) correctEndpointUniqObjCountWithOverwriteSetting(overwrite, workload int) {
	// this function should only be invoked by an endpoint result
	// unique obj count should be consist with overwrite setting
	if overwrite == 1 && r.UniqObjNum > 0 {
		r.UniqObjNum = 1
	} else if overwrite == 2 && r.UniqObjNum > workload {
		r.UniqObjNum = workload
	}
}

// this function will return a results struct will all the worker results merged
func processEndpointResults(endpointResultMap map[string]*result, endpoints []string) results {
	if len(endpoints) == 1 {
		testResult := results{}
		testResult.CummulativeResult = *endpointResultMap[endpoints[0]]
		testResult.CummulativeResult.Endpoint = ""
		return testResult
	}
	testResult := results{}
	testResult.CummulativeResult = NewResult()
	testResult.PerEndpointResult = make([]*result, len(endpointResultMap))
	counter := 0
	for _, endpointResult := range endpointResultMap {
		testResult.PerEndpointResult[counter] = endpointResult
		counter++
		mergeResult(&testResult.CummulativeResult, endpointResult)
	}
	return testResult
}

func processTestResult(testResult *results, args parameters) {
	cummulativeResult := &testResult.CummulativeResult
	cummulativeResult.Operation = args.optype
	cummulativeResult.Concurrency = args.concurrency
	cummulativeResult.Fanout = args.fanout.value
	setupResultStat(cummulativeResult)

	for _, endpointResult := range testResult.PerEndpointResult {
		setupResultStat(endpointResult)
	}

	cummulativeResult.Category = args.bucketname + "-" + cummulativeResult.Operation + "-" + strconv.Itoa(cummulativeResult.Concurrency) + "-" + strconv.FormatInt(cummulativeResult.sumObjSize, 10)
	rand.Seed(time.Now().Unix())
	cummulativeResult.UniqueName = cummulativeResult.Category + "-" + time.Now().UTC().Format(time.RFC3339) + "-" + strconv.Itoa(rand.Intn(100))
}

func setupResultStat(testResult *result) {
	elapsedTime := testResult.elapsedTime
	calcStats(testResult, testResult.Concurrency, elapsedTime)
	roundResult(testResult)
	processPercentiles(testResult)

	minReqTime := time.Duration(testResult.latencies.Min() * 1e4)
	maxReqTime := time.Duration(testResult.latencies.Max() * 1e4)
	testResult.TotalElapsedTime = float64(elapsedTime) / float64(time.Millisecond)
	testResult.MinimumRequestTime = float64(minReqTime) / float64(time.Millisecond)
	testResult.MaximumRequestTime = float64(maxReqTime) / float64(time.Millisecond)
}

func calcStats(results *result, concurrency int, elapsedTime time.Duration) {
	mean := results.elapsedSum / time.Duration(results.Count)
	results.AverageRequestTime = float64(mean) / float64(time.Millisecond)
	results.NominalRequestsPerSec = 1.0 / mean.Seconds() * float64(concurrency)
	results.ActualRequestsPerSec = float64(results.Count) / elapsedTime.Seconds()
	results.ContentThroughput = float64(results.sumObjSize) / 1024 / 1024 / elapsedTime.Seconds()
	results.AverageObjectSize = float64(results.sumObjSize) / float64(results.Count)
}

func roundResult(results *result) {
	results.NominalRequestsPerSec = roundFloat(results.NominalRequestsPerSec, 1)
	results.ActualRequestsPerSec = roundFloat(results.ActualRequestsPerSec, 1)
	results.ContentThroughput = roundFloat(results.ContentThroughput, 6)
	results.AverageObjectSize = roundFloat(results.AverageObjectSize, 0)
}

var percentiles []float64 = []float64{50, 75, 90, 95, 99, 99.9}

func processPercentiles(results *result) {
	results.Percentiles = make(map[string]float64)
	for _, percentile := range percentiles {
		quantileValue := float64(results.latencies.ValueAtQuantile(percentile)) / 1e2
		results.Percentiles[convertFloatToString(percentile)] = quantileValue
	}
}

func printResponseTimeDistribution(percentilesMap map[string]float64) {
	fmt.Println("Response Time Percentiles")
	for _, percentile := range percentiles {
		key := convertFloatToString(percentile)
		quantileValue := convertFloatToString(percentilesMap[key])
		fmt.Printf("%-5v  :   %-5v\n", key, quantileValue+" ms")
	}
}

func convertFloatToString(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func roundFloat(in float64, point int) float64 {
	return float64(int(math.Round(in*math.Pow10(point)))) / math.Pow10(point)
}

func printTestResult(testResult *results, isJson bool) {
	if isJson {
		printJsonResult(*testResult)
	} else {
		printResults(*testResult)
	}
}

func printResults(testResult results) {
	if len(testResult.PerEndpointResult) > 1 {
		fmt.Println("\n\t--- Result per Endpoint ---")
		for _, endpointResult := range testResult.PerEndpointResult {
			printResult(*endpointResult)
		}
	}

	fmt.Println("\n\t--- Total Results ---")
	printResult(testResult.CummulativeResult)
	HistogramSummary(testResult.CummulativeResult.latencies)
}

func printResult(results result) {
	if results.Endpoint != "" {
		fmt.Printf("- Endpoint: %s\n", results.Endpoint)
	} else { // Total result prints the operation & concurrency rather than endpoint
		fmt.Printf("Operation: %s\n", results.Operation)
	}
	fmt.Printf("Concurrency: %d\n", results.Concurrency)
	fmt.Printf("Total number of requests: %d\n", results.Count)

	if results.Operation == "put" && results.Fanout >= 0 {
		fmt.Printf("Number of Fanout copies: %d\n", results.Fanout)
		if results.UniqObjNum != 0 {
			fmt.Printf("Total number of unique objects: %d\n", results.UniqObjNum * results.Fanout)
		}
	} else if results.UniqObjNum != 0 {
		fmt.Printf("Total number of unique objects: %d\n", results.UniqObjNum)
	}

	fmt.Printf("Failed requests: %d\n", results.Failcount)

	fmt.Printf("Total elapsed time: %s\n", time.Duration(results.TotalElapsedTime*float64(time.Millisecond)))
	fmt.Printf("Average request time: %s\n", time.Duration(results.AverageRequestTime*float64(time.Millisecond)))
	fmt.Printf("Minimum request time: %s\n", time.Duration(results.MinimumRequestTime*float64(time.Millisecond)))
	fmt.Printf("Maximum request time: %s\n", time.Duration(results.MaximumRequestTime*float64(time.Millisecond)))

	fmt.Printf("Nominal requests/s: %.1f\n", results.NominalRequestsPerSec)
	fmt.Printf("Actual requests/s: %.1f\n", results.ActualRequestsPerSec)
	fmt.Printf("Content throughput: %.6f MB/s\n", results.ContentThroughput)
	fmt.Printf("Average Object Size: %v\n", results.AverageObjectSize)

	printResponseTimeDistribution(results.Percentiles)
}

func printJsonResult(testResult results) {
	// switch between human-readable json output & machine-friendly json output
	// jsonResult, err := json.MarshalIndent(testResult, "", "    ")
	jsonResult, err := json.Marshal(testResult)
	if err != nil {
		fmt.Println("Error when parsing result to json")
		return
	}
	fmt.Println(string(jsonResult))
}

func main() {
	args := parseArgs()

	if args.cpuprofile != "" {
		f, err := os.Create(args.cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var totalResults results
	if args.concurrency != 0 {
		_, totalResults = runtest(args)
	} else {
		previous := 0.0
		result := 0.0
		for c := 8; c < 1024 && result >= previous; c = c + 8 {
			previous = result
			args.concurrency = c
			result, _ = runtest(args)
			fmt.Printf("Concurrency %d ===> %.1f requests/s\n", c, result)
		}
	}

	if args.logging {
		f, err := os.Create(args.logdetail)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		for _, v := range detailed {
			fmt.Fprintf(f, "%f,%f\n", v.ts.Sub(detailed[0].ts).Seconds(), v.elapsed.Seconds())
		}
	}

	if args.loglatency != "" {
		f, err := os.Create(args.loglatency)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		fmt.Fprintf(f, "from(ms) to(ms) count(operations)\n")
		for _, v := range totalResults.CummulativeResult.latencies.Distribution() {
			fmt.Fprintf(f, "%f %f %d\n", float64(v.From)/1e2, float64(v.To)/1e2, v.Count)
		}
	}

	if totalResults.CummulativeResult.Failcount > 0 {
		os.Exit(1)
	}
}

func MakeHTTPClient() *http.Client {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	return &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   60 * time.Second,
				KeepAlive: 180 * time.Second,
				DualStack: true,
			}).DialContext,
			DisableCompression:  true, // Non-default
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100, // Non-default
			IdleConnTimeout:     90 * time.Second,
			TLSClientConfig:     tlsConfig, // Non-default
			TLSHandshakeTimeout: 60 * time.Second,
		},
	}
}

func MakeS3Service(hclient *http.Client, retrySleep, retries int, endpoint, region, consistencyControl string, credentials *credentials.Credentials) *s3.S3 {
	s3Config := aws.NewConfig().
		WithRegion(region).
		WithCredentials(credentials).
		WithEndpoint(endpoint).
		WithHTTPClient(hclient).
		WithDisableComputeChecksums(true).
		WithS3ForcePathStyle(true)
	if retrySleep == 0 {
		s3Config.Retryer = NewCustomRetryer(retries)
	} else {
		s3Config.Retryer = NewRetryerWithSleep(retries, retrySleep)
	}
	s3Session, err := session.NewSession(s3Config)
	if err != nil {
		log.Fatal("Failed to create an S3 session", err)
	}

	svc := s3.New(s3Session)

	svc.Client.Handlers.Send.PushFront(func(r *request.Request) {
		userAgent := userAgentString + r.HTTPRequest.UserAgent()
		r.HTTPRequest.Header.Set("User-Agent", userAgent)
		if consistencyControl != "" {
			r.HTTPRequest.Header.Set("Consistency-Control", consistencyControl)
		}
	})

	return svc
}

// HistogramSummary will generate a power of 2 histogram summary where every successive bin is 2x the last one.
// The bins are latencies in milliseconds and the values are operation counts.
func HistogramSummary(h *hdrhistogram.Histogram) {
	dist := h.Distribution()

	bars := len(dist)
	var sum, start, end int64
	end = 2

	var counts []hdrhistogram.Bar
	var max int64

	// Collect all bins in a new histogram scaled such that each bin is a power of 2.
	for _, v := range dist {
		if v.To >= end*100 {
			counts = append(counts, hdrhistogram.Bar{From: start, To: end - 1, Count: sum})

			if sum > max {
				max = sum
			}

			start = end
			end = end << 1
			sum = 0
		}
		sum += v.Count
	}

	counts = append(counts, hdrhistogram.Bar{From: start, To: int64(math.Ceil(float64((dist[bars-1].To)/100) + 1)), Count: sum})

	if sum > max {
		max = sum
	}

	// The width we want to print one end of a bin interval with to maintain nice alignment.
	// Take the end of the last bin and convert it to milliseconds then count the number of
	// digits in the base 10 representation.
	intervalWidth := int(math.Floor(math.Log10(float64(dist[bars-1].To)/100) + 1))

	// Count the number of digits in the base 10 representation of the maximum count value
	// in all bins. This will be used to set the width of all other bin counts.
	countWidth := int(math.Floor(math.Log10(float64(max)) + 1))

	fmt.Printf("%-[1]*[2]s : Operations\n", intervalWidth*2, "Latency(ms)")

	// Maximum bin width to print out using '|'
	maxBinWidth := 80.0

	for _, v := range counts {
		bin := strings.Repeat("|", int(maxBinWidth*float64(v.Count)/float64(max)))
		fmt.Printf("%[1]*[3]d - %-[1]*[4]d : %-[2]*[5]d |%s\n", intervalWidth, countWidth, v.From, v.To, v.Count, bin)
	}
}
