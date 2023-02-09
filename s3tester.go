package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/codahale/hdrhistogram"
	"golang.org/x/time/rate"
)

const (
	// VERSION is displayed with help, bump when updating
	VERSION = "3.0.0"
	// for identifying s3tester requests in the user-agent header
	userAgentString = "s3tester/"

	// params.Overwrite options
	overwriteSameKey        = 1
	overwriteClobberThreads = 2
)

var (
	// Preserves quoted whitespace but does not support complex shell escapes
	commandExp = regexp.MustCompile(`"[^"]*?"|'[^']*?'|\S+`)
)

type results struct {
	// CumulativeResult has an incorrectly spelled JSON field to match historical results
	CumulativeResult  Result    `json:"cummulativeResult"`
	PerEndpointResult []*Result `json:"endpointResult,omitempty"`
	parameters        Parameters
}

func (r results) writeDetailedLog(detailLogFile io.Writer) error {
	command, err := json.Marshal(r.parameters)
	if err != nil {
		return err
	}
	fmt.Fprintf(detailLogFile, "Detailed log for command: %s\n", string(command))
	for _, v := range r.CumulativeResult.detailed {
		fmt.Fprintf(detailLogFile, "%f,%f\n", v.ts.Sub(r.CumulativeResult.detailed[0].ts).Seconds(), v.elapsed.Seconds())
	}
	return nil
}

func (r results) writeLatencyLog(latencyLogFile io.Writer) error {
	command, err := json.Marshal(r.parameters)
	if err != nil {
		return err
	}
	fmt.Fprintf(latencyLogFile, "Detailed log for command: %s\n", string(command))
	fmt.Fprintf(latencyLogFile, "from(ms) to(ms) count(operations)\n")
	for _, v := range r.CumulativeResult.latencies.Distribution() {
		fmt.Fprintf(latencyLogFile, "%f %f %d\n", float64(v.From)/1e2, float64(v.To)/1e2, v.Count)
	}
	return nil
}

// Result includes performance results of individual runs or aggregated runs
type Result struct {
	Category   string `json:"category,omitempty"`
	UniqueName string `json:"uniqueName,omitempty"`

	Endpoint    string `json:"endpoint,omitempty"`
	Bucket      string `json:"bucket,omitempty"`
	Operation   string `json:"operation,omitempty"`
	Concurrency int    `json:"concurrency,omitempty"`
	UniqObjNum  int    `json:"totalUniqueObjects"`
	Count       int    `json:"totalRequests"`
	Failcount   int    `json:"failedRequests"`

	TotalElapsedTime   float64 `json:"totalElapsedTime (ms)"`
	AverageRequestTime float64 `json:"averageRequestTime (ms)"`
	MinimumRequestTime float64 `json:"minimumRequestTime (ms)"`
	MaximumRequestTime float64 `json:"maximumRequestTime (ms)"`

	NominalRequestsPerSec float64            `json:"nominalRequestsPerSec"`
	ActualRequestsPerSec  float64            `json:"actualRequestsPerSec"`
	ContentThroughput     float64            `json:"contentThroughput (MB/s)"`
	AverageObjectSize     float64            `json:"averageObjectSize"`
	TotalObjectSize       int64              `json:"totalObjectSize"`
	Percentiles           map[string]float64 `json:"responseTimePercentiles(ms)"`

	elapsedSum  time.Duration
	data        []detail
	latencies   *hdrhistogram.Histogram
	startTime   time.Time
	elapsedTime time.Duration
	detailed    []detail
}

// NewResult constructs an instance of Result used to store performance metrics
func NewResult() Result {
	// Tracking values between 1 hundredth of a millisecond up to (10 hours in hundredths of milliseconds) to 4 significant digits.
	// At the maximum value of 10 hours the resolution will be 3.6 seconds or better.
	var tensOfMicrosecondsPerSecond int64 = 1e5

	// An hdr histogram has a fixed sized based on its precision and value range. This configuration uses 2.375 MiB per client (concurrency parameter).
	// The min and max values aren't strict, a certain range to either size will be kept but distant values are discarded. Notably zero is kept with
	// a min of 1.
	h := hdrhistogram.New(1, 10*3600*tensOfMicrosecondsPerSecond, 4)
	return Result{latencies: h}
}

// RecordLatency records the given latency
func (r *Result) RecordLatency(l time.Duration) {
	// Record latency as hundredths of milliseconds.
	r.latencies.RecordValue(l.Nanoseconds() / 1e4)
}

// detail holds metrics for individual S3 requests.
type detail struct {
	ts      time.Time
	elapsed time.Duration
}

// IsErrorRetryable returns true if the request that threw the given error can be retried
func IsErrorRetryable(err error) bool {
	if err != nil {
		if err, ok := err.(awserr.Error); ok {
			// inconsistency can lead to a multipart complete not seeing a part just uploaded.
			return err.Code() == "InvalidPart"
		}
	}
	return false
}

// CustomRetryer is a wrapper struct for the default SDK retryer
type CustomRetryer struct {
	maxRetries     int
	defaultRetryer request.Retryer
}

// ShouldRetry returns true if a given failed request is retryable
func (d CustomRetryer) ShouldRetry(r *request.Request) bool {
	if IsErrorRetryable(r.Error) {
		return true
	}
	return d.defaultRetryer.ShouldRetry(r)
}

// RetryRules returns the length of time to sleep in between retries of failed requests
func (d CustomRetryer) RetryRules(r *request.Request) time.Duration {
	return d.defaultRetryer.RetryRules(r)
}

// MaxRetries returns the max number of retries
func (d CustomRetryer) MaxRetries() int {
	return d.maxRetries
}

// NewCustomRetryer constructs a CustomRetryer, allowing configuration of the max number of retries to perform on failure
func NewCustomRetryer(retries int) *CustomRetryer {
	return &CustomRetryer{maxRetries: retries, defaultRetryer: client.DefaultRetryer{NumMaxRetries: retries}}
}

// RetryerWithSleep supplements CustomRetryer with a configurable sleep period
type RetryerWithSleep struct {
	base               *CustomRetryer
	retrySleepPeriodMs int
}

// RetryRules returns the sleep period in between retries
func (d RetryerWithSleep) RetryRules(r *request.Request) time.Duration {
	return time.Millisecond * time.Duration(d.retrySleepPeriodMs)
}

// ShouldRetry returns true if a given failed request is retryable
func (d RetryerWithSleep) ShouldRetry(r *request.Request) bool {
	return d.base.ShouldRetry(r)
}

// MaxRetries returns the max number of retries
func (d RetryerWithSleep) MaxRetries() int {
	return d.base.MaxRetries()
}

// NewRetryerWithSleep constructs a new RetryerWithSleep, allowing configuration of the max number of retries on failure and the length of time to sleep in between retries
func NewRetryerWithSleep(retries int, sleepPeriodMs int) *RetryerWithSleep {
	return &RetryerWithSleep{retrySleepPeriodMs: sleepPeriodMs, base: NewCustomRetryer(retries)}
}

func randMinMax(source *rand.Rand, min int64, max int64) int64 {
	return min + source.Int63n(max-min+1)
}

// DurationSetting is used to configure duration based S3 operations where we execute S3 operations for a fixed amount of time instead of a fixed amount of requests
type DurationSetting struct {
	applicable bool
	runstart   time.Time
	maxRunTime time.Duration
}

// NewDurationSetting constructs a DurationSetting instance used to configure duration based instances of s3tester
func NewDurationSetting(duration int, runStart time.Time) *DurationSetting {
	if duration != 0 {
		maxRunTime := time.Duration(duration * 1e9)
		return &DurationSetting{applicable: true, runstart: runStart, maxRunTime: maxRunTime}
	}
	return &DurationSetting{applicable: false}
}

func (ds *DurationSetting) enabled() bool {
	if ds.applicable {
		return time.Since(ds.runstart) >= ds.maxRunTime
	}
	return false
}

func runtest(ctx context.Context, config *Config, args Parameters, sysInterruptHandler SyscallHandler) results {
	c := make(chan Result, args.Concurrency)
	startTime := time.Now()
	sysInterruptHandler.setTestStartTime(startTime)

	startTestWorker(ctx, c, config, args, sysInterruptHandler)
	testResult := collectWorkerResult(c, args, startTime)
	processTestResult(&testResult, args)
	return testResult
}

func startTestWorker(ctx context.Context, c chan<- Result, config *Config, args Parameters, sysInterruptHandler SyscallHandler) {
	credential, err := loadCredentialProfile(args.Profile, args.NoSignRequest)
	if err != nil {
		log.Fatalf("Failed loading credentials.\nPlease specify env variable AWS_SHARED_CREDENTIALS_FILE if you put credential file other than AWS CLI configuration directory: %v", err)
	}
	limiter := rate.NewLimiter(args.ratePerSecond, 1)
	workersPerEndpoint := args.Concurrency / len(args.endpoints)
	var workerChans []*workerChan
	var workersWG sync.WaitGroup

	var mixedWorkloadDecoder *json.Decoder
	if args.MixedWorkload != "" {
		f, err := os.Open(args.MixedWorkload)
		if err != nil {
			log.Fatalf("Error opening mixed workload file: %s", err)
		}
		defer f.Close()
		mixedWorkloadDecoder = json.NewDecoder(f)
	}

	if mixedWorkloadDecoder != nil {
		workerChans = createChannels(args.Concurrency, &workersWG)

		go func() {
			if err := SetupOps(&args, workerChans, credential, mixedWorkloadDecoder); err != nil {
				log.Fatalf("Failed to generate requests: %v", err)
			}
			closeAllWorkerChannels(workerChans)
		}()
	}

	for i, endpoint := range args.endpoints {
		endpointStartTime := time.Now()
		for currentEndpointWorkerID := 0; currentEndpointWorkerID < workersPerEndpoint; currentEndpointWorkerID++ {
			workerID := i*workersPerEndpoint + currentEndpointWorkerID
			var workChan *workerChan
			// if mixed workload, setup a channel for each worker
			if mixedWorkloadDecoder != nil {
				workChan = workerChans[workerID]
				workChan.wg.Add(1)
			}

			go worker(ctx, c, config, args, credential, workerID, endpoint, endpointStartTime,
				limiter, workChan, sysInterruptHandler)
		}
	}
	if mixedWorkloadDecoder != nil {
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

// ReceiveS3Op receives s3 operations from the mixed workload decoder and dispatches them
func ReceiveS3Op(ctx context.Context, svc *s3.S3, httpClient *http.Client, args *Parameters, durationLimit *DurationSetting, limiter *rate.Limiter, workersChan *workerChan, r *Result, sysInterruptHandler SyscallHandler, debug bool) {
	for op := range workersChan.workChan {
		args.Size = int64(op.Size)
		args.Bucket = op.Bucket
		// need to mock up garbage metadata if it is a SUPD S3 event
		if op.Event == "updatemeta" {
			args.Metadata = metadataValue(int(op.Size))
		}
		sendRequest(ctx, svc, httpClient, op.Event, op.Key, args, r, limiter, sysInterruptHandler, debug)
		if durationLimit.enabled() {
			return
		}
	}
	workersChan.wg.Done()
}

func sendRequest(ctx context.Context, svc *s3.S3, httpClient *http.Client, opType string, keyName string, args *Parameters, r *Result, limiter *rate.Limiter, sysInterruptHandler SyscallHandler, debug bool) {
	r.Count++
	start := time.Now()
	err := DispatchOperation(ctx, svc, httpClient, opType, keyName, args, r, int64(args.Requests), sysInterruptHandler, debug)
	elapsed := time.Since(start)
	r.RecordLatency(elapsed)
	if err != nil {
		r.Failcount++
		log.Printf("Failed %s on object bucket '%s/%s': %v", args.Operation, args.Bucket, keyName, err)
	}
	r.elapsedSum += elapsed

	if r.data != nil {
		r.data = append(r.data, detail{start, elapsed})
	}

	if limiter.Limit() != rate.Inf {
		limiter.Wait(context.Background())
	}
}

func worker(ctx context.Context, results chan<- Result, config *Config, args Parameters, credentials *credentials.Credentials,
	id int, endpoint string, runstart time.Time, limiter *rate.Limiter, workerChan *workerChan, sysInterruptHandler SyscallHandler) {
	httpClient := MakeHTTPClient()
	svc := MakeS3Service(httpClient, config, &args, endpoint, credentials)
	var source *rand.Rand

	r := NewResult()
	r.Endpoint = endpoint
	r.Bucket = args.Bucket
	r.startTime = runstart
	var resultWG sync.WaitGroup
	resultWG.Add(1)

	sysInterruptHandler.addWorker()
	go sysInterruptHandler.contextCancelListener(ctx, svc, &r, &args, &resultWG)

	if isLoggingDetails {
		r.data = make([]detail, 0, args.Requests/(args.Concurrency)*args.attempts)
	}

	if args.hasRandomSize() {
		source = rand.New(rand.NewSource(args.max - args.min))
	}

	if args.hasRandomRange() {
		source = rand.New(rand.NewSource(args.randomRangeSize))
	}

	durationLimit := NewDurationSetting(args.Duration, runstart)

	if workerChan != nil {
		ReceiveS3Op(ctx, svc, httpClient, &args, durationLimit, limiter, workerChan, &r, sysInterruptHandler, config.Debug)
	} else {
		maxRequestsPerWorker := int64(args.Requests / args.Concurrency)
		durationWithoutRequests := args.Operation == "options" || args.Operation == "put" || args.Operation == "multipartput"
		if durationLimit.applicable && durationWithoutRequests {
			maxRequestsPerWorker = math.MaxInt64 / int64(args.Concurrency)
		}

		for j := int64(0); j < maxRequestsPerWorker; j++ {
			var keyName string
			switch args.Overwrite {
			case 1:
				keyName = args.Prefix
			case 2:
				keyName = args.Prefix + "-" + strconv.FormatInt(j, 10)
			default:
				keyName = args.Prefix + "-" + strconv.FormatInt(int64(id)*maxRequestsPerWorker+j, 10)
			}

			if args.Operation != "options" {
				r.UniqObjNum++
			}

			for repcount := 0; repcount < args.attempts; repcount++ {
				if source != nil {
					if args.hasRandomSize() {
						//size command line arg usually sets the size for each request we need to overwrite
						// with new random size per request
						newSize := randMinMax(source, args.min, args.max)
						args.Size = newSize
					} else if args.hasRandomRange() {
						// range is inclusive
						maxRangeStartIndex := args.randomRangeMax - args.randomRangeSize + 1
						rangeStart := randMinMax(source, args.randomRangeMin, maxRangeStartIndex)
						rangeEnd := rangeStart + args.randomRangeSize - 1
						args.Range = fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)
						args.Size = args.randomRangeSize
					}
				}
				sendRequest(ctx, svc, httpClient, args.Operation, keyName, &args, &r, limiter, sysInterruptHandler, config.Debug)

				if durationLimit.enabled() {
					results <- r
					return
				}
			}

			// If we reached maximum requests when duration has not finished then go back to the beginning of the keys
			if j == maxRequestsPerWorker-1 && durationLimit.applicable {
				j = int64(-1)
			}
		}
	}

	resultWG.Done()
	results <- r
}

func collectWorkerResult(c <-chan Result, args Parameters, startTime time.Time) results {
	workersPerEndpoint := args.Concurrency / len(args.endpoints)
	workerWorkload := args.Requests / args.Concurrency
	endpointResultMap := make(map[string]*Result)
	var detailed []detail
	if isLoggingDetails {
		detailed = make([]detail, 0)
	}

	for i := 0; i < args.Concurrency; i++ {
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
			finishEndpointResultCollection(endpointResultMap[r.Endpoint], r.startTime, args.attempts, args.Overwrite, workerWorkload)
		}

		if detailed != nil {
			detailed = append(detailed, r.data...)
		}
	}

	testResult := processEndpointResults(endpointResultMap, args.endpoints)
	testResult.correctResultsUniqObjCountWithOverwriteSetting(args.Overwrite, workerWorkload)
	testResult.CumulativeResult.Bucket = args.Bucket
	testResult.CumulativeResult.elapsedTime = time.Since(startTime)
	testResult.CumulativeResult.detailed = detailed
	return testResult
}

func finishEndpointResultCollection(endpointResult *Result, startTime time.Time, repeat, overwrite, workload int) {
	endpointResult.elapsedTime = time.Since(startTime)

	if endpointResult.UniqObjNum < endpointResult.Failcount {
		endpointResult.UniqObjNum = 0
	} else {
		endpointResult.UniqObjNum -= endpointResult.Failcount
	}

	endpointResult.correctEndpointUniqObjCountWithOverwriteSetting(overwrite, workload)
}

func mergeResult(aggregateResults, r *Result) {
	aggregateResults.latencies.Merge(r.latencies)
	aggregateResults.TotalObjectSize += r.TotalObjectSize
	aggregateResults.UniqObjNum += r.UniqObjNum
	aggregateResults.Count += r.Count
	aggregateResults.Failcount += r.Failcount
	aggregateResults.elapsedSum += r.elapsedSum
}

func (r *Result) correctEndpointUniqObjCountWithOverwriteSetting(overwrite, workload int) {
	// this function should only be invoked by an endpoint result
	// unique obj count should be consist with overwrite setting
	if overwrite == overwriteSameKey && r.UniqObjNum > 0 {
		r.UniqObjNum = 1
	} else if overwrite == overwriteClobberThreads && r.UniqObjNum > workload {
		r.UniqObjNum = workload
	}
}

func (r *results) correctResultsUniqObjCountWithOverwriteSetting(overwrite, workload int) {
	if overwrite == overwriteSameKey {
		r.CumulativeResult.UniqObjNum = 1
	} else if overwrite == overwriteClobberThreads {
		r.CumulativeResult.UniqObjNum = workload
	}
}

// this function will return a results struct will all the worker results merged
func processEndpointResults(endpointResultMap map[string]*Result, endpoints []string) results {
	if len(endpoints) == 1 {
		testResult := results{}
		testResult.CumulativeResult = *endpointResultMap[endpoints[0]]
		return testResult
	}
	testResult := results{}
	testResult.CumulativeResult = NewResult()
	testResult.CumulativeResult.Endpoint = strings.Join(endpoints, textSeriesSeparator)
	testResult.PerEndpointResult = make([]*Result, len(endpointResultMap))
	counter := 0
	for _, endpointResult := range endpointResultMap {
		testResult.PerEndpointResult[counter] = endpointResult
		counter++
		mergeResult(&testResult.CumulativeResult, endpointResult)
	}
	return testResult
}

func processTestResult(testResult *results, args Parameters) {
	testResult.parameters = args
	cumulativeResult := &testResult.CumulativeResult
	cumulativeResult.Operation = args.Operation
	cumulativeResult.Concurrency = args.Concurrency
	setupResultStat(cumulativeResult)

	for _, endpointResult := range testResult.PerEndpointResult {
		setupResultStat(endpointResult)
	}

	cumulativeResult.Category = args.Bucket + "-" + cumulativeResult.Operation + "-" + strconv.Itoa(cumulativeResult.Concurrency) + "-" + strconv.FormatInt(cumulativeResult.TotalObjectSize, 10)
	rand.Seed(time.Now().Unix())
	cumulativeResult.UniqueName = cumulativeResult.Category + "-" + time.Now().UTC().Format(time.RFC3339) + "-" + strconv.Itoa(rand.Intn(100))
}

func setupResultStat(testResult *Result) {
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

func calcStats(results *Result, concurrency int, elapsedTime time.Duration) {
	mean := results.elapsedSum / time.Duration(results.Count)
	results.AverageRequestTime = float64(mean) / float64(time.Millisecond)
	results.NominalRequestsPerSec = 1.0 / mean.Seconds() * float64(concurrency)
	results.ActualRequestsPerSec = float64(results.Count) / elapsedTime.Seconds()
	results.ContentThroughput = float64(results.TotalObjectSize) / 1024 / 1024 / elapsedTime.Seconds()
	results.AverageObjectSize = float64(results.TotalObjectSize) / float64(results.Count)
}

func roundResult(results *Result) {
	results.NominalRequestsPerSec = roundFloat(results.NominalRequestsPerSec, 3)
	results.ActualRequestsPerSec = roundFloat(results.ActualRequestsPerSec, 3)
	results.ContentThroughput = roundFloat(results.ContentThroughput, 6)
	results.AverageObjectSize = roundFloat(results.AverageObjectSize, 0)
}

var percentiles = []float64{50, 75, 90, 95, 99, 99.9}

func processPercentiles(results *Result) {
	results.Percentiles = make(map[string]float64)
	for _, percentile := range percentiles {
		quantileValue := float64(results.latencies.ValueAtQuantile(percentile)) / 1e2
		results.Percentiles[formatFloat(percentile)] = quantileValue
	}
}

func roundFloat(in float64, point int) float64 {
	return float64(int(math.Round(in*math.Pow10(point)))) / math.Pow10(point)
}

func main() {
	config, err := parse(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}

	if config.CPUProfile != "" {
		f, err := os.Create(config.CPUProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	if config.Describe {
		b, err := json.MarshalIndent(Workload{Workload: config.worklist}, "", "    ")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(b))
	} else {
		isLoggingDetails = config.LogDetail != ""
		results := executeTester(context.Background(), config)
		failCount, err := handleTesterResults(config, results)
		if err != nil {
			log.Fatal(err)
		}
		if failCount > 0 {
			pprof.StopCPUProfile()
			// finalizer for 'f' will be invoked on exit
			os.Exit(1)
		}
	}
}

var isLoggingDetails bool

func executeTester(ctx context.Context, config *Config) []results {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	syscallCatcher := syscallSignal(make(chan os.Signal, 1))
	go syscallCatcher.run(cancel)

	results := make([]results, 0)
	for _, task := range config.worklist {
		sysInterruptHandler := NewSyscallParams(task)
		go attach(ctx, sysInterruptHandler, &results, config, task)
		results = append(results, executeSingleTest(ctx, config, task, sysInterruptHandler))
		sysInterruptHandler.detach()
	}

	return results
}

// SyscallHandler is an interface for exiting the program cleanly when a system interrupt is captured
type SyscallHandler interface {
	contextCancelListener(ctx context.Context, svc *s3.S3, testResult *Result, args *Parameters, resultWG *sync.WaitGroup)
	processAndPrintCollectedResults(results *[]results, config *Config, args Parameters)
	detach()
	setTestStartTime(time time.Time)
	addWorker()
	addMultipartUpload(key, bucket string, uploadID string)
	doneMultipartUpload(key, bucket string, uploadID string)
	abortMultipartRequests(svc *s3.S3)
	done() <-chan struct{}
}

// SyscallCatcher is an interface for run() which captures system interrupts
type SyscallCatcher interface {
	run(syscallHandleFunc func())
}

// SyscallParams is used to stop the workers and collect their results when a system interrupt is captured
type SyscallParams struct {
	workersInProgress          *sync.WaitGroup
	multipartUploads           map[string][2]string
	mpUploadsMutex             *sync.Mutex
	stopAllBackgroundListeners chan struct{}
	results                    chan Result
	startTime                  time.Time
}

type syscallSignal chan os.Signal

// cancel context by calling given syscallHandleFunc
// when system interrupt is raised.
func (catcher syscallSignal) run(syscallHandleFunc func()) {
	signal.Notify(catcher, syscall.SIGINT)
	defer signal.Stop(catcher)

	<-catcher
	syscallHandleFunc()
}

// NewSyscallParams constructs a SyscallParams instance used to stop and collect workers' results when a system interrupt is captured
func NewSyscallParams(args Parameters) *SyscallParams {
	syscallParams := SyscallParams{
		workersInProgress:          &sync.WaitGroup{},
		multipartUploads:           make(map[string][2]string),
		mpUploadsMutex:             &sync.Mutex{},
		stopAllBackgroundListeners: make(chan struct{}, 1),
		results:                    make(chan Result, args.Concurrency),
		startTime:                  time.Now(),
	}
	return &syscallParams
}

// call processAndPrintCollectedResults if context is cancelled.
func attach(ctx context.Context, handler SyscallHandler, results *[]results,
	config *Config, args Parameters) {
	select {
	case <-ctx.Done():
		handler.processAndPrintCollectedResults(results, config, args)
	case <-handler.done():
		return
	}
}

// Wait until all workers push their results into the handler.
// Once results are ready, process and print them before exiting the program.
func (handler *SyscallParams) processAndPrintCollectedResults(results *[]results, config *Config, args Parameters) {
	handler.workersInProgress.Wait()
	testResult := collectWorkerResult(handler.results, args, handler.startTime)
	processTestResult(&testResult, args)
	*results = append(*results, testResult)
	handleTesterResults(config, *results)
	os.Exit(0)
}

// Signal all background listeners for syscall handling to shut down
// and wait until they all exit.
// Listeners include: contextCancelListener(...), attach(...)
func (handler *SyscallParams) detach() {
	close(handler.stopAllBackgroundListeners)
	handler.workersInProgress.Wait()
}

func (handler *SyscallParams) setTestStartTime(time time.Time) {
	handler.startTime = time
}

func (handler *SyscallParams) addWorker() {
	handler.workersInProgress.Add(1)
}

func (handler *SyscallParams) addMultipartUpload(key, bucket string, uploadID string) {
	handler.mpUploadsMutex.Lock()
	handler.multipartUploads[uploadID] = [2]string{bucket, key}
	handler.mpUploadsMutex.Unlock()
}

func (handler *SyscallParams) doneMultipartUpload(key, bucket string, uploadID string) {
	handler.mpUploadsMutex.Lock()
	delete(handler.multipartUploads, uploadID)
	handler.mpUploadsMutex.Unlock()
}

func (handler *SyscallParams) done() <-chan struct{} {
	return handler.stopAllBackgroundListeners
}

// When context is cancelled, save test result that has been run so far.
// Exit if test is done (= handler detached)
func (handler *SyscallParams) contextCancelListener(ctx context.Context, svc *s3.S3, testResult *Result, args *Parameters, resultWG *sync.WaitGroup) {
	defer handler.workersInProgress.Done()

	select {
	case <-ctx.Done():
		if args.Operation == "multipartput" {
			handler.abortMultipartRequests(svc)
		}
		resultWG.Wait()
		handler.results <- *testResult
	case <-handler.done():
		return
	}
}

func (handler *SyscallParams) abortMultipartRequests(svc *s3.S3) {
	handler.mpUploadsMutex.Lock()
	defer handler.mpUploadsMutex.Unlock()

	for uploadID := range handler.multipartUploads {
		params := &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(handler.multipartUploads[uploadID][0]),
			Key:      aws.String(handler.multipartUploads[uploadID][1]),
			UploadId: aws.String(uploadID),
		}
		_, err := svc.AbortMultipartUpload(params)
		if err != nil {
			log.Printf("Failed to abort multipart upload on system interrupt: %s", err)
		}
	}
}

func executeSingleTest(ctx context.Context, config *Config, test Parameters, sysInterruptHandler SyscallHandler) results {
	if test.Wait != 0 {
		time.Sleep(time.Duration(test.Wait) * time.Second)
	}

	if err := ExecuteCommand(test.CommandBefore); err != nil {
		log.Fatal(err)
	}

	workResult := runtest(ctx, config, test, sysInterruptHandler)

	if err := ExecuteCommand(test.CommandAfter); err != nil {
		log.Fatal(err)
	}

	return workResult
}

func handleTesterResults(config *Config, testResults []results) (int, error) {
	failCount := 0

	// handle the errors in the end, so the results will still be rendered
	var detailLogFile *os.File
	var detailLogErr error
	if config.LogDetail != "" {
		if detailLogFile, detailLogErr = os.Create(config.LogDetail); detailLogFile != nil {
			defer detailLogFile.Close()
		}
	}

	var latencyLogFile *os.File
	var latencyLogErr error
	if config.LogLatency != "" {
		if latencyLogFile, latencyLogErr = os.Create(config.LogLatency); latencyLogFile != nil {
			defer latencyLogFile.Close()
		}
	}

	// Collect printable results
	printableResults := make([]results, 0)
	for _, testResult := range testResults {
		printableResults = append(printableResults, testResult)
	}

	// Print the printable results
	if config.JSON {
		printJSONResults(printableResults, config.Workload)
	} else {
		printReadableResults(printableResults)
	}

	for _, testResult := range testResults {
		if detailLogFile != nil {
			if err := testResult.writeDetailedLog(detailLogFile); err != nil {
				detailLogErr = err
			}
		}

		if latencyLogFile != nil {
			if err := testResult.writeLatencyLog(latencyLogFile); err != nil {
				latencyLogErr = err
			}
		}

		failCount += testResult.CumulativeResult.Failcount
	}

	if detailLogErr != nil {
		return failCount, fmt.Errorf("Error writing detailed log to file: %v", detailLogErr)
	}
	if latencyLogErr != nil {
		return failCount, fmt.Errorf("Error writing latency log to file: %v", latencyLogErr)
	}

	return failCount, nil
}

func printReadableResults(testResults []results) {
	for _, testResult := range testResults {
		if len(testResult.PerEndpointResult) > 1 {
			fmt.Println("\n\t--- Result per Endpoint ---")
			for _, endpointResult := range testResult.PerEndpointResult {
				printResult(*endpointResult)
			}
		}

		fmt.Println("\n\t--- Total Results ---")
		fmt.Printf("Operation: %s\n", testResult.CumulativeResult.Operation)
		printResult(testResult.CumulativeResult)
		HistogramSummary(testResult.CumulativeResult.latencies)
	}
}

func printResult(results Result) {
	fmt.Printf("Endpoint: %s\n", results.Endpoint)
	fmt.Printf("Concurrency: %d\n", results.Concurrency)
	fmt.Printf("Total number of requests: %d\n", results.Count)

	if results.UniqObjNum != 0 {
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
	fmt.Printf("Total Object Size: %v\n", results.TotalObjectSize)

	printResponseTimeDistribution(results.Percentiles)
}

func printResponseTimeDistribution(percentilesMap map[string]float64) {
	fmt.Println("Response Time Percentiles")
	for _, percentile := range percentiles {
		key := formatFloat(percentile)
		quantileValue := formatFloat(percentilesMap[key])
		fmt.Printf("%-5v  :   %-5v\n", key, quantileValue+" ms")
	}
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

func printJSONResults(testResults []results, configWorkload string) {
	if len(testResults) == 0 {
		fmt.Println("{}")
		return
	}

	var jsonResults []byte
	var err error
	if configWorkload == "" {
		// For backwards compatability (single result expected - no workload)
		jsonResults, err = json.Marshal(testResults[0])
	} else {
		jsonResults, err = json.Marshal(testResults)
	}
	if err != nil {
		log.Printf("Error when parsing result to json: %v", err)
		return
	}
	fmt.Println(string(jsonResults))
}

// MakeHTTPClient constructs a new http.Client using s3tester's default settings
func MakeHTTPClient() *http.Client {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}

	return &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   60 * time.Second,
				KeepAlive: 30 * time.Second,
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

// MakeS3Service creates a new Amazon S3 session from the given parameters
func MakeS3Service(client *http.Client, config *Config, args *Parameters, endpoint string, credentials *credentials.Credentials) *s3.S3 {
	s3Config := aws.NewConfig().
		WithRegion(args.Region).
		WithCredentials(credentials).
		WithEndpoint(endpoint).
		WithHTTPClient(client).
		WithDisableComputeChecksums(true)
	if args.AddressingStyle == addressingStylePath {
		s3Config.WithS3ForcePathStyle(true)
	}
	if config.RetrySleep == 0 {
		s3Config.Retryer = NewCustomRetryer(config.Retries)
	} else {
		s3Config.Retryer = NewRetryerWithSleep(config.Retries, config.RetrySleep)
	}
	s3Session, err := session.NewSession(s3Config)
	if err != nil {
		log.Fatal("Failed to create an S3 session", err)
	}

	svc := s3.New(s3Session)

	svc.Client.Handlers.Send.PushFront(func(r *request.Request) {
		userAgent := userAgentString + r.HTTPRequest.UserAgent()
		r.HTTPRequest.Header.Set("User-Agent", userAgent)
		for key, value := range args.Header {
			r.HTTPRequest.Header.Set(key, value)
		}
	})

	svc.Client.Handlers.Build.PushFront(func(r *request.Request) {
		if args.QueryParams != "" {
			q := r.HTTPRequest.URL.Query()
			values, err := url.ParseQuery(args.QueryParams)
			if err != nil {
				log.Fatalf("Unable to parse query params: %v", err)
			}

			for k, v := range values {
				for _, s := range v {
					q.Add(k, s)
				}
			}
			r.HTTPRequest.URL.RawQuery = q.Encode()
		}
	})

	if config.Debug {
		svc.Client.Handlers.UnmarshalMeta.PushBack(func(r *request.Request) {
			if r.HTTPResponse.StatusCode < 200 || r.HTTPResponse.StatusCode >= 300 {
				b, err := readErrorResponse(r)
				if err != nil {
					log.Printf("request %v %v not successful: %v %v", r.HTTPRequest.Method, r.HTTPRequest.URL.EscapedPath(), r.HTTPResponse.StatusCode, err)
				} else {
					log.Printf("request %v %v not successful: %v %v", r.HTTPRequest.Method, r.HTTPRequest.URL.EscapedPath(), r.HTTPResponse.StatusCode, strings.ReplaceAll(string(b), "\n", ""))
				}
			}
		})
	}

	return svc
}

// Reads the first 1000 bytes of the response body for printing to stderr, and restores the response body so it can still be read elsewhere
func readErrorResponse(r *request.Request) ([]byte, error) {
	buffer := make([]byte, 1000)
	_, err := r.HTTPResponse.Body.Read(buffer)
	r.HTTPResponse.Body = ioutil.NopCloser(io.MultiReader(bytes.NewReader(buffer), r.HTTPResponse.Body))
	if err == io.EOF {
		return buffer, nil
	}
	return buffer, err
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

	// If there are no results, display a count of zero for the maximum range that could have been recorded
	var intervalWidth int
	var lastCount hdrhistogram.Bar
	if bars == 0 {
		intervalWidth = 1
		lastCount = hdrhistogram.Bar{From: h.LowestTrackableValue(), To: h.HighestTrackableValue() / 100, Count: 0}
	} else {
		// The width we want to print one end of a bin interval with to maintain nice alignment.
		// Take the end of the last bin and convert it to milliseconds then count the number of
		// digits in the base 10 representation.
		intervalWidth = int(math.Floor(math.Log10(float64(dist[bars-1].To)/100) + 1))
		lastCount = hdrhistogram.Bar{From: start, To: int64(math.Ceil(float64((dist[bars-1].To)/100) + 1)), Count: sum}
	}
	counts = append(counts, lastCount)

	if sum > max {
		max = sum
	}

	// Count the number of digits in the base 10 representation of the maximum count value
	// in all bins. This will be used to set the width of all other bin counts.
	countWidth := int(math.Floor(math.Log10(float64(max)) + 1))
	if countWidth <= 0 {
		countWidth = 1
	}

	fmt.Printf("%-[1]*[2]s : Operations\n", intervalWidth*2, "Latency(ms)")

	// Maximum bin width to print out using '|'
	maxBinWidth := 80.0

	for _, v := range counts {
		bin := ""
		if max != 0 {
			bin = strings.Repeat("|", int(maxBinWidth*float64(v.Count)/float64(max)))
		}
		fmt.Printf("%[1]*[3]d - %-[1]*[4]d : %-[2]*[5]d |%s\n", intervalWidth, countWidth, v.From, v.To, v.Count, bin)
	}
}

// HasPrefixAndSuffix returns true if s has the provided leading prefix and it's also the trailing suffix.
func HasPrefixAndSuffix(s string, prefix string) bool {
	if len(s) >= 2*len(prefix) && strings.HasPrefix(s, prefix) && strings.HasSuffix(s, prefix) {
		return true
	}
	return false
}

// ParseCommand splits the given command string into separate arguments
func ParseCommand(command string) []string {
	args := commandExp.FindAllString(command, -1)
	for i, arg := range args {
		// Exclude a single type of surrounding quotes
		if HasPrefixAndSuffix(arg, `"`) || HasPrefixAndSuffix(arg, `'`) {
			args[i] = args[i][1 : len(args[i])-1]
		} else if strings.HasPrefix(arg, "$") {
			// directly substitute variables from env
			if strings.HasPrefix(arg, "${") && strings.HasSuffix(arg, "}") {
				args[i] = os.Getenv(arg[2 : len(arg)-1])
			} else {
				args[i] = os.Getenv(arg[1:])
			}
		}
	}
	return args
}

// ExecuteCommand parses the given command and executes it
func ExecuteCommand(command string) error {
	args := ParseCommand(command)
	if len(args) != 0 {
		cmd := exec.Command(args[0], args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return err
		}
	}
	return nil
}
