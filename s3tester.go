package main

import (
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/golang/time/rate"
	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	// VERSION is displayed with help, bump when updating
	VERSION = "1.1.7"
	// for identifying s3tester requests in the user-agent header
	userAgentString = "s3tester/"
)

// result holds the performance metrics for a single goroutine that are later aggregated.
type result struct {
	uniqObjNum        int
	count             int
	failcount         int
	sumObjSize        int64
	elapsedSum        time.Duration
	elapsedSumSquared float64
	min               time.Duration
	max               time.Duration
	data              []detail
}

// detail holds metrics for individual S3 requests.
type detail struct {
	ts      time.Time
	elapsed time.Duration
}

// stats hold aggregate statistics
// nominalThroughput and actualThroughput measure the throughput with units of #requests/s
// bodyThrougput measures the throughput with units of MB/s
type stats struct {
	mean             time.Duration
	stddev           time.Duration
	nominalThrougput float64
	actualThrougput  float64
	bodyThrougput    float64
}

// intFlag is used to differentiate user-defined value from default value
type intFlag struct {
	set   bool
	value int
}

type parameters struct {
	concurrency       int
	osize             int64
	endpoint          string
	optype            string
	bucketname        string
	objectprefix      string
	tagging           string
	metadata          string
	ratePerSecond     rate.Limit
	logging           bool
	objrange          string
	reducedRedundancy bool
	overwrite         int
	retries           int
	retrySleep        int
	lockstep          bool
	repeat            int
	region            string
	partsize          int64
	verify            bool
	nrequests         *intFlag
	duration          *intFlag
}

type CustomRetryer struct {
	maxRetries         int
	retrySleepPeriodMs int
	defaultRetryer     request.Retryer
}

func (d CustomRetryer) RetryRules(r *request.Request) time.Duration {
	return time.Millisecond * time.Duration(d.retrySleepPeriodMs)
}

func (d CustomRetryer) ShouldRetry(r *request.Request) bool {
	return d.defaultRetryer.ShouldRetry(r)
}

func (d CustomRetryer) MaxRetries() int {
	return d.maxRetries
}

func parseMetadataString(metaString string) map[string]*string {
	meta := make(map[string]*string)
	if metaString != "" {
		// metadata is supplied like so: key1=value2&key2=value2&...
		pairs := strings.Split(metaString, "&")
		for index := range pairs {
			keyvalue := strings.Split(pairs[index], "=")
			if len(keyvalue) != 2 {
				log.Fatal("Invalid metadata string supplied. Must be formatted like: 'key1=value1&key2=value2...'")
			}
			meta[keyvalue[0]] = &keyvalue[1]
		}
	}
	return meta
}

func (intf *intFlag) Set(content string) error {
	val, err := strconv.Atoi(content)
	if err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	intf.value = val
	intf.set = true
	return nil
}

func (intf *intFlag) String() string {
	return strconv.Itoa(intf.value)
}

func NewintFlag(content int) intFlag {
	intf := intFlag{}
	intf.value = content
	intf.set = false
	return intf
}

// Retrieves objects from Amazon S3.
func identityGetObject(c *s3.S3, input *s3.GetObjectInput, verify bool) (output *s3.GetObjectOutput, err error) {
	req, out := c.GetObjectRequest(input)
	output = out
	req.HTTPRequest.Header.Set("Accept-Encoding", "identity")
	err = req.Send()
	if err == nil && req.HTTPResponse.Body != nil {
		if !verify {
			io.Copy(ioutil.Discard, req.HTTPResponse.Body)
		} else {
			key := []byte(*input.Key)
			buffer := make([]byte, 1024)
			index := 0
			var read int
			var readError error = nil
			// keep reading until we reach EOF (or some other error)
		loop:
			for readError == nil {
				read, readError = req.HTTPResponse.Body.Read(buffer)
				for i := 0; i < read; i++ {
					if buffer[i] != key[index%len(key)] {
						readError = errors.New("Retrieved data different from expected")
						break loop
					}
					index++
				}
			}

			if readError != io.EOF {
				err = readError
			}
		}
		req.HTTPResponse.Body.Close()
	}
	return
}

var detailed []detail

func runtest(args parameters) (float64, bool) {
	c := make(chan result, args.concurrency)
	creds := credentials.NewEnvCredentials()

	sc := s3.StorageClassStandard
	if args.reducedRedundancy {
		sc = s3.StorageClassReducedRedundancy
	}

	limiter := rate.NewLimiter(args.ratePerSecond, 1)

	var maxRunTime time.Duration
	if args.duration.set {
		maxRunTime = time.Duration(args.duration.value * 1000000000)
	}

	runstart := time.Now()
	for i := 0; i < args.concurrency; i++ {
		var obj *DummyReader
		if args.optype == "multipartput" {
			obj = NewDummyReader(args.partsize)
		} else {
			obj = NewDummyReader(args.osize)
		}

		go func(id int) {
			tlc := &tls.Config{
				InsecureSkipVerify: true,
			}
			tr := &http.Transport{
				TLSClientConfig:    tlc,
				DisableCompression: true,
				Dial: (&net.Dialer{
					Timeout:   60 * time.Second,
					KeepAlive: 180 * time.Second,
				}).Dial,
				TLSHandshakeTimeout: 60 * time.Second,
				IdleConnTimeout:     0,
			}
			httpClient := &http.Client{Transport: tr}

			s3Config := aws.NewConfig().
				WithRegion(args.region).
				WithCredentials(creds).
				WithEndpoint(args.endpoint).
				WithHTTPClient(httpClient).
				WithDisableComputeChecksums(true).
				WithS3ForcePathStyle(true)
			if args.retrySleep == 0 {
				s3Config.Retryer = client.DefaultRetryer{args.retries}
			} else {
				s3Config.Retryer = &CustomRetryer{maxRetries: args.retries, retrySleepPeriodMs: args.retrySleep, defaultRetryer: client.DefaultRetryer{args.retries}}
			}
			s3Session, err := session.NewSession(s3Config)
			if err != nil {
				log.Fatal("Failed to create an S3 session", err)
			}

			svc := s3.New(s3Session)

			svc.Client.Handlers.Send.PushFront(func(r *request.Request) {
				userAgent := userAgentString + r.HTTPRequest.UserAgent()
				r.HTTPRequest.Header.Set("User-Agent", userAgent)
			})

			var r = result{0, 0, 0, 0, 0, 0, 0, 0, []detail{}}

			if args.logging {
				r.data = make([]detail, 0, args.nrequests.value/args.concurrency*args.repeat)
			}

			var runNum uint64

			if args.duration.set {
				runNum = math.MaxUint64
			} else {
				runNum = uint64(args.nrequests.value/args.concurrency) - 1
			}

			for j := 0; uint64(j) <= runNum; j++ {
				obj.Offset = 0
				limiter.Wait(context.Background())

				var start time.Time
				var err error
				var keyName = args.objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(j)

				switch {
				case args.overwrite == 1:
					keyName = args.objectprefix
				case args.overwrite == 2:
					keyName = args.objectprefix + "-" + strconv.Itoa(j)

				}

				_ = args.lockstep

				for repcount := 0; repcount < args.repeat; repcount++ {
					switch {
					case args.optype == "options":
						r.count++
						if req, err := http.NewRequest("OPTIONS", args.endpoint+"/", nil); err != nil {
							log.Print("Creating OPTIONS request failed:", err)
							r.failcount++
						} else {
							start = time.Now()
							if resp, doerr := httpClient.Do(req); doerr != nil {
								log.Print("OPTIONS request failed:", doerr)
								r.failcount++
							} else {
								io.Copy(ioutil.Discard, resp.Body)
								resp.Body.Close()
							}
						}

					case args.optype == "put":
						cl := args.osize
						obj.SetData(keyName)
						r.uniqObjNum++

						params := &s3.PutObjectInput{
							Bucket:        aws.String(args.bucketname),
							Key:           aws.String(keyName),
							ContentLength: &cl,
							Body:          obj,
							StorageClass:  &sc,
							Metadata:      parseMetadataString(args.metadata),
						}
						if args.tagging != "" {
							params.SetTagging(args.tagging)
						}
						r.count++
						start = time.Now()
						_, err = svc.PutObject(params)
						if err == nil {
							r.sumObjSize += cl
						}

					case args.optype == "puttagging":
						// generate tagging struct
						var tags s3.Tagging
						tags.TagSet = make([]*s3.Tag, 0)
						if args.tagging != "" {
							// tags are supplied like so: tag1=value2&tag2=value&...
							pairs := strings.Split(args.tagging, "&")
							for index := range pairs {
								keyvalue := strings.Split(pairs[index], "=")
								if len(keyvalue) != 2 {
									log.Fatal("Invalid tagging string supplied. Must be formatted like: 'tag1=value1&tage2=value2...'")
								}
								t := s3.Tag{
									Key:   aws.String(keyvalue[0]),
									Value: aws.String(keyvalue[1]),
								}
								tags.TagSet = append(tags.TagSet, &t)
							}
						}

						params := &s3.PutObjectTaggingInput{
							Bucket:  aws.String(args.bucketname),
							Key:     aws.String(keyName),
							Tagging: &tags,
						}
						r.uniqObjNum++
						r.count++
						_, err = svc.PutObjectTagging(params)

					case args.optype == "updatemeta":
						// generate tagging struct
						params := &s3.CopyObjectInput{
							Bucket:            aws.String(args.bucketname),
							Key:               aws.String(keyName),
							CopySource:        aws.String(args.bucketname + "/" + keyName),
							MetadataDirective: aws.String("REPLACE"),
							Metadata:          parseMetadataString(args.metadata),
						}
						r.uniqObjNum++
						r.count++
						_, err = svc.CopyObject(params)

					case args.optype == "multipartput":
						cl := args.partsize
						obj.SetData(keyName)
						r.uniqObjNum++
						params := &s3.CreateMultipartUploadInput{
							Bucket:       aws.String(args.bucketname),
							Key:          aws.String(keyName),
							StorageClass: &sc,
							Metadata:     parseMetadataString(args.metadata),
						}

						numparts := int64(math.Ceil(float64(args.osize) / float64(args.partsize)))
						// this is for if the last part won't be the same size
						lastobj := obj
						if numparts != args.osize/args.partsize {
							lastobj = NewDummyReader(args.osize - args.partsize*(numparts-1))
							lastobj.SetData(keyName)
						}

						r.count++
						start = time.Now()
						var output *s3.CreateMultipartUploadOutput
						output, err = svc.CreateMultipartUpload(params)

						if err == nil {
							uploadId := output.UploadId
							partdata := make([]*s3.CompletedPart, 0, numparts)
							uparams := &s3.UploadPartInput{
								Bucket:        aws.String(args.bucketname),
								Key:           aws.String(keyName),
								ContentLength: &cl,
								Body:          obj,
								UploadId:      uploadId,
							}
							for partnum := int64(1); partnum <= numparts-1; partnum++ {
								/* In a more realistic scenario we would want to upload parts concurrently,
								but concurrency is already one of the test options... might want to figure out
								if/how this should consider the concurrency option before adding concurrency here.
								*/
								uparams.SetPartNumber(partnum)

								var uoutput *s3.UploadPartOutput
								uoutput, err = svc.UploadPart(uparams)
								if err != nil {
									break
								}
								part := &s3.CompletedPart{}
								part.SetPartNumber(partnum)
								part.SetETag(*uoutput.ETag)
								partdata = append(partdata, part)
							}

							// Don't upload the last part if we had any part upload failures.
							if err == nil {
								uparams.SetBody(lastobj)
								uparams.SetContentLength(lastobj.Size)
								uparams.SetPartNumber(numparts)
								var uoutput *s3.UploadPartOutput
								uoutput, err = svc.UploadPart(uparams)
								if err == nil {
									part := &s3.CompletedPart{}
									part.SetPartNumber(numparts)
									part.SetETag(*uoutput.ETag)
									partdata = append(partdata, part)
								}
							}

							if err != nil {
								aparams := &s3.AbortMultipartUploadInput{
									Bucket:   aws.String(args.bucketname),
									Key:      aws.String(keyName),
									UploadId: uploadId,
								}
								svc.AbortMultipartUpload(aparams)
							} else {
								cparams := &s3.CompleteMultipartUploadInput{
									Bucket:   aws.String(args.bucketname),
									Key:      aws.String(keyName),
									UploadId: uploadId,
								}

								cpartdata := &s3.CompletedMultipartUpload{Parts: partdata}
								cparams.SetMultipartUpload(cpartdata)

								_, err = svc.CompleteMultipartUpload(cparams)
								if err == nil {
									r.sumObjSize += args.osize
								}
							}
						}

					case args.optype == "get":
						params := &s3.GetObjectInput{
							Bucket: aws.String(args.bucketname),
							Key:    aws.String(keyName),
							Range:  aws.String(args.objrange),
						}
						var out *s3.GetObjectOutput
						r.uniqObjNum++
						r.count++
						start = time.Now()
						out, err = identityGetObject(svc, params, args.verify)
						if err == nil {
							r.sumObjSize += *out.ContentLength
						}

					case args.optype == "head":
						params := &s3.HeadObjectInput{
							Bucket: aws.String(args.bucketname),
							Key:    aws.String(keyName),
						}
						r.uniqObjNum++
						r.count++
						start = time.Now()
						_, err = svc.HeadObject(params)

					case args.optype == "delete":
						params := &s3.DeleteObjectInput{
							Bucket: aws.String(args.bucketname),
							Key:    aws.String(keyName),
						}
						r.uniqObjNum++
						r.count++
						start = time.Now()
						_, err = svc.DeleteObject(params)

					case args.optype == "randget":
						var objnum int
						if runNum <= 0 {
							objnum = 0
						} else {
							objnum = rand.Intn(int(runNum))
						}
						getParams := &s3.GetObjectInput{
							Bucket: aws.String(args.bucketname),
							Key:    aws.String(args.objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(objnum)),
							Range:  aws.String(args.objrange),
						}
						var out *s3.GetObjectOutput
						r.uniqObjNum++
						r.count++
						start = time.Now()
						out, err = identityGetObject(svc, getParams, args.verify)
						if err == nil {
							r.sumObjSize += *out.ContentLength
						}

					case args.optype == "putget":
						cl := args.osize
						obj.SetData(keyName)
						r.uniqObjNum++
						putParams := &s3.PutObjectInput{
							Bucket:        aws.String(args.bucketname),
							Key:           aws.String(keyName),
							ContentLength: &cl,
							Body:          obj,
							StorageClass:  &sc,
							Metadata:      parseMetadataString(args.metadata),
						}
						if args.tagging != "" {
							putParams.SetTagging(args.tagging)
						}

						getParams := &s3.GetObjectInput{
							Bucket: aws.String(args.bucketname),
							Key:    aws.String(keyName),
							Range:  aws.String(args.objrange),
						}
						r.count++
						start = time.Now()
						_, err = svc.PutObject(putParams)
						if err == nil {
							var out *s3.GetObjectOutput
							r.sumObjSize += cl
							r.count++
							out, err = identityGetObject(svc, getParams, args.verify)
							if err == nil {
								r.sumObjSize += *out.ContentLength
							}
						}

					case args.optype == "putget9010r":
						cl := args.osize
						obj.SetData(keyName)
						r.uniqObjNum++
						putParams := &s3.PutObjectInput{
							Bucket:        aws.String(args.bucketname),
							Key:           aws.String(keyName),
							ContentLength: &cl,
							Body:          obj,
							StorageClass:  &sc,
							Metadata:      parseMetadataString(args.metadata),
						}
						if args.tagging != "" {
							putParams.SetTagging(args.tagging)
						}
						r.count++
						start = time.Now()
						_, err = svc.PutObject(putParams)
						if err == nil {
							r.sumObjSize += cl
						}
						if j%10 == 0 {
							objnum := rand.Intn(j + 1)
							getParams := &s3.GetObjectInput{
								Bucket: aws.String(args.bucketname),
								Key:    aws.String(args.objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(objnum)),
								Range:  aws.String(args.objrange),
							}
							if err == nil {
								var out *s3.GetObjectOutput
								r.count++
								out, err = identityGetObject(svc, getParams, args.verify)
								if err == nil {
									r.sumObjSize += *out.ContentLength
								}
							}
						}
					}
					if err != nil {
						r.failcount++
						awsErr, ok := err.(awserr.Error)
						if awsErr != nil && ok {
							log.Print("Failed:", awsErr.Code(), awsErr.Error(), awsErr.Message())
						} else {
							log.Print("Failed:", err.Error())
						}
					}
					elapsed := time.Since(start)

					if j == 0 {
						r.min = elapsed
					} else if elapsed < r.min {
						r.min = elapsed
					}

					if elapsed > r.max {
						r.max = elapsed
					}

					r.elapsedSum += elapsed
					r.elapsedSumSquared += elapsed.Seconds() * elapsed.Seconds()

					if args.logging {
						r.data = append(r.data, detail{start, elapsed})
					}

					if args.duration.set && time.Since(runstart) >= maxRunTime {
						c <- r
						return
					}
				}
			}
			c <- r
		}(i)
	}

	// Aggregate results across all clients.
	aggregateResults := result{0, 0, 0, 0, 0, 0, 0, 0, []detail{}}
	aggregateResults.min = math.MaxInt64

	if args.logging {
		detailed = make([]detail, 0, args.concurrency*aggregateResults.count)
	}

	for i := 0; i < args.concurrency; i++ {
		r := <-c
		aggregateResults.sumObjSize += r.sumObjSize
		aggregateResults.uniqObjNum += int(math.Ceil(float64(r.uniqObjNum) / float64(args.repeat)))
		aggregateResults.count += r.count
		aggregateResults.failcount += r.failcount
		aggregateResults.elapsedSum += r.elapsedSum
		aggregateResults.elapsedSumSquared += r.elapsedSumSquared

		if r.min < aggregateResults.min {
			aggregateResults.min = r.min
		}

		if r.max > aggregateResults.max {
			aggregateResults.max = r.max
		}

		if args.logging {
			detailed = append(detailed, r.data...)
		}
	}

	if aggregateResults.failcount > 0 && aggregateResults.uniqObjNum >= aggregateResults.failcount {
		aggregateResults.uniqObjNum -= aggregateResults.failcount
	}

	runelapsed := time.Since(runstart)
	printResults(aggregateResults, args.optype, args.concurrency, runelapsed)

	return float64(aggregateResults.count) / runelapsed.Seconds(), aggregateResults.failcount > 0
}

func calcStats(results result, concurrency int, runtime time.Duration) stats {
	var runStats stats

	runStats.mean = results.elapsedSum / time.Duration(results.count)

	stddev := math.Sqrt(results.elapsedSumSquared/float64(results.count) - runStats.mean.Seconds()*runStats.mean.Seconds())
	runStats.stddev = time.Duration(int64(stddev * 1000000000))

	runStats.nominalThrougput = 1.0 / runStats.mean.Seconds() * float64(concurrency)
	runStats.actualThrougput = float64(results.count) / runtime.Seconds()
	runStats.bodyThrougput = float64(results.sumObjSize) / 1024 / 1024 / runtime.Seconds()

	return runStats
}

func printResults(results result, optype string, concurrency int, runtime time.Duration) {
	runStats := calcStats(results, concurrency, runtime)

	fmt.Printf("Operation: %s\n", optype)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Total number of requests: %d\n", results.count)
	fmt.Printf("Total number of unique objects: %d\n", results.uniqObjNum)
	fmt.Printf("Failed requests: %d\n", results.failcount)
	fmt.Printf("Total elapsed time: %s\n", runtime)
	fmt.Printf("Average request time: %s\n", runStats.mean)
	fmt.Printf("Standard deviation: %s\n", runStats.stddev)
	fmt.Printf("Minimum request time: %s\n", results.min)
	fmt.Printf("Maximum request time: %s\n", results.max)
	fmt.Printf("Nominal requests/s: %.1f\n", runStats.nominalThrougput)
	fmt.Printf("Actual requests/s: %.1f\n", runStats.actualThrougput)
	fmt.Printf("Content throughput: %.6f MB/s\n", runStats.bodyThrougput)
}

func main() {
	optypes := []string{"put", "multipartput", "get", "puttagging", "updatemeta", "randget", "delete", "options", "head", "putget", "putget9010r"}
	operationListString := strings.Join(optypes[:], ", ")

	var nrequests intFlag
	var duration intFlag

	nrequests = NewintFlag(1000)

	flag.Var(&duration, "duration", "Test duration in seconds")
	flag.Var(&nrequests, "requests", "Total number of requests")

	var concurrency = flag.Int("concurrency", 1, "Maximum concurrent requests (0=scan concurrency, run with ulimit -n 16384)")
	var osize = flag.Int64("size", 30*1024, "Object size. Note that s3tester is not ideal for very large objects as the entire body must be read for v4 signing and the aws sdk does not support v4 chunked. Performance may degrade as size increases due to the use of v4 signing without chunked support")
	var endpoint = flag.String("endpoint", "https://127.0.0.1:18082", "target endpoint")
	var optype = flag.String("operation", "put", "operation type: "+operationListString)
	var bucketname = flag.String("bucket", "test", "bucket name (needs to exist)")
	var objectprefix = flag.String("prefix", "testobject", "object name prefix")
	var tagging = flag.String("tagging", "", "The tag-set for the object. The tag-set must be formatted as such: 'tag1=value1&tage2=value2'. Used for put, puttagging, putget and putget9010r.")
	var metadata = flag.String("metadata", "", "The metadata to use for the objects. The string must be formatted as such: 'key1=value1&key2=value2'. Used for put, updatemeta, multipartput, putget and putget9010r.")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var logdetail = flag.String("logdetail", "", "write detailed log to file")
	var maxRate = flag.Float64("ratelimit", math.MaxFloat64, "the total number of operations per second across all threads")
	var objrange = flag.String("range", "", "Specify range header for GET requests")
	var reducedRedundancy = flag.Bool("rr", false, "Reduced redundancy storage for PUT requests")
	var overwrite = flag.Int("overwrite", 0, "Turns a PUT/GET/HEAD into an operation on the same s3 key. (1=all writes/reads are to same object, 2=threads clobber each other but each write/read is to unique objects).")
	var retries = flag.Int("retries", 0, "Number of retry attempts. Default is 0.")
	var retrySleep = flag.Int("retrysleep", 0, "How long to sleep in between each retry in milliseconds. Default (0) is to use the default retry method which is an exponential backoff.")
	var lockstep = flag.Bool("lockstep", false, "Force all threads to advance at the same rate rather than run independently")
	var repeat = flag.Int("repeat", 1, "Repeat each S3 operation this many times")
	var region = flag.String("region", "us-east-1", "Region to send requests to")
	var partsize = flag.Int64("partsize", 5*(1<<20), "Size of each part (min 5MiB); only has an effect when a multipart put is used")
	var verify = flag.Bool("verify", false, "Verify the retrieved data on a get operation")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "This tool is for generating high performance S3 load against an S3 server.\n")
		fmt.Fprintf(os.Stderr, "Credentials are read from the environment variables AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY.\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Key naming is unique per key unless the 'overwrite' option is used. The naming is as follows:\n")
		fmt.Fprintf(os.Stderr, "    Key names are equal to \"<prefix>-N-M\" where N is 0..concurrency-1 and M is the request within that connection.\n")
		fmt.Fprintf(os.Stderr, "This means all the various client operations (GET, PUT, DELETE, etc) will use the same object names assuming the same parameters are used.\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "You can control how many client machine cores the tester uses by setting the GOMAXPROCS env variable, e.g. to use 8 cores:\n")
		fmt.Fprintf(os.Stderr, "    export GOMAXPROCS=8\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Each concurrent stream is done on a persistent HTTP connection.\n")
		fmt.Fprintf(os.Stderr, "You should expect the very first request on a persistent to take a little longer due to TLS handshake.\n")
		fmt.Fprintf(os.Stderr, "You can see this effect by setting requests=concurrency\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Client performance will vary based on your machine but as a rule of thumb a modern Core i7 \n")
		fmt.Fprintf(os.Stderr, "should be able to generate about 5000 30K PUTs/second (should saturate a 1GBps network interface) \n")
		fmt.Fprintf(os.Stderr, "and 20000 DELETE requests/s (with GOMAXPROCS=# of cores and concurrency>=# of cores)\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nVersion: "+VERSION+"\n")
	}
	flag.Parse()

	var ratePerSecond = rate.Limit(*maxRate)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var opTypeExists = false
	for op := range optypes {
		if optypes[op] == *optype {
			opTypeExists = true
		}
	}

	if !opTypeExists {
		log.Fatal("operation type must be one of: " + operationListString)
	}

	if nrequests.set && nrequests.value <= 0 {
		log.Fatal("Number of requests must be > 0")
	}

	if *concurrency <= 0 {
		log.Fatal("Concurrency must be > 0")
	}

	if duration.set && nrequests.set {
		log.Fatal("Using both \"durtation\" and \"nrequests\" are not allowed. Please choose only one of these options.")
	}

	if nrequests.value < *concurrency {
		log.Fatal("Number of requests must be greater or equal to concurrency")
	}

	if *optype == "multipartput" {
		if *partsize < 5*(1<<20) {
			log.Fatal("Part size should be 5MiB at minimum")
		}
		if int(math.Ceil(float64(*osize)/float64(*partsize))) > 10000 {
			log.Fatal("The multipart upload will use too many parts (max 10000)")
		}
	}

	var hasfailed bool

	args := parameters{
		concurrency:       *concurrency,
		osize:             *osize,
		endpoint:          *endpoint,
		optype:            *optype,
		bucketname:        *bucketname,
		objectprefix:      *objectprefix,
		ratePerSecond:     ratePerSecond,
		logging:           *logdetail != "",
		objrange:          *objrange,
		reducedRedundancy: *reducedRedundancy,
		overwrite:         *overwrite,
		retries:           *retries,
		retrySleep:        *retrySleep,
		lockstep:          *lockstep,
		repeat:            *repeat,
		region:            *region,
		partsize:          *partsize,
		verify:            *verify,
		tagging:           *tagging,
		metadata:          *metadata,
		nrequests:         &nrequests,
		duration:          &duration,
	}

	if *concurrency != 0 {
		_, hasfailed = runtest(args)
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
	if *logdetail != "" {
		f, err := os.Create(*logdetail)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		for _, v := range detailed {
			fmt.Fprintf(f, "%f,%f\n", v.ts.Sub(detailed[0].ts).Seconds(), v.elapsed.Seconds())
		}
	}
	if hasfailed {
		os.Exit(1)
	}
}

// implements io.ReadSeeker
type DummyReader struct {
	Size   int64
	Offset int64
	Data   []byte
}

func NewDummyReader(size int64) *DummyReader {
	return &DummyReader{Size: size}
}

func (r *DummyReader) SetData(data string) {
	r.Data = []byte(data)
}

func (r *DummyReader) Read(p []byte) (n int, err error) {
	if len(r.Data) == 0 {
		n, err = 0, errors.New("Data needs to be set before reading")
		return
	}

	if r.Offset >= r.Size {
		n, err = 0, io.EOF
		return
	}

	read := int(r.Size - r.Offset)
	if len(p) < read {
		read = len(p)
	}

	for i := 0; i < read; i++ {
		p[i] = r.Data[(i+int(r.Offset))%len(r.Data)]
	}

	r.Offset += int64(read)
	n, err = read, nil
	return
}

func (r *DummyReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		if offset >= 0 && offset <= r.Size {
			r.Offset = offset
			return r.Offset, nil
		}
		return r.Offset, errors.New(fmt.Sprintf("SeekStart: Cannot seek past start or end of file. offset: %d, size: %d", offset, r.Size))
	case io.SeekCurrent:
		off := offset + r.Offset
		if off >= 0 && off <= r.Size {
			r.Offset = off
			return off, nil
		}
		return r.Offset, errors.New(fmt.Sprintf("SeekCurrent: Cannot seek past start or end of file. offset: %d, size: %d", off, r.Size))
	case io.SeekEnd:
		off := r.Size - offset
		if off >= 0 && off <= r.Size {
			r.Offset = off
			return off, nil
		}
		return r.Offset, errors.New(fmt.Sprintf("SeekEnd: Cannot seek past start or end of file. offset: %d, size: %d", off, r.Size))
	}
	return 0, errors.New("Invalid value of whence")
}
