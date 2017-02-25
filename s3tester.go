// Copyright (c) 2015-2017 NetApp Inc.

// s3tester is a lightweight S3 API peformance test utility.
package main

import (
	"bytes"
	"crypto/tls"
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
	"time"

	"github.com/golang/time/rate"
	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// VERSION is displayed with help, bump when updating
const VERSION = "1.0.1-open"

// result holds the performance metrics for a single goroutine that are later aggregated.
type result struct {
	count             int
	failcount         int
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
type stats struct {
	mean             time.Duration
	stddev           time.Duration
	nominalThrougput float64
	actualThrougput  float64
}

// Retrieve S3 objects while overriding accept-encoding to prevent the server from using gzip
func identityGetObject(c *s3.S3, input *s3.GetObjectInput) (output *s3.GetObjectOutput, err error) {
	req, out := c.GetObjectRequest(input)
	output = out
	req.HTTPRequest.Header.Set("Accept-Encoding", "identity")
	err = req.Send()
	if err == nil && req.HTTPResponse.Body != nil {
		io.Copy(ioutil.Discard, req.HTTPResponse.Body)
		req.HTTPResponse.Body.Close()
	}
	return
}

var detailed []detail

func runtest(concurrency int, nrequests int, osize int, endpoint string, optype string,
	bucketname string, objectprefix string, ratePerSecond rate.Limit, logging bool, objrange string, reducedRedundancy bool, putOverwrite bool) (float64, bool) {

	obj := make([]byte, osize)

	for i := range obj {
		obj[i] = 'X'
	}
	c := make(chan result, concurrency)
	creds := credentials.NewEnvCredentials()

	sc := s3.StorageClassStandard
	if reducedRedundancy {
		sc = s3.StorageClassReducedRedundancy
	}

	limiter := rate.NewLimiter(ratePerSecond, 1)

	runstart := time.Now()
	for i := 0; i < concurrency; i++ {
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
			}
			client := &http.Client{Transport: tr}
			s3Config := aws.NewConfig().
				WithRegion("us-east-1").
				WithMaxRetries(0).
				WithCredentials(creds).
				WithEndpoint(endpoint).
				WithHTTPClient(client).
				WithDisableComputeChecksums(true).
				WithS3ForcePathStyle(true)
			s3Session, err := session.NewSession(s3Config)
			if err != nil {
				log.Fatal("Failed to create an S3 session", err)
			}

			svc := s3.New(s3Session)

			var r = result{0, 0, 0, 0, 0, 0, []detail{}}

			if logging {
				r.data = make([]detail, 0, nrequests/concurrency)
			}

			r.count = nrequests / concurrency

			for j := 0; j < r.count; j++ {
				limiter.Wait(context.Background())

				var start time.Time
				var err error

				switch {
				case optype == "options":
					if req, err := http.NewRequest("OPTIONS", endpoint+"/", nil); err != nil {
						log.Print("Creating OPTIONS request failed:", err)
						r.failcount++
					} else {
						start = time.Now()
						if resp, doerr := client.Do(req); doerr != nil {
							log.Print("OPTIONS request failed:", doerr)
							r.failcount++
						} else {
							io.Copy(ioutil.Discard, resp.Body)
							resp.Body.Close()
						}
					}

				case optype == "put":
					cl := int64(osize)

					var keyName = objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(j)
					if putOverwrite {
						keyName = objectprefix
					}

					params := &s3.PutObjectInput{
						Bucket:        aws.String(bucketname),
						Key:           aws.String(keyName),
						ContentLength: &cl,
						Body:          bytes.NewReader(obj),
						StorageClass:  &sc,
					}
					start = time.Now()
					_, err = svc.PutObject(params)

				case optype == "get":
					params := &s3.GetObjectInput{
						Bucket: aws.String(bucketname),
						Key:    aws.String(objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(j)),
						Range:  aws.String(objrange),
					}
					start = time.Now()
					_, err = identityGetObject(svc, params)

				case optype == "head":
					params := &s3.HeadObjectInput{
						Bucket: aws.String(bucketname),
						Key:    aws.String(objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(j)),
					}
					start = time.Now()
					_, err = svc.HeadObject(params)

				case optype == "delete":
					params := &s3.DeleteObjectInput{
						Bucket: aws.String(bucketname),
						Key:    aws.String(objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(j)),
					}
					start = time.Now()
					_, err = svc.DeleteObject(params)

				case optype == "randget":
					objnum := rand.Intn(r.count)
					getParams := &s3.GetObjectInput{
						Bucket: aws.String(bucketname),
						Key:    aws.String(objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(objnum)),
						Range:  aws.String(objrange),
					}
					start = time.Now()
					_, err = identityGetObject(svc, getParams)

				case optype == "putget":
					cl := int64(osize)
					putParams := &s3.PutObjectInput{
						Bucket:        aws.String(bucketname),
						Key:           aws.String(objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(j)),
						ContentLength: &cl,
						Body:          bytes.NewReader(obj),
						StorageClass:  &sc,
					}
					getParams := &s3.GetObjectInput{
						Bucket: aws.String(bucketname),
						Key:    aws.String(objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(j)),
						Range:  aws.String(objrange),
					}
					start = time.Now()
					_, err = svc.PutObject(putParams)
					if err == nil {
						_, err = identityGetObject(svc, getParams)
					}

				case optype == "putget9010r":
					cl := int64(osize)
					putParams := &s3.PutObjectInput{
						Bucket:        aws.String(bucketname),
						Key:           aws.String(objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(j)),
						ContentLength: &cl,
						Body:          bytes.NewReader(obj),
						StorageClass:  &sc,
					}
					start = time.Now()
					_, err = svc.PutObject(putParams)
					if j%10 == 0 {
						objnum := rand.Intn(j + 1)
						getParams := &s3.GetObjectInput{
							Bucket: aws.String(bucketname),
							Key:    aws.String(objectprefix + "-" + strconv.Itoa(id) + "-" + strconv.Itoa(objnum)),
							Range:  aws.String(objrange),
						}
						if err == nil {
							_, err = identityGetObject(svc, getParams)
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

				if logging {
					r.data = append(r.data, detail{start, elapsed})
				}
			}
			c <- r
		}(i)
	}

	// Aggregate results across all clients.
	aggregateResults := result{0, 0, 0, 0, 0, 0, []detail{}}
	aggregateResults.min = math.MaxInt64

	if logging {
		detailed = make([]detail, 0, concurrency*aggregateResults.count)
	}

	for i := 0; i < concurrency; i++ {
		r := <-c
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

		if logging {
			detailed = append(detailed, r.data...)
		}
	}

	runelapsed := time.Since(runstart)
	printResults(aggregateResults, optype, concurrency, runelapsed)

	return float64(aggregateResults.count) / runelapsed.Seconds(), aggregateResults.failcount > 0
}

func calcStats(results result, concurrency int, runtime time.Duration) stats {
	var runStats stats

	runStats.mean = results.elapsedSum / time.Duration(results.count)

	stddev := math.Sqrt(results.elapsedSumSquared/float64(results.count) - runStats.mean.Seconds()*runStats.mean.Seconds())
	runStats.stddev = time.Duration(int64(stddev * 1000000000))

	runStats.nominalThrougput = 1.0 / runStats.mean.Seconds() * float64(concurrency)
	runStats.actualThrougput = float64(results.count) / runtime.Seconds()

	return runStats
}

func printResults(results result, optype string, concurrency int, runtime time.Duration) {
	runStats := calcStats(results, concurrency, runtime)

	fmt.Printf("Operation: %s\n", optype)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Total number of requests: %d\n", results.count)
	fmt.Printf("Failed requests: %d\n", results.failcount)
	fmt.Printf("Total elapsed time: %s\n", runtime)
	fmt.Printf("Average request time: %s\n", runStats.mean)
	fmt.Printf("Standard deviation: %s\n", runStats.stddev)
	fmt.Printf("Minimum request time: %s\n", results.min)
	fmt.Printf("Maximum request time: %s\n", results.max)
	fmt.Printf("Nominal requests/s: %.1f\n", runStats.nominalThrougput)
	fmt.Printf("Actual requests/s: %.1f\n", runStats.actualThrougput)
}

func main() {
	var concurrency = flag.Int("concurrency", 1, "Maximum concurrent requests (0=scan concurrency, run with ulimit -n 16384)")
	var nrequests = flag.Int("requests", 1000, "Total number of requests")
	var osize = flag.Int("size", 30*1024, "Object size")
	var endpoint = flag.String("endpoint", "https://127.0.0.1:18082", "target endpoint")
	var optype = flag.String("operation", "put", "operation type: put, get, randget, putget, putget9010r, head, delete or options")
	var bucketname = flag.String("bucket", "test", "bucket name (needs to exist)")
	var objectprefix = flag.String("prefix", "testobject", "object name prefix")
	var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	var logdetail = flag.String("logdetail", "", "write detailed log to file")
	var maxRate = flag.Float64("ratelimit", math.MaxFloat64, "the total number of operations per second across all threads")
	var objrange = flag.String("range", "", "Specify range header for GET requests")
	var reducedRedundancy = flag.Bool("rr", false, "Reduced redundancy storage for PUT requests")
	var putOverwrite = flag.Bool("overwrite", false, "Turns a PUT into an overwrite of the same s3 key.")

	flag.Usage = func() {
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

	if *optype != "put" && *optype != "get" && *optype != "randget" && *optype != "delete" &&
		*optype != "options" && *optype != "head" && *optype != "putget" && *optype != "putget9010r" {
		log.Fatal("operation type must be either put, get, randget, putget, putget9010r, head, delete or options")
	}

	if *nrequests < *concurrency {
		log.Fatal("Number of requests must be greater or equal to concurrency")
	}

	var hasfailed bool

	if *concurrency != 0 {
		_, hasfailed = runtest(*concurrency, *nrequests, *osize, *endpoint, *optype, *bucketname, *objectprefix, ratePerSecond, *logdetail != "", *objrange, *reducedRedundancy, *putOverwrite)
	} else {
		previous := 0.0
		result := 0.0
		for c := 8; c < 1024 && result >= previous; c = c + 8 {
			previous = result
			result, _ = runtest(c, *nrequests, *osize, *endpoint, *optype, *bucketname, *objectprefix, ratePerSecond, *logdetail != "", *objrange, *reducedRedundancy, *putOverwrite)
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
