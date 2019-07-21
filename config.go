package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"golang.org/x/time/rate"
	"log"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// intFlag is used to differentiate user-defined value from default value
type intFlag struct {
	set   bool
	value int
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

type parameters struct {
	concurrency        int
	osize              int64
	endpoints          []string
	optype             string
	bucketname         string
	objectprefix       string
	tagging            string
	metadata           string
	consistencyControl string
	ratePerSecond      rate.Limit
	logging            bool
	logdetail          string
	loglatency         string
	objrange           string
	reducedRedundancy  bool
	overwrite          int
	retries            int
	retrySleep         int
	lockstep           bool
	attempts           int
	region             string
	partsize           int64
	verify             int
	min                int64
	max                int64
	jsonDecoder        *json.Decoder
	nrequests          *intFlag
	duration           *intFlag
	cpuprofile         string
	isJson             bool
	tier               string
	days               int64
	profile            string
	nosign             bool
	fanout             *intFlag
}

func parseArgs() parameters {
	return parseAndValidate(os.Args[1:])
}

func parse(cmdline []string) (parameters, error) {
	optypes := []string{"put", "multipartput", "get", "puttagging", "updatemeta", "randget", "delete", "options", "head", "restore"}
	operationListString := strings.Join(optypes[:], ", ")

	consistencyControlTypes := []string{"all", "available", "strong-global", "strong-site", "read-after-new-write", "weak"}
	consistencyControlString := strings.Join(consistencyControlTypes[:], ", ")

	var duration intFlag
	nrequests := intFlag{value: 1000, set: false}
	fanout := intFlag{value:-1, set: false}

	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	flags.Var(&duration, "duration", "Test duration in seconds")
	flags.Var(&nrequests, "requests", "Total number of requests")
	flags.Var(&fanout, "fanout", "Total number of unique fanout copies")

	var concurrency = flags.Int("concurrency", 1, "Maximum concurrent requests (0=scan concurrency, run with ulimit -n 16384)")
	var osize = flags.Int64("size", 30*1024, "Object size. Note that s3tester is not ideal for very large objects as the entire body must be read for v4 signing and the aws sdk does not support v4 chunked. Performance may degrade as size increases due to the use of v4 signing without chunked support")
	var consistencyControl = flags.String("consistency", "", "The StorageGRID consistency control to use for all requests. Does nothing against non StorageGRID systems. ("+consistencyControlString+")")
	var endpoint = flags.String("endpoint", "https://127.0.0.1:18082", "target endpoint(s). If multiple endpoints are specified separate them with a ','. Note: the concurrency must be a multiple of the number of endpoints.")
	var optype = flags.String("operation", "put", "operation type: "+operationListString)
	var bucketname = flags.String("bucket", "test", "bucket name (needs to exist)")
	var objectprefix = flags.String("prefix", "testobject", "object name prefix")
	var tagging = flags.String("tagging", "", "The tag-set for the object. The tag-set must be formatted as such: 'tag1=value1&tage2=value2'. Used for put, puttagging, putget and putget9010r.")
	var metadata = flags.String("metadata", "", "The metadata to use for the objects. The string must be formatted as such: 'key1=value1&key2=value2'. Used for put, updatemeta, multipartput, putget and putget9010r.")
	var cpuprofile = flags.String("cpuprofile", "", "write cpu profile to file")
	var logdetail = flags.String("logdetail", "", "write detailed log to file")
	var loglatency = flags.String("loglatency", "", "write latency histogram to file")
	var maxRate = flags.Float64("ratelimit", math.MaxFloat64, "the total number of operations per second across all threads")
	var objrange = flags.String("range", "", "Specify range header for GET requests")
	var reducedRedundancy = flags.Bool("rr", false, "Reduced redundancy storage for PUT requests")
	var overwrite = flags.Int("overwrite", 0, "Turns a PUT/GET/HEAD into an operation on the same s3 key. (1=all writes/reads are to same object, 2=threads clobber each other but each write/read is to unique objects).")
	var retries = flags.Int("retries", 0, "Number of retry attempts. Default is 0.")
	var retrySleep = flags.Int("retrysleep", 0, "How long to sleep in between each retry in milliseconds. Default (0) is to use the default retry method which is an exponential backoff.")
	var lockstep = flags.Bool("lockstep", false, "Force all threads to advance at the same rate rather than run independently")
	var repeat = flags.Int("repeat", 0, "Repeat each S3 operation this many times, by default doesn't repeat (i.e. repeat=0)")
	var region = flags.String("region", "us-east-1", "Region to send requests to")
	var partsize = flags.Int64("partsize", 5*(1<<20), "Size of each part (min 5MiB); only has an effect when a multipart put is used")
	var verify = flags.Int("verify", 0, "Verify the retrieved data on a get operation - (0=disable verify(default), 1=normal put data, 2=multipart put data). If verify=2, partsize is required and default partsize is set to 5242880.")

	var uniformDist = flags.String("uniformDist", "", "Generates a uniform distribution of object sizes given a min-max size (10-20)")
	var isJson = flags.Bool("json", false, "The result will be printed out in JSON format if this flag exists")
	var tier = flags.String("tier", "standard", "The retrieval option for restoring an object. One of expedited, standard, or bulk. AWS default option is standard if not specified")
	var days = flags.Int64("days", 1, "The number of days that the restored object will be available for")
	var workload = flags.String("workload", "", "Filepath to a Mixedworkload JSON formatted file which allows a user to specify a mixture of operations. A sample mixed workload file must be in the format\n'{'mixedWorkload':\n[{'operation':'put','ratio':25},\n{'operationType':'get','ratio':25},\n{'operationType':'updatemeta','ratio':25},\n{'operationType':'delete','ratio':25}]}'.  \nNOTE: The order of operations specified will generate the requests in the same order.\nI.E. If you have delete followed by a put, but no objects on your grid to delete, all your deletes will fail.")
	var profile = flags.String("profile", "", "Use a specific profile from AWS CLI credential file (https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html).")
	var nosign = flags.Bool("no-sign-request", false, "Do not sign requests. Credentials will not be loaded if this argument is provided.")

	flags.Usage = func() {
		fmt.Fprintf(os.Stderr, "This tool is for generating high performance S3 load against an S3 server.\n")
		fmt.Fprintf(os.Stderr, "It reads credentials from the environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY or loads credentials generated by AWS CLI.\n")
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
		flags.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nVersion: "+VERSION+"\n")
	}
	flags.Parse(cmdline)

	var ratePerSecond = rate.Limit(*maxRate)

	var opTypeExists = false
	for op := range optypes {
		if optypes[op] == *optype {
			opTypeExists = true
		}
	}

	if !opTypeExists {
		return parameters{}, fmt.Errorf("operation type must be one of: %s", operationListString)
	}

	if *consistencyControl != "" {
		var consistencyControlExists = false
		for _, op := range consistencyControlTypes {
			if op == *consistencyControl {
				consistencyControlExists = true
			}
		}

		if !consistencyControlExists {
			return parameters{}, fmt.Errorf("%s consistency is not one of: %s", *consistencyControl, consistencyControlString)
		}
	}

	if nrequests.set && nrequests.value <= 0 {
		return parameters{}, errors.New("Number of requests must be > 0")
	}

	if fanout.set {
		if *optype == "put"{
			if !(1 <= fanout.value && fanout.value <= 10000){
				return parameters{}, errors.New("Value of fanout for PUT operation must be between 1 and 10,000")
			}
		} else if *optype == "get"{
			if !(0 <= fanout.value && fanout.value <= 9999){
				return parameters{}, errors.New("Value of fanout for GET operation must be between 0 and 9,999")
			}
		} else {
			return parameters{}, errors.New("Fanout is (currently) only supported in GET or PUT operations")
		}
	}

	if *concurrency <= 0 {
		return parameters{}, errors.New("Concurrency must be > 0")
	}

	if duration.set {
		// TODO: because of the new naming schema, duration with "get"/"randget"/"puttagging"/"updatemeta"/"head"/"restore" won't work
		if *optype == "get" || *optype == "randget" || *optype == "puttagging" || *optype == "updatemeta" || *optype == "head" || *optype == "restore" {
			return parameters{}, fmt.Errorf("Using \"duration\" with operation type  \"%s\" is not supported.", *optype)
		}
		if (*optype == "get" || *optype == "randget") && !nrequests.set {
			return parameters{}, fmt.Errorf("Using \"duration\" with operation type  \"%s\" requires \"requests\" to be set to that of a previous put run.", *optype)
		} else if !(*optype == "get" || *optype == "randget") && nrequests.set {
			return parameters{}, errors.New("Using both \"duration\" and \"requests\" is not supported. Please choose only one of these options.")
		}
	}

	if nrequests.value < *concurrency {
		return parameters{}, errors.New("Number of requests must be greater or equal to concurrency")
	}

	endpoints, err := validateEndpoint(*endpoint)
	if err != nil {
		return parameters{}, err
	}
	var jsonDecoder *json.Decoder

	if *workload != "" {
		if jsonDecoder, err = openFile(*workload); err != nil {
			return parameters{}, fmt.Errorf("Error opening workload file: %s", err)
		}
		if len(endpoints) != 1 {
			return parameters{}, errors.New("Cannot specify a workload file and additional endpoints. Only one of these is supported at a time")
		}
	}

	if (*concurrency)%len(endpoints) != 0 {
		return parameters{}, errors.New("The concurrency must be multiple of endpoint list length")
	}

	if *retries < 0 {
		return parameters{}, errors.New("Retries must be >= 0")
	}

	if *repeat < 0 {
		return parameters{}, errors.New("Repeat must be >= 0")
	}

	if *nosign && *profile != "" {
		return parameters{}, errors.New("Cannot load credential profile if argument nosign is provided")
	}

	// attempts indicate the number of times we perform S3 operation, the default attempts is 1
	attempts := 1 + *repeat

	if *optype == "multipartput" {
		if *partsize < 5*(1<<20) {
			return parameters{}, errors.New("Part size should be 5MiB at minimum")
		}
		if int(math.Ceil(float64(*osize)/float64(*partsize))) > 10000 {
			return parameters{}, errors.New("The multipart upload will use too many parts (max 10000)")
		}
	}
	var min int64
	var max int64

	if *uniformDist != "" {
		var err error
		wasError := false
		if *optype != "put" {
			wasError = true
		}
		dist := strings.Split(*uniformDist, "-")
		if len(dist) == 2 {
			min, err = strconv.ParseInt(dist[0], 10, 64)
			if err != nil || min < 0 {
				wasError = true
			}
			max, err = strconv.ParseInt(dist[1], 10, 64)
			if err != nil || max < 0 {
				wasError = true
			}
			if max < min {
				wasError = true
			}
		} else {
			wasError = true
		}
		if wasError {
			return parameters{}, errors.New("UniDist must be in form 'min-max', where min and max are > 0,  min < max, and have a put set as operation type")
		}
	}

	if !strings.EqualFold(*tier, "Standard") && !strings.EqualFold(*tier, "Expedited") && !strings.EqualFold(*tier, "Bulk") {
		return parameters{}, errors.New("Restore tier must be one of Standard, Expedited, or Bulk. Case Insensitive")
	}

	if *days < 1 {
		return parameters{}, errors.New("Restore days must be a positive, non-zero integer")
	}

	args := parameters{
		concurrency:        *concurrency,
		osize:              *osize,
		consistencyControl: *consistencyControl,
		endpoints:          endpoints,
		optype:             *optype,
		bucketname:         *bucketname,
		objectprefix:       *objectprefix,
		ratePerSecond:      ratePerSecond,
		logging:            *logdetail != "",
		logdetail:          *logdetail,
		loglatency:         *loglatency,
		objrange:           *objrange,
		reducedRedundancy:  *reducedRedundancy,
		overwrite:          *overwrite,
		retries:            *retries,
		retrySleep:         *retrySleep,
		lockstep:           *lockstep,
		attempts:           attempts,
		region:             *region,
		jsonDecoder:        jsonDecoder,
		partsize:           *partsize,
		verify:             *verify,
		tagging:            *tagging,
		metadata:           *metadata,
		min:                min,
		max:                max,
		nrequests:          &nrequests,
		duration:           &duration,
		cpuprofile:         *cpuprofile,
		isJson:             *isJson,
		tier:               *tier,
		days:               *days,
		profile:            *profile,
		nosign:             *nosign,
		fanout:             &fanout,
	}

	return args, nil
}

func parseAndValidate(cmdline []string) (args parameters) {
	var err error
	args, err = parse(cmdline)
	if err != nil {
		log.Fatal(err)
	}
	return
}

// Validate input enpoint string, reject invalid URLs and duplicate URLs
func validateEndpoint(endpoint string) ([]string, error) {
	endpointSet := make(map[string]struct{})
	endpoints := make([]string, 0)
	for _, endpoint := range strings.Split(endpoint, ",") {
		trimEndpoint := strings.Trim(endpoint, " ")
		if _, err := url.ParseRequestURI(trimEndpoint); err != nil {
			return nil, errors.New("URL \"" + trimEndpoint + "\" is not a valid endpoint")
		}
		// check if map contains this key to identify duplicate URLs
		if _, hasKey := endpointSet[trimEndpoint]; hasKey {
			return nil, errors.New("URL \"" + trimEndpoint + "\" is a duplicate endpoint")
		}
		endpointSet[trimEndpoint] = struct{}{}
		endpoints = append(endpoints, trimEndpoint)
	}
	return endpoints, nil
}
