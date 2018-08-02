# COSBench2 - S3 Performance Benchmarking 


COSBench2 is a lightweight high performance, S3 API performance test command-line utility. 

Because COSBench2 is focussed on S3, it is more performant and accurate for S3 storage benchmarking.
If you want to compare S3 with other object protocols - consider the older Cosbench (See https://github.com/intel-cloud/cosbench/)

This tool is in active development - please submit feature requests in the issues page


## cosbench2 latest version

	 1.1.6

# Installation

     $ go get github.com/cosbench2/cosbench2.git
## Minimum Requirements
	
	 Go 1.7 or higher

# Usage

## Setting your s3 credentials

cosbench2 retrieves the access key and the secret access key from the environment variables as shown below:

    $ export AWS_ACCESS_KEY=AKIAINZFCN46TISVUUCA
    $ export AWS_SECRET_ACCESS_KEY=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

## Command line options

	./cosbench2 --help
	Usage of ./cosbench2:
  	-bucket string
        	bucket name (needs to exist) (default "test")
  	-concurrency int
        	Maximum concurrent requests (0=scan concurrency, run with ulimit -n 16384) (default 1)
  	-cpuprofile string
        	write cpu profile to file
  	-duration value
        	Test duration in seconds
  	-endpoint string
        	target endpoint (default "https://127.0.0.1:18082")
  	-lockstep
        	Force all threads to advance at the same rate rather than run independently
  	-logdetail string
        	write detailed log to file
  	-metadata string
        	The metadata to use for the objects. The string must be formatted as such: 'key1=value1&key2=value2'. Used for put, updatemeta, multipartput, putget and putget9010r.
  	-operation string
        	operation type: put, multipartput, get, puttagging, updatemeta, randget, delete, options, head, putget, putget9010r (default "put")
  	-overwrite int
        	Turns a PUT/GET/HEAD into an operation on the same s3 key. (1=all writes/reads are to same object, 2=threads clobber each other but each write/read is to unique objects).
  	-partsize int
        	Size of each part (min 5MiB); only has an effect when a multipart put is used (default 5242880)
  	-prefix string
        	object name prefix (default "testobject")
  	-range string
        	Specify range header for GET requests
  	-ratelimit float
        	the total number of operations per second across all threads (default 1.7976931348623157e+308)
  	-region string
        	Region to send requests to (default "us-east-1")
  	-repeat int
        	Repeat each S3 operation this many times (default 1)
  	-requests value
        	Total number of requests (default 1000)
  	-retries int
        	Number of retry attempts. Default is 0.
  	-retrysleep int
        	How long to sleep in between each retry in milliseconds. Default (0) is to use the default retry method which is an exponential backoff.
  	-rr
        	Reduced redundancy storage for PUT requests
  	-size int
        	Object size. Note that s3tester is not ideal for very large objects as the entire body must be read for v4 signing and the aws sdk does not support v4 chunked. Performance may degrade as size increases due to the use of v4 signing without chunked support (default 30720)
  	-tagging string
        	The tag-set for the object. The tag-set must be formatted as such: 'tag1=value1&tage2=value2'. Used for put, puttagging, putget and putget9010r.
  	-verify
        	Verify the retrieved data on a get operation 

## Exit code
`1` One or more requests has failed.

# Examples

## Writing objects into a bucket

    ./cosbench2 -concurrency=128 -size=20000000 -operation=put -requests=200000 -endpoint="10.96.105.5:8082" -prefix=3

- Starts writing objects into the default bucket "test".
- The bucket needs to be created prior to running s3tester.
- The naming of the ingested objects will be `3-thread#-object#` where "3" is the prefix specified, `thread#` is 0..127 corresponding to the thread that performed the PUT operation (out of the specified 128 concurrent requests) and `object#` is a sequential number starting from zero within a thread.
- This command will perform a total of 20,000 PUT requests (or in this case slightly less because 20,000 does not divide by 128).
- The object size is 20,000,000 bytes.
- Replace the sample IP/port combination with the one you are using.

## Reading objects from a bucket (and other operations)
    ./cosbench2 -concurrency=128 -operation=get -requests=200000 -endpoint="10.96.105.5:8082" -prefix=3

- Matches the request above and will read the same objects written in the same sequence.
- If you use the `randget` operation the objects will be read in random order simulating a random-access workload.
- If you use the `head` operation then the S3 HEAD operation will be performed against the objects in sequence.
- If you use the `delete` operation then the objects will be deleted.

## Mixed workloads
- A simple 90% PUT 10% random GET workload is available via the `putget9010r` operation.  This will issue 10% random GET requests to any of the objects written so far (by those 90% PUT operations).
- More complex workloads can be simulated by running multiple instanes of this command line.  You will need to isolate those multiple instances by writing objects with a different prefix or writing to different buckets.
- The rate limiting option `-ratelimit` is also useful in constructing more complex workloads with multiple instances and precise control of the ratios of the different requests.  You will need to set a limit below the maximum possible value in this sccenario.

# Interpreting the results
	Operation: putget9010r
	Concurrency: 128
	Total number of requests: 1024
	Total number of unique objects: 896
	Failed requests: 0
	Total elapsed time: 2.878469613s
	Average request time: 304.195727ms
	Standard deviation: 214.351491ms
	Minimum request time: 67.416972ms
	Maximum request time: 1.420411561s
	Nominal requests/s: 420.8
	Actual requests/s: 355.7
	Content throughput: 6.785290 MB/s


- `Nominal requests/s` is calculated ignoring any client side overheads.  This number will always be higher than actual requests/s.  If those two numbers diverge significantly it can be an indication that the client machine isn't capable of generating the required workload and you may want to consider using multiple machines.
- `Actual requests/s` is the total number of requests divided by the total elapsed time in seconds.
- `Content throughtput` is the total amount of data ingested and retrieved in MB divided by the total elapsed time in seconds.
- `Total number of unique objects` is the total number of unique objects being operated on successfully. In this example, 896 unique objects have been PUT into the bucket, and 128 GET requests have been made to a random selection of those 896 unique objects.
- The `-logdetail` option allows for capturing all the request latencies into a `.csv` file for further analysis.
