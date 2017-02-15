# About

s3tester is a lightweight S3 API performance test command-line utility.

# Installation

     $ go get github.com/NetApp-StorageGRID/s3tester

# Usage

## Setting your s3 credentials

s3tester retrieves the access key and the secret access key from the environment variables as shown below:

    $ export AWS_ACCESS_KEY=AKIAINZFCN46TISVUUCA
    $ export AWS_SECRET_ACCESS_KEY=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

## Command line options
    ./s3tester --help
    Usage of ./s3tester:
    -bucket string
          bucket name (needs to exist) (default "test")
    -concurrency int
          Maximum concurrent requests (0=scan concurrency, run with ulimit -n 16384) (default 1)
    -cpuprofile string
          write cpu profile to file
    -endpoint string
          target endpoint (default "https://127.0.0.1:18082")
    -logdetail string
          write detailed log to file
    -operation string
          operation type: put, get, randget, putget, putget9010r, head, delete or options (default "put")
    -overwrite
          Turns a PUT into an overwrite of the same s3 key.
    -prefix string
          object name prefix (default "testobject")
    -range string
          Specify range header for GET requests
    -ratelimit float
          the total number of operations per second across all threads (default 1.7976931348623157e+308)
    -requests int
          Total number of requests (default 1000)
    -rr
        Reduced redundancy storage for PUT requests
    -size int
          Object size (default 30720)

## Exit code
`1` One or more requests has failed.

# Examples

## Writing objects into a bucket

    ./s3tester -concurrency=128 -size=20000000 -operation=put -requests=200000 -endpoint="10.96.105.5:8082" -prefix=3

- Starts writing objects into the default bucket "test".
- The bucket needs to be created prior to running s3tester.
- The naming of the ingested objects will be `3-thread#-object#` where "3" is the prefix specified, `thread#` is 0..127 corresponding to the thread that performed the PUT operation (out of the specified 128 concurrent requests) and `object#` is a sequential number starting from zero within a thread.
- This command will perform a total of 20,000 PUT requests (or in this case slightly less because 20,000 does not divide by 128).
- The object size is 20,000,000 bytes.
- Replace the sample IP/port combination with the one you are using.

## Reading objects from a bucket (and other operations)
    ./s3tester -concurrency=128 -operation=get -requests=200000 -endpoint="10.96.105.5:8082" -prefix=3

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
    Total number of requests: 99968
    Failed requests: 0
    Total elapsed time: 45m57.732936898s
    Average request time: 3.211318463s
    Standard deviation: 3.1998675s
    Minimum request time: 223.383452ms
    Maximum request time: 1m29.086180712s
    Nominal requests/s: 39.9
    Actual requests/s: 36.3

- `Nominal requests/s` is calculated ignoring any client side overheads.  This number will always be higher than actual requests/s.  If those two numbers diverge significantly it can be an indication that the client machine isn't capable of generating the required workload and you may want to consider using multiple machines.
- `Actual requests/s` is the total number of requests divided by the total elapsed time in seconds.
- The `-logdetail` option allows for capturing all the request latencies into a `.csv` file for further analysis.