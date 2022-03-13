# s3tester - S3 Performance Benchmarking 

The goal of s3tester is to be a lightweight S3 performance testing utility. It is solely focused on S3 testing.

This tool is in active development - please submit feature requests in the issues page.

## Releases

Find and download latest release [here](https://github.com/s3tester/s3tester/releases).

## Minimum Requirements
	
- Go 1.7 or higher

## Installation

```
$ go get github.com/s3tester/s3tester
```
	
If you don't want to build from source you can download the compiled version of s3tester for Windows or Linux from [github.com/s3tester/s3tester/releases](https://www.github.com/s3tester/s3tester/releases)

## Usage

### Setting your S3 credentials

There are multiple options for setting credentials.

- Using environment Variables:

```
$ export AWS_ACCESS_KEY_ID=AKIAINZFCN46TISVUUCA
$ export AWS_SECRET_ACCESS_KEY=VInXxOfGtEIwVck4AdtUDavmJf/qt3jaJEAvSKZO
```

- Using AWS credential file: see the `--profile` option below for details.

### Command line options

| Parameter | Type  | Note  |
| :---   | :---: | :--- |
| addressing-style | string | Whether to use virtual-hosted style addresses (bucket name is in the hostname) or path-style addresses (bucket name is part of the path). Value must be one of `virtual` or `path`. Default: `path` |
| bucket | string | Bucket name (mandatory). Default: `test` |
| concurrency | int | Maximum concurrent requests. `0`: scan concurrency, run with `ulimit -n 16384`. Default: `1` |
| consistency | string | The StorageGRID consistency control to use for all requests. Does nothing against non StorageGRID systems. (`all`, `available`, `strong-global`, `strong-site`, `read-after-new-write`, `weak`) | 
| cpuprofile | string | Write CPU profile to file |
| days | int | The number of days that the restored object will be available for. Default: `1` |
| debug | boolean | Print response body on request failure. |
| describe | boolean | Instead of running tests, show the consolidated list of test parameters that will be used when a test is run. |
| duration | int | Test duration in seconds. Mutually exclusive with `requests` |
| endpoint | string | target endpoint(s). If multiple endpoints are specified separate them with a `,`. Note: the concurrency must be a multiple of the number of endpoints. Default: `"https://127.0.0.1:18082"` |
| header | - | Specify one or more headers of the form `<header-name>: <header-value>`. |
| json | boolean | The result will be printed out in JSON format if this flag exists. Default: `false` |
| lockstep | boolean | Force all threads to advance at the same rate rather than run independently |
| logdetail | string | Write detailed log to file |
| loglatency | string | Write latency histogram to file | 
| metadata  | string | The metadata to use for the objects. The string must be formatted as such: `'key1=value1&key2=value2'`. Used for `put`, `updatemeta`, `multipartput`, `putget` and `putget9010r` |
| metadata-directive | string | Specifies whether the metadata is copied from the source object or if it is replaced with the metadata provided in the object copy request. Value must be one of `COPY` or `REPLACE`. Default: `COPY` |
| mixed-workload | string | Path to a JSON file that specifies a mixture of operations. |
| no-sign-request | boolean | Do not sign requests. Credentials will not be loaded if this argument is provided |
| operation | string | Operation type: `put`, `multipartput`, `get`, `puttagging`, `updatemeta`, `randget`, `delete`, `options`, `head`, `restore`. Default: `put`
| overwrite | int | Turns a PUT/GET/HEAD into an operation on the same S3 key. `1`: all writes/reads are to same object, `2`: threads clobber each other but each write/read is to unique objects | 
| partsize | int | Size of each part in bytes. Only has an effect when a multipart put is used. Min: `5242880` (5MiB). Default: `5242880` (5MiB)|
| prefix | string | Object name prefix. Default: `testobject` |
| profile | string | Use a specific profile from [AWS CLI credential file](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html) |
| query-params | string | Specify one or more custom query parameters of the form `<queryparam-name>=<queryparam-value>` or `<queryparam-name>` separated by ampersands. |
| random-range | string | Used to perform random range GET requests. Format is `<min>-<max>/<size>`, where `<size>` is the number of bytes per GET request, and `<min>-<max>` is an inclusive byte range within the object . Ex: Use 0-399/100 to perform random 100-byte reads within the first 400 bytes of an object. |
| range | string | Specify range header for GET requests |
| ratelimit | float | The total number of operations per second across all threads. Default: `1.7976931348623157e+308` | 
| region | string | Region to send requests to. Default: `us-east-1` |
| repeat | int | Repeat each S3 operation this many times: Default: `0` (do not repeat) |
| requests | int | Total number of requests. Mutually exclusive with `duration`. Default: `1000` |
| retries | int | Number of retry attempts. Default: `0` |
| retrysleep | int | How long to sleep in between each retry in milliseconds. Default: `0` (exponential backoff) |
| rr | - | Reduced redundancy storage for PUT requests | 
| size | int | Object size in bytes. Default: `30720` |
| tagging | string | The tag-set for the object. The tag-set must be formatted as such: `'tag1=value1&tag2=value2'`. Used for `put`, `puttagging`, `putget` and `putget9010r` |
| tagging-directive | string | Specifies whether the object tag-set is copied from the source object or if it is replaced with the tag-set provided in the object copy request. Value must be one of 'COPY' or 'REPLACE'. Default: `COPY` |
| tier | string | The retrieval option for restoring an object. One of `expedited`, `standard`, or `bulk`. AWS default option is standard if not specified. Default: `standard` |
| uniformDist | string | Generates a uniform distribution of object sizes given a min-max size. Allowed values: `10` to `20`) |
| verify | int | Verify the retrieved data on a get operation. `0`: disable verify (default); `1`: normal put data, `2`: multipart put data. If verify equals `2`, partsize is required (default `partsize` is `5242880` bytes) |
| workload | string | File path to a JSON file that describes a workload to be run. The file is parsed with the Go template package and must produce JSON that is valid according to the workload schema |

#### workload JSON Sample File
```raw
{
  "global": {
    "concurrency": 4,
    "prefix": "test",
    "requests": 20
  },
  "workload": [
    {
      "bucket": "b1",
      "operation": "put"
    },
    {
      "bucket": "b2",
      "copy-source-bucket": "b1",
      "operation": "copy"
    },
    {
      "bucket": "b2",
      "operation": "get"
    },
    {
      "bucket": "b2",
      "operation": "head"
    },
    {
      "bucket": "b1",
      "operation": "delete"
    },
    {
      "bucket": "b2",
      "operation": "delete"
    }
  ]
}
```
**NOTE:** [File Sample](example/workload.json) and [Template Support](example/templated-workload.json)

#### mixedWorkload JSON Sample File

```raw
{
	"mixedWorkload": [{
		"operationType": "put",
		"ratio": 25
	}, {
		"operationType": "get",
		"ratio": 25
	}, {
		"operationType": "updatemeta",
		"ratio": 25
	}, {
		"operationType": "delete",
		"ratio": 25
	}]
}
```

**NOTE:** The order of operations specified will generate the requests in the same order. That is, if you have DELETE followed by a PUT, but no objects on your grid to delete, all your deletes will fail.

## Exit codes

- `1`: one or more requests has failed

## Examples

### Writing objects into a bucket

```raw
./s3tester -concurrency=128 -size=20000000 -operation=put -requests=20000 -endpoint="https://10.96.105.5:18443" -prefix=3
```

- Starts writing objects into the default bucket `test`.
- The bucket needs to be created prior to running s3tester.
- The naming of the ingested objects will be `3-object#` where `3` is the prefix specified and `object#` is a sequential number starting from zero and going to the number of requests.
- This command will perform a total of 20,000 PUT requests (or in this case slightly less because 20,000 does not divide by 128).
- The object size is 20,000,000 bytes.
- Replace the sample IP/port combination with the one you are using.

### Reading objects from a bucket (and other operations)

```raw
./s3tester -concurrency=128 -operation=get -requests=200000 -endpoint="https://10.96.105.5:18443" -prefix=3
```

- Matches the request above and will read the same objects written in the same sequence.
- If you use the `randget` operation the objects will be read in random order simulating a random-access workload.
- If you use the `head` operation then the S3 HEAD operation will be performed against the objects in sequence.
- If you use the `delete` operation then the objects will be deleted.

As of version 2.1.0 the concurrency on a retrieval operation can be different from the concurrency used to ingest the objects. The goal is to save time by ingesting data once and retrieving at different concurrencies to observe the impact on performance. However, the number of requests has to match the number that was actually ingested. For example, if we ingest with concurrency 1000 and requests set to 1100 then only 1000 requests will actually be ingested (1100 - 1100%1000) to keep the number of requests per client thread equal. Now when performing the retrieval the number of requests specified must be 1000, not 1100.

## Interpreting the results

```raw
        --- Total Results ---
Operation: put
Concurrency: 64
Total number of requests: 99968
Total number of unique objects: 99968
Failed requests: 0
Total elapsed time: 2m43.251246249s
Average request time: 101.057175ms
Minimum request time: 13.84ms
Maximum request time: 712.75ms
Nominal requests/s: 633.3
Actual requests/s: 612.4
Content throughput: 2.392018 MB/s
Average Object Size: 4096
Response Time Percentiles
50     :   93.91 ms
75     :   114.68 ms
90     :   140.4 ms
95     :   166 ms
99     :   331.71 ms
99.9   :   492.57 ms
Latency(ms) : Operations
  0 - 1   : 0     |
  2 - 3   : 0     |
  4 - 7   : 0     |
  8 - 15  : 7     |
 16 - 31  : 945   ||
 32 - 63  : 12093 ||||||||||||||
 64 - 127 : 71662 |||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
128 - 255 : 13671 ||||||||||||||||
256 - 511 : 1505  ||
512 - 713 : 85    |
```

- `Nominal requests/s` is calculated ignoring any client side overheads.  This number will always be higher than actual requests/s.  If those two numbers diverge significantly it can be an indication that the client machine isn't capable of generating the required workload and you may want to consider using multiple machines.
- `Actual requests/s` is the total number of requests divided by the total elapsed time in seconds.
- `Content throughput` is the total amount of data ingested and retrieved in MB divided by the total elapsed time in seconds.
- `Total number of unique objects` is the total number of unique objects being operated on successfully.

For per request details, s3tester can be run with the `-logdetail` option for capturing all the request latencies into a `.csv` file.
