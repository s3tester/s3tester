# Changelog
## v3.1.1
### Added
- support for AssumeRole credential

### Changed
- fixed panic that could occur when interrupted by SIGINT
- fixed reported average object size incorrect when requests fail
- prevent incompatible duration and mixed-workload arguments from being used together

## v3.1.0
### Added
- environment variable S3TESTER_PRINT_RESPONSE_HEADERS can be specified to print specific response header values when request fails
- go.sum file
- partsize and size options support suffixes such as MB and GiB
- increment and suffix-naming options for adjusting key names

### Changed
- made naming scheme consistent for concurrent operations when duration option used
- fixed some issues that could occur when interrupted by SIGINT
- mixed-workload uses the bucket parameter value directly instead of appending "s3tester" suffix to the bucket name

### Removed
- ability of mixed-workload mode to create bucket
- non-functional lockstep option

## v3.0.0
### Added
- custom headers
- sequential workload files
- bucket in json results
- addressingStyleVirtual option
- random range option
- custom query parameters option
- meta-data-directive option
- tagging-directive option

### Changed
- duration option to support other flag settings

### Removed
- replay option and the previous workload option was renamed to mixed-workload
- consistency control option

