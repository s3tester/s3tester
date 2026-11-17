package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
)

// Benchmarks for the GET request path and for key distribution strategies.
//
// End-to-end GET (one full s3tester run per iteration against in-process endpoints):
//
//	go test -run '^$' -bench BenchmarkGetEndToEnd -benchtime 1x -count 5
//
// Tunable with S3TESTER_BENCH_ENDPOINTS (default 4), S3TESTER_BENCH_CONCURRENCY (default 8096)
// and S3TESTER_BENCH_REQUESTS (default 2000000). Client and server share the host's CPUs, so the
// absolute numbers are only meaningful as an A/B comparison on the same machine.
//
// Key generation in isolation, with roughly 8096 goroutines:
//
//	go test -run '^$' -bench BenchmarkKeySource -count 5

const benchTargetGoroutines = 8096

var benchKeySink atomic.Pointer[string]

func benchEnvInt(tb testing.TB, name string, def int) int {
	tb.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		tb.Fatalf("%s must be a positive integer, got %q", name, v)
	}
	return n
}

// newBenchEndpoint starts a minimal S3-like endpoint that answers every request with body and
// counts requests. It keeps no per-request state so it can sustain long, high-concurrency runs.
func newBenchEndpoint(body []byte, count *atomic.Uint64) *httptest.Server {
	contentLength := strconv.Itoa(len(body))
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count.Add(1)
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Length", contentLength)
		w.Header().Set("ETag", `"bench"`)
		if r.Method == http.MethodGet {
			w.Write(body)
		}
	}))
}

func BenchmarkGetEndToEnd(b *testing.B) {
	numEndpoints := benchEnvInt(b, "S3TESTER_BENCH_ENDPOINTS", 4)
	concurrency := benchEnvInt(b, "S3TESTER_BENCH_CONCURRENCY", benchTargetGoroutines)
	requests := benchEnvInt(b, "S3TESTER_BENCH_REQUESTS", 2000000)
	if concurrency%numEndpoints != 0 {
		b.Fatalf("concurrency %d must be a multiple of endpoints %d", concurrency, numEndpoints)
	}

	assignments := []struct{ assignment, order string }{
		{keyAssignmentStatic, keyOrderSequential},
		{keyAssignmentDynamic, keyOrderSequential},
		{keyAssignmentDynamic, keyOrderRandom},
	}
	for _, size := range []int{1, 1024} {
		for _, a := range assignments {
			b.Run(fmt.Sprintf("size=%dB/endpoints=%d/concurrency=%d/keys=%s-%s", size, numEndpoints, concurrency, a.assignment, a.order), func(b *testing.B) {
				benchGetEndToEnd(b, size, numEndpoints, concurrency, requests, a.assignment, a.order)
			})
		}
	}
}

func benchGetEndToEnd(b *testing.B, size, numEndpoints, concurrency, requests int, assignment, order string) {
	body := make([]byte, size)
	counts := make([]atomic.Uint64, numEndpoints)
	endpoints := make([]string, numEndpoints)
	for i := range endpoints {
		ts := newBenchEndpoint(body, &counts[i])
		defer ts.Close()
		endpoints[i] = ts.URL
	}

	config := testArgs(b, "get", endpoints[0])
	args := config.worklist[0]
	args.endpoints = endpoints
	args.Concurrency = concurrency
	args.Requests = requests
	args.Size = byteSize(size)
	args.KeyAssignment = assignment
	args.KeyOrder = order
	args.KeySeed = 1

	var totalRequests int
	var totalSeconds, p50, p99, maxMs, finishSpreadMs float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		syscallParams := NewSyscallParams(args)
		res := runtest(context.Background(), config, args, syscallParams)
		syscallParams.detach()

		cr := res.CumulativeResult
		if cr.Failcount != 0 {
			b.Fatalf("%d of %d requests failed", cr.Failcount, cr.Count)
		}
		totalRequests += cr.Count
		totalSeconds += cr.elapsedTime.Seconds()
		p50 += cr.Percentiles["50"]
		p99 += cr.Percentiles["99"]
		maxMs += cr.MaximumRequestTime
		finishSpreadMs += cr.LastWorkerFinishTime - cr.FirstWorkerFinishTime
	}
	b.StopTimer()

	n := float64(b.N)
	b.ReportMetric(float64(totalRequests)/totalSeconds, "req/s")
	b.ReportMetric(p50/n, "p50-ms")
	b.ReportMetric(p99/n, "p99-ms")
	b.ReportMetric(maxMs/n, "max-ms")
	b.ReportMetric(finishSpreadMs/n, "finish-spread-ms")
	for i := range counts {
		b.Logf("endpoint %d served %d requests", i, counts[i].Load())
	}
}

// runKeyBench runs next from benchTargetGoroutines goroutines (rounded to a multiple of GOMAXPROCS).
func runKeyBench(b *testing.B, newWorker func(workerID uint64) func() string) {
	procs := runtime.GOMAXPROCS(0)
	b.SetParallelism((benchTargetGoroutines + procs - 1) / procs)
	var ids atomic.Uint64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		next := newWorker(ids.Add(1) - 1)
		var key string
		for pb.Next() {
			key = next()
		}
		benchKeySink.Store(&key)
	})
}

func BenchmarkKeySource(b *testing.B) {
	const prefix = "testobject"
	const concurrency = benchTargetGoroutines
	const perWorker = uint64(1 << 40)
	formatString := generateFormatString(0, perWorker, perWorker*concurrency)

	for _, incrementing := range []bool{false, true} {
		suffix := ""
		if incrementing {
			suffix = "/incrementing"
		}

		b.Run("static-separate"+suffix, func(b *testing.B) {
			runKeyBench(b, func(id uint64) func() string {
				var j uint64
				return func() string {
					k := generateKeyName(prefix, j, perWorker, concurrency, id, 0, formatString, incrementing, true, false, nil)
					j++
					return k
				}
			})
		})

		b.Run("static-together"+suffix, func(b *testing.B) {
			runKeyBench(b, func(id uint64) func() string {
				var j uint64
				return func() string {
					k := generateKeyName(prefix, j, perWorker, concurrency, id, 0, formatString, incrementing, false, false, nil)
					j++
					return k
				}
			})
		})

		b.Run("dynamic-sequential"+suffix, func(b *testing.B) {
			keys := &keyCursor{total: 1 << 62, wrap: true}
			runKeyBench(b, func(uint64) func() string {
				return func() string {
					i, _ := keys.claim()
					return formatKeyName(prefix, i, incrementing, formatString)
				}
			})
		})

		b.Run("dynamic-random"+suffix, func(b *testing.B) {
			perm := newKeyPermutation(10_000_000_000, 0x5eed)
			keys := &keyCursor{total: 10_000_000_000, wrap: true, perm: &perm}
			runKeyBench(b, func(uint64) func() string {
				return func() string {
					i, _ := keys.claim()
					return formatKeyName(prefix, i, incrementing, formatString)
				}
			})
		})
	}
}
