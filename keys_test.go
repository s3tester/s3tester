package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestKeyPermutationIsBijection(t *testing.T) {
	for _, n := range []uint64{1, 2, 3, 1000, 4097, 100000} {
		p := newKeyPermutation(n, 42)
		seen := make([]bool, n)
		for i := uint64(0); i < n; i++ {
			v := p.at(i)
			if v >= n || seen[v] {
				t.Fatalf("n=%d: index %d mapped to %d, which is out of range or repeated", n, i, v)
			}
			seen[v] = true
		}
	}
}

func TestKeyPermutationDependsOnSeed(t *testing.T) {
	a, b := newKeyPermutation(1000, 1), newKeyPermutation(1000, 2)
	same := 0
	for i := uint64(0); i < 1000; i++ {
		if a.at(i) == b.at(i) {
			same++
		}
	}
	if same > 50 {
		t.Fatalf("permutations with different seeds agree on %d of 1000 positions", same)
	}
}

func TestKeyCursorStopsAfterTotal(t *testing.T) {
	c := &keyCursor{total: 3}
	for want := uint64(0); want < 3; want++ {
		if got, ok := c.claim(); !ok || got != want {
			t.Fatalf("claim() = %d, %v; want %d, true", got, ok, want)
		}
	}
	if _, ok := c.claim(); ok {
		t.Fatal("claim() succeeded after every key was claimed")
	}
}

func TestKeyCursorWraps(t *testing.T) {
	c := &keyCursor{total: 3, wrap: true}
	for i, want := range []uint64{0, 1, 2, 0, 1} {
		if got, ok := c.claim(); !ok || got != want {
			t.Fatalf("claim %d = %d, %v; want %d, true", i, got, ok, want)
		}
	}
}

func TestKeyCursorUnbounded(t *testing.T) {
	c := &keyCursor{}
	c.next.Store(1 << 40)
	if got, ok := c.claim(); !ok || got != 1<<40 {
		t.Fatalf("claim() = %d, %v; want %d, true", got, ok, uint64(1<<40))
	}
}

func TestNewKeyCursor(t *testing.T) {
	if newKeyCursor(&Parameters{KeyAssignment: keyAssignmentStatic, Requests: 10}) != nil {
		t.Fatal("static key assignment should not use a cursor")
	}
	c := newKeyCursor(&Parameters{KeyAssignment: keyAssignmentDynamic, Operation: "get", Requests: 10, Duration: 5, KeyOrder: keyOrderRandom})
	if c.total != 10 || !c.wrap || c.perm == nil {
		t.Fatalf("get with duration and requests: got total=%d wrap=%v perm=%v", c.total, c.wrap, c.perm != nil)
	}
	c = newKeyCursor(&Parameters{KeyAssignment: keyAssignmentDynamic, Operation: "put", Duration: 5})
	if c.total != 0 {
		t.Fatalf("put with duration should be unbounded, got total=%d", c.total)
	}
}

// keyRecorder is an endpoint that records the object path of every request and optionally
// delays each response.
type keyRecorder struct {
	server *httptest.Server
	mu     sync.Mutex
	paths  []string
}

func newKeyRecorder(t *testing.T, delay time.Duration) *keyRecorder {
	t.Helper()
	k := &keyRecorder{}
	t.Cleanup(func() { k.server.Close() })
	k.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k.mu.Lock()
		k.paths = append(k.paths, r.URL.Path)
		k.mu.Unlock()
		time.Sleep(delay)
		w.Header().Set("Content-Length", "1")
		w.Write([]byte("x"))
	}))
	return k
}

func (k *keyRecorder) requests() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.paths...)
}

func runDynamicTest(t *testing.T, recorders []*keyRecorder, modify func(*Parameters)) results {
	t.Helper()
	endpoints := make([]string, len(recorders))
	for i, rec := range recorders {
		endpoints[i] = rec.server.URL
	}
	config := testArgs(t, "get", endpoints[0])
	args := config.worklist[0]
	args.endpoints = endpoints
	args.KeyAssignment = keyAssignmentDynamic
	args.KeyOrder = keyOrderSequential
	modify(&args)
	syscallParams := NewSyscallParams(args)
	defer syscallParams.detach()
	res := runtest(context.Background(), config, args, syscallParams)
	if res.CumulativeResult.Failcount != 0 {
		t.Fatalf("%d requests failed", res.CumulativeResult.Failcount)
	}
	return res
}

func requireEveryKeyOnce(t *testing.T, recorders []*keyRecorder, n int) {
	t.Helper()
	seen := make(map[string]int)
	for _, rec := range recorders {
		for _, p := range rec.requests() {
			seen[p]++
		}
	}
	for i := 0; i < n; i++ {
		p := "/test/object-" + strconv.Itoa(i)
		if seen[p] != 1 {
			t.Fatalf("%s was requested %d times, want 1", p, seen[p])
		}
		delete(seen, p)
	}
	if len(seen) != 0 {
		t.Fatalf("unexpected requests: %v", seen)
	}
}

func TestDynamicRequestsEveryKeyOnceAcrossEndpoints(t *testing.T) {
	tests := []struct {
		order string
		seed  int64
	}{
		{keyOrderSequential, 0},
		{keyOrderRandom, 7},
	}
	for _, tc := range tests {
		t.Run(tc.order, func(t *testing.T) {
			recorders := []*keyRecorder{newKeyRecorder(t, 0), newKeyRecorder(t, 0)}
			// 101 requests does not divide evenly by concurrency; every key must still be used.
			res := runDynamicTest(t, recorders, func(args *Parameters) {
				args.Requests = 101
				args.Concurrency = 4
				args.KeyOrder = tc.order
				args.KeySeed = tc.seed
			})
			requireEveryKeyOnce(t, recorders, 101)
			if res.CumulativeResult.Count != 101 || res.CumulativeResult.UniqObjNum != 101 {
				t.Fatalf("got %d requests and %d unique objects, want 101 of each", res.CumulativeResult.Count, res.CumulativeResult.UniqObjNum)
			}
			if res.CumulativeResult.KeySeed != tc.seed {
				t.Fatalf("result key seed = %d, want %d", res.CumulativeResult.KeySeed, tc.seed)
			}
		})
	}
}

func TestDynamicShiftsWorkAwayFromSlowEndpoint(t *testing.T) {
	slow, fast := newKeyRecorder(t, 20*time.Millisecond), newKeyRecorder(t, 0)
	recorders := []*keyRecorder{slow, fast}

	res := runDynamicTest(t, recorders, func(args *Parameters) {
		args.Requests = 200
		args.Concurrency = 2
	})
	requireEveryKeyOnce(t, recorders, 200)
	if s, f := len(slow.requests()), len(fast.requests()); s >= f {
		t.Fatalf("slow endpoint served %d requests and fast endpoint served %d; want the fast endpoint to serve more", s, f)
	}
	cr := res.CumulativeResult
	if !cr.firstFinish.After(cr.startTime) || cr.lastFinish.Before(cr.firstFinish) {
		t.Fatalf("invalid worker finish times: start=%v first=%v last=%v", cr.startTime, cr.firstFinish, cr.lastFinish)
	}
	// The workers finish within about one slow request of each other.
	if spread := cr.lastFinish.Sub(cr.firstFinish); spread > 200*time.Millisecond {
		t.Fatalf("workers finished %v apart", spread)
	}
}

func TestDynamicWrapsForDuration(t *testing.T) {
	rec := newKeyRecorder(t, 0)

	res := runDynamicTest(t, []*keyRecorder{rec}, func(args *Parameters) {
		args.Requests = 10
		args.Concurrency = 2
		args.Duration = 1
	})
	if res.CumulativeResult.Count <= 10 {
		t.Fatalf("expected more than 10 requests in 1 second, got %d", res.CumulativeResult.Count)
	}
	for _, p := range rec.requests() {
		n, err := strconv.Atoi(p[len("/test/object-"):])
		if err != nil || n < 0 || n >= 10 {
			t.Fatalf("request for %s is outside the 10-key range", p)
		}
	}
}

func TestKeyAssignmentValidation(t *testing.T) {
	tests := []struct {
		flags []string
		ok    bool
	}{
		{[]string{"-key-assignment=dynamic"}, true},
		{[]string{"-key-assignment=Dynamic", "-key-order=Random"}, true},
		{[]string{"-key-assignment=bogus"}, false},
		{[]string{"-key-order=bogus", "-key-assignment=dynamic"}, false},
		{[]string{"-key-order=random"}, false},
		{[]string{"-key-order=sequential"}, false},
		{[]string{"-key-assignment=static", "-key-order=sequential"}, false},
		{[]string{"-key-seed=5"}, false},
		{[]string{"-key-assignment=dynamic", "-key-seed=5"}, false},
		{[]string{"-key-assignment=dynamic", "-key-order=sequential", "-key-seed=5"}, false},
		{[]string{"-key-assignment=dynamic", "-key-order=random", "-key-seed=5"}, true},
		{[]string{"-key-assignment=dynamic", "-key-order=sequential"}, true},
		{[]string{"-key-assignment=dynamic", "-overwrite=1"}, false},
		{[]string{"-key-assignment=dynamic", "-key-order=random", "-operation=put", "-duration=5"}, false},
		{[]string{"-key-assignment=dynamic", "-operation=put", "-duration=5"}, true},
	}
	for _, tc := range tests {
		_, err := parse(generateValidCmdlineSetting(tc.flags...))
		if (err == nil) != tc.ok {
			t.Errorf("%v: got error %v, want success=%v", tc.flags, err, tc.ok)
		}
	}
}

func TestKeyOrderRandomPicksSeed(t *testing.T) {
	config, err := parse(generateValidCmdlineSetting("-key-assignment=dynamic", "-key-order=random"))
	if err != nil {
		t.Fatal(err)
	}
	if seed := config.worklist[0].KeySeed; seed <= 0 || seed >= 1<<53 {
		t.Fatalf("chosen seed %d is outside (0, 2^53)", seed)
	}
	config, err = parse(generateValidCmdlineSetting("-key-assignment=dynamic", "-key-order=random", "-key-seed=99"))
	if err != nil {
		t.Fatal(err)
	}
	if config.worklist[0].KeySeed != 99 {
		t.Fatalf("got seed %d, want 99", config.worklist[0].KeySeed)
	}
}

func TestWorkloadKeySeedIsExact(t *testing.T) {
	config, err := parse(generateValidCmdlineSetting())
	if err != nil {
		t.Fatal(err)
	}
	// 2^53+1 is the smallest integer a float64 cannot represent.
	const seed = 9007199254740993
	data := `{"workload": [{"key-assignment": "dynamic", "key-order": "random", "key-seed": 9007199254740993}]}`
	w, err := createWorklist(config.worklist[0], []byte(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	if w[0].KeySeed != seed {
		t.Fatalf("got seed %d, want %d", w[0].KeySeed, int64(seed))
	}
}

func TestWorkloadRejectsTrailingData(t *testing.T) {
	if _, err := createWorklist(Parameters{}, []byte(`{} {}`), nil); err == nil {
		t.Fatal("expected an error for data after the top-level object")
	}
}
