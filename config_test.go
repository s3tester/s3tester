package main

import (
	"golang.org/x/time/rate"
	"math"
	"strconv"
	"testing"
)

func TestDefaultArgs(t *testing.T) {
	args := parseAndValidate([]string{})

	if args.concurrency != 1 {
		t.Fatalf("wrong default concurrency: %v", args.concurrency)
	}

	if args.osize != 30*1024 {
		t.Fatalf("wrong default osize: %v", args.osize)
	}

	if args.endpoints[0] != "https://127.0.0.1:18082" {
		t.Fatalf("wrong default endpoint: %v", args.endpoints[0])
	}

	if len(args.endpoints) != 1 {
		t.Fatalf("wrong default endpoint list size: %v", len(args.endpoints))
	}

	if args.optype != "put" {
		t.Fatalf("wrong default optype: %v", args.optype)
	}

	if args.bucketname != "test" {
		t.Fatalf("wrong default bucketname: %s", args.bucketname)
	}

	if args.objectprefix != "testobject" {
		t.Fatalf("wrong default objectprefix: %v", args.objectprefix)
	}

	if args.tagging != "" {
		t.Fatalf("wrong default tagging: %v", args.tagging)
	}

	if args.metadata != "" {
		t.Fatalf("wrong default metadata: %v", args.metadata)
	}

	if args.logdetail != "" {
		t.Fatalf("wrong default logdetail: %v", args.logdetail)
	}

	if args.ratePerSecond != rate.Limit(math.MaxFloat64) {
		t.Fatalf("wrong default maxRate: %v", args.ratePerSecond)
	}

	if args.objrange != "" {
		t.Fatalf("wrong default objrange: %v", args.objrange)
	}

	if args.consistencyControl != "" {
		t.Fatalf("wrong default consistencyControl: %v", args.consistencyControl)
	}

	if args.reducedRedundancy {
		t.Fatalf("wrong default reducedRedundancy: %v", args.reducedRedundancy)
	}

	if args.overwrite != 0 {
		t.Fatalf("wrong default overwrite: %v", args.overwrite)
	}

	if args.retries != 0 {
		t.Fatalf("wrong default retries: %v", args.retries)
	}

	if args.retrySleep != 0 {
		t.Fatalf("wrong default retrySleep: %v", args.retrySleep)
	}

	if args.lockstep {
		t.Fatalf("wrong default lockstep: %v", args.lockstep)
	}

	if args.attempts != 1 {
		t.Fatalf("wrong default attempts: %v", args.attempts)
	}

	if args.region != "us-east-1" {
		t.Fatalf("wrong default region: %v", args.region)
	}

	if args.partsize != 5*(1<<20) {
		t.Fatalf("wrong default partsize: %v", args.partsize)
	}

	if args.verify != 0 {
		t.Fatalf("wrong default verify: %v", args.verify)
	}

	if args.nrequests.set {
		t.Fatalf("args.nrequests should not be set by default")
	}

	if args.duration.set {
		t.Fatalf("args.duration should not be set by default")
	}

	if args.nrequests.value != 1000 {
		t.Fatalf("args.nrequests should be set by default to 1000 (%d)", args.nrequests.value)
	}

	if args.duration.set {
		t.Fatalf("args.duration should not be set by default")
	}

	if args.tier != "standard" {
		t.Fatalf("wrong default restore tier")
	}

	if args.days != 1 {
		t.Fatalf("wrong default restore days")
	}

	if args.profile != "" {
		t.Fatalf("wrong default profile")
	}

	if args.nosign != false {
		t.Fatalf("wrong default nosign")
	}
}

func TestNonDefaultMetadata(t *testing.T) {
	cmdline := []string{"-metadata=key=value"}
	args, _ := parse(cmdline)

	if args.metadata != "key=value" {
		t.Fatalf("wrong metadata: %s", args.metadata)
	}
}

func TestNonDefaultTagging(t *testing.T) {
	cmdline := []string{"-tagging=key=value"}
	args, _ := parse(cmdline)

	if args.tagging != "key=value" {
		t.Fatalf("wrong metadata: %s", args.metadata)
	}
}

func TestNotAllowToSetDurationAndNumberRequests(t *testing.T) {
	cmdline := []string{"-duration=1", "-requests=1"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("duration and requests should not both be set")
	}
}

func TestRepeatMustBeGreaterThanZero(t *testing.T) {
	cmdline := []string{"-repeat=-1"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("repeat cannot be zero")
	}
}

func TestConcurrencyMustBeGreaterThanZero(t *testing.T) {
	cmdline := []string{"-concurrency=0"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("concurrency cannot be zero")
	}

	cmdline = []string{"-concurrency=-1"}
	_, err = parse(cmdline)

	if err == nil {
		t.Fatalf("concurrency cannot be negative")
	}
}

func TestRequestsMustBeGreaterThanZero(t *testing.T) {
	cmdline := []string{"-requests=0"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("number of requests cannot be zero")
	}

	cmdline = []string{"-requests=-1"}
	_, err = parse(cmdline)

	if err == nil {
		t.Fatalf("number of requests cannot be negative")
	}
}

func TestTooSmallMultipartPartSizes(t *testing.T) {
	partsize := 5 * (1 << 20)
	partsize = partsize - 100
	cmdline := []string{"-operation=multipartput", "-partsize=" + strconv.Itoa(partsize)}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("part sizes less than 5 MiB should fail")
	}
}

func TestTooManyMultipartParts(t *testing.T) {
	partsize := 5 * (1 << 20)
	size := partsize * 10001
	cmdline := []string{"-operation=multipartput", "-partsize=" + strconv.Itoa(partsize), "-size=" + strconv.Itoa(size)}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("should be limited to 10,000 parts")
	}
}

func TestRetriesMustBeGreaterThanEqualZero(t *testing.T) {
	cmdline := []string{"-retries=-1"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("retries cannot be negative")
	}
}

func TestInvalidOperation(t *testing.T) {
	cmdline := []string{"-operation=fakeop"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid operations should fail")
	}
}

func TestInvalidConsistency(t *testing.T) {
	cmdline := []string{"-consistency=fake"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid consistency should fail")
	}
}

func TestValidConsistency(t *testing.T) {
	cmdline := []string{"-consistency=all"}
	_, err := parse(cmdline)

	if err != nil {
		t.Fatalf("all consistency should succeed")
	}
}

func TestValidEndpoint(t *testing.T) {
	oneEndpoint := []string{"-endpoint=https://127.0.0.1:18082"}
	_, err := parse(oneEndpoint)

	if err != nil {
		t.Fatalf("all endpoint should succeed")
	}

	twoEndpoint := []string{"-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082", "-concurrency=2"}
	args, newErr := parse(twoEndpoint)

	if newErr != nil {
		t.Fatalf("all endpoint should succeed")
	}

	if len(args.endpoints) != 2 {
		t.Fatalf("should have 2 endpoints in the list")
	}
}

func TestInvalidEndpoint(t *testing.T) {
	oneEndpoint := []string{"-endpoint=test"}
	_, err := parse(oneEndpoint)

	if err == nil {
		t.Fatalf("all endpoint should not succeed")
	}

	twoEndpoint := []string{"-endpoint=https://127.0.0.1:18082,test"}
	_, newErr := parse(twoEndpoint)

	if newErr == nil {
		t.Fatalf("all endpoint should not succeed")
	}
}

func TestValidEndpointConcurrancy(t *testing.T) {
	oneEndpoint := []string{"-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082", "-concurrency=10"}
	args, err := parse(oneEndpoint)

	if err != nil {
		t.Fatalf("all endpoint & concurrancy should succeed")
	}

	if len(args.endpoints) != 2 {
		t.Fatalf("should be 2")
	}
}

func TestInvalidEndpointConcurrancy(t *testing.T) {
	oneEndpoint := []string{"-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082", "-concurrency=9"}
	_, err := parse(oneEndpoint)

	if err == nil {
		t.Fatalf("all endpoint & concurrancy should not succeed")
	}

	twoEndpoint := []string{"-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082"}
	_, newErr := parse(twoEndpoint)

	if newErr == nil {
		t.Fatalf("default concurrancy should fail")
	}
}

func TestDuplicateEndpoints(t *testing.T) {
	hasDuplicatePass := []string{"-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082,https://127.0.0.2:18082", "-concurrency=10"}
	_, hasDuplicateErr := parse(hasDuplicatePass)

	if hasDuplicateErr == nil {
		t.Fatalf("duplicate should fail")
	}

	hasDuplicateFail := []string{"-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082,https://127.0.0.2:18082", "-concurrency=9"}
	_, hasDuplicateFailErr := parse(hasDuplicateFail)

	if hasDuplicateFailErr == nil {
		t.Fatalf("duplicate should fail")
	}
}

func TestEndpointsWithWhiteSpace(t *testing.T) {
	cmd := []string{"-endpoint=https://127.0.0.1:18082,      https://127.0.0.2:18082", "-concurrency=10"}
	args, err := parse(cmd)

	if err != nil {
		t.Fatalf("white space should pass")
	}

	if len(args.endpoints) != 2 {
		t.Fatalf("should be 2")
	}
}

func TestValidTier(t *testing.T) {
	cmdline := []string{"-tier=Standard"}
	_, err := parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = []string{"-tier=Bulk"}
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = []string{"-tier=Expedited"}
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = []string{"-tier=standard"}
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = []string{"-tier=bulk"}
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = []string{"-tier=expedited"}
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}
}

func TestInvalidTier(t *testing.T) {
	cmdline := []string{"-tier=fake"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid restore tier should fail")
	}
}

func TestValidDays(t *testing.T) {
	cmdline := []string{"-days=5"}
	_, err := parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}
}

func TestInvalidDays(t *testing.T) {
	cmdline := []string{"-days=0"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid restore tier should fail")
	}
}

func TestProfileAndNosign(t *testing.T) {
	cmdline := []string{"-profile=tester", "-no-sign-request"}
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid profile and nosign should fail")
	}
}
