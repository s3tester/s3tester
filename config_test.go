package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"testing"

	"golang.org/x/time/rate"
)

func generateValidCmdlineSetting(toAdds ...string) []string {
	defaultCmdlineSetting := []string{}
	for _, toAdd := range toAdds {
		defaultCmdlineSetting = append(defaultCmdlineSetting, toAdd)
	}
	return defaultCmdlineSetting
}

func getArgs(config *Config) Parameters {
	if len(config.worklist) == 1 {
		return config.worklist[0]
	}
	return Parameters{}
}

func TestParametersMerge(t *testing.T) {
	p := Parameters{}
	fields := map[string]interface{}{}
	fields["operation"] = "put"
	fields["bucket"] = "bucket"
	err := p.Merge(fields, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Operation != "put" {
		t.Fatal()
	}
	if p.Bucket != "bucket" {
		t.Fatal()
	}
}

func TestParametersMergeNilEmpty(t *testing.T) {
	p := Parameters{}
	err := p.Merge(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = p.Merge(map[string]interface{}{}, []string{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestParametersMergeIgnore(t *testing.T) {
	p := Parameters{}
	fields := map[string]interface{}{}
	fields["operation"] = "put"
	ignore := []string{"operation", "invalid"}
	err := p.Merge(fields, ignore)
	if err != nil {
		t.Fatal(err)
	}
	if p.Operation != "" {
		t.Fatal()
	}
	if len(fields) != 1 {
		t.Fatal()
	}
}

func CheckArgValidity(tb testing.TB, flagName string, argValidity map[string]bool) {
	for flagValue, valid := range argValidity {
		cmdline := generateValidCmdlineSetting(fmt.Sprintf("-%s=%s", flagName, flagValue))
		_, err := parse(cmdline)
		if !valid && err == nil {
			tb.Fatalf("%s flag value %s is invalid, should fail", flagName, flagValue)
		} else if valid && err != nil {
			tb.Fatalf("%s flag value %s is valid, should not fail", flagName, flagValue)
		}
	}
}

func TestDefaultArgs(t *testing.T) {
	config, err := parse([]string{})

	// default tester settings
	if err != nil {
		t.Fatalf("should build a config with default settings")
	}

	if config.LogDetail != "" {
		t.Fatalf("wrong default logdetail: %v", config.LogDetail)
	}

	if config.Retries != 0 {
		t.Fatalf("wrong default retries: %v", config.Retries)
	}

	if config.RetrySleep != 0 {
		t.Fatalf("wrong default retrySleep: %v", config.RetrySleep)
	}

	// default worklist settings
	if len(config.worklist) != 1 {
		t.Fatalf("should only build one test")
	}

	args := getArgs(config)

	if args.Size != 30*1024 {
		t.Fatalf("wrong default size: %v", args.Size)
	}

	if len(args.endpoints) != 1 {
		t.Fatalf("wrong default endpoint list size: %v", len(args.endpoints))
	}

	if args.Operation != "put" {
		t.Fatalf("wrong default operation: %v", args.Operation)
	}

	if args.Prefix != "testobject" {
		t.Fatalf("wrong default objectprefix: %v", args.Prefix)
	}

	if args.Tagging != "" {
		t.Fatalf("wrong default tagging: %v", args.Tagging)
	}

	if args.Metadata != "" {
		t.Fatalf("wrong default metadata: %v", args.Metadata)
	}

	if args.ratePerSecond != rate.Limit(math.MaxFloat64) {
		t.Fatalf("wrong default maxRate: %v", args.ratePerSecond)
	}

	if args.Range != "" {
		t.Fatalf("wrong default objrange: %v", args.Range)
	}

	if args.Overwrite != 0 {
		t.Fatalf("wrong default overwrite: %v", args.Overwrite)
	}

	if args.attempts != 1 {
		t.Fatalf("wrong default attempts: %v", args.attempts)
	}

	if args.Region != "us-east-1" {
		t.Fatalf("wrong default region: %v", args.Region)
	}

	if args.PartSize != 5*(1<<20) {
		t.Fatalf("wrong default partsize: %v", args.PartSize)
	}

	if args.Verify != 0 {
		t.Fatalf("wrong default verify: %v", args.Verify)
	}

	if args.Tier != "standard" {
		t.Fatalf("wrong default restore tier")
	}

	if args.Days != 1 {
		t.Fatalf("wrong default restore days")
	}

	if args.Profile != "" {
		t.Fatalf("wrong default profile")
	}

	if args.NoSignRequest != false {
		t.Fatalf("wrong default nosign")
	}

	if args.SuffixNaming != "separate" {
		t.Fatalf("wrong default suffix-naming")
	}

	if args.Incrementing != false {
		t.Fatalf("wrong default incrementing")
	}
}

func TestNonDefaultMetadata(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-metadata=key=value")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatal(err)
	}
	args := getArgs(config)

	if args.Metadata != "key=value" {
		t.Fatalf("wrong metadata: %s", args.Metadata)
	}
}

func TestNonDefaultTagging(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-tagging=key=value")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatal(err)
	}
	args := getArgs(config)
	if args.Tagging != "key=value" {
		t.Fatalf("wrong metadata: %s", args.Metadata)
	}
}

func TestNotAllowDurationAndDeleteOps(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-duration=1", "-operation=delete")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("duration and operation delete can not both be set")
	}
}

func TestNotAllowDurationAndRequestAndUnsupportedOps(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-duration=1", "-requests=1", "-operation=put")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("duration, request and operation put can not all be set")
	}
}

func TestNotAllowDurationAndUnsupportedOps(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-duration=1", "-operation=get")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("duration and operation get can not both be set without requests set")
	}
}

func TestNotAllowDurationAndMixedWorkloadOps(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-duration=1", "-mixed-workload=/path/to/the-workload-file.json")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("duration and mixed-workload cannot both be set")
	}
}

func TestRepeatMustBeGreaterThanZero(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-repeat=-1")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("repeat cannot be zero")
	}
}

func TestConcurrencyMustBeGreaterThanZero(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-concurrency=0")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("concurrency cannot be zero")
	}

	cmdline = generateValidCmdlineSetting("-concurrency=-1")
	_, err = parse(cmdline)

	if err == nil {
		t.Fatalf("concurrency cannot be negative")
	}
}

func TestRequestsMustBeGreaterThanZero(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-requests=0")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("number of requests cannot be zero")
	}

	cmdline = generateValidCmdlineSetting("-requests=-1")
	_, err = parse(cmdline)

	if err == nil {
		t.Fatalf("number of requests cannot be negative")
	}
}

func TestTooSmallMultipartPartSizes(t *testing.T) {
	partsize := 5 * (1 << 20)
	partsize = partsize - 100
	cmdline := generateValidCmdlineSetting("-operation=multipartput", "-partsize="+strconv.Itoa(partsize))
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("part sizes less than 5 MiB should fail")
	}
}

func TestTooManyMultipartParts(t *testing.T) {
	partsize := 5 * (1 << 20)
	size := partsize * 10001
	cmdline := generateValidCmdlineSetting("-operation=multipartput", "-partsize="+strconv.Itoa(partsize), "-size="+strconv.Itoa(size))
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("should be limited to 10,000 parts")
	}
}

func TestRetriesMustBeGreaterThanEqualZero(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-retries=-1")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("retries cannot be negative")
	}
}

func TestInvalidOperation(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-operation=fake")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid operations should fail")
	}
}

func TestValidCustomHeader(t *testing.T) {
	cmdline := []string{"-header=key1:val1", "-header=key2:val2", "-header=key2:val2"}
	config, newErr := parse(cmdline)
	if newErr != nil {
		t.Fatalf("all endpoints and concurrencies should succeed")
	}
	args := getArgs(config)
	if len(args.Header) != 2 { // overwrites one
		t.Fatalf("wrong number of custom headers: %v", len(args.Header))
	}
	if args.Header["key1"] != "val1" {
		t.Fatalf("wrong custom headers: %v", args.Header["key1"])
	}
	if args.Header["key2"] != "val2" {
		t.Fatalf("wrong custom headers: %v", args.Header["key2"])
	}
}

func TestParseConcurrencyValue(t *testing.T) {
	args := generateValidCmdlineSetting("-concurrency=1")
	config, err := parse(args)
	if err != nil {
		t.Fatal("failed parsing concurrency value")
	}
	checkExpectInt(t, 1, config.worklist[0].Concurrency)
}

func TestConcurrencyValueValidity(t *testing.T) {
	concurrencyValidity := map[string]bool{
		"-123": false,
		"-1":   false,
		"0":    false,
		"1":    true,
		"123":  true,
	}
	CheckArgValidity(t, "concurrency", concurrencyValidity)
}

func TestValidEndpoint(t *testing.T) {
	oneEndpoint := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082")
	_, err := parse(oneEndpoint)

	if err != nil {
		t.Fatalf("all endpoint should succeed")
	}

	twoEndpoint := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082", "-concurrency=2")
	config, newErr := parse(twoEndpoint)
	if newErr != nil {
		t.Fatalf("All endpoints should succeed: %v", newErr)
	}
	args := getArgs(config)
	if len(args.endpoints) != 2 {
		t.Fatalf("should have 2 endpoints in the list")
	}
}

func TestInvalidEndpoint(t *testing.T) {
	oneEndpoint := generateValidCmdlineSetting("-endpoint=test")
	_, err := parse(oneEndpoint)

	if err == nil {
		t.Fatalf("all endpoint should not succeed")
	}

	twoEndpoint := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082,test")
	_, newErr := parse(twoEndpoint)

	if newErr == nil {
		t.Fatalf("all endpoint should not succeed")
	}
}

func TestValidEndpointConcurrency(t *testing.T) {
	twoEndpoint := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082", "-concurrency=10")
	config, newErr := parse(twoEndpoint)
	if newErr != nil {
		t.Fatalf("all endpoints and concurrencies should succeed")
	}
	args := getArgs(config)
	if len(args.endpoints) != 2 {
		t.Fatalf("should be 2")
	}
}

func TestInvalidEndpointConcurrency(t *testing.T) {
	oneEndpoint := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082", "-concurrency=9")
	_, err := parse(oneEndpoint)

	if err == nil {
		t.Fatalf("all endpoints and concurrencies should not succeed")
	}

	twoEndpoint := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082")
	_, newErr := parse(twoEndpoint)

	if newErr == nil {
		t.Fatalf("default concurrency should fail")
	}
}

func TestDuplicateEndpoints(t *testing.T) {
	hasDuplicatePass := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082,https://127.0.0.2:18082", "-concurrency=10")
	_, hasDuplicateErr := parse(hasDuplicatePass)

	if hasDuplicateErr == nil {
		t.Fatalf("duplicate should fail")
	}

	hasDuplicateFail := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082,https://127.0.0.2:18082,https://127.0.0.2:18082", "-concurrency=9")
	_, hasDuplicateFailErr := parse(hasDuplicateFail)

	if hasDuplicateFailErr == nil {
		t.Fatalf("duplicate should fail")
	}
}

func TestEndpointsWithWhiteSpace(t *testing.T) {
	cmd := generateValidCmdlineSetting("-endpoint=https://127.0.0.1:18082,      https://127.0.0.2:18082", "-concurrency=10")
	config, err := parse(cmd)
	if err != nil {
		t.Fatalf("white space should pass")
	}
	args := getArgs(config)
	if len(args.endpoints) != 2 {
		t.Fatalf("should be 2")
	}
}

func TestValidTier(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-tier=Standard")
	_, err := parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = generateValidCmdlineSetting("-tier=Bulk")
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = generateValidCmdlineSetting("-tier=Expedited")
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = generateValidCmdlineSetting("-tier=standard")
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = generateValidCmdlineSetting("-tier=bulk")
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}

	cmdline = generateValidCmdlineSetting("-tier=expedited")
	_, err = parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}
}

func TestInvalidTier(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-tier=fake")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid restore tier should fail")
	}
}

func TestValidDays(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-days=5")
	_, err := parse(cmdline)

	if err != nil {
		t.Fatalf("valid restore tier should succeed")
	}
}

func TestInvalidDays(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-days=0")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid restore tier should fail")
	}
}

func TestValidSuffixNaming(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-suffix-naming=Together")
	_, err := parse(cmdline)

	if err != nil {
		t.Fatalf("valid suffix-naming should succeed")
	}
}

func TestInvalidSuffixNaming(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-suffix-naming=fakevalue")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid suffix-naming should fail")
	}
}

func TestProfileAndNosign(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-profile=tester", "-no-sign-request")
	_, err := parse(cmdline)

	if err == nil {
		t.Fatalf("invalid profile and nosign should fail")
	}
}

func createWorkloadJSON(tb testing.TB, jsonName, jsonString string) {
	tb.Helper()
	file, err := os.Create(jsonName)
	if err != nil {
		tb.Fatal(err)
	}
	fmt.Fprintf(file, jsonString)
	file.Close()
}

func checkExpectInt(t *testing.T, expect, actual int) {
	if expect != actual {
		t.Fatalf("Expected: %d, Got: %d", expect, actual)
	}
}

func checkExpectString(t *testing.T, expect, actual string) {
	if expect != actual {
		t.Fatalf("Expected: %s, Got: %s", expect, actual)
	}
}

func TestValidWorkload(t *testing.T) {
	workload := `{
		"global": {
			"concurrency": 100,
			"requests": 600,
			"bucket": "src",
			"endpoint": "https://test.com",
			"operation": "head"
		},
		"workload": [
			{"operation": "put"},
			{"operation": "get","requests": 500},
			{"prefix":"test","wait":1,"operation": "delete","concurrency":200},
			{}
		]
	}`
	workloadFileName := "validWorkload.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-prefix=notTest", "-workload=" + workloadFileName})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}

	if len(test.worklist) != 4 {
		t.Fatalf("should have exactly 4 tasks")
	}

	// checking 1st task
	checkExpectInt(t, 600, test.worklist[0].Requests)
	checkExpectInt(t, 100, test.worklist[0].Concurrency)
	checkExpectString(t, "src", test.worklist[0].Bucket)
	checkExpectString(t, "https://test.com", test.worklist[0].endpoints[0])
	checkExpectString(t, "put", test.worklist[0].Operation)
	checkExpectString(t, "notTest", test.worklist[0].Prefix)
	checkExpectInt(t, 0, test.worklist[0].Wait)

	// checking 2nd task (perworkload priority applied)
	checkExpectInt(t, 500, test.worklist[1].Requests)
	checkExpectInt(t, 100, test.worklist[1].Concurrency)
	checkExpectString(t, "src", test.worklist[1].Bucket)
	checkExpectString(t, "https://test.com", test.worklist[1].endpoints[0])
	checkExpectString(t, "get", test.worklist[1].Operation)
	checkExpectString(t, "notTest", test.worklist[1].Prefix)

	// checking 3rd task (cmdline priority applied)
	checkExpectInt(t, 600, test.worklist[2].Requests)
	checkExpectInt(t, 200, test.worklist[2].Concurrency)
	checkExpectInt(t, 1, test.worklist[2].Wait)
	checkExpectString(t, "src", test.worklist[2].Bucket)
	checkExpectString(t, "https://test.com", test.worklist[2].endpoints[0])
	checkExpectString(t, "delete", test.worklist[2].Operation)
	checkExpectString(t, "notTest", test.worklist[2].Prefix)

	// checking 4th task (global priority applied)
	checkExpectInt(t, 600, test.worklist[3].Requests)
	checkExpectInt(t, 100, test.worklist[3].Concurrency)
	checkExpectString(t, "src", test.worklist[3].Bucket)
	checkExpectString(t, "https://test.com", test.worklist[3].endpoints[0])
	checkExpectString(t, "head", test.worklist[3].Operation)
	checkExpectString(t, "notTest", test.worklist[3].Prefix)
	checkExpectInt(t, 0, test.worklist[3].Wait)
}

func TestValidTemplatedWorkload(t *testing.T) {
	workload := `{
		"global": {
			"concurrency": 100,
			"requests": 600,
			"bucket": "src",
			"endpoint": "https://test.com",
			"operation": "head"
		},
		"workload": [
			{{range $i, $v := makeSlice "put" "get" "head" "delete"}}{{if ne $i 0}}{{","}}{{end}}{{print "{\"operation\": \"" $v "\"}"}}{{end}}
		]
	}`
	workloadFileName := "validWorkload.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-prefix=notTest", "-workload=" + workloadFileName})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}

	if len(test.worklist) != 4 {
		t.Fatalf("should have exactly 4 tasks")
	}

	// checking 1st task
	checkExpectInt(t, 600, test.worklist[0].Requests)
	checkExpectInt(t, 100, test.worklist[0].Concurrency)
	checkExpectString(t, "src", test.worklist[0].Bucket)
	checkExpectString(t, "https://test.com", test.worklist[0].endpoints[0])
	checkExpectString(t, "put", test.worklist[0].Operation)
	checkExpectString(t, "notTest", test.worklist[0].Prefix)
	checkExpectInt(t, 0, test.worklist[0].Wait)

	// checking 2nd task (perworkload priority applied)
	checkExpectInt(t, 600, test.worklist[1].Requests)
	checkExpectInt(t, 100, test.worklist[1].Concurrency)
	checkExpectString(t, "src", test.worklist[1].Bucket)
	checkExpectString(t, "https://test.com", test.worklist[1].endpoints[0])
	checkExpectString(t, "get", test.worklist[1].Operation)
	checkExpectString(t, "notTest", test.worklist[1].Prefix)
	checkExpectInt(t, 0, test.worklist[1].Wait)

	// checking 3rd task (cmdline priority applied)
	checkExpectInt(t, 600, test.worklist[2].Requests)
	checkExpectInt(t, 100, test.worklist[2].Concurrency)
	checkExpectString(t, "src", test.worklist[2].Bucket)
	checkExpectString(t, "https://test.com", test.worklist[2].endpoints[0])
	checkExpectString(t, "head", test.worklist[2].Operation)
	checkExpectString(t, "notTest", test.worklist[2].Prefix)
	checkExpectInt(t, 0, test.worklist[2].Wait)

	// checking 4th task (global priority applied)
	checkExpectInt(t, 600, test.worklist[3].Requests)
	checkExpectInt(t, 100, test.worklist[3].Concurrency)
	checkExpectString(t, "src", test.worklist[3].Bucket)
	checkExpectString(t, "https://test.com", test.worklist[3].endpoints[0])
	checkExpectString(t, "delete", test.worklist[3].Operation)
	checkExpectString(t, "notTest", test.worklist[3].Prefix)
	checkExpectInt(t, 0, test.worklist[3].Wait)
}

func TestInvalidTemplatedWorkload(t *testing.T) {
	workload := `{
		"global": {
			"concurrency": 100,
			"requests": 600,
			"bucket": "src",
			"endpoint": "https://test.com",
			"operation": "head"
		},
		"workload": [
			{{invalid}}
		]
	}`
	workloadFileName := "validWorkload.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	_, err := parse([]string{"-prefix=notTest", "-workload=" + workloadFileName})
	if err == nil {
		t.Fatal(err)
	}
}

func TestWorkloadUsingBothRequestsAndDurationAndUnsupportedOps(t *testing.T) {
	workload := `{
		"global": {
			"concurrency": 100,
			"bucket": "src",
			"requests": 600,
			"endpoint": "https://test.com"
		},
		"workload": [
			{"operation": "options", "duration":10}
		]
	}`
	workloadFileName := "invalid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	_, err := parse([]string{"-workload=" + workloadFileName})
	if err == nil {
		t.Fatalf("workload using both requests and duration should fail with operation option")
	}
}

func TestWorkloadUsingBothRequestsAndDurationAndAndSupportedOps(t *testing.T) {
	workload := `{
		"global": {
			"concurrency": 100,
			"bucket": "src",
			"requests": 600,
			"endpoint": "https://test.com"
		},
		"workload": [
			{"operation": "get", "duration":10}
		]
	}`
	workloadFileName := "validWorkload.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	_, err := parse([]string{"-workload=" + workloadFileName})
	if err != nil {
		t.Fatalf("workload using both requests and duration should not fail with operation get")
	}
}

func TestWorkloadWithTwoEndpoints(t *testing.T) {
	workload := `{
		"global": {
			"concurrency": 100,
			"bucket": "src",
			"requests": 600,
			"endpoint": "https://test.com,https://test2.com"
		},
		"workload": [
			{"operation": "options"}
		]
	}`
	workloadFileName := "invalid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-workload=" + workloadFileName})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}
	if len(test.worklist) != 1 {
		t.Fatalf("should have exactly 1 tasks")
	}
	checkExpectInt(t, 600, test.worklist[0].Requests)
	checkExpectInt(t, 100, test.worklist[0].Concurrency)
	checkExpectString(t, "src", test.worklist[0].Bucket)
	checkExpectInt(t, 2, len(test.worklist[0].endpoints))
	checkExpectString(t, "https://test.com", test.worklist[0].endpoints[0])
	checkExpectString(t, "https://test2.com", test.worklist[0].endpoints[1])
	checkExpectString(t, "options", test.worklist[0].Operation)
}

func TestCreateWorklistEmpty(t *testing.T) {
	p := Parameters{}
	data := `{}`
	ignore := []string{}
	w, err := createWorklist(p, []byte(data), ignore)
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != 0 {
		t.Fatal()
	}
}

func TestCreateWorklistWithNil(t *testing.T) {
	p := Parameters{}
	w, err := createWorklist(p, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != 0 {
		t.Fatal()
	}

	data := `{}`
	w, err = createWorklist(p, []byte(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != 0 {
		t.Fatal()
	}
}

func TestCreateWorklist(t *testing.T) {
	p := Parameters{Tier: "standard", Days: 1}
	w, err := createWorklist(p, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != 0 {
		t.Fatal()
	}

	data := `{
		"global": {
			"concurrency": 8,
			"bucket": "bucket",
			"requests": 8,
			"endpoint": "https://test.com"
		},
		"workload": [
			{
				"operation": "put",
				"addressing-style": "path"
			},
			{
				"operation": "delete",
				"addressing-style": "virtual"
			}
		]
	}`
	w, err = createWorklist(p, []byte(data), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(w) != 2 {
		t.Fatalf("Expected %d worklists but found: %d", 2, len(w))
	}

	firstWorklist, secondWorklist := w[0], w[1]
	if firstWorklist.Bucket != "bucket" {
		t.Fatalf("Expected bucket name: %s but found: %s", "bucket", firstWorklist.Bucket)
	}
	if firstWorklist.Operation != "put" {
		t.Fatalf("Expected operation: %s but found: %s", "put", firstWorklist.Operation)
	}
	if firstWorklist.AddressingStyle != "path" {
		t.Fatalf("Expected addressing-style: %s but found: %s", "path", firstWorklist.AddressingStyle)
	}

	if secondWorklist.Bucket != "bucket" {
		t.Fatalf("Expected bucket name: %s but found: %s", "bucket", secondWorklist.Bucket)
	}
	if secondWorklist.Operation != "delete" {
		t.Fatalf("Expected operation: %s but found: %s", "put", secondWorklist.Operation)
	}
	if secondWorklist.AddressingStyle != "virtual" {
		t.Fatalf("Expected addressing-style: %s but found: %s", "virtual", firstWorklist.AddressingStyle)
	}
}

func TestWorkloadWithHeaders(t *testing.T) {
	workload := ` {
		"workload": [
			{"header":
				{
					"header1":"value1",
					"header2":"value2"
				}
			}
		]
	}`
	workloadFileName := "valid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-workload=" + workloadFileName})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}
	args := test.worklist[0]
	if len(test.worklist) != 1 {
		t.Fatalf("should have exactly 1 tasks")
	}
	if len(args.Header) != 2 {
		t.Fatalf("should have 2 headers")
	}
	checkExpectString(t, "value1", args.Header["header1"])
	checkExpectString(t, "value2", args.Header["header2"])
}

func TestCmdlineWorkloadWithHeaders(t *testing.T) {
	workload := ` {
		"workload": [
			{"header":
				{
					"header1":"value1",
					"header2":"value2"
				}
			}
		]
	}`
	workloadFileName := "valid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-workload=" + workloadFileName, "-header=header:value", "-header=another-header:another-value"})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}
	args := test.worklist[0]
	if len(test.worklist) != 1 {
		t.Fatalf("should have exactly 1 tasks")
	}
	if len(args.Header) != 2 {
		t.Fatalf("should have 2 headers")
	}
	checkExpectString(t, "value", args.Header["header"])
	checkExpectString(t, "another-value", args.Header["another-header"])
}

func TestGlobalWorkloadWithHeaders(t *testing.T) {
	workload := ` {
		"global": {
			"header": {
				"header1":"value2",
				"header2":"value1"
			}
		},
		"workload": [
			{"header":
				{
					"header1":"value1",
					"header2":"value2"
				}
			}
		]
	}`
	workloadFileName := "valid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-workload=" + workloadFileName})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}
	args := test.worklist[0]
	if len(test.worklist) != 1 {
		t.Fatalf("should have exactly 1 tasks")
	}
	if len(args.Header) != 2 {
		t.Fatalf("should have 2 headers")
	}
	checkExpectString(t, "value1", args.Header["header1"])
	checkExpectString(t, "value2", args.Header["header2"])
}

func TestGlobalWorkloadNotApplyWithHeaders(t *testing.T) {
	workload := ` {
		"global": {
			"header": {
				"header1":"value2",
				"header2":"value1"
			}
		},
		"workload": [
			{"header":
				{
					"header1":"value1",
					"header2":"value2"
				}
			},
			{}
		]
	}`
	workloadFileName := "valid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-workload=" + workloadFileName})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}
	if len(test.worklist) != 2 {
		t.Fatalf("should have exactly 2 tasks")
	}
	args := test.worklist[0]
	if len(args.Header) != 2 {
		t.Fatalf("should have 2 headers")
	}
	checkExpectString(t, "value1", args.Header["header1"])
	checkExpectString(t, "value2", args.Header["header2"])

	args2 := test.worklist[1]
	if len(args2.Header) != 2 {
		t.Fatalf("should have 2 headers")
	}
	checkExpectString(t, "value2", args2.Header["header1"])
	checkExpectString(t, "value1", args2.Header["header2"])
}

func TestReduceRedundancyBackwardsCompatibility(t *testing.T) {
	workload := ` {
		"workload": [
			{"header":
				{
					"header1":"value1",
					"header2":"value2"
				}
			}
		]
	}`
	workloadFileName := "valid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-workload=" + workloadFileName, "-rr=true"})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}
	args := test.worklist[0]
	if len(test.worklist) != 1 {
		t.Fatalf("should have exactly 1 tasks")
	}
	if len(args.Header) != 1 {
		t.Fatalf("should have 1 headers")
	}
	checkExpectString(t, "REDUCED_REDUNDANCY", args.Header["x-amz-storage-class"])
}

func TestNoReduceRedundancyBackwardsCompatibility(t *testing.T) {
	workload := ` {
		"workload": [
			{"header":
				{
					"header1":"value1",
					"header2":"value2"
				}
			}
		]
	}`
	workloadFileName := "valid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-workload=" + workloadFileName, "-rr=false"})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}
	args := test.worklist[0]
	if len(test.worklist) != 1 {
		t.Fatalf("should have exactly 1 tasks")
	}
	if len(args.Header) != 2 {
		t.Fatalf("should have 2 headers")
	}
	checkExpectString(t, "value1", args.Header["header1"])
	checkExpectString(t, "value2", args.Header["header2"])
}

func TestConsistencyControlBackwardsCompatibility(t *testing.T) {
	workload := ` {
		"workload": [
			{"header":
				{
					"header1":"value1",
					"header2":"value2"
				}
			}
		]
	}`
	workloadFileName := "valid.json"
	createWorkloadJSON(t, workloadFileName, workload)
	defer os.Remove(workloadFileName)
	test, err := parse([]string{"-workload=" + workloadFileName, "-consistency=all"})
	if err != nil {
		t.Fatalf("valid workload should not fail %s", err)
	}
	args := test.worklist[0]
	if len(test.worklist) != 1 {
		t.Fatalf("should have exactly 1 tasks")
	}
	if len(args.Header) != 1 {
		t.Fatalf("should have 1 headers")
	}
	checkExpectString(t, "all", args.Header["Consistency-Control"])
}

func TestInvalidConsistencyControlBackwardsCompatibility(t *testing.T) {
	_, err := parse([]string{"-consistency=invalid"})
	if err == nil {
		t.Fatalf("invalid workload should fail %s", err)
	}
}

func TestRandomRangeValidation(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-operation=get", "-range=\"bytes=0-10\"", "-random-range=0-10/1")
	_, err := parse(cmdline)
	if err.Error() != randomRangeWithRangeErr {
		t.Fatalf("invalid random-range should fail with error message: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=put", "-random-range=0-10/1")
	_, err = parse(cmdline)
	if err == nil || err.Error() != randomRangeWithoutGetErr {
		t.Fatalf("invalid random-range should fail with error message: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-random-range=11-10/1")
	_, err = parse(cmdline)
	if err == nil || err.Error() != randomRangeInvalidMinMaxErr {
		t.Fatalf("invalid random-range should fail with error message: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-random-range=500-600/200")
	_, err = parse(cmdline)
	if err == nil || err.Error() != randomRangeInvalidSizeErr {
		t.Fatalf("invalid random-range should fail with error message: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-random-range=0-10/12")
	_, err = parse(cmdline)
	if err == nil || err.Error() != randomRangeInvalidSizeErr {
		t.Fatalf("invalid random-range should fail with error message: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-random-range=0-10/11")
	_, err = parse(cmdline)
	if err != nil {
		t.Fatalf("Expected no error, but got: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-random-range=0-10/")
	_, err = parse(cmdline)
	if err == nil || err.Error() != randomRangeInvalidSizeErr {
		t.Fatalf("invalid random-range should fail with error message: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-random-range=0-10")
	_, err = parse(cmdline)
	if err == nil || err.Error() != randomRangeInvalidFormat {
		t.Fatalf("invalid random-range should fail with error message: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-random-range=0-0/100")
	_, err = parse(cmdline)
	if err == nil || err.Error() != randomRangeInvalidMinMaxErr {
		t.Fatalf("invalid random-range should fail with error message: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-random-range=0-100/10")
	var config *Config
	config, err = parse(cmdline)
	if err != nil {
		t.Fatalf("Expected no error, but got: %s", err)
	}

	args := config.worklist[0]
	if args.randomRangeMin != 0 {
		t.Fatalf("Expected randomRangeMin: 0, but got: %d", args.randomRangeMin)
	}
	if args.randomRangeMax != 100 {
		t.Fatalf("Expected randomRangeMax: 100, but got: %d", args.randomRangeMax)
	}
}

func TestUniformDist(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-operation=get", "-uniformDist=1000-100000")
	_, err := parse(cmdline)
	if err != nil {
		t.Fatalf("Expected no error, but got: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=put", "-uniformDist=1000-100000")
	_, err = parse(cmdline)
	if err != nil {
		t.Fatalf("Expected no error, but got: %s", err)
	}

	cmdline = generateValidCmdlineSetting("-operation=delete", "-uniformDist=1000-100000")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to invalid operation type")
	}

	cmdline = generateValidCmdlineSetting("-operation=put", "-uniformDist=-1000-100000")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to invalid range")
	}

	cmdline = generateValidCmdlineSetting("-operation=put", "-uniformDist=100000-1000")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to invalid range")
	}
}

func TestRangeValidation(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-operation=get", "-range=bytes=0-10", "-random-range=0-10/1")
	_, err := parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to including both -range and -random-range")
	}

	cmdline = generateValidCmdlineSetting("-operation=put", "-range=bytes=0-10")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to invalid operation type")
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-range=bytes=11-10")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to invalid range")
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-range=bytes=500-")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to missing RangeMax")
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-range=bytes=-600")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to missing RangeMin")
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-range=0-10")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to invalid range format")
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-range=bytes=0-10, 20-30")
	_, err = parse(cmdline)
	if err == nil {
		t.Fatalf("Expected command to fail due to invalid range format")
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-range=bytes=10.0-15.0")
	_, err = parse(cmdline)
	expectedErr := "Unable to parse range: min must be >= 0 and an integer"
	if err == nil || err.Error() != expectedErr {
		t.Fatalf("invalid range should fail with error message: %s", expectedErr)
	}

	cmdline = generateValidCmdlineSetting("-operation=get", "-range=bytes=0-10")
	_, err = parse(cmdline)
	if err != nil {
		t.Fatalf("Expected no err, but got %s", err)
	}
}

func TestIsDurationOperation(t *testing.T) {
	type Test struct {
		operation string
		duration  int
		expected  bool
	}
	tests := []Test{
		{"get", 5, false},
		{"put", 0, false},
		{"put", 1, true},
		{"multipartput", 5, true},
		{"options", 10, true},
	}
	for _, test := range tests {
		params := Parameters{
			Operation: test.operation,
			Duration:  test.duration,
		}
		if params.IsDurationOperation() != test.expected {
			t.Fatalf("IsDurationOperation with operation=%s and duration=%d should return %v", test.operation, test.duration, test.expected)
		}
	}
}

func TestExactByteSize(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-size=1231")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatalf("expected no error, but got %v", err)
	}
	expect := byteSize(1231)
	got := config.worklist[0].Size
	if got != expect {
		t.Fatalf("expected %v, got %v", expect, got)
	}
}

func TestExactPartSize(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-partsize=15125712")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatalf("expected no error, but got %v", err)
	}
	expect := byteSize(15125712)
	got := config.worklist[0].PartSize
	if got != expect {
		t.Fatalf("expected %v, got %v", expect, got)
	}
}

func TestMetricByteSize(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-size=2MB")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatalf("expected no error, but got %v", err)
	}
	expect := byteSize(2 * 1000 * 1000)
	got := config.worklist[0].Size
	if got != expect {
		t.Fatalf("expected %v, got %v", expect, got)
	}
}

func TestBinaryByteSize(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-size=2MiB")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatalf("expected no error, but got %v", err)
	}
	expect := byteSize(2 * 1024 * 1024)
	got := config.worklist[0].Size
	if got != expect {
		t.Fatalf("expected %v, got %v", expect, got)
	}
}

func TestMetricBytePartSize(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-partsize=5GB")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatalf("expected no error, but got %v", err)
	}
	expect := byteSize(5 * 1000 * 1000 * 1000)
	got := config.worklist[0].PartSize
	if got != expect {
		t.Fatalf("expected %v, got %v", expect, got)
	}
}

func TestBinaryBytePartSize(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-partsize=5GiB")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatalf("expected no error, but got %v", err)
	}
	expect := byteSize(5 * 1024 * 1024 * 1024)
	got := config.worklist[0].PartSize
	if got != expect {
		t.Fatalf("expected %v, got %v", expect, got)
	}
}

func TestInvalidSize(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-size=3VB")
	_, err := parse(cmdline)
	if err == nil {
		t.Fatalf("expected error, but got none")
	}
}

func TestInvalidPartSize(t *testing.T) {
	cmdline := generateValidCmdlineSetting("-partsize=3QB")
	_, err := parse(cmdline)
	if err == nil {
		t.Fatalf("expected error, but got none")
	}
}

func TestNegativeSize(t *testing.T) {
	_, err := parseByteSize("-1")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestNegativeByteSize(t *testing.T) {
	_, err := parseByteSize("-23KB")
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
}
