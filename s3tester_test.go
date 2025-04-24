package main

import (
	// "encoding/json"
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
)

const (
	bodyString    = "hello"
	bodyLength    = int64(len(bodyString))
	xmlBodyString = `<CopyObjectResult>
	<ETag>"a1a160470db7a1c3907e5152afa0b90c"</ETag>
	<LastModified>2020-07-12T12:34:00.999999999Z</LastModified>
</CopyObjectResult>`

	accessKey string = "AWS_ACCESS_KEY_ID"
	secretKey string = "AWS_SECRET_ACCESS_KEY"
)

func ResponseSuccessful(response *http.Response, err error) bool {
	return err == nil && response.StatusCode >= 200 && response.StatusCode < 300
}

// A key to use for customizing per-request results
type Request struct {
	URI    string
	Method string
}

type Response struct {
	Body    string
	Status  int
	Headers map[string]string
}

// A helper struct for stubbing out HTTP endpoints,
// and collecting all received requests.
type HTTPHelper struct {
	mu               sync.RWMutex
	Server           *httptest.Server
	requests         []*http.Request      // stores all the http requests received by this test server
	bodies           []string             // stores all the request bodies received by this test server
	perRequestResult map[Request]Response // maps requests to results; for customizing server results on a per-request basis
}

func (h *HTTPHelper) Endpoint() string {
	return h.Server.URL
}

func (h *HTTPHelper) Empty() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.requests) == 0
}

func (h *HTTPHelper) Size() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.requests)
}

func (h *HTTPHelper) NumBodies() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.bodies)
}

func (h *HTTPHelper) NumRequests() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.requests)
}

func (h *HTTPHelper) Request(index int) *http.Request {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.requests[index]
}

func (h *HTTPHelper) Body(index int) string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.bodies[index]
}

func (h *HTTPHelper) AppendRequest(req *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.requests = append(h.requests, req)
}

func (h *HTTPHelper) AppendBody(body string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.bodies = append(h.bodies, body)
}

func (h *HTTPHelper) RequestResult(req Request) (Response, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	r, ok := h.perRequestResult[req]
	return r, ok
}

func (h *HTTPHelper) SetRequestResult(req Request, res Response) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.perRequestResult[req] = res
}

// RangeRequests invokes on f on each request received. Do not invoke other instance
// methods of h in f() or they will deadlock.
func (h *HTTPHelper) RangeRequests(f func(*http.Request)) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, req := range h.requests {
		f(req)
	}
}

// Shutdown closes all active goroutines and perform cleanup of HttpHelper class.
func (h *HTTPHelper) Shutdown() {
	h.Server.Close()
}

func setValidAccessKeyEnv() {
	os.Setenv(accessKey, "PUIQNFNDWLHT95AMJVOW")
	os.Setenv(secretKey, "rFM0SP5t6na2WkU1Uk8LvIpEukqt/GJc8A/CphkD")
}

func NewHTTPHelper(tb testing.TB, getResponseBody string, headers map[string]string) *HTTPHelper {
	tb.Helper()
	// ensure env that s3tester expects exists.
	setValidAccessKeyEnv()

	helper := &HTTPHelper{perRequestResult: make(map[Request]Response)}
	handle := http.NewServeMux()
	handle.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		helper.AppendRequest(r)

		s := ""
		if r.Body != nil {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				tb.Fatal(err)
			}
			s = string(body)
		}
		helper.AppendBody(s)

		if response, ok := helper.RequestResult(Request{URI: r.URL.RequestURI(), Method: r.Method}); ok {
			for key, value := range response.Headers {
				w.Header().Set(key, value)
			}

			w.WriteHeader(response.Status)
			w.Write([]byte(response.Body))
		}

		for key, value := range headers {
			w.Header().Set(key, value)
		}

		if r.Method == "GET" || r.Method == "POST" || (r.Method == "PUT" && len(getResponseBody) > 0) {
			if _, ok := headers["Content-Length"]; !ok {
				w.Header().Set("Content-Length", fmt.Sprintf("%d", len(getResponseBody)))
			}
			w.Write([]byte(getResponseBody))
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})
	handle.HandleFunc("/not", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	ts := httptest.NewServer(handle)
	helper.Server = ts
	return helper
}

type S3TesterHelper struct {
	*HTTPHelper
	endpoints []*HTTPHelper
	args      Parameters
	config    *Config
}

func initS3TesterHelper(tb testing.TB, op string) S3TesterHelper {
	tb.Helper()
	emptyHeaders := make(map[string]string)
	httpHelper := NewHTTPHelper(tb, bodyString, emptyHeaders)
	config := testArgs(tb, op, httpHelper.Endpoint())
	return S3TesterHelper{HTTPHelper: httpHelper, config: config, args: config.worklist[0]}
}

func initWorkloadTesterHelper(tb testing.TB, workload string) S3TesterHelper {
	tb.Helper()
	emptyHeaders := make(map[string]string)
	httpHelper := NewHTTPHelper(tb, bodyString, emptyHeaders)
	config := createTesterWithWorkload(tb, workload, httpHelper.Endpoint())
	return S3TesterHelper{HTTPHelper: httpHelper, config: config}
}

func initMixedWorkloadTesterHelper(tb testing.TB) S3TesterHelper {
	tb.Helper()
	emptyHeaders := make(map[string]string)
	httpHelper := NewHTTPHelper(tb, xmlBodyString, emptyHeaders)
	config := createTesterWithMixedWorkload(tb, httpHelper.Endpoint())
	return S3TesterHelper{HTTPHelper: httpHelper, config: config, args: config.worklist[0]}
}

func initS3TesterHelperWithData(t *testing.T, op, responseBody string) S3TesterHelper {
	emptyHeaders := make(map[string]string)
	httpHelper := NewHTTPHelper(t, responseBody, emptyHeaders)
	config := testArgs(t, op, httpHelper.Endpoint())
	return S3TesterHelper{HTTPHelper: httpHelper, config: config, args: config.worklist[0]}
}

func initMultiS3TesterHelper(t *testing.T, op string, endpoints int) S3TesterHelper {
	emptyHeaders := make(map[string]string)
	return initMultiS3TesterHelperWithHeader(t, op, endpoints, emptyHeaders)
}

func initMultiS3TesterHelperWithHeader(t *testing.T, op string, endpoints int, header map[string]string) S3TesterHelper {
	return initMultiS3TesterHelperWithHeaderAndData(t, op, endpoints, header, bodyString)
}

func initMultiS3TesterHelperWithData(t *testing.T, op string, endpoints int, responseBody string) S3TesterHelper {
	emptyHeaders := make(map[string]string)
	return initMultiS3TesterHelperWithHeaderAndData(t, op, endpoints, emptyHeaders, responseBody)
}

func initMultiS3TesterHelperWithHeaderAndData(t *testing.T, op string, endpoints int, header map[string]string, responseBody string) S3TesterHelper {
	endpointList := make([]string, endpoints)
	httpHelperList := make([]*HTTPHelper, endpoints)
	for i := 0; i < endpoints; i++ {
		httpHelper := NewHTTPHelper(t, responseBody, header)
		httpHelperList[i] = httpHelper
		endpointList[i] = httpHelper.Endpoint()
	}
	config := testArgs(t, op, endpointList[0])
	config.worklist[0].endpoints = endpointList
	config.worklist[0].Requests = endpoints
	return S3TesterHelper{endpoints: httpHelperList, config: config, args: config.worklist[0]}
}

func (h *S3TesterHelper) ShutdownMultiServer() {
	for _, endpoint := range h.endpoints {
		endpoint.Shutdown()
	}
}

func (h *S3TesterHelper) runTesterWithoutValidation(t *testing.T) results {
	setValidAccessKeyEnv()
	syscallParams := NewSyscallParams(h.args)
	defer syscallParams.detach()
	return runtest(context.Background(), h.config, h.args, syscallParams)
}

func (h *S3TesterHelper) runTester(t *testing.T) results {
	t.Helper()
	testResults := h.runTesterWithoutValidation(t)
	if testResults.CumulativeResult.Failcount > 0 {
		t.Fatalf("Failed to run test. %d failures.", testResults.CumulativeResult.Failcount)
	}

	if h.Empty() {
		t.Fatalf("Did not receive any requests")
	}

	if h.NumBodies() != h.NumRequests() {
		t.Fatalf("numBodies %d does not match numRequests %d", h.NumBodies(), h.NumRequests())
	}

	return testResults
}

func (h *S3TesterHelper) runTesterWithMultiEndpoints(t *testing.T) results {
	testResults := h.runTesterWithoutValidation(t)
	if testResults.CumulativeResult.Failcount > 0 {
		t.Fatalf("Failed to run test. %d failures.", testResults.CumulativeResult.Failcount)
	}

	for _, endpoint := range h.endpoints {
		if endpoint.Empty() {
			t.Fatalf("Did not receive any requests")
		}

		if endpoint.NumBodies() != endpoint.NumRequests() {
			t.Fatalf("numBodies %d does not match numRequests %d", endpoint.NumBodies(), endpoint.NumRequests())
		}
	}

	return testResults
}

func createTesterWithWorkload(tb testing.TB, workload, endpoint string) *Config {
	tb.Helper()
	return createTesterRequiredJSON(tb, workload, []string{"-workload=file.json", "-endpoint=" + endpoint})
}

func createTesterWithMixedWorkload(tb testing.TB, endpoint string) *Config {
	tb.Helper()
	config, err := parse(generateValidCmdlineSetting("-requests=1", "-concurrency=1", "-size=0", "-bucket=test", "-prefix=object", "-mixed-workload=mixed-workload.json", "-endpoint="+endpoint))
	if err != nil {
		tb.Fatal(err)
	}
	return config
}

func createTesterRequiredJSON(tb testing.TB, content string, tags []string) *Config {
	tb.Helper()
	workloadFileName := "file.json"
	createWorkloadJSON(tb, workloadFileName, content)
	defer os.Remove(workloadFileName)
	config, err := parse(tags)
	if err != nil {
		tb.Fatal(err)
	}
	return config
}

func createFile(tb testing.TB, fileName, fileContent string) *os.File {
	file, err := os.Create(fileName)
	if err != nil {
		tb.Fatal(err)
	}
	fmt.Fprintf(file, fileContent)
	return file
}

func testArgs(tb testing.TB, op string, endpoint string) *Config {
	tb.Helper()
	cmdline := generateValidCmdlineSetting("-requests=1", "-concurrency=1", "-endpoint="+endpoint, "-operation="+op, "-bucket=test", "-prefix=object")
	config, err := parse(cmdline)
	if err != nil {
		tb.Fatal(err)
	}
	config.worklist[0].Concurrency = 1
	config.worklist[0].Size = 0
	config.worklist[0].Operation = op
	config.worklist[0].Bucket = "test"
	config.worklist[0].Prefix = "object"
	return config
}

type s3XmlErrorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func generateErrorXML(tb testing.TB, code string) (response string) {
	tb.Helper()
	b, err := xml.Marshal(s3XmlErrorResponse{Code: code, Message: code})
	if err != nil {
		tb.Fatal(err)
	}
	response = string(b)
	return
}

func TestLoadAnonymousCredential(t *testing.T) {
	config := testArgs(t, "put", "www.example.com:18082")
	args := config.worklist[0]
	args.NoSignRequest = true
	svc, err := MakeS3Service(http.DefaultClient, config, &args, args.Endpoint)
	if err != nil {
		t.Fatal("Error creating S3 service", err)
	}
	if svc.Config.Credentials != credentials.AnonymousCredentials {
		t.Fatalf("not standard anonymous credentials: %v", svc.Config.Credentials)
	}
}

func TestLoadEnvCredential(t *testing.T) {
	testAccessKey := "testkey"
	testSecretKey := testAccessKey + "/" + testAccessKey
	os.Setenv(accessKey, testAccessKey)
	os.Setenv(secretKey, testSecretKey)

	config := testArgs(t, "put", "www.example.com:18082")
	args := config.worklist[0]
	svc, err := MakeS3Service(http.DefaultClient, config, &args, args.Endpoint)
	if err != nil {
		t.Fatal("Error creating S3 service", err)
	}

	val, err := svc.Config.Credentials.Get()
	if err != nil {
		t.Fatal("error getting Credential", err)
	}

	if val.AccessKeyID != testAccessKey || val.SecretAccessKey != testSecretKey {
		t.Fatalf("Credentials are wrong, %s | %s", val.AccessKeyID, val.SecretAccessKey)
	}
	if val.ProviderName != "EnvConfigCredentials" {
		t.Fatalf("Expecting it to use EnvConfigCredentials, but it's using %s", val.ProviderName)
	}
}

func TestLoadDefaultCliCredential(t *testing.T) {
	config := testArgs(t, "put", "www.example.com:18082")
	args := config.worklist[0]

	os.Setenv(accessKey, "")
	os.Setenv(secretKey, "")
	defaultPath := ""
	system := runtime.GOOS
	if system == "darwin" || system == "linux" {
		os.Setenv("HOME", "/user")
		defaultPath = "/user/.aws/credentials"
	} else if system == "windows" {
		os.Setenv("UserProfile", "\\user\\")
		defaultPath = "\\user\\.aws\\credentials"
	}
	if _, err := os.Stat(defaultPath); os.IsNotExist(err) {
		args.Profile = ""
		_, err := MakeS3Service(http.DefaultClient, config, &args, args.Endpoint)
		if err == nil {
			t.Fatalf("No credential should be loaded")
		}

		args.Profile = "test"
		_, err = MakeS3Service(http.DefaultClient, config, &args, args.Endpoint)
		if err == nil {
			t.Fatalf("No credential should be loaded")
		}
	} else {
		args.Profile = ""
		_, err := MakeS3Service(http.DefaultClient, config, &args, args.Endpoint)
		if err != nil {
			t.Fatalf("Default credential should be loaded")
		}
	}
}

func TestLoadDefaultCredentialProfileFromFile(t *testing.T) {
	config := testArgs(t, "put", "www.example.com:18082")
	args := config.worklist[0]
	testAccessKey := "testkey"
	user1AccessKey := testAccessKey + testAccessKey
	testSecretKey := testAccessKey + "/" + testAccessKey
	user1SecretKey := testSecretKey + testSecretKey
	fileName := "credential_test"
	fileContent := []byte("[default]\naws_access_key_id=" + testAccessKey + "\naws_secret_access_key=" + testSecretKey + "\n\n" + "[user1]\naws_access_key_id=" + user1AccessKey + "\naws_secret_access_key=" + user1SecretKey)
	os.WriteFile(fileName, fileContent, 0644)
	defer os.Remove(fileName)
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	os.Setenv("AWS_SHARED_CREDENTIALS_FILE", dir+"/"+fileName)
	svc, err := MakeS3Service(http.DefaultClient, config, &args, args.Endpoint)
	if err != nil {
		t.Fatal("Error creating S3 service", err)
	}

	val, err := svc.Config.Credentials.Get()
	if err != nil {
		t.Fatal(err)
	}
	if val.AccessKeyID != testAccessKey || val.SecretAccessKey != testSecretKey {
		t.Fatalf("Credentials are wrong, %s | %s", val.AccessKeyID, val.SecretAccessKey)
	}
	if !strings.HasPrefix(val.ProviderName, "SharedConfigCredentials") {
		t.Fatalf("Expecting it to use SharedConfigCredentials, but it's using %s", val.ProviderName)
	}
}

func TestLoadCredentialProfileFromFile(t *testing.T) {
	testAccessKey := "testkey"
	user1AccessKey := testAccessKey + testAccessKey
	testSecretKey := testAccessKey + "/" + testAccessKey
	user1SecretKey := testSecretKey + testSecretKey
	fileName := "credential_test"
	fileContent := []byte("[default]\naws_access_key_id=" + testAccessKey + "\naws_secret_access_key=" + testSecretKey + "\n\n" + "[user1]\naws_access_key_id=" + user1AccessKey + "\naws_secret_access_key=" + user1SecretKey)
	os.WriteFile(fileName, fileContent, 0644)
	defer os.Remove(fileName)
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	os.Setenv("AWS_SHARED_CREDENTIALS_FILE", dir+"/"+fileName)

	config := testArgs(t, "put", "www.example.com:18082")
	args := config.worklist[0]
	args.Profile = "user1"
	svc, err := MakeS3Service(http.DefaultClient, config, &args, args.Endpoint)
	if err != nil {
		t.Fatal("Error creating S3 service", err)
	}
	val, err := svc.Config.Credentials.Get()
	if err != nil {
		t.Fatal(err)
	}
	if val.AccessKeyID != user1AccessKey || val.SecretAccessKey != user1SecretKey {
		t.Fatalf("Credentials are wrong, %s | %s", val.AccessKeyID, val.SecretAccessKey)
	}
	if !strings.HasPrefix(val.ProviderName, "SharedConfigCredentials") {
		t.Fatalf("Expecting it to use SharedConfigCredentials, but it's using %s", val.ProviderName)
	}
}

func TestMainWithGet(t *testing.T) {
	h := initS3TesterHelper(t, "get")
	defer h.Shutdown()
	cpuProfileFile := "test1"
	logDetailFile := "test2"
	logLatencyFile := "test3"
	setValidAccessKeyEnv()
	os.Args = []string{"nothing", "-operation=get", "-endpoint=" + h.Endpoint(), "-requests=1", "-concurrency=1", "-bucket=test", "-prefix=object", "-json",
		"-logdetail=" + logDetailFile, "-loglatency=" + logLatencyFile, "-cpuprofile=" + cpuProfileFile}
	defer os.Remove(cpuProfileFile)
	defer os.Remove(logDetailFile)
	defer os.Remove(logLatencyFile)

	config, err := parse(os.Args[1:])
	if err != nil {
		t.Fatal(err)
	}
	results := executeTester(context.Background(), config)
	failCount, err := handleTesterResults(config, results)
	if err != nil {
		t.Fatal(err)
	}
	if failCount > 0 {
		t.Fatal(failCount)
	}

	// validate server received requests
	if h.Request(0).Method != "GET" {
		t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "" {
		t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}

	if _, err := os.Stat(logLatencyFile); os.IsNotExist(err) {
		t.Fatalf("Log latency file didn't create")
	}
}

func TestSingleEndpointOverwrite1(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.Requests = 5
	h.args.Concurrency = 5
	h.args.Overwrite = 1
	testResults := h.runTester(t)

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(0).Method)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "" {
		t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}

	for i := 0; i < 5; i++ {
		if h.Request(i).URL.Path != "/test/object" {
			t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
		}
	}

	if testResults.CumulativeResult.UniqObjNum != 1 {
		t.Fatalf("Expect only one unique object for overwrite=1, but got %d", testResults.CumulativeResult.UniqObjNum)
	}
}

func TestSingleEndpointOverwrite2(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.Requests = 5
	h.args.Concurrency = 5
	h.args.Overwrite = 2
	testResults := h.runTester(t)

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(0).Method)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "" {
		t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}

	for i := 0; i < 5; i++ {
		if h.Request(i).URL.Path != "/test/object-0" {
			t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
		}
	}

	if testResults.CumulativeResult.UniqObjNum != 1 {
		t.Fatalf("Expect one unique object for overwrite=2, but got %d", testResults.CumulativeResult.UniqObjNum)
	}
}

func TestMultiEndpointOverwrite1(t *testing.T) {
	endpoints := 2
	op := "put"
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.Requests = 10
	h.args.Concurrency = 10
	h.args.Overwrite = 1
	testResults := h.runTesterWithMultiEndpoints(t)

	for _, h := range h.endpoints {
		for i := 0; i < 5; i++ {
			if h.Request(i).Method != "PUT" {
				t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(i).Method)
			}
			if h.Request(i).Header.Get("Consistency-Control") != "" {
				t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(i).Header.Get("Consistency-Control"))
			}
			if h.Request(i).URL.Path != "/test/object" {
				t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
			}
		}
	}

	if testResults.CumulativeResult.UniqObjNum != 1 {
		t.Fatalf("Expect 1 unique object for multiendpoint overwrite=1, but got %d", testResults.CumulativeResult.UniqObjNum)
	}
}

func TestMultiEndpointOverwrite2(t *testing.T) {
	endpoints := 2
	op := "put"
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.Requests = 8
	h.args.Concurrency = 4
	h.args.Overwrite = 2
	testResults := h.runTesterWithMultiEndpoints(t)

	for _, h := range h.endpoints {
		for i := 0; i < 4; i++ {
			if h.Request(i).Method != "PUT" {
				t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(i).Method)
			}
			if h.Request(i).Header.Get("Consistency-Control") != "" {
				t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(i).Header.Get("Consistency-Control"))
			}
			if h.Request(i).URL.Path != "/test/object-0" && h.Request(i).URL.Path != "/test/object-1" {
				t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
			}
		}
	}

	if testResults.CumulativeResult.UniqObjNum != 2 {
		t.Fatalf("Expect 2 unique objects for multiendpoint overwrite=2, but got %d", testResults.CumulativeResult.UniqObjNum)
	}
}

func TestConcurrentDurationOperation(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.Duration = 1
	h.args.Concurrency = 10
	h.runTester(t)

	// Initialize a set of expected paths to be run
	expectedRequestPaths := make(map[string]struct{})
	for i := 0; i < h.NumRequests(); i++ {
		path := fmt.Sprintf("/test/object-%d", i)
		expectedRequestPaths[path] = struct{}{}
	}

	for i := 0; i < h.NumRequests(); i++ {
		delete(expectedRequestPaths, h.Request(i).URL.Path)
	}

	if len(expectedRequestPaths) != 0 {
		t.Fatalf("Concurrent duration operation should send request to monotonically incrementing keys")
	}
}

func TestGETWithDurationAndRequests(t *testing.T) {
	h := initS3TesterHelper(t, "get")
	defer h.Shutdown()
	h.args.Requests = 1
	h.args.Duration = 1

	start := time.Now()
	h.runTester(t)
	elapsed := time.Since(start)

	if elapsed < time.Duration(h.args.Duration)*time.Second || h.NumRequests() <= 1 {
		t.Fatalf("GET with duration and requests should keep sending requests for duration argument")
	}
}

func TestGet(t *testing.T) {
	h := initS3TesterHelper(t, "get")
	defer h.Shutdown()
	testResults := h.runTester(t)

	if h.Request(0).Method != "GET" {
		t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if testResults.CumulativeResult.TotalObjectSize != bodyLength {
		t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", bodyLength, testResults.CumulativeResult.TotalObjectSize)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "" {
		t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}
}

func TestGetWhenLessDataReturnedThanContentLength(t *testing.T) {
	headers := make(map[string]string)
	headers["Content-Length"] = "100"
	httpHelper := NewHTTPHelper(t, bodyString, headers)
	config := testArgs(t, "get", httpHelper.Endpoint())
	h := S3TesterHelper{HTTPHelper: httpHelper, config: config, args: config.worklist[0]}
	defer h.Shutdown()
	testResults := h.runTesterWithoutValidation(t)
	if testResults.CumulativeResult.Failcount != 1 {
		t.Fatalf("Test should have failed. %d failures.", testResults.CumulativeResult.Failcount)
	}
}

func TestGetWithCustomHeader(t *testing.T) {
	emptyHeaders := make(map[string]string)
	httpHelper := NewHTTPHelper(t, bodyString, emptyHeaders)
	cmdline := generateValidCmdlineSetting("-requests=1", "-concurrency=1", "-endpoint="+httpHelper.Endpoint(), "-operation=get", "-bucket=test", "-prefix=object", "-size=0",
		"-header=Consistency-Control: all", "-header=abc:def", "-header=none:")
	config, err := parse(cmdline)
	if err != nil {
		t.Fatal(err)
	}
	h := S3TesterHelper{HTTPHelper: httpHelper, config: config, args: config.worklist[0]}
	defer h.Shutdown()
	h.runTester(t)

	if h.Request(0).Method != "GET" {
		t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "all" {
		t.Fatalf("Get should have set custom header Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}
	if h.Request(0).Header.Get("abc") != "def" {
		t.Fatalf("Get should have set custom header value for abc to def (actual: %s)", h.Request(0).Header.Get("abc"))
	}
	if h.Request(0).Header.Get("none") != "" {
		t.Fatalf("Get should have set custom header value for none to empty string (actual: %s)", h.Request(0).Header.Get("none"))
	}
}

func TestGetWithVerification(t *testing.T) {
	keys := []string{"object-0", "object-", "o", "object-0object-0", "object-0object-"}

	for _, k := range keys {
		h := initS3TesterHelperWithData(t, "get", k)
		defer h.Shutdown()
		h.args.Verify = 1
		h.args.Size = byteSize(len(k))
		testResults := h.runTester(t)

		if h.Request(0).Method != "GET" {
			t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
		}

		if h.Request(0).URL.Path != "/test/object-0" {
			t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
		}

		if testResults.CumulativeResult.TotalObjectSize != int64(len(k)) {
			t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", bodyLength, testResults.CumulativeResult.TotalObjectSize)
		}

		if h.Request(0).Header.Get("Consistency-Control") != "" {
			t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
		}
	}
}

func TestGetWithVerificationAndWrongSize(t *testing.T) {
	keys := []string{"object-0", "object-", "o", "object-0object-0", "object-0object-"}

	for _, k := range keys {
		h := initS3TesterHelperWithData(t, "get", k)
		defer h.Shutdown()
		h.args.Verify = 1
		h.args.Size = 5
		testResults := h.runTesterWithoutValidation(t)

		if testResults.CumulativeResult.Failcount != 1 {
			t.Fatalf("Test should have failed. %d failures.", testResults.CumulativeResult.Failcount)
		}
	}
}

func TestHead(t *testing.T) {
	h := initS3TesterHelper(t, "head")
	defer h.Shutdown()
	h.runTester(t)

	if h.Request(0).Method != "HEAD" {
		t.Fatalf("Wrong request type issued. Expected HEAD but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}
}

func TestOptions(t *testing.T) {
	h := initS3TesterHelper(t, "options")
	defer h.Shutdown()
	h.runTester(t)

	if h.Request(0).Method != "OPTIONS" {
		t.Fatalf("Wrong request type issued. Expected OPTIONS but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}
}

func TestOptionsFailure(t *testing.T) {
	h := initS3TesterHelper(t, "options")
	defer h.Shutdown()

	req := Request{URI: "/", Method: "OPTIONS"}
	resp := Response{Body: "", Status: 500}
	h.SetRequestResult(req, resp)

	testResults := h.runTesterWithoutValidation(t)

	if testResults.CumulativeResult.Failcount != 1 {
		t.Fatalf("Expected 1 failure. %d failures.", testResults.CumulativeResult.Failcount)
	}

	if h.Request(0).Method != "OPTIONS" {
		t.Fatalf("Wrong request type issued. Expected OPTIONS but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}
}

func TestPut(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.Size = 6
	testResults := h.runTester(t)

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if testResults.CumulativeResult.TotalObjectSize != 6 {
		t.Fatalf("TotalObjectSize is wrong size. Expected 6, but got %d", testResults.CumulativeResult.TotalObjectSize)
	}
}

func TestMultiplePuts(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.Requests = 5
	h.args.Size = 6
	testResults := h.runTester(t)

	if h.Size() != 5 {
		t.Fatalf("Should be 5 requests (%d)", h.Size())
	}

	for i := 0; i < h.args.Requests; i++ {
		if h.Request(i).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(i).Method)
		}
		if h.Request(i).URL.Path != "/test/object-"+strconv.Itoa(i) {
			t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
		}
	}

	if testResults.CumulativeResult.TotalObjectSize != 30 {
		t.Fatalf("TotalObjectSize is wrong size. Expected 30, but got %d", testResults.CumulativeResult.TotalObjectSize)
	}

	if testResults.CumulativeResult.UniqObjNum != 5 {
		t.Fatalf("uniqObjNum is %d. Expected 5.", testResults.CumulativeResult.UniqObjNum)
	}
}

func TestMultiplePutsWithRepeat(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.Requests = 5
	h.args.Size = 6
	h.args.attempts = 2 // uniqObjNum should still just be 5 even though we do each PUT twice
	testResults := h.runTester(t)

	if h.Size() != 10 {
		t.Fatalf("Should be 10 requests (%d)", h.Size())
	}

	for i := 0; i < h.args.Requests; i++ {
		if h.Request(i).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(i).Method)
		}
		if h.Request(i).URL.Path != "/test/object-"+strconv.Itoa(i/2) {
			t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
		}
	}

	if testResults.CumulativeResult.TotalObjectSize != 60 {
		t.Fatalf("TotalObjectSize is wrong size. Expected 60, but got %d", testResults.CumulativeResult.TotalObjectSize)
	}

	if testResults.CumulativeResult.UniqObjNum != 5 {
		t.Fatalf("uniqObjNum is %d. Expected 5.", testResults.CumulativeResult.UniqObjNum)
	}

	if testResults.CumulativeResult.Count != 10 {
		t.Fatalf("count is %d. Expected 10.", testResults.CumulativeResult.Count)
	}

	for i := 0; i < h.args.Requests*h.args.attempts-2; i = i + 2 {
		if h.Body(i) != h.Body(i+1) {
			t.Fatalf("wrong body for objects %d (%s) and %d (%s)", i, h.Body(i), i+1, h.Body(i+1))
		}
	}
}

func TestConcurrentPuts(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.Concurrency = 2
	h.args.Requests = 2
	h.runTester(t)

	if h.Size() != 2 {
		t.Fatalf("Should be 2 requests (%d)", h.Size())
	}

	foundPaths := []bool{false, false}
	for i := 0; i < h.args.Concurrency; i++ {
		if h.Request(i).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(i).Method)
		}
		if h.Request(i).URL.Path == "/test/object-0" {
			foundPaths[0] = true
		}
		if h.Request(i).URL.Path == "/test/object-0" {
			foundPaths[1] = true
		}
	}

	if !foundPaths[0] || !foundPaths[1] {
		t.Fatalf("Did not receive all expected PUTS. %v", foundPaths)
	}
}

func TestDelete(t *testing.T) {
	h := initS3TesterHelper(t, "delete")
	defer h.Shutdown()
	h.runTester(t)

	if h.Request(0).Method != "DELETE" {
		t.Fatalf("Wrong request type issued. Expected DELETE but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}
}

func TestPutTagging(t *testing.T) {
	h := initS3TesterHelper(t, "puttagging")
	defer h.Shutdown()
	h.args.Tagging = "tag1=value1"
	h.runTester(t)

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected DELETE but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).URL.RawQuery != "tagging=" {
		t.Fatalf("Wrong query param: %s", h.Request(0).URL.RawQuery)
	}

	type Tag struct {
		Key   string
		Value string
	}

	type TagSet struct {
		Tag []Tag
	}

	type Tagging struct {
		TagSet []TagSet
	}

	actualTagging := Tagging{}
	xml.Unmarshal([]byte(h.Body(0)), &actualTagging)

	expectedTag := Tag{Key: "tag1", Value: "value1"}
	expectedTagset := TagSet{[]Tag{expectedTag}}
	expectedTagging := Tagging{TagSet: []TagSet{expectedTagset}}

	if len(actualTagging.TagSet) != 1 || len(actualTagging.TagSet[0].Tag) != 1 || actualTagging.TagSet[0].Tag[0] != expectedTagging.TagSet[0].Tag[0] {
		t.Fatalf("Wrong body: %s", h.Body(0))
	}
}

func TestMetadataUpdate(t *testing.T) {
	h := initS3TesterHelperWithData(t, "updatemeta", xmlBodyString)
	defer h.Shutdown()
	h.args.Metadata = "key1=value1"
	h.runTester(t)

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).Header.Get("x-amz-metadata-directive") != "REPLACE" {
		t.Fatalf("Not replacing metadata, so not a metadata update: %s", h.Request(0).Header.Get("x-amz-metadata-directive"))
	}

	if h.Request(0).Header.Get("x-amz-copy-source") != "test/object-0" {
		t.Fatalf("Not copying to same key as destination, so not a metadata update: %s", h.Request(0).Header.Get("x-amz-copy-source"))
	}

	if h.Request(0).Header.Get("x-amz-meta-key1") != "value1" {
		t.Fatalf("Wrong metadata header received: %s", h.Request(0).Header.Get("x-amz-meta-key1"))
	}
}

func TestMultipartPut(t *testing.T) {
	headers := make(map[string]string)
	headers["ETag"] = "7e10e7d25dc4581d89b9285be5f384fd"
	httpHelper := NewHTTPHelper(t, bodyString, headers)
	config := testArgs(t, "multipartput", httpHelper.Endpoint())
	h := S3TesterHelper{HTTPHelper: httpHelper, config: config, args: config.worklist[0]}
	h.args.PartSize = 100
	h.args.Size = 200
	h.args.Metadata = "key1=value1"
	defer h.Shutdown()

	uid := "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"

	multipartInitiateBody :=
		`<?xml version="1.0" encoding="UTF-8"?>
		<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <Bucket>test</Bucket>
		  <Key>object-0</Key>
		  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
		</InitiateMultipartUploadResult>`
	multipartInitiateReq := Request{URI: "/test/object-0?uploads=", Method: "POST"}
	multipartInitiateResponse := Response{Body: multipartInitiateBody, Status: 200}
	h.SetRequestResult(multipartInitiateReq, multipartInitiateResponse)

	multipartCompleteBody :=
		`<?xml version="1.0" encoding="UTF-8"?>
		<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>
		  <Bucket>test</Bucket>
		  <Key>object-0</Key>
		  <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
		</CompleteMultipartUploadResult>`
	multipartCompleteReq := Request{URI: fmt.Sprintf("/test/object-0?uploadId=%s", uid), Method: "POST"}
	multipartCompleteResponse := Response{Body: multipartCompleteBody, Status: 200}
	h.SetRequestResult(multipartCompleteReq, multipartCompleteResponse)

	h.runTester(t)

	// initiate, 2 parts, complete
	if h.Size() != 4 {
		t.Fatalf("Should received 4 requests. Had %v requests", h.Size())
	}

	if h.Request(0).Method != "POST" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "POST", h.Request(0).Method)
	}
	if h.Request(0).URL.Path == "/test/object-0" && h.Request(0).URL.RawQuery != "uploads=" {
		t.Fatalf("Wrong url path. Expected /test/object-0?uploads, got %s?%s", h.Request(0).URL.Path, h.Request(0).URL.RawQuery)
	}

	if h.Request(0).Header.Get("x-amz-meta-key1") != "value1" {
		t.Fatalf("Wrong metadata header received: %s", h.Request(0).Header.Get("x-amz-meta-key1"))
	}

	if h.Request(1).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "PUT", h.Request(1).Method)
	}
	if h.Request(1).URL.Path == "/test/object-0" && h.Request(1).URL.RawQuery != fmt.Sprintf("partNumber=1&uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("partNumber=1&uploadId=%s", uid), h.Request(1).URL.Path, h.Request(1).URL.RawQuery)
	}
	if h.Request(1).Header.Get("Content-Length") != "100" {
		t.Fatalf("Wrong content length: %v", h.Request(1).Header.Get("Content-Length"))
	}

	if h.Request(2).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "PUT", h.Request(1).Method)
	}
	if h.Request(2).URL.Path == "/test/object-0" && h.Request(2).URL.RawQuery != fmt.Sprintf("partNumber=2&uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("partNumber=2&uploadId=%s", uid), h.Request(2).URL.Path, h.Request(2).URL.RawQuery)
	}
	if h.Request(2).Header.Get("Content-Length") != "100" {
		t.Fatalf("Wrong content length: %v", h.Request(2).Header.Get("Content-Length"))
	}

	if h.Request(3).Method != "POST" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "POST", h.Request(3).Method)
	}
	if h.Request(3).URL.Path == "/test/object-0" && h.Request(3).URL.RawQuery != fmt.Sprintf("uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("uploadId=%s", uid), h.Request(3).URL.Path, h.Request(3).URL.RawQuery)
	}
}

func TestMultipartPutWithUnevenPartsize(t *testing.T) {
	headers := make(map[string]string)
	headers["ETag"] = "7e10e7d25dc4581d89b9285be5f384fd"
	httpHelper := NewHTTPHelper(t, bodyString, headers)
	config := testArgs(t, "multipartput", httpHelper.Endpoint())
	h := S3TesterHelper{HTTPHelper: httpHelper, config: config, args: config.worklist[0]}
	h.args.PartSize = 110
	h.args.Size = 200
	h.args.Metadata = "key1=value1"
	defer h.Shutdown()

	uid := "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"

	multipartInitiateBody :=
		`<?xml version="1.0" encoding="UTF-8"?>
		<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <Bucket>test</Bucket>
		  <Key>object-0</Key>
		  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
		</InitiateMultipartUploadResult>`
	multipartInitiateReq := Request{URI: "/test/object-0?uploads=", Method: "POST"}
	multipartInitiateResponse := Response{Body: multipartInitiateBody, Status: 200}
	h.SetRequestResult(multipartInitiateReq, multipartInitiateResponse)

	multipartCompleteBody :=
		`<?xml version="1.0" encoding="UTF-8"?>
		<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>
		  <Bucket>test</Bucket>
		  <Key>object-0</Key>
		  <ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
		</CompleteMultipartUploadResult>`
	multipartCompleteReq := Request{URI: fmt.Sprintf("/test/object-0?uploadId=%s", uid), Method: "POST"}
	multipartCompleteResponse := Response{Body: multipartCompleteBody, Status: 200}
	h.SetRequestResult(multipartCompleteReq, multipartCompleteResponse)

	h.runTester(t)

	// initiate, 2 parts, complete
	if h.Size() != 4 {
		t.Fatalf("Should received 4 requests. Had %v requests", h.Size())
	}

	if h.Request(0).Method != "POST" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "POST", h.Request(0).Method)
	}
	if h.Request(0).URL.Path == "/test/object-0" && h.Request(0).URL.RawQuery != "uploads=" {
		t.Fatalf("Wrong url path. Expected /test/object-0?uploads, got %s?%s", h.Request(0).URL.Path, h.Request(0).URL.RawQuery)
	}

	if h.Request(0).Header.Get("x-amz-meta-key1") != "value1" {
		t.Fatalf("Wrong metadata header received: %s", h.Request(0).Header.Get("x-amz-meta-key1"))
	}

	if h.Request(1).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "PUT", h.Request(1).Method)
	}
	if h.Request(1).URL.Path == "/test/object-0" && h.Request(1).URL.RawQuery != fmt.Sprintf("partNumber=1&uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("partNumber=1&uploadId=%s", uid), h.Request(1).URL.Path, h.Request(1).URL.RawQuery)
	}
	if h.Request(1).Header.Get("Content-Length") != "110" {
		t.Fatalf("Wrong content length: %v", h.Request(1).Header.Get("Content-Length"))
	}

	if h.Request(2).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "PUT", h.Request(1).Method)
	}
	if h.Request(2).URL.Path == "/test/object-0" && h.Request(2).URL.RawQuery != fmt.Sprintf("partNumber=2&uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("partNumber=2&uploadId=%s", uid), h.Request(2).URL.Path, h.Request(2).URL.RawQuery)
	}
	if h.Request(2).Header.Get("Content-Length") != "90" {
		t.Fatalf("Wrong content length: %v", h.Request(2).Header.Get("Content-Length"))
	}

	if h.Request(3).Method != "POST" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "POST", h.Request(3).Method)
	}
	if h.Request(3).URL.Path == "/test/object-0" && h.Request(3).URL.RawQuery != fmt.Sprintf("uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("uploadId=%s", uid), h.Request(3).URL.Path, h.Request(3).URL.RawQuery)
	}
}

func TestMultipartPutFailureCallsAbort(t *testing.T) {
	headers := make(map[string]string)
	headers["ETag"] = "7e10e7d25dc4581d89b9285be5f384fd"
	httpHelper := NewHTTPHelper(t, bodyString, headers)
	config := testArgs(t, "multipartput", httpHelper.Endpoint())
	h := S3TesterHelper{HTTPHelper: httpHelper, config: config, args: config.worklist[0]}
	h.args.PartSize = 100
	h.args.Size = 200
	h.args.Metadata = "key1=value1"
	defer h.Shutdown()

	uid := "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"

	multipartInitiateBody :=
		`<?xml version="1.0" encoding="UTF-8"?>
		<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <Bucket>test</Bucket>
		  <Key>object-0</Key>
		  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
		</InitiateMultipartUploadResult>`
	multipartInitiateReq := Request{URI: "/test/object-0?uploads=", Method: "POST"}
	multipartInitiateResponse := Response{Body: multipartInitiateBody, Status: 200}
	h.SetRequestResult(multipartInitiateReq, multipartInitiateResponse)

	failedUploadPartReq := Request{URI: fmt.Sprintf("/test/object-0?partNumber=1&uploadId=%s", uid), Method: "PUT"}
	failedUploadPartResponse := Response{Body: generateErrorXML(t, "BadRequest"), Status: 400}
	h.SetRequestResult(failedUploadPartReq, failedUploadPartResponse)

	testResults := h.runTesterWithoutValidation(t)
	if testResults.CumulativeResult.Failcount != 1 {
		t.Fatalf("Should have failed to run test. %d failures.", testResults.CumulativeResult.Failcount)
	}

	// initiate, 1 part, abort
	if h.Size() != 3 {
		t.Fatalf("Should received 3 requests. Had %v requests", h.Size())
	}

	if h.Request(0).Method != "POST" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "POST", h.Request(0).Method)
	}
	if h.Request(0).URL.Path == "/test/object-0" && h.Request(0).URL.RawQuery != "uploads=" {
		t.Fatalf("Wrong url path. Expected /test/object-0?uploads, got %s?%s", h.Request(0).URL.Path, h.Request(0).URL.RawQuery)
	}

	if h.Request(0).Header.Get("x-amz-meta-key1") != "value1" {
		t.Fatalf("Wrong metadata header received: %s", h.Request(0).Header.Get("x-amz-meta-key1"))
	}

	if h.Request(1).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "PUT", h.Request(1).Method)
	}
	if h.Request(1).URL.Path == "/test/object-0" && h.Request(1).URL.RawQuery != fmt.Sprintf("partNumber=1&uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("partNumber=1&uploadId=%s", uid), h.Request(1).URL.Path, h.Request(1).URL.RawQuery)
	}
	if h.Request(1).Header.Get("Content-Length") != "100" {
		t.Fatalf("Wrong content length: %v", h.Request(1).Header.Get("Content-Length"))
	}

	if h.Request(2).Method != "DELETE" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "DELETE", h.Request(2).Method)
	}
	if h.Request(2).URL.Path == "/test/object-0" && h.Request(2).URL.RawQuery != fmt.Sprintf("uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("uploadId=%s", uid), h.Request(2).URL.Path, h.Request(2).URL.RawQuery)
	}
}

func TestUniformPuts(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	h.args.Requests = 10
	h.args.min = 10
	h.args.max = 20
	defer h.Shutdown()
	testResults := h.runTester(t)
	avgSize := float64(testResults.CumulativeResult.TotalObjectSize) / float64(testResults.CumulativeResult.Count)
	if testResults.CumulativeResult.TotalObjectSize > 200 || testResults.CumulativeResult.TotalObjectSize < 100 {
		t.Fatalf("sumObjectSize should be bounded by 100 to 200, but got %d", testResults.CumulativeResult.TotalObjectSize)
	}
	if avgSize < 10 || avgSize > 20 {
		t.Fatalf("avgSize is wrong size. Expected between 10-20, but got %v", avgSize)
	}
}

func TestMultiEndpointHead(t *testing.T) {
	op := "head"
	runNormalMultiEndpointTest(t, op)
}

func TestMultiEndpointOptions(t *testing.T) {
	op := "options"
	runNormalMultiEndpointTest(t, op)
}

func TestMultiEndpointGet(t *testing.T) {
	op := "get"
	runNormalMultiEndpointTest(t, op)
}

func TestMultiEndpointPut(t *testing.T) {
	op := "put"
	runNormalMultiEndpointTest(t, op)
}

func TestMultiEndpointDelete(t *testing.T) {
	op := "delete"
	runNormalMultiEndpointTest(t, op)
}

func runNormalMultiEndpointTest(t *testing.T, op string) {
	endpoints := 2
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.Concurrency = 2
	h.args.Size = 5
	testResults := h.runTesterWithMultiEndpoints(t)

	if op != "delete" && op != "head" && op != "options" && testResults.CumulativeResult.TotalObjectSize != 2*5 {
		t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", 2*5, testResults.CumulativeResult.TotalObjectSize)
	}

	for i, h := range h.endpoints {
		if h.Request(0).Method != strings.ToUpper(op) {
			t.Fatalf("Wrong request type issued. Expected %s but got %s", strings.ToUpper(op), h.Request(0).Method)
		}

		if h.NumRequests() != 1 {
			t.Fatalf("Wrong request number. Expected %d but got %d", 1, h.NumRequests())
		}

		if op != "options" && h.Request(0).URL.Path != "/test/object-"+strconv.Itoa(i) {
			t.Fatalf("Wrong url path: %s, %v", h.Request(0).URL.Path, "/test/object-"+strconv.Itoa(i))
		}

		if op == "options" && h.Request(0).URL.Path != "/" {
			t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
		}

		if h.Request(0).Header.Get("Consistency-Control") != "" {
			t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
		}
	}

	if testResults.CumulativeResult.Failcount != 0 {
		t.Fatalf("Should not have failed. %d failures.", testResults.CumulativeResult.Failcount)
	}

	validateEndpointResult(t, testResults, endpoints, h)
}

func TestLargePutRequestToMultiEndpoint(t *testing.T) {
	endpoints := 5
	op := "put"
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.Requests = 1000
	h.args.Concurrency = 10
	h.args.Size = 5
	testResults := h.runTesterWithMultiEndpoints(t)

	for i, h := range h.endpoints {
		if h.Request(0).Method != strings.ToUpper(op) {
			t.Fatalf("Wrong request type issued. Expected %s but got %s", strings.ToUpper(op), h.Request(0).Method)
		}

		if h.NumRequests() != 1000/endpoints {
			t.Fatalf("Wrong request number. Expected %d but got %d", 1000/endpoints, h.NumRequests())
		}

		if testResults.PerEndpointResult[i].TotalObjectSize != 1000 {
			t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", 1000, testResults.PerEndpointResult[i].TotalObjectSize)
		}
	}

	validateEndpointResult(t, testResults, endpoints, h)
}

func TestMultiEndpointPutTagging(t *testing.T) {
	endpoints := 2
	op := "puttagging"
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.Concurrency = 2
	h.args.Tagging = "tag1=value1"
	testResults := h.runTesterWithMultiEndpoints(t)

	for i, h := range h.endpoints {
		if h.Request(0).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected DELETE but got %s", h.Request(0).Method)
		}

		if h.Request(0).URL.Path != "/test/object-"+strconv.Itoa(i) {
			t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
		}

		if h.Request(0).URL.RawQuery != "tagging=" {
			t.Fatalf("Wrong query param: %s", h.Request(0).URL.RawQuery)
		}

		type Tag struct {
			Key   string
			Value string
		}

		type TagSet struct {
			Tag []Tag
		}

		type Tagging struct {
			TagSet []TagSet
		}

		actualTagging := Tagging{}
		xml.Unmarshal([]byte(h.Body(0)), &actualTagging)

		expectedTag := Tag{Key: "tag1", Value: "value1"}
		expectedTagset := TagSet{[]Tag{expectedTag}}
		expectedTagging := Tagging{TagSet: []TagSet{expectedTagset}}

		if len(actualTagging.TagSet) != 1 || len(actualTagging.TagSet[0].Tag) != 1 || actualTagging.TagSet[0].Tag[0] != expectedTagging.TagSet[0].Tag[0] {
			t.Fatalf("Wrong body: %s", h.Body(0))
		}
	}

	validateEndpointResult(t, testResults, endpoints, h)
}

func TestMultiEndpointMetadataUpdate(t *testing.T) {
	endpoints := 2
	op := "updatemeta"
	h := initMultiS3TesterHelperWithData(t, op, endpoints, xmlBodyString)
	defer h.ShutdownMultiServer()
	h.args.Concurrency = 2
	h.args.Metadata = "key1=value1"
	testResults := h.runTesterWithMultiEndpoints(t)

	for i, h := range h.endpoints {
		if h.Request(0).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected DELETE but got %s", h.Request(0).Method)
		}

		if h.Request(0).URL.Path != "/test/object-"+strconv.Itoa(i) {
			t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
		}

		if h.Request(0).Header.Get("x-amz-metadata-directive") != "REPLACE" {
			t.Fatalf("Not replacing metadata, so not a metadata update: %s", h.Request(0).Header.Get("x-amz-metadata-directive"))
		}

		if h.Request(0).Header.Get("x-amz-copy-source") != "test/object-"+strconv.Itoa(i) {
			t.Fatalf("Not copying to same key as destination, so not a metadata update: %s", h.Request(0).Header.Get("x-amz-copy-source"))
		}

		if h.Request(0).Header.Get("x-amz-meta-key1") != "value1" {
			t.Fatalf("Wrong metadata header received: %s", h.Request(0).Header.Get("x-amz-meta-key1"))
		}
	}

	validateEndpointResult(t, testResults, endpoints, h)
}

func TestMultiEndpointMultipartPut(t *testing.T) {
	endpoints := 2
	op := "multipartput"
	headers := make(map[string]string)
	headers["ETag"] = "7e10e7d25dc4581d89b9285be5f384fd"
	h := initMultiS3TesterHelperWithHeader(t, op, endpoints, headers)
	defer h.ShutdownMultiServer()
	h.args.PartSize = 100
	h.args.Size = 200
	h.args.Metadata = "key1=value1"
	h.args.Concurrency = 2
	h.args.Metadata = "key1=value1"

	uid := "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"

	for i, h := range h.endpoints {
		multipartInitiateBody :=
			`<?xml version="1.0" encoding="UTF-8"?>
			<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Bucket>test</Bucket>
			<Key>object-` + strconv.Itoa(i) + `</Key>
			<UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
			</InitiateMultipartUploadResult>`
		multipartInitiateReq := Request{URI: "/test/object-" + strconv.Itoa(i) + "?uploads=", Method: "POST"}
		multipartInitiateResponse := Response{Body: multipartInitiateBody, Status: 200}
		h.SetRequestResult(multipartInitiateReq, multipartInitiateResponse)

		multipartCompleteBody :=
			`<?xml version="1.0" encoding="UTF-8"?>
			<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Location>http://Example-Bucket.s3.amazonaws.com/Example-Object</Location>
			<Bucket>test</Bucket>
			<Key>object-` + strconv.Itoa(i) + `</Key>
			<ETag>"3858f62230ac3c915f300c664312c11f-9"</ETag>
			</CompleteMultipartUploadResult>`
		multipartCompleteReq := Request{URI: fmt.Sprintf("/test/object-"+strconv.Itoa(i)+"?uploadId=%s", uid), Method: "POST"}
		multipartCompleteResponse := Response{Body: multipartCompleteBody, Status: 200}
		h.SetRequestResult(multipartCompleteReq, multipartCompleteResponse)
	}

	testResults := h.runTesterWithMultiEndpoints(t)

	for i, h := range h.endpoints {
		// initiate, 2 parts, complete
		if h.Size() != 4 {
			t.Fatalf("Should received 4 requests. Had %v requests", h.Size())
		}

		if h.Request(0).Method != "POST" {
			t.Fatalf("Wrong request type issued. Expected %s but got %s", "POST", h.Request(0).Method)
		}
		if h.Request(0).URL.Path == "/test/object-"+strconv.Itoa(i)+"-0" && h.Request(0).URL.RawQuery != "uploads=" {
			t.Fatalf("Wrong url path. Expected /test/object-0?uploads, got %s?%s", h.Request(0).URL.Path, h.Request(0).URL.RawQuery)
		}

		if h.Request(0).Header.Get("x-amz-meta-key1") != "value1" {
			t.Fatalf("Wrong metadata header received: %s", h.Request(0).Header.Get("x-amz-meta-key1"))
		}

		if h.Request(1).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected %s but got %s", "PUT", h.Request(1).Method)
		}
		if h.Request(1).URL.Path == "/test/object-"+strconv.Itoa(i)+"-0" && h.Request(1).URL.RawQuery != fmt.Sprintf("partNumber=1&uploadId=%s", uid) {
			t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("partNumber=1&uploadId=%s", uid), h.Request(1).URL.Path, h.Request(1).URL.RawQuery)
		}
		if h.Request(1).Header.Get("Content-Length") != "100" {
			t.Fatalf("Wrong content length: %v", h.Request(1).Header.Get("Content-Length"))
		}

		if h.Request(2).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected %s but got %s", "PUT", h.Request(1).Method)
		}
		if h.Request(2).URL.Path == "/test/object-"+strconv.Itoa(i)+"-0" && h.Request(2).URL.RawQuery != fmt.Sprintf("partNumber=2&uploadId=%s", uid) {
			t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("partNumber=2&uploadId=%s", uid), h.Request(2).URL.Path, h.Request(2).URL.RawQuery)
		}
		if h.Request(2).Header.Get("Content-Length") != "100" {
			t.Fatalf("Wrong content length: %v", h.Request(2).Header.Get("Content-Length"))
		}

		if h.Request(3).Method != "POST" {
			t.Fatalf("Wrong request type issued. Expected %s but got %s", "POST", h.Request(3).Method)
		}
		if h.Request(3).URL.Path == "/test/object-"+strconv.Itoa(i)+"-0" && h.Request(3).URL.RawQuery != fmt.Sprintf("uploadId=%s", uid) {
			t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("uploadId=%s", uid), h.Request(3).URL.Path, h.Request(3).URL.RawQuery)
		}
	}

	validateEndpointResult(t, testResults, endpoints, h)
}

func validateEndpointResult(t *testing.T, testResults results, endpoints int, h S3TesterHelper) {
	if len(testResults.PerEndpointResult) != endpoints {
		t.Fatalf("Result list %d should equal to endpoints %d.", len(testResults.PerEndpointResult), endpoints)
	}

	endpointList := make([]string, 0)

	for _, endpointResult := range testResults.PerEndpointResult {
		if endpointResult.Concurrency != h.args.Concurrency/endpoints {
			t.Fatalf("The concurrency of one endpoint %d should equal to total concurrency divide by endpoints %d.", endpointResult.Concurrency, h.args.Concurrency/endpoints)
		}

		if endpointResult.AverageObjectSize != testResults.CumulativeResult.AverageObjectSize {
			t.Fatalf("The AverageObjectSize of one endpoint %f should equal to AverageObjectSize of the total result %f.", endpointResult.AverageObjectSize, testResults.CumulativeResult.AverageObjectSize)
		}

		if testResults.CumulativeResult.AverageObjectSize != 0 && endpointResult.UniqObjNum != h.args.Requests/endpoints {
			t.Fatalf("The UniqObjNum of one endpoint %d should equal to total requests divide by endpoints %d.", endpointResult.UniqObjNum, h.args.Requests/endpoints)
		}

		if h.args.Operation != "putget" && endpointResult.Count != h.args.Requests/endpoints {
			t.Fatalf("The Count of one endpoint %d should equal to total requests divide by endpoints %d.", endpointResult.Count, h.args.Requests/endpoints)
		}

		endpointList = append(endpointList, endpointResult.Endpoint)
	}

	sort.Slice(endpointList, func(i, j int) bool { return endpointList[i] < endpointList[j] })
	sort.Slice(h.args.endpoints, func(i, j int) bool { return h.args.endpoints[i] < h.args.endpoints[j] })
	if !reflect.DeepEqual(endpointList, h.args.endpoints) {
		t.Fatalf("The endpoint list of result should deeply equal to args.endpoints")
	}
}

func TestMergeSingleSuccessResult(t *testing.T) {
	config := testArgs(t, "put", "http://test1.com")
	args := config.worklist[0]
	args.Concurrency = 100
	startTime := time.Now()
	c := make(chan Result, args.Concurrency)
	addFakeResultToChannel(c, args, true, startTime)
	testResults := collectWorkerResult(c, args, startTime)
	validateEndpointResults(t, testResults.PerEndpointResult, args, 300, 300, 0)
	validateAggregateResult(t, &testResults.CumulativeResult, args, 300, 300, 0)
	processTestResult(&testResults, args)
	validateTestResult(t, &testResults.CumulativeResult, testResults.PerEndpointResult, args)
}

func TestMergeSingleFailResult(t *testing.T) {
	config := testArgs(t, "put", "http://test1.com")
	args := config.worklist[0]
	args.Concurrency = 100
	startTime := time.Now()
	c := make(chan Result, args.Concurrency)
	addFakeResultToChannel(c, args, false, startTime)
	testResults := collectWorkerResult(c, args, startTime)
	validateEndpointResults(t, testResults.PerEndpointResult, args, 0, 300, 300)
	validateAggregateResult(t, &testResults.CumulativeResult, args, 0, 300, 300)
	processTestResult(&testResults, args)
	validateTestResult(t, &testResults.CumulativeResult, testResults.PerEndpointResult, args)
}

func addFakeResultToChannel(c chan<- Result, args Parameters, isSuccess bool, startTime time.Time) {
	for _, endpoint := range args.endpoints {
		for currentEndpointWorkerID := 0; currentEndpointWorkerID < args.Concurrency/len(args.endpoints); currentEndpointWorkerID++ {
			go createFakeResult(c, endpoint, isSuccess, startTime)
		}
	}
}

func createFakeResult(c chan<- Result, endpoint string, isSuccess bool, startTime time.Time) {
	fakeResult := NewResult()
	fakeResult.TotalObjectSize = 13
	fakeResult.UniqObjNum = 3
	fakeResult.Count = 3
	if isSuccess {
		fakeResult.Failcount = 0
	} else {
		fakeResult.Failcount = 3
	}
	fakeResult.elapsedSum = 15
	fakeResult.Endpoint = endpoint
	fakeResult.startTime = startTime
	c <- fakeResult
}

func validateEndpointResults(t *testing.T, endpointResults []*Result, args Parameters, expectedUniqObjNum, expectedCount, expectedFailcount int) {
	if len(args.endpoints) != 1 && len(endpointResults) != len(args.endpoints) {
		t.Fatalf("The endpointResults length %d should equal to args.endpoints length %d.", len(endpointResults), len(args.endpoints))
	}
	for _, endpointResult := range endpointResults {
		if endpointResult.Operation != "" {
			t.Fatalf("The endpointResult.Operation should be an empty string %s.", endpointResult.Operation)
		}
		if endpointResult.Category != "" {
			t.Fatalf("The endpointResult.Category should be an empty string %s.", endpointResult.Category)
		}
		if endpointResult.UniqueName != "" {
			t.Fatalf("The endpointResult.UniqueName should be an empty string %s.", endpointResult.UniqueName)
		}
		if endpointResult.Concurrency != args.Concurrency/len(args.endpoints) {
			t.Fatalf("The endpointResult.Concurrency %d should equal to workerNum %d.", endpointResult.Concurrency, args.Concurrency/len(args.endpoints))
		}
		if endpointResult.UniqObjNum != expectedUniqObjNum {
			t.Fatalf("The endpointResult.UniqObjNum %d should equal to expectedUniqObjNum %d.", endpointResult.UniqObjNum, expectedUniqObjNum)
		}
		if endpointResult.Count != expectedCount {
			t.Fatalf("The endpointResult.Count %d should equal to expectedCount %d.", endpointResult.Count, expectedCount)
		}
		if endpointResult.Failcount != expectedFailcount {
			t.Fatalf("The endpointResult.Failcount %d should equal to expectedFailcount %d.", endpointResult.Failcount, expectedFailcount)
		}
	}
}

func validateAggregateResult(t *testing.T, aggregateResults *Result, args Parameters, expectedUniqObjNum, expectedCount, expectedFailcount int) {
	if aggregateResults.Operation != "" {
		t.Fatalf("The aggregateResults.Operation should be an empty string %s.", aggregateResults.Operation)
	}
	if aggregateResults.Category != "" {
		t.Fatalf("The aggregateResults.Category should be an empty string %s.", aggregateResults.Category)
	}
	if aggregateResults.UniqueName != "" {
		t.Fatalf("The aggregateResults.UniqueName should be an empty string %s.", aggregateResults.UniqueName)
	}
	if aggregateResults.UniqObjNum != expectedUniqObjNum {
		t.Fatalf("The aggregateResults.UniqObjNum %d should equal to expectedUniqObjNum %d.", aggregateResults.UniqObjNum, expectedUniqObjNum)
	}
	if aggregateResults.Count != expectedCount {
		t.Fatalf("The aggregateResults.Count %d should equal to expectedCount %d.", aggregateResults.Count, expectedCount)
	}
	if aggregateResults.Failcount != expectedFailcount {
		t.Fatalf("The aggregateResults.Failcount %d should equal to expectedFailcount %d.", aggregateResults.Failcount, expectedFailcount)
	}
}

func validateTestResult(t *testing.T, aggregateResults *Result, perEndpointResult []*Result, args Parameters) {
	if aggregateResults.Operation != args.Operation {
		t.Fatalf("The aggregateResults.Operation %s should be %s.", aggregateResults.Operation, args.Operation)
	}
	if aggregateResults.Category == "" {
		t.Fatal("The aggregateResults.Operation should not be empty.")
	}
	if aggregateResults.UniqueName == "" {
		t.Fatal("The aggregateResults.UniqueName should not be empty.")
	}

	if len(args.endpoints) > 1 && len(perEndpointResult) != len(args.endpoints) {
		t.Fatalf("Endpoint list length wrong %d", len(perEndpointResult))
	}
	if len(args.endpoints) == 1 && perEndpointResult != nil {
		t.Fatal("Endpoint list should be nil")
	}

	for _, endpointResult := range perEndpointResult {
		if endpointResult.Endpoint == "" {
			t.Fatal("The endpointResult.Endpoint should not be empty.")
		}
		if endpointResult.Operation != "" {
			t.Fatalf("The endpointResult.Operation should be an empty string %s.", endpointResult.Operation)
		}
		if endpointResult.Category != "" {
			t.Fatalf("The endpointResult.Category should be an empty string %s.", endpointResult.Category)
		}
		if endpointResult.UniqueName != "" {
			t.Fatalf("The endpointResult.UniqueName should be an empty string %s.", endpointResult.UniqueName)
		}
	}
}

func TestRestoreObject(t *testing.T) {
	h := initS3TesterHelper(t, "restore")
	defer h.Shutdown()
	h.args.Tier = "Expedited"
	h.args.Days = 4
	h.runTester(t)
	tierInBody := strings.Split(strings.Split(h.Body(0), "<Tier>")[1], "</Tier>")[0]
	daysInBody, err := strconv.ParseInt(strings.Split(strings.Split(h.Body(0), "<Days>")[1], "</Days>")[0], 10, 64)
	if err != nil {
		t.Fatal(err)
	}

	if h.Request(0).Method != "POST" {
		t.Fatalf("Wrong request type issued. Expected POST but got %s", h.Request(0).Method)
	}

	if h.args.Tier != tierInBody {
		t.Fatalf("Wrong restore tier. Expected %s but got %s", h.args.Tier, tierInBody)
	}

	if h.args.Days != daysInBody {
		t.Fatalf("Wrong restore days. Expected %d but got %d", h.args.Days, daysInBody)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}
}

func TestExecuteValidWorkload(t *testing.T) {
	workload := `{
		"global": {
			"concurrency": 1,
			"requests": 1,
			"bucket": "test",
			"endpoint": "https://test.com",
			"operation": "head"
		},
		"workload": [
			{"operation": "put"},
			{"operation": "get", "bucket":"anotherbucket"},
			{"prefix":"obj","operation":"delete"},
			{}
		]
	}`

	h := initWorkloadTesterHelper(t, workload)
	setValidAccessKeyEnv()
	testResults := executeTester(context.Background(), h.config)

	if h.Empty() {
		t.Fatalf("Did not receive any requests")
	}

	if h.NumBodies() != h.NumRequests() {
		t.Fatalf("numBodies %d does not match numRequests %d", h.NumBodies(), h.NumRequests())
	}

	if h.Size() != 4 {
		t.Fatalf("should receive 4 requests but got %d", h.Size())
	}

	checkRequests(t, h, 0, "PUT", "/test/testobject-0")
	checkRequests(t, h, 1, "GET", "/anotherbucket/testobject-0")
	checkRequests(t, h, 2, "DELETE", "/test/obj-0")
	checkRequests(t, h, 3, "HEAD", "/test/testobject-0")

	totalRequest := 0
	failCount := 0
	testResults.Range(func(r *results) error {
		totalRequest += r.CumulativeResult.Count
		failCount += r.CumulativeResult.Failcount
		return nil
	})

	checkExpectInt(t, 4, totalRequest)
	checkExpectInt(t, 0, failCount)
}

func checkRequests(t *testing.T, h S3TesterHelper, i int, expectMethod, expectPath string) {
	if h.Request(i).Method != expectMethod {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", expectMethod, h.Request(0).Method)
	}

	if h.Request(i).URL.Path != expectPath {
		t.Fatalf("Wrong url path: %s, expected %s", h.Request(0).URL.Path, expectPath)
	}
}

const MixedWorkloadFile = "mixed-workload.json"

func TestMixedPutGet(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":50},
											  {"operationType":"get","ratio":50}]
											}`
	file := createFile(t, MixedWorkloadFile, sampleMixedWorkload)
	defer file.Close()
	defer os.Remove(MixedWorkloadFile)
	h := initMixedWorkloadTesterHelper(t)

	h.args.Requests = 10
	h.args.Concurrency = 1
	h.args.Size = 100
	h.args.Bucket = "not"

	results := h.runTester(t)

	if h.Size() != 10 {
		t.Fatalf("Should received 10 requests. Had %v requests", h.Size())
	}

	if results.CumulativeResult.TotalObjectSize != 500+5*int64(len(xmlBodyString)) {
		t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", 500+5*int64(len(xmlBodyString)), results.CumulativeResult.TotalObjectSize)
	}

	defer h.Shutdown()
}

func TestMixedPutGetLargeMultiCon(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":100}]
											}`
	file := createFile(t, MixedWorkloadFile, sampleMixedWorkload)
	defer file.Close()
	defer os.Remove(MixedWorkloadFile)
	h := initMixedWorkloadTesterHelper(t)

	h.args.Requests = 250
	h.args.Concurrency = 5
	h.args.Size = 30
	h.args.Bucket = "not"

	results := h.runTester(t)

	if h.Size() != 250 {
		t.Fatalf("Should received 250 requests. Had %v requests", h.Size())
	}

	if results.CumulativeResult.TotalObjectSize != (250 * 30) {
		t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", (125 * 30), results.CumulativeResult.TotalObjectSize)
	}

	if results.CumulativeResult.Failcount != 0 {
		t.Fatalf("Test should have no failures. %d failures.", results.CumulativeResult.Failcount)
	}

	defer h.Shutdown()
}

func TestMixedPutGetLargeMultiCon2(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":50},
											  {"operationType":"get","ratio":50}]
											}`
	file := createFile(t, MixedWorkloadFile, sampleMixedWorkload)
	defer file.Close()
	defer os.Remove(MixedWorkloadFile)
	h := initMixedWorkloadTesterHelper(t)

	h.args.Requests = 250
	h.args.Concurrency = 5
	h.args.Size = 30
	h.args.Bucket = "not"

	results := h.runTester(t)

	if h.Size() != 250 {
		t.Fatalf("Should received 250 requests. Had %v requests", h.Size())
	}

	if results.CumulativeResult.TotalObjectSize != 125*30+125*int64(len(xmlBodyString)) {
		t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", 125*30+125*int64(len(xmlBodyString)), results.CumulativeResult.TotalObjectSize)
	}

	if results.CumulativeResult.Failcount != 0 {
		t.Fatalf("Test should have no failures. %d failures.", results.CumulativeResult.Failcount)
	}

	defer h.Shutdown()
}

func TestMixedPutGetDeleteSmall(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":25},
											  {"operationType":"get","ratio":25},
											  {"operationType":"updatemeta","ratio":25},
											  {"operationType":"delete","ratio":25}]
											}`
	file := createFile(t, MixedWorkloadFile, sampleMixedWorkload)
	defer file.Close()
	defer os.Remove(MixedWorkloadFile)
	h := initMixedWorkloadTesterHelper(t)

	h.args.Requests = 4
	h.args.Concurrency = 1
	h.args.Size = 30
	h.args.Bucket = "not"

	results := h.runTester(t)

	if h.Size() != 4 {
		t.Fatalf("Should received 4 requests. Had %v requests", h.Size())
	}

	if results.CumulativeResult.TotalObjectSize != 30+int64(len(xmlBodyString)) {
		t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", 30+int64(len(xmlBodyString)), results.CumulativeResult.TotalObjectSize)
	}

	if results.CumulativeResult.Failcount != 0 {
		t.Fatalf("Test should have no failures. %d failures.", results.CumulativeResult.Failcount)
	}
	requests := []string{"PUT", "GET", "PUT", "DELETE"}

	for i := 0; i < 4; i++ {
		if h.Request(i).Method != requests[i] {
			t.Fatalf("Expected %v method, but got %v", requests[i], h.Request(i).Method)
		}

	}

	defer h.Shutdown()
}

func TestMixedPutGetDeleteLarge(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":25},
											  {"operationType":"get","ratio":25},
											  {"operationType":"updatemeta","ratio":25},
											  {"operationType":"delete","ratio":25}]
											}`

	file := createFile(t, MixedWorkloadFile, sampleMixedWorkload)
	defer file.Close()
	defer os.Remove(MixedWorkloadFile)
	h := initMixedWorkloadTesterHelper(t)

	h.args.Requests = 100
	h.args.Concurrency = 4
	h.args.Size = 30
	h.args.Bucket = "not"

	results := h.runTester(t)

	if h.Size() != 100 {
		t.Fatalf("Should received 100 requests. Had %v requests", h.Size())
	}

	if results.CumulativeResult.TotalObjectSize != 25*30+25*int64(len(xmlBodyString)) {
		t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", 25*30+25*int64(len(xmlBodyString)), results.CumulativeResult.TotalObjectSize)
	}

	if results.CumulativeResult.Failcount != 0 {
		t.Fatalf("Test should have no failures. %d failures.", results.CumulativeResult.Failcount)
	}
	defer h.Shutdown()

}

func BenchmarkRuntest(b *testing.B) {
	for i := 0; i < b.N; i++ {
		runLargePerformanceTest()
	}
}

func runLargePerformanceTest() {
	endpoints := 1
	op := "put"
	h := initMultiS3TesterHelper(nil, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.Requests = 1000000
	h.args.Concurrency = 1000
	h.args.Size = 5
	setValidAccessKeyEnv()
	syscallParams := NewSyscallParams(h.args)
	defer syscallParams.detach()
	runtest(context.Background(), h.config, h.args, syscallParams)
}

var commandExpTests = []struct {
	in  string
	out []string
}{
	{``, nil},
	{` `, nil},
	{`a`, []string{`a`}},
	{`aa bb cc`, []string{`aa`, `bb`, `cc`}},
	{`a "b c"`, []string{`a`, `"b c"`}},
	{`a 'b c'`, []string{`a`, `'b c'`}},
	{`a "b c'`, []string{`a`, `"b`, `c'`}},
	{`a 'b c"`, []string{`a`, `'b`, `c"`}},
	{`a b" 'c`, []string{`a`, `b"`, `'c`}},
	{`a b' "c`, []string{`a`, `b'`, `"c`}},
	{`a b"b c`, []string{`a`, `b"b`, `c`}},
	{`a b'b c`, []string{`a`, `b'b`, `c`}},
	{`'bbb" 'b"`, []string{`'bbb" '`, `b"`}},
	{`a" b "c`, []string{`a"`, `b`, `"c`}}, // Characters and quotes are separate matches, unlike shell "a b c"
	{`"\"\""`, []string{`"\"`, `\""`}},     // Escapes are not supported, unlike with shell
}

func TestCommandExp(t *testing.T) {
	for _, tt := range commandExpTests {
		t.Run(tt.in, func(t *testing.T) {
			out := commandExp.FindAllString(tt.in, -1)
			if !reflect.DeepEqual(out, tt.out) {
				t.Errorf("got %#v, want %#v", out, tt.out)
			}
		})
	}
}

func TestHasPrefixAndSuffix(t *testing.T) {
	if !HasPrefixAndSuffix("aba", "") {
		t.Error()
	}
	if !HasPrefixAndSuffix("aba", "a") {
		t.Error()
	}
	if !HasPrefixAndSuffix("aabaa", "aa") {
		t.Error()
	}
	if HasPrefixAndSuffix("a", "a") {
		t.Error()
	}
	if HasPrefixAndSuffix("aaa", "aa") {
		t.Error()
	}
	if HasPrefixAndSuffix("abc", "a") {
		t.Error()
	}
	if HasPrefixAndSuffix("abc", "c") {
		t.Error()
	}
}

func TestParseCommand(t *testing.T) {
	if args := ParseCommand(""); len(args) != 0 {
		t.Error()
	}

	want := []string{"a b", "c d", "\"e", "f'"}
	if args := ParseCommand(`"a b" 'c d' "e f'`); !reflect.DeepEqual(args, want) {
		t.Errorf("got %#v, want %#v", args, want)
	}

	err := os.Setenv("TESTVAR", "somevalue")
	if err != nil {
		t.Fatalf("got error on Setenv: %v", err)
	}
	want = []string{"a $TESTVAR", "somevalue", "somevalue", "${}"}
	if args := ParseCommand("'a $TESTVAR' $TESTVAR ${TESTVAR} '${}'"); !reflect.DeepEqual(args, want) {
		t.Errorf("got %#v, want %#v", args, want)
	}
}

func TestExecuteNoCommand(t *testing.T) {
	if err := ExecuteCommand(""); err != nil {
		t.Error(err)
	}
	if err := ExecuteCommand(" "); err != nil {
		t.Error(err)
	}
	if err := ExecuteCommand("\t"); err != nil {
		t.Error(err)
	}
}

func TestEmptyHistogramSummary(t *testing.T) {
	HistogramSummary(NewResult().latencies)
}

func TestVirtualHostedStyleURL(t *testing.T) {
	host := "www.example.com:18082"
	bucket := "test-bucket"
	objectKey := "test-object"
	expectedVirtualStyleURL := fmt.Sprintf("%s.%s/%s", bucket, host, objectKey)
	expectedPathStyleURL := fmt.Sprintf("%s/%s/%s", host, bucket, objectKey)
	var urlSent string
	extractURLBeforeSend := func(r *request.Request) {
		urlSent = fmt.Sprintf("%s%s", r.HTTPRequest.URL.Host, r.HTTPRequest.URL.Path)
	}

	config := testArgs(t, "put", host)
	config.worklist[0].Bucket = bucket
	config.worklist[0].Prefix = objectKey
	args := config.worklist[0]
	httpClient := http.Client{Timeout: time.Nanosecond}

	// Run with virtual style enabled
	args.AddressingStyle = addressingStyleVirtual
	svc, err := MakeS3Service(&httpClient, config, &args, args.Endpoint)
	if err != nil {
		t.Fatalf("Error creating S3 service: %v", err)
	}
	svc.Client.Handlers.Send.PushBack(extractURLBeforeSend)
	Put(context.Background(), svc, args.Bucket, args.Prefix, "", int64(args.Size), make(map[string]*string, 0))
	if urlSent != expectedVirtualStyleURL {
		t.Fatalf("URL sent: %s is NOT virtual hosted style: %s", urlSent, expectedVirtualStyleURL)
	}

	// Run with virtual style disabled (= forced path style)
	args.AddressingStyle = addressingStylePath
	svc, err = MakeS3Service(&httpClient, config, &args, args.Endpoint)
	if err != nil {
		t.Fatalf("Error creating S3 service: %v", err)
	}
	svc.Client.Handlers.Send.PushBack(extractURLBeforeSend)
	Put(context.Background(), svc, args.Bucket, args.Prefix, "", int64(args.Size), make(map[string]*string, 0))
	if urlSent != expectedPathStyleURL {
		t.Fatalf("URL sent: %s is NOT path style: %s", urlSent, expectedPathStyleURL)
	}
}

func TestRandomRangeMultipleOptions(t *testing.T) {
	h := initS3TesterHelperWithData(t, "get", strings.Repeat("a", 100))
	h.args.Requests = 500
	h.args.RandomRange = "0-10000"
	h.args.randomRangeMin = 0
	h.args.randomRangeMax = 10000
	h.args.randomRangeSize = 100
	defer h.Shutdown()
	testResults := h.runTester(t)

	if avgObjectSize := testResults.CumulativeResult.TotalObjectSize / int64(h.NumRequests()); avgObjectSize != int64(h.args.randomRangeSize) {
		t.Fatalf("Wrong average object size. Expected: %v, but got %v", h.args.randomRangeSize, avgObjectSize)
	}

	h.RangeRequests(func(request *http.Request) {
		rangeHeader := request.Header.Get("Range")
		re := regexp.MustCompile(`^\D*`) //strip 'bytes' in range header value
		boundaries := strings.Split(re.ReplaceAllString(rangeHeader, ""), "-")
		min, _ := strconv.ParseInt(boundaries[0], 10, 64)
		max, _ := strconv.ParseInt(boundaries[1], 10, 64)

		if rangeSize := max - min + 1; int64(rangeSize) != h.args.randomRangeSize {
			t.Fatalf("Wrong range size. Expected: %v, but got %v", h.args.randomRangeSize, rangeSize)
		}

		if min < h.args.randomRangeMin {
			t.Fatalf("Wrong range start index. Expected value smaller than %v, but got %v", h.args.randomRangeMin, min)
		}

		if max > h.args.randomRangeMax {
			t.Fatalf("Wrong range start index. Expected value smaller than %v, but got %v", h.args.randomRangeMax, max)
		}
	})
}

func TestRandomRangeOnlyOneOption(t *testing.T) {
	h := initS3TesterHelperWithData(t, "get", strings.Repeat("a", 100))
	h.args.Requests = 100
	h.args.RandomRange = "450-549/100"
	h.args.randomRangeMin = 450
	h.args.randomRangeMax = 549
	h.args.randomRangeSize = 100
	defer h.Shutdown()
	testResults := h.runTester(t)

	if avgObjectSize := testResults.CumulativeResult.TotalObjectSize / int64(h.NumRequests()); avgObjectSize != int64(h.args.randomRangeSize) {
		t.Fatalf("Wrong average object size. Expected: %v, but got %v", h.args.randomRangeSize, avgObjectSize)
	}

	h.RangeRequests(func(request *http.Request) {
		rangeHeader := request.Header.Get("Range")
		expectedRangeHeader := fmt.Sprintf("bytes=%d-%d", h.args.randomRangeMin, h.args.randomRangeMax)

		if rangeHeader != expectedRangeHeader {
			t.Fatalf("Wrong range header. Expected: %s, but got %s", expectedRangeHeader, rangeHeader)
		}
	})
}

func TestRandomRangeOnlyOneOptionWithVerify(t *testing.T) {
	h := initS3TesterHelperWithData(t, "get", strings.Repeat("object-0", 100)[450:550])
	h.args.RandomRange = "450-549/100"
	h.args.randomRangeMin = 450
	h.args.randomRangeMax = 549
	h.args.randomRangeSize = 100
	h.args.Verify = 1
	defer h.Shutdown()
	testResults := h.runTester(t)

	if avgObjectSize := testResults.CumulativeResult.TotalObjectSize / int64(h.NumRequests()); avgObjectSize != int64(h.args.randomRangeSize) {
		t.Fatalf("Wrong average object size. Expected: %v, but got %v", h.args.randomRangeSize, avgObjectSize)
	}

	h.RangeRequests(func(request *http.Request) {
		rangeHeader := request.Header.Get("Range")
		expectedRangeHeader := fmt.Sprintf("bytes=%d-%d", h.args.randomRangeMin, h.args.randomRangeMax)

		if rangeHeader != expectedRangeHeader {
			t.Fatalf("Wrong range header. Expected: %s, but got %s", expectedRangeHeader, rangeHeader)
		}
	})
}

func TestRangeReadWithVerify(t *testing.T) {
	testData := []struct {
		contentRange string
		rangeMin     int
		rangeMax     int
		rangeSize    int
	}{
		// large range read
		{contentRange: "bytes=0-799", rangeMin: 0, rangeMax: 799, rangeSize: 800},

		// range read aligned to key
		{contentRange: "bytes=400-479", rangeMin: 400, rangeMax: 479, rangeSize: 80},

		// unaligned range read
		{contentRange: "bytes=117-593", rangeMin: 117, rangeMax: 593, rangeSize: 477},

		// small range read
		{contentRange: "bytes=799-799", rangeMin: 799, rangeMax: 799, rangeSize: 1},
	}

	for i, test := range testData {
		testcase := fmt.Sprintf("%d Range: %s", i, test.contentRange)
		t.Run(testcase, func(t *testing.T) {
			responseBody := strings.Repeat("object-0", 100)[test.rangeMin : test.rangeMax+1]

			h := initS3TesterHelperWithData(t, "get", responseBody)
			h.args.Verify = 1
			h.args.Range = test.contentRange
			h.args.Size = byteSize(len(responseBody))
			defer h.Shutdown()
			testResults := h.runTester(t)

			if avgObjectSize := testResults.CumulativeResult.TotalObjectSize / int64(h.NumRequests()); avgObjectSize != int64(test.rangeSize) {
				t.Fatalf("Wrong average object size. Expected: %v, but got %v", test.rangeSize, avgObjectSize)
			}

			h.RangeRequests(func(request *http.Request) {
				rangeHeader := request.Header.Get("Range")

				if rangeHeader != test.contentRange {
					t.Fatalf("Wrong range header. Expected: %s, but got %s", test.contentRange, rangeHeader)
				}
			})
		})
	}
}

func TestInvalidRangeReadWithVerify(t *testing.T) {
	testData := []struct {
		contentRange string
		rangeMin     int
		rangeMax     int
	}{
		// Response body is 1 byte ahead
		{contentRange: "bytes=0-399", rangeMin: 1, rangeMax: 400},

		// Response body is 1 byte behind
		{contentRange: "bytes=400-479", rangeMin: 399, rangeMax: 478},
	}

	for i, test := range testData {
		testcase := fmt.Sprintf("%d Range: %s", i, test.contentRange)
		t.Run(testcase, func(t *testing.T) {
			responseBody := strings.Repeat("object-0", 100)[test.rangeMin : test.rangeMax+1]

			h := initS3TesterHelperWithData(t, "get", responseBody)
			h.args.Verify = 1
			h.args.Range = test.contentRange
			h.args.Size = byteSize(len(responseBody))
			defer h.Shutdown()
			testResults := h.runTesterWithoutValidation(t)

			if testResults.CumulativeResult.Failcount != 1 {
				t.Fatalf("Test should have failed. %d failures.", testResults.CumulativeResult.Failcount)
			}
		})
	}
}

func TestMultipartPutRangeRead(t *testing.T) {
	// Multipartput data with partSize=85
	var multipartPutData = strings.Repeat((strings.Repeat("object-0", 10) + "objec"), 10)
	var partSize byteSize = 85

	testData := []struct {
		contentRange string
		rangeMin     int
		rangeMax     int
		rangeSize    int
	}{
		// multipart range read with rangeMin < partSize
		{contentRange: "bytes=0-849", rangeMin: 0, rangeMax: 849, rangeSize: 850},

		// multipart range read with rangeMin > partSize
		{contentRange: "bytes=313-794", rangeMin: 313, rangeMax: 794, rangeSize: 482},
	}

	for i, test := range testData {
		testcase := fmt.Sprintf("%d Range: %s", i, test.contentRange)
		t.Run(testcase, func(t *testing.T) {
			responseBody := multipartPutData[test.rangeMin : test.rangeMax+1]

			h := initS3TesterHelperWithData(t, "get", responseBody)
			h.args.Verify = 2
			h.args.PartSize = partSize
			h.args.Range = test.contentRange
			h.args.Size = byteSize(len(responseBody))
			defer h.Shutdown()
			testResults := h.runTester(t)

			if avgObjectSize := testResults.CumulativeResult.TotalObjectSize / int64(h.NumRequests()); avgObjectSize != int64(test.rangeSize) {
				t.Fatalf("Wrong average object size. Expected: %v, but got %v", test.rangeSize, avgObjectSize)
			}

			h.RangeRequests(func(request *http.Request) {
				rangeHeader := request.Header.Get("Range")

				if rangeHeader != test.contentRange {
					t.Fatalf("Wrong range header. Expected: %s, but got %s", test.contentRange, rangeHeader)
				}
			})
		})
	}
}

func TestMultipartPutNormalRead(t *testing.T) {
	// Multipartput data with partSize=85
	var multipartPutData = strings.Repeat((strings.Repeat("object-0", 10) + "objec"), 10)
	var partSize byteSize = 85

	h := initS3TesterHelperWithData(t, "get", multipartPutData)
	h.args.Verify = 2
	h.args.Size = byteSize(len(multipartPutData))
	h.args.PartSize = partSize
	defer h.Shutdown()
	testResults := h.runTester(t)

	if h.Request(0).Method != "GET" {
		t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if testResults.CumulativeResult.TotalObjectSize != int64(len(multipartPutData)) {
		t.Fatalf("TotalObjectSize is wrong size. Expected %d, but got %d", bodyLength, testResults.CumulativeResult.TotalObjectSize)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "" {
		t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}
}

func TestQueryParams(t *testing.T) {
	h := initS3TesterHelperWithData(t, "get", strings.Repeat("object-0", 10))
	h.args.Size = 80
	h.args.QueryParams = "test-query-param"
	defer h.Shutdown()
	h.runTester(t)

	if h.Request(0).Method != "GET" {
		t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).URL.RawQuery != "test-query-param=" {
		t.Fatalf("Wrong query parameters: %s", h.Request(0).URL.RawQuery)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "" {
		t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}
}

func TestAdditionalQueryParams(t *testing.T) {
	h := initS3TesterHelper(t, "puttagging")
	defer h.Shutdown()
	h.args.Tagging = "tag1=value1"
	h.args.QueryParams = "test=1"
	h.runTester(t)

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected DELETE but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).URL.RawQuery != "tagging=&test=1" {
		t.Fatalf("Wrong query param: %s", h.Request(0).URL.RawQuery)
	}
}

func TestMultipleQueryParams(t *testing.T) {
	h := initS3TesterHelper(t, "puttagging")
	defer h.Shutdown()
	h.args.Tagging = "tag1=value1"
	h.args.QueryParams = "test=1&test2=2&test3"
	h.runTester(t)

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected DELETE but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).URL.RawQuery != "tagging=&test=1&test2=2&test3=" {
		t.Fatalf("Wrong query param: %s", h.Request(0).URL.RawQuery)
	}
}

func TestDebugArgument(t *testing.T) {
	h := initS3TesterHelper(t, "get")
	h.config.Debug = true
	defer h.Shutdown()

	debugGetReq := Request{URI: "/test/object-0", Method: "GET"}
	debugGetResponse := Response{Body: generateErrorXML(t, "BadRequest"), Status: 400}
	h.SetRequestResult(debugGetReq, debugGetResponse)

	var buf bytes.Buffer
	log.SetOutput(&buf)
	testResults := h.runTesterWithoutValidation(t)

	if h.Request(0).Method != "GET" {
		t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if testResults.CumulativeResult.Failcount != 1 {
		t.Fatalf("Test should have failed. %d failures.", testResults.CumulativeResult.Failcount)
	}

	// verify the response body was correctly written back by checking the error message constructed from the error xml
	if !strings.Contains(buf.String(), "Failed get on object bucket 'test/object-0': BadRequest: BadRequest") {
		t.Fatalf("Response body was not written back correctly: %s", buf.String())
	}
}

func TestCorrectEndpointUniqObjCountWithOverwriteForDurationSetting(t *testing.T) {
	r := Result{UniqObjNum: 500}
	p := Parameters{Requests: 2000, endpoints: make([]string, 2)}

	r.correctEndpointUniqObjCountWithOverwriteSetting(3, 1000, &p)
	if r.UniqObjNum != 500 {
		t.Fatalf("Got %v, want 500", r.UniqObjNum)
	}

	r.UniqObjNum = 8000
	r.correctEndpointUniqObjCountWithOverwriteSetting(3, 1000, &p)

	if r.UniqObjNum != p.Requests/len(p.endpoints) {
		t.Fatalf("Got %v, want %v", r.UniqObjNum, p.Requests/len(p.endpoints))
	}

}

func TestCorrectResultsUniqObjCountWithOverwriteForDurationSetting(t *testing.T) {
	var r results
	r.CumulativeResult.UniqObjNum = 500

	r.correctResultsUniqObjCountWithOverwriteSetting(3, 1000, 1000)
	if r.CumulativeResult.UniqObjNum != 500 {
		t.Fatalf("Got %v, want 500", r.CumulativeResult.UniqObjNum)
	}

	r.correctResultsUniqObjCountWithOverwriteSetting(3, 1000, 250)
	if r.CumulativeResult.UniqObjNum != 250 {
		t.Fatalf("Got %v, want 250", r.CumulativeResult.UniqObjNum)
	}
}

func TestGenerateKeyNameBasicSeparate(t *testing.T) {
	durationRequestCount := new(uint64)
	keyName1 := generateKeyName("prefix", 4, 1000, 1, 0, 0, "", false, true, false, durationRequestCount)
	expectedName1 := "prefix-4"

	if expectedName1 != keyName1 {
		t.Fatalf("Got %v, expected %v", keyName1, expectedName1)
	}

	keyName2 := generateKeyName("testobject", 77, 2000, 1, 0, 0, "", false, true, false, durationRequestCount)
	expectedName2 := "testobject-77"

	if expectedName2 != keyName2 {
		t.Fatalf("Got %v, expected %v", keyName2, expectedName2)
	}
}

func TestGenerateKeyNameMultipleWorkersSeparate(t *testing.T) {
	durationRequestCount := new(uint64)
	keyName1 := generateKeyName("prefix", 0, 1000, 12, 0, 0, "", false, true, false, durationRequestCount)
	expectedName1 := "prefix-0"

	if expectedName1 != keyName1 {
		t.Fatalf("Got %v, expected %v", keyName1, expectedName1)
	}

	keyName2 := generateKeyName("prefix", 0, 1000, 12, 2, 0, "", false, true, false, durationRequestCount)
	expectedName2 := "prefix-2000"

	if expectedName2 != keyName2 {
		t.Fatalf("Got %v, expected %v", keyName2, expectedName2)
	}

	keyName3 := generateKeyName("prefix", 998, 1000, 12, 3, 0, "", false, true, false, durationRequestCount)
	expectedName3 := "prefix-3998"

	if expectedName3 != keyName3 {
		t.Fatalf("Got %v, expected %v", keyName3, expectedName3)
	}

	keyName4 := generateKeyName("testobject", 7, 444, 10, 3, 0, "", false, true, false, durationRequestCount)
	expectedName4 := "testobject-1339"

	if expectedName4 != keyName4 {
		t.Fatalf("Got %v, expected %v", keyName4, expectedName4)
	}
}

func TestGenerateKeyNameMultipleWorkersTogether(t *testing.T) {
	durationRequestCount := new(uint64)
	keyName1 := generateKeyName("prefix", 0, 1000, 12, 0, 0, "", false, false, false, durationRequestCount)
	expectedName1 := "prefix-0"

	if expectedName1 != keyName1 {
		t.Fatalf("Got %v, expected %v", keyName1, expectedName1)
	}

	keyName2 := generateKeyName("prefix", 0, 1000, 12, 2, 0, "", false, false, false, durationRequestCount)
	expectedName2 := "prefix-2"

	if expectedName2 != keyName2 {
		t.Fatalf("Got %v, expected %v", keyName2, expectedName2)
	}

	keyName3 := generateKeyName("prefix", 998, 1000, 10, 3, 0, "", false, false, false, durationRequestCount)
	expectedName3 := "prefix-9983"

	if expectedName3 != keyName3 {
		t.Fatalf("Got %v, expected %v", keyName3, expectedName3)
	}

	keyName4 := generateKeyName("testobject", 7, 444, 10, 3, 0, "", false, false, false, durationRequestCount)
	expectedName4 := "testobject-73"

	if expectedName4 != keyName4 {
		t.Fatalf("Got %v, expected %v", keyName4, expectedName4)
	}
}

// overwrite 1
func TestGenerateKeyNameOverwriteClobberAll(t *testing.T) {
	durationRequestCount := new(uint64)
	keyName1 := generateKeyName("onlyname", 0, 1000, 10, 0, 1, "", false, true, false, durationRequestCount)
	expectedName := "onlyname"

	if expectedName != keyName1 {
		t.Fatalf("Got %v, expected %v", keyName1, expectedName)
	}

	keyName2 := generateKeyName("onlyname", 500, 1000, 10, 2, 1, "", false, true, false, durationRequestCount)
	if expectedName != keyName2 {
		t.Fatalf("Got %v, expected %v", keyName2, expectedName)
	}

	keyName3 := generateKeyName("onlyname", 0, 1000, 10, 0, 1, "", false, false, false, durationRequestCount)
	if expectedName != keyName3 {
		t.Fatalf("Got %v, expected %v", keyName3, expectedName)
	}

	keyName4 := generateKeyName("onlyname", 500, 1000, 10, 2, 1, "blah", true, false, false, durationRequestCount)
	if expectedName != keyName4 {
		t.Fatalf("Got %v, expected %v", keyName4, expectedName)
	}

	keyName5 := generateKeyName("onlyname", 500, math.MaxUint64, 10, 2, 1, "blah", true, true, true, durationRequestCount)
	if expectedName != keyName5 {
		t.Fatalf("Got %v, expected %v", keyName5, expectedName)
	}
}

// overwrite 2
func TestGenerateKeyNameOverwriteClobberSome(t *testing.T) {
	durationRequestCount := new(uint64)
	keyName1 := generateKeyName("prefix", 0, 1000, 10, 0, 2, "", false, true, false, durationRequestCount)
	expectedName1 := "prefix-0"

	if expectedName1 != keyName1 {
		t.Fatalf("Got %v, expected %v", keyName1, expectedName1)
	}

	keyName2 := generateKeyName("prefix", 500, 1000, 10, 2, 2, "", false, true, false, durationRequestCount)
	expectedName2 := "prefix-500"
	if expectedName2 != keyName2 {
		t.Fatalf("Got %v, expected %v", keyName2, expectedName2)
	}

	keyName3 := generateKeyName("testname", 33, 1000, 10, 0, 2, "", false, false, false, durationRequestCount)
	expectedName3 := "testname-33"
	if expectedName3 != keyName3 {
		t.Fatalf("Got %v, expected %v", keyName3, expectedName3)
	}

	keyName4 := generateKeyName("testobject", 250, math.MaxUint64, 10, 2, 2, "", false, true, true, durationRequestCount)
	expectedName4 := "testobject-250"
	if expectedName4 != keyName4 {
		t.Fatalf("Got %v, expected %v", keyName4, expectedName4)
	}
}

func TestGenerateKeyNameIncrementing(t *testing.T) {
	durationRequestCount := new(uint64)

	formatString := generateFormatString(0, 998, 9980)
	keyName := generateKeyName("testobject", 98, 998, 10, 0, 0, formatString, true, true, false, durationRequestCount)
	expectedName := "testobject-0098"
	if expectedName != keyName {
		t.Fatalf("Got %v, expected %v", keyName, expectedName)
	}

	// separate
	formatString2 := generateFormatString(0, 500, 6000)
	keyName2 := generateKeyName("testobject", 47, 500, 12, 3, 0, formatString2, true, true, false, durationRequestCount)
	expectedName2 := "testobject-1547"
	if expectedName2 != keyName2 {
		t.Fatalf("Got %v, expected %v", keyName2, expectedName2)
	}

	keyName3 := generateKeyName("testobject", 33, 500, 12, 1, 0, formatString2, true, true, false, durationRequestCount)
	expectedName3 := "testobject-0533"
	if expectedName3 != keyName3 {
		t.Fatalf("Got %v, expected %v", keyName3, expectedName3)
	}

	// together
	keyName4 := generateKeyName("testname", 1, 500, 12, 3, 0, formatString2, true, false, false, durationRequestCount)
	expectedName4 := "testname-0015"
	if expectedName4 != keyName4 {
		t.Fatalf("Got %v, expected %v", keyName4, expectedName4)
	}

	keyName5 := generateKeyName("testname", 10, 500, 12, 3, 0, formatString2, true, false, false, durationRequestCount)
	expectedName5 := "testname-0123"
	if expectedName5 != keyName5 {
		t.Fatalf("Got %v, expected %v", keyName5, expectedName5)
	}

	// test overwrite 2; special case for this
	formatString3 := generateFormatString(2, 7000, 21000)
	keyName6 := generateKeyName("overwrite", 123, 7000, 3, 1, 2, formatString3, true, true, false, durationRequestCount)
	expectedName6 := "overwrite-0123"
	if expectedName6 != keyName6 {
		t.Fatalf("Got %v, expected %v", keyName6, expectedName6)
	}
}

func TestGenerateKeyNameDuration(t *testing.T) {
	durationRequestCount := new(uint64)

	formatString := generateFormatString(0, math.MaxUint64, 0) // match normal incoming arguments for "duration"
	keyName := generateKeyName("duration", 2020, math.MaxUint64, 3, 1, 0, formatString, false, true, true, durationRequestCount)
	expectedName := "duration-0"
	if expectedName != keyName {
		t.Fatalf("Got %v, expected %v", keyName, expectedName)
	}

	// should increment based on the durationRequestCount, most of the other arguments don't matter (except overwrite, which overrides duration)
	keyName2 := generateKeyName("duration", 2023, math.MaxUint64, 12, 4, 0, formatString, false, false, true, durationRequestCount)
	expectedName2 := "duration-1"
	if expectedName2 != keyName2 {
		t.Fatalf("Got %v, expected %v", keyName2, expectedName2)
	}

	*durationRequestCount += uint64(1000)
	keyName3 := generateKeyName("duration", 2024, math.MaxUint64, 9, 0, 0, formatString, false, true, true, durationRequestCount)
	expectedName3 := "duration-1002"
	if expectedName3 != keyName3 {
		t.Fatalf("Got %v, expected %v", keyName3, expectedName3)
	}

	keyName4 := generateKeyName("duration", 1998, math.MaxUint64, 11, 0, 0, formatString, true, true, true, durationRequestCount)
	expectedName4 := "duration-00000000000000001003"
	if expectedName4 != keyName4 {
		t.Fatalf("Got %v, expected %v", keyName4, expectedName4)
	}
}
