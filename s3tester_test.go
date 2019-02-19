package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
)

const (
	bodyString = "hello"
	bodyLength = int64(len(bodyString))

	accessKey string = "AWS_ACCESS_KEY_ID"
	secretKey string = "AWS_SECRET_ACCESS_KEY"
)

func ResponseSuccessful(response *http.Response, err error) (success bool) {
	success = err == nil
	if success {
		success = response.StatusCode >= 200 && response.StatusCode < 300
	}
	return
}

// A key to use for customizing per-request results
type Request struct {
	Uri    string
	Method string
}

type Response struct {
	Body    string
	Status  int
	Headers map[string]string
}

// A helper struct for stubbing out HTTP endpoints,
// and collecting all received requests.
type HttpHelper struct {
	Server           *httptest.Server
	Requests         *[]*http.Request
	Bodies           *[]string
	Endpoint         string
	perRequestResult *map[Request]Response // maps requests to results; for customizing server results on a per-request basis
}

func (h *HttpHelper) Empty() bool {
	return len(*h.Requests) == 0
}

func (h *HttpHelper) Size() int {
	return len(*h.Requests)
}

func (h *HttpHelper) NumBodies() int {
	return len(*h.Bodies)
}

func (h *HttpHelper) NumRequests() int {
	return len(*h.Requests)
}

func (h *HttpHelper) Request(index int) *http.Request {
	return (*h.Requests)[index]
}

func (h *HttpHelper) Body(index int) string {
	return (*h.Bodies)[index]
}

func (h *HttpHelper) SetRequestResult(req Request, res Response) {
	(*h.perRequestResult)[req] = res
}

// Close all active goroutines
// and perform cleanup of HttpHelper class.
func (h *HttpHelper) Shutdown() {
	h.Server.Close()
}

func setValidAccessKeyEnv() {
	os.Setenv(accessKey, "PUIQNFNDWLHT95AMJVOW")
	os.Setenv(secretKey, "rFM0SP5t6na2WkU1Uk8LvIpEukqt/GJc8A/CphkD")
}

func NewHttpHelper(t *testing.T, getResponseBody string, headers map[string]string) HttpHelper {
	// ensure envs that s3tester expects exist.
	setValidAccessKeyEnv()

	var reqs []*http.Request
	var bodies []string
	perRequestResult := make(map[Request]Response)
	// some of the s3tester tests hit this handler concurrently;
	// so need to handle concurrent updates to the reqs and bodies.

	mutex := &sync.Mutex{}
	handle := http.NewServeMux()
	handle.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		reqs = append(reqs, r)

		s := ""
		if r.Body != nil {
			body, _ := ioutil.ReadAll(r.Body)
			s = string(body)
		}
		bodies = append(bodies, s)

		if response, ok := perRequestResult[Request{Uri: r.URL.RequestURI(), Method: r.Method}]; ok {
			for key, value := range response.Headers {
				w.Header().Set(key, value)
			}

			w.WriteHeader(response.Status)
			w.Write([]byte(response.Body))
			return
		}

		for key, value := range headers {
			w.Header().Set(key, value)
		}

		if r.Method == "GET" {
			w.Write([]byte(getResponseBody))
		} else {
			w.WriteHeader(http.StatusNoContent)
		}
	})
	handle.HandleFunc("/nots3tester", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	ts := httptest.NewServer(handle)
	return HttpHelper{Server: ts, Requests: &reqs, Bodies: &bodies, Endpoint: ts.URL, perRequestResult: &perRequestResult}
}

type S3TesterHelper struct {
	*HttpHelper
	endpoints []*HttpHelper
	args      parameters
}

func initS3TesterHelper(t *testing.T, op string) S3TesterHelper {
	emptyHeaders := make(map[string]string)
	httpHelper := NewHttpHelper(t, bodyString, emptyHeaders)
	return S3TesterHelper{HttpHelper: &httpHelper, args: testArgs(op, httpHelper.Endpoint)}
}

func initS3TesterHelperWithData(t *testing.T, op, responseBody string) S3TesterHelper {
	emptyHeaders := make(map[string]string)
	httpHelper := NewHttpHelper(t, responseBody, emptyHeaders)
	return S3TesterHelper{HttpHelper: &httpHelper, args: testArgs(op, httpHelper.Endpoint)}
}

func initMultiS3TesterHelper(t *testing.T, op string, endpoints int) S3TesterHelper {
	emptyHeaders := make(map[string]string)
	return initMultiS3TesterHelperWithHeader(t, op, endpoints, emptyHeaders)
}

func initMultiS3TesterHelperWithHeader(t *testing.T, op string, endpoints int, header map[string]string) S3TesterHelper {
	endpointList := make([]string, endpoints)
	httpHelperList := make([]*HttpHelper, endpoints)
	for i := 0; i < endpoints; i++ {
		httpHelper := NewHttpHelper(t, bodyString, header)
		httpHelperList[i] = &httpHelper
		endpointList[i] = httpHelper.Endpoint
	}
	tempArgs := testArgs(op, endpointList[0])
	tempArgs.endpoints = endpointList
	tempArgs.nrequests.value = endpoints
	return S3TesterHelper{endpoints: httpHelperList, args: tempArgs}
}

func (h *S3TesterHelper) ShutdownMultiServer() {
	for _, endpoint := range h.endpoints {
		endpoint.Shutdown()
	}
}

func (h *S3TesterHelper) runTesterWithoutValidation(t *testing.T) results {
	setValidAccessKeyEnv()
	_, testResults := runtest(h.args)
	return testResults
}

func (h *S3TesterHelper) runTester(t *testing.T) results {
	testResults := h.runTesterWithoutValidation(t)
	if testResults.CummulativeResult.Failcount > 0 {
		t.Fatalf("Failed to run test. %d failures.", testResults.CummulativeResult.Failcount)
	}

	if h.Empty() {
		t.Fatalf("Did not receive any requests")
	}

	if h.NumBodies() != h.NumRequests() {
		t.Fatalf("numBodies (%d, %v) does not match numRequests (%d, %v)", h.NumBodies(), h.Bodies, h.NumRequests(), h.Requests)
	}

	return testResults
}

func (helper *S3TesterHelper) runTesterWithMultiEndpoints(t *testing.T) results {
	testResults := helper.runTesterWithoutValidation(t)
	if testResults.CummulativeResult.Failcount > 0 {
		t.Fatalf("Failed to run test. %d failures.", testResults.CummulativeResult.Failcount)
	}

	for _, h := range helper.endpoints {
		if h.Empty() {
			t.Fatalf("Did not receive any requests")
		}

		if h.NumBodies() != h.NumRequests() {
			t.Fatalf("numBodies (%d, %v) does not match numRequests (%d, %v)", h.NumBodies(), h.Bodies, h.NumRequests(), h.Requests)
		}
	}

	return testResults
}

func testArgs(op string, endpoint string) (args parameters) {
	nrequests := intFlag{value: 1, set: true}

	args = parseAndValidate([]string{})
	args.concurrency = 1
	args.osize = 0
	args.endpoints, _ = validateEndpoint(endpoint)
	args.optype = op
	args.bucketname = "test"
	args.objectprefix = "object"
	args.nrequests = &nrequests
	return
}

type s3XmlErrorResponse struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func generateErrorXml(code string) (response string) {
	b, _ := xml.Marshal(s3XmlErrorResponse{Code: code, Message: code})
	response = string(b)
	return
}

func TestLoadAnonymousCredential(t *testing.T) {
	cre, _ := loadCredentialProfile("", true)
	if cre != credentials.AnonymousCredentials {
		t.Fatalf("not standard anonymous credentials")
	}
}

func TestLoadEnvCredential(t *testing.T) {
	testAccessKey := "testkey"
	testSecretKey := testAccessKey + "/" + testAccessKey
	os.Setenv(accessKey, testAccessKey)
	os.Setenv(secretKey, testSecretKey)
	cre, err := loadCredentialProfile("", false)
	if err != nil {
		t.Fatalf("Error loading env credentials")
	}
	val, _ := cre.Get()
	if val.AccessKeyID != testAccessKey || val.SecretAccessKey != testSecretKey {
		t.Fatalf("Credentials are wrong, %s | %s", val.AccessKeyID, val.SecretAccessKey)
	}
	if val.ProviderName != "EnvProvider" {
		t.Fatalf("Expecting it to use EnvProvider, but it's using %s", val.ProviderName)
	}
}

func TestLoadDefaultCliCredential(t *testing.T) {
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
		_, err := loadCredentialProfile("", false)
		if err == nil {
			t.Fatalf("No credential should be loaded")
		}
		_, err = loadCredentialProfile("test", false)
		if err == nil {
			t.Fatalf("No credential should be loaded")
		}
	} else {
		_, err := loadCredentialProfile("", false)
		if err != nil {
			t.Fatalf("Default credential should be loaded")
		}
	}
}

func TestLoadDefaultCredentialProfileFromFile(t *testing.T) {
	testAccessKey := "testkey"
	user1AccessKey := testAccessKey + testAccessKey
	testSecretKey := testAccessKey + "/" + testAccessKey
	user1SecretKey := testSecretKey + testSecretKey
	fileName := "credential_test"
	fileContent := []byte("[default]\naws_access_key_id=" + testAccessKey + "\naws_secret_access_key=" + testSecretKey + "\n\n" + "[user1]\naws_access_key_id=" + user1AccessKey + "\naws_secret_access_key=" + user1SecretKey)
	ioutil.WriteFile(fileName, fileContent, 0644)
	defer os.Remove(fileName)
	dir, _ := os.Getwd()
	os.Setenv("AWS_SHARED_CREDENTIALS_FILE", dir+"/"+fileName)
	cre, err := loadCredentialProfile("", false)
	if err != nil {
		t.Fatalf("Error loading test file")
	}
	val, _ := cre.Get()
	if val.AccessKeyID != testAccessKey || val.SecretAccessKey != testSecretKey {
		t.Fatalf("Credentials are wrong, %s | %s", val.AccessKeyID, val.SecretAccessKey)
	}
	if val.ProviderName != "SharedCredentialsProvider" {
		t.Fatalf("Expecting it to use SharedCredentialsProvider, but it's using %s", val.ProviderName)
	}
}

func TestLoadCredentialProfileFromFile(t *testing.T) {
	testAccessKey := "testkey"
	user1AccessKey := testAccessKey + testAccessKey
	testSecretKey := testAccessKey + "/" + testAccessKey
	user1SecretKey := testSecretKey + testSecretKey
	fileName := "credential_test"
	fileContent := []byte("[default]\naws_access_key_id=" + testAccessKey + "\naws_secret_access_key=" + testSecretKey + "\n\n" + "[user1]\naws_access_key_id=" + user1AccessKey + "\naws_secret_access_key=" + user1SecretKey)
	ioutil.WriteFile(fileName, fileContent, 0644)
	defer os.Remove(fileName)
	dir, _ := os.Getwd()
	os.Setenv("AWS_SHARED_CREDENTIALS_FILE", dir+"/"+fileName)
	cre, err := loadCredentialProfile("user1", false)
	if err != nil {
		t.Fatalf("Error loading test file")
	}
	val, _ := cre.Get()
	if val.AccessKeyID != user1AccessKey || val.SecretAccessKey != user1SecretKey {
		t.Fatalf("Credentials are wrong, %s | %s", val.AccessKeyID, val.SecretAccessKey)
	}
	if val.ProviderName != "SharedCredentialsProvider" {
		t.Fatalf("Expecting it to use SharedCredentialsProvider, but it's using %s", val.ProviderName)
	}
}

func TestMainWithGet(t *testing.T) {
	h := initS3TesterHelper(t, "get")
	defer h.Shutdown()
	cpuProfileFile := "test1"
	logDetailFile := "test2"
	logLatencyFile := "test3"
	setValidAccessKeyEnv()
	os.Args = []string{"nothing", "-operation=get", "-endpoint=" + h.Endpoint, "-requests=1", "-concurrency=1", "-bucket=test", "-prefix=object", "-json",
		"-logdetail=" + logDetailFile, "-loglatency=" + logLatencyFile, "-cpuprofile=" + cpuProfileFile}
	defer os.Remove(cpuProfileFile)
	defer os.Remove(logDetailFile)
	defer os.Remove(logLatencyFile)

	main()

	// validate server recieved requests
	if h.Request(0).Method != "GET" {
		t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "" {
		t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}

	// valiate locally generated files
	if _, err := os.Stat(cpuProfileFile); os.IsNotExist(err) {
		t.Fatalf("CPU profile file didn't create")
	}

	if _, err := os.Stat(logDetailFile); os.IsNotExist(err) {
		t.Fatalf("Log detail file didn't create")
	}

	if _, err := os.Stat(logLatencyFile); os.IsNotExist(err) {
		t.Fatalf("Log latency file didn't create")
	}
}

func TestSingleEndpointOverwrite1(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.nrequests.value = 5
	h.args.concurrency = 5
	h.args.overwrite = 1
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

	if testResults.CummulativeResult.UniqObjNum != 1 {
		t.Fatalf("Expect only one unique object for overwrite=1, but got %d", testResults.CummulativeResult.UniqObjNum)
	}
}

func TestSingleEndpointOverwrite2(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.nrequests.value = 5
	h.args.concurrency = 5
	h.args.overwrite = 2
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

	if testResults.CummulativeResult.UniqObjNum != 1 {
		t.Fatalf("Expect one unique object for overwrite=2, but got %d", testResults.CummulativeResult.UniqObjNum)
	}
}

func TestMultiEndpointOverwrite1(t *testing.T) {
	endpoints := 2
	op := "put"
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.nrequests.value = 10
	h.args.concurrency = 10
	h.args.overwrite = 1
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

	if testResults.CummulativeResult.UniqObjNum != 2 {
		t.Fatalf("Expect 2 unique object for multiendpoint overwrite=1, but got %d", testResults.CummulativeResult.UniqObjNum)
	}
}

func TestMultiEndpointOverwrite2(t *testing.T) {
	endpoints := 2
	op := "put"
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.nrequests.value = 10
	h.args.concurrency = 10
	h.args.overwrite = 2
	testResults := h.runTesterWithMultiEndpoints(t)

	for _, h := range h.endpoints {
		for i := 0; i < 5; i++ {
			if h.Request(i).Method != "PUT" {
				t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(i).Method)
			}
			if h.Request(i).Header.Get("Consistency-Control") != "" {
				t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(i).Header.Get("Consistency-Control"))
			}
			if h.Request(i).URL.Path != "/test/object-0" {
				t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
			}
		}
	}

	if testResults.CummulativeResult.UniqObjNum != 2 {
		t.Fatalf("Expect 2 unique object for multiendpoint overwrite=2, but got %d", testResults.CummulativeResult.UniqObjNum)
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

	if testResults.CummulativeResult.sumObjSize != bodyLength {
		t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", bodyLength, testResults.CummulativeResult.sumObjSize)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "" {
		t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}
}

func TestGetWhenLessDataReturnedThanContentLength(t *testing.T) {
	headers := make(map[string]string)
	headers["Content-Length"] = "100"
	httpHelper := NewHttpHelper(t, bodyString, headers)
	h := S3TesterHelper{HttpHelper: &httpHelper, args: testArgs("get", httpHelper.Endpoint)}
	defer h.Shutdown()
	testResults := h.runTesterWithoutValidation(t)
	if testResults.CummulativeResult.Failcount != 1 {
		t.Fatalf("Test should have failed. %d failures.", testResults.CummulativeResult.Failcount)
	}
}

func TestGetWithConsistencyControl(t *testing.T) {
	h := initS3TesterHelper(t, "get")
	defer h.Shutdown()
	h.args.consistencyControl = "all"
	h.runTester(t)

	if h.Request(0).Method != "GET" {
		t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if h.Request(0).Header.Get("Consistency-Control") != "all" {
		t.Fatalf("Get should have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
	}
}

func TestGetWithVerification(t *testing.T) {
	keys := []string{"object-0", "object-", "o", "object-0object-0", "object-0object-"}

	for _, k := range keys {
		h := initS3TesterHelperWithData(t, "get", k)
		defer h.Shutdown()
		h.args.verify = 1
		testResults := h.runTester(t)

		if h.Request(0).Method != "GET" {
			t.Fatalf("Wrong request type issued. Expected GET but got %s", h.Request(0).Method)
		}

		if h.Request(0).URL.Path != "/test/object-0" {
			t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
		}

		if testResults.CummulativeResult.sumObjSize != int64(len(k)) {
			t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", bodyLength, testResults.CummulativeResult.sumObjSize)
		}

		if h.Request(0).Header.Get("Consistency-Control") != "" {
			t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
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

func TestPut(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.osize = 6
	testResults := h.runTester(t)

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(0).Method)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}

	if testResults.CummulativeResult.sumObjSize != 6 {
		t.Fatalf("sumObjSize is wrong size. Expected 6, but got %d", testResults.CummulativeResult.sumObjSize)
	}
}

func TestMultiplePuts(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	nrequests := intFlag{value: 5, set: true}
	h.args.nrequests = &nrequests
	h.args.osize = 6
	testResults := h.runTester(t)

	if h.Size() != 5 {
		t.Fatalf("Should be 5 requests (%d)", h.Size())
	}

	for i := 0; i < h.args.nrequests.value; i++ {
		if h.Request(i).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(i).Method)
		}
		if h.Request(i).URL.Path != "/test/object-"+strconv.Itoa(i) {
			t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
		}
	}

	if testResults.CummulativeResult.sumObjSize != 30 {
		t.Fatalf("sumObjSize is wrong size. Expected 30, but got %d", testResults.CummulativeResult.sumObjSize)
	}

	if testResults.CummulativeResult.UniqObjNum != 5 {
		t.Fatalf("uniqObjNum is %d. Expected 5.", testResults.CummulativeResult.UniqObjNum)
	}
}

func TestMultiplePutsWithRepeat(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()

	nrequests := intFlag{value: 5, set: true}
	h.args.nrequests = &nrequests
	h.args.osize = 6
	h.args.attempts = 2 // uniqObjNum should still just be 5 even though we do each PUT twice
	testResults := h.runTester(t)

	if h.Size() != 10 {
		t.Fatalf("Should be 10 requests (%d)", h.Size())
	}

	for i := 0; i < h.args.nrequests.value; i++ {
		if h.Request(i).Method != "PUT" {
			t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(i).Method)
		}
		if h.Request(i).URL.Path != "/test/object-"+strconv.Itoa(i/2) {
			t.Fatalf("Wrong url path: %s", h.Request(i).URL.Path)
		}
	}

	if testResults.CummulativeResult.sumObjSize != 60 {
		t.Fatalf("sumObjSize is wrong size. Expected 60, but got %d", testResults.CummulativeResult.sumObjSize)
	}

	if testResults.CummulativeResult.UniqObjNum != 5 {
		t.Fatalf("uniqObjNum is %d. Expected 5.", testResults.CummulativeResult.UniqObjNum)
	}

	if testResults.CummulativeResult.Count != 10 {
		t.Fatalf("count is %d. Expected 10.", testResults.CummulativeResult.Count)
	}

	for i := 0; i < h.args.nrequests.value*h.args.attempts-2; i = i + 2 {
		if h.Body(i) != h.Body(i+1) {
			t.Fatalf("wrong body for objects %d (%s) and %d (%s)", i, h.Body(i), i+1, h.Body(i+1))
		}
	}
}

func TestConcurrentPuts(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	defer h.Shutdown()
	h.args.concurrency = 2
	nrequests := intFlag{value: 2, set: true}
	h.args.nrequests = &nrequests
	h.runTester(t)

	if h.Size() != 2 {
		t.Fatalf("Should be 2 requests (%d)", h.Size())
	}

	foundPaths := []bool{false, false}
	for i := 0; i < h.args.concurrency; i++ {
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
	h.args.tagging = "tag1=value1"
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
	h := initS3TesterHelper(t, "updatemeta")
	defer h.Shutdown()
	h.args.metadata = "key1=value1"
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
	httpHelper := NewHttpHelper(t, bodyString, headers)
	h := S3TesterHelper{HttpHelper: &httpHelper, args: testArgs("multipartput", httpHelper.Endpoint)}
	h.args.partsize = 100
	h.args.osize = 200
	h.args.metadata = "key1=value1"
	defer h.Shutdown()

	uid := "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"

	multipartInitiateBody :=
		`<?xml version="1.0" encoding="UTF-8"?>
		<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <Bucket>test</Bucket>
		  <Key>object-0</Key>
		  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
		</InitiateMultipartUploadResult>`
	multipartInitiateReq := Request{Uri: "/test/object-0?uploads=", Method: "POST"}
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
	multipartCompleteReq := Request{Uri: fmt.Sprintf("/test/object-0?uploadId=%s", uid), Method: "POST"}
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
	httpHelper := NewHttpHelper(t, bodyString, headers)
	h := S3TesterHelper{HttpHelper: &httpHelper, args: testArgs("multipartput", httpHelper.Endpoint)}
	h.args.partsize = 110
	h.args.osize = 200
	h.args.metadata = "key1=value1"
	defer h.Shutdown()

	uid := "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"

	multipartInitiateBody :=
		`<?xml version="1.0" encoding="UTF-8"?>
		<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <Bucket>test</Bucket>
		  <Key>object-0</Key>
		  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
		</InitiateMultipartUploadResult>`
	multipartInitiateReq := Request{Uri: "/test/object-0?uploads=", Method: "POST"}
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
	multipartCompleteReq := Request{Uri: fmt.Sprintf("/test/object-0?uploadId=%s", uid), Method: "POST"}
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
	httpHelper := NewHttpHelper(t, bodyString, headers)
	h := S3TesterHelper{HttpHelper: &httpHelper, args: testArgs("multipartput", httpHelper.Endpoint)}
	h.args.partsize = 100
	h.args.osize = 200
	h.args.metadata = "key1=value1"
	defer h.Shutdown()

	uid := "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"

	multipartInitiateBody :=
		`<?xml version="1.0" encoding="UTF-8"?>
		<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
		  <Bucket>test</Bucket>
		  <Key>object-0</Key>
		  <UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
		</InitiateMultipartUploadResult>`
	multipartInitiateReq := Request{Uri: "/test/object-0?uploads=", Method: "POST"}
	multipartInitiateResponse := Response{Body: multipartInitiateBody, Status: 200}
	h.SetRequestResult(multipartInitiateReq, multipartInitiateResponse)

	failedUploadPartReq := Request{Uri: fmt.Sprintf("/test/object-0?partNumber=1&uploadId=%s", uid), Method: "PUT"}
	failedUploadPartResponse := Response{Body: generateErrorXml("BadRequest"), Status: 400}
	h.SetRequestResult(failedUploadPartReq, failedUploadPartResponse)

	testResults := h.runTesterWithoutValidation(t)
	if testResults.CummulativeResult.Failcount != 1 {
		t.Fatalf("Should have failed to run test. %d failures.", testResults.CummulativeResult.Failcount)
	}

	// initiate, 1 part, abort
	if h.Size() != 3 {
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

	if h.Request(2).Method != "DELETE" {
		t.Fatalf("Wrong request type issued. Expected %s but got %s", "DELETE", h.Request(2).Method)
	}
	if h.Request(2).URL.Path == "/test/object-0" && h.Request(2).URL.RawQuery != fmt.Sprintf("uploadId=%s", uid) {
		t.Fatalf("Wrong url path. Expected %s?%s, got %s?%s", "/test/object-0", fmt.Sprintf("uploadId=%s", uid), h.Request(2).URL.Path, h.Request(2).URL.RawQuery)
	}
}

func TestUniformPuts(t *testing.T) {
	h := initS3TesterHelper(t, "put")
	nrequest := intFlag{value: 10, set: true}
	h.args.nrequests = &nrequest
	h.args.min = 10
	h.args.max = 20
	defer h.Shutdown()
	testResults := h.runTester(t)
	avgSize := float64(testResults.CummulativeResult.sumObjSize) / float64(testResults.CummulativeResult.Count)
	if testResults.CummulativeResult.sumObjSize > 200 || testResults.CummulativeResult.sumObjSize < 100 {
		t.Fatalf("sumObjectSize should be bounded by 100 to 200, but got %d", testResults.CummulativeResult.sumObjSize)
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
	h.args.concurrency = 2
	h.args.osize = 5
	testResults := h.runTesterWithMultiEndpoints(t)

	if op != "delete" && op != "head" && op != "options" && testResults.CummulativeResult.sumObjSize != 2*5 {
		t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", 2*5, testResults.CummulativeResult.sumObjSize)
	}

	for i, h := range h.endpoints {
		if h.Request(0).Method != strings.ToUpper(op) {
			t.Fatalf("Wrong request type issued. Expected %s but got %s", strings.ToUpper(op), h.Request(0).Method)
		}

		if len(*h.Requests) != 1 {
			t.Fatalf("Wrong request number. Expected %d but got %d", 1, len(*h.Requests))
		}

		if op != "options" && h.Request(0).URL.Path != "/test/object-"+strconv.Itoa(i) {
			fmt.Println(h.Request(0).URL.Path)
			fmt.Println("/test/object-" + strconv.Itoa(i))
			t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
		}

		if op == "options" && h.Request(0).URL.Path != "/" {
			t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
		}

		if h.Request(0).Header.Get("Consistency-Control") != "" {
			t.Fatalf("Get should not have set Consistency-Control (actual: %s)", h.Request(0).Header.Get("Consistency-Control"))
		}
	}

	if testResults.CummulativeResult.Failcount != 0 {
		t.Fatalf("Should not have failed. %d failures.", testResults.CummulativeResult.Failcount)
	}

	validateEndpointResult(t, testResults, endpoints, h)
}

func TestLargePutRequestToMultiEndpoint(t *testing.T) {
	endpoints := 5
	op := "put"
	requests := 1000
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	nrequest := intFlag{value: requests, set: true}
	h.args.nrequests = &nrequest
	h.args.concurrency = 10
	h.args.osize = 5
	testResults := h.runTesterWithMultiEndpoints(t)

	for i, h := range h.endpoints {
		if h.Request(0).Method != strings.ToUpper(op) {
			t.Fatalf("Wrong request type issued. Expected %s but got %s", strings.ToUpper(op), h.Request(0).Method)
		}

		if len(*h.Requests) != requests/endpoints {
			t.Fatalf("Wrong request number. Expected %d but got %d", requests/endpoints, len(*h.Requests))
		}

		if testResults.PerEndpointResult[i].sumObjSize != 1000 {
			t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", 1000, testResults.PerEndpointResult[i].sumObjSize)
		}
	}

	validateEndpointResult(t, testResults, endpoints, h)
}

func TestMultiEndpointPutTagging(t *testing.T) {
	endpoints := 2
	op := "puttagging"
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.concurrency = 2
	h.args.tagging = "tag1=value1"
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
	h := initMultiS3TesterHelper(t, op, endpoints)
	defer h.ShutdownMultiServer()
	h.args.concurrency = 2
	h.args.metadata = "key1=value1"
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
	h.args.partsize = 100
	h.args.osize = 200
	h.args.metadata = "key1=value1"
	h.args.concurrency = 2
	h.args.metadata = "key1=value1"

	uid := "VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA"

	for i, h := range h.endpoints {
		multipartInitiateBody :=
			`<?xml version="1.0" encoding="UTF-8"?>
			<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
			<Bucket>test</Bucket>
			<Key>object-` + strconv.Itoa(i) + `</Key>
			<UploadId>VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA</UploadId>
			</InitiateMultipartUploadResult>`
		multipartInitiateReq := Request{Uri: "/test/object-" + strconv.Itoa(i) + "?uploads=", Method: "POST"}
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
		multipartCompleteReq := Request{Uri: fmt.Sprintf("/test/object-"+strconv.Itoa(i)+"?uploadId=%s", uid), Method: "POST"}
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
		if endpointResult.Concurrency != h.args.concurrency/endpoints {
			t.Fatalf("The concurrency of one endpoint %d should equal to total concurrency divide by endpoints %d.", endpointResult.Concurrency, h.args.concurrency/endpoints)
		}

		if endpointResult.AverageObjectSize != testResults.CummulativeResult.AverageObjectSize {
			t.Fatalf("The AverageObjectSize of one endpoint %f should equal to AverageObjectSize of the total result %f.", endpointResult.AverageObjectSize, testResults.CummulativeResult.AverageObjectSize)
		}

		if testResults.CummulativeResult.AverageObjectSize != 0 && endpointResult.UniqObjNum != h.args.nrequests.value/endpoints {
			t.Fatalf("The UniqObjNum of one endpoint %d should equal to total requests divide by endpoints %d.", endpointResult.UniqObjNum, h.args.nrequests.value/endpoints)
		}

		if h.args.optype != "putget" && endpointResult.Count != h.args.nrequests.value/endpoints {
			t.Fatalf("The Count of one endpoint %d should equal to total requests divide by endpoints %d.", endpointResult.Count, h.args.nrequests.value/endpoints)
		}

		endpointList = append(endpointList, endpointResult.Endpoint)
	}

	sort.Slice(endpointList, func(i, j int) bool { return endpointList[i] < endpointList[j] })
	sort.Slice(h.args.endpoints, func(i, j int) bool { return h.args.endpoints[i] < h.args.endpoints[j] })
	if !reflect.DeepEqual(endpointList, h.args.endpoints) {
		t.Fatalf("The endpoint list of result should deelpy equal to args.endpoints")
	}
}

func TestMergeSingleSuccessResult(t *testing.T) {
	args := testArgs("put", "http://test1.com")
	args.concurrency = 100
	startTime := time.Now()
	c := make(chan result, args.concurrency)
	addFakeResultToChannel(c, args, true, startTime)
	testResults := collectWorkerResult(c, args, startTime)
	validateEndpointResults(t, testResults.PerEndpointResult, args, 300, 300, 0)
	validateAggregateResult(t, &testResults.CummulativeResult, args, 300, 300, 0)
	processTestResult(&testResults, args)
	validateTestResult(t, &testResults.CummulativeResult, testResults.PerEndpointResult, args)
}

func TestMergeSingleFailResult(t *testing.T) {
	args := testArgs("put", "http://test1.com")
	args.concurrency = 100
	startTime := time.Now()
	c := make(chan result, args.concurrency)
	addFakeResultToChannel(c, args, false, startTime)
	testResults := collectWorkerResult(c, args, startTime)
	validateEndpointResults(t, testResults.PerEndpointResult, args, 0, 300, 300)
	validateAggregateResult(t, &testResults.CummulativeResult, args, 0, 300, 300)
	processTestResult(&testResults, args)
	validateTestResult(t, &testResults.CummulativeResult, testResults.PerEndpointResult, args)
}

func TestMergeFourSuccessResult(t *testing.T) {
	args := testArgs("put", "http://test1.com,http://test2.com,http://test3.com,http://test4.com")
	args.concurrency = 100
	startTime := time.Now()
	c := make(chan result, args.concurrency)
	addFakeResultToChannel(c, args, true, startTime)
	testResults := collectWorkerResult(c, args, startTime)
	validateEndpointResults(t, testResults.PerEndpointResult, args, 75, 75, 0)
	validateAggregateResult(t, &testResults.CummulativeResult, args, 300, 300, 0)
	processTestResult(&testResults, args)
	validateTestResult(t, &testResults.CummulativeResult, testResults.PerEndpointResult, args)
}

func TestMergeFourFailedResult(t *testing.T) {
	args := testArgs("put", "http://test1.com,http://test2.com,http://test3.com,http://test4.com")
	args.concurrency = 100
	startTime := time.Now()
	c := make(chan result, args.concurrency)
	addFakeResultToChannel(c, args, false, startTime)
	testResults := collectWorkerResult(c, args, startTime)
	validateEndpointResults(t, testResults.PerEndpointResult, args, 0, 75, 75)
	validateAggregateResult(t, &testResults.CummulativeResult, args, 0, 300, 300)
	processTestResult(&testResults, args)
	validateTestResult(t, &testResults.CummulativeResult, testResults.PerEndpointResult, args)
}

func TestMixedResult(t *testing.T) {
	args := testArgs("put", "http://test1.com,http://test2.com,http://test3.com,http://test4.com")
	args.concurrency = 100
	startTime := time.Now()
	c := make(chan result, args.concurrency*2)
	addFakeResultToChannel(c, args, true, startTime)
	addFakeResultToChannel(c, args, false, startTime)
	args.concurrency = 200
	testResults := collectWorkerResult(c, args, startTime)
	validateEndpointResults(t, testResults.PerEndpointResult, args, 75, 150, 75)
	validateAggregateResult(t, &testResults.CummulativeResult, args, 300, 600, 300)
	processTestResult(&testResults, args)
	validateTestResult(t, &testResults.CummulativeResult, testResults.PerEndpointResult, args)
}

func addFakeResultToChannel(c chan<- result, args parameters, isSuccess bool, startTime time.Time) {
	for _, endpoint := range args.endpoints {
		for currEndpointWorkerId := 0; currEndpointWorkerId < args.concurrency/len(args.endpoints); currEndpointWorkerId++ {
			go createFakeResult(c, endpoint, isSuccess, startTime)
		}
	}
}

func createFakeResult(c chan<- result, endpoint string, isSuccess bool, startTime time.Time) {
	fakeResult := NewResult()
	fakeResult.sumObjSize = 13
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

func validateEndpointResults(t *testing.T, endpointResults []*result, args parameters, expectedUniqObjNum, expectedCount, expectedFailcount int) {
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
		if endpointResult.Concurrency != args.concurrency/len(args.endpoints) {
			t.Fatalf("The endpointResult.Concurrency %d should equal to workerNum %d.", endpointResult.Concurrency, args.concurrency/len(args.endpoints))
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

func validateAggregateResult(t *testing.T, aggregateResults *result, args parameters, expectedUniqObjNum, expectedCount, expectedFailcount int) {
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

func validateTestResult(t *testing.T, aggregateResults *result, perEndpointResult []*result, args parameters) {
	if aggregateResults.Operation != args.optype {
		t.Fatalf("The aggregateResults.Operation %s should be %s.", aggregateResults.Operation, args.optype)
	}
	if aggregateResults.Category == "" {
		t.Fatal("The aggregateResults.Operation should not be empty.")
	}
	if aggregateResults.UniqueName == "" {
		t.Fatal("The aggregateResults.UniqueName should not be empty.")
	}
	if aggregateResults.Endpoint != "" {
		t.Fatal("The aggregateResults.Endpoint should be empty.")
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
	h.args.tier = "Expedited"
	h.args.days = 4
	h.runTester(t)
	tierInBody := strings.Split(strings.Split(h.Body(0), "<Tier>")[1], "</Tier>")[0]
	daysInBody, _ := strconv.ParseInt(strings.Split(strings.Split(h.Body(0), "<Days>")[1], "</Days>")[0], 10, 64)

	if h.Request(0).Method != "POST" {
		t.Fatalf("Wrong request type issued. Expected POST but got %s", h.Request(0).Method)
	}

	if h.args.tier != tierInBody {
		t.Fatalf("Wrong restore tier. Expected %s but got %s", h.args.tier, tierInBody)
	}

	if h.args.days != daysInBody {
		t.Fatalf("Wrong restore days. Expected %d but got %d", h.args.days, daysInBody)
	}

	if h.Request(0).URL.Path != "/test/object-0" {
		t.Fatalf("Wrong url path: %s", h.Request(0).URL.Path)
	}
}

func TestSingleWorkerReplay(t *testing.T) {
	sampleS3RQ := `{"replay":[
    [
    {"key":"testobject-0", "bucket":"not","op":"put","size":10000},
    {"key":"testobject-0", "bucket":"not","op":"head","size":10000}
    ],
    [
    {"key":"testobject-0", "bucket":"not","op":"updatemeta","size":10000},
    {"key":"testobject-0", "bucket":"not","op":"get","size":10000}
    ],
    [
    {"key":"testobject-0", "bucket":"not","op":"delete","size":10000}
    ]
    ]}`
	// use default value to load initS3TesterHelper
	h := initS3TesterHelper(t, "")
	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleS3RQ))

	h.args.concurrency = 1
	h.runTester(t)

	defer h.Shutdown()

	if h.Size() != 5 {
		t.Fatalf("Should received 4 requests. Had %v requests", h.Size())
	}
	m := make(map[string]int, 5)
	for i := 0; i < 5; i++ {
		m[h.Request(i).Method] += 1
	}
	expectedM := map[string]int{"PUT": 2, "DELETE": 1, "GET": 1, "HEAD": 1}

	if !reflect.DeepEqual(m, expectedM) {
		t.Fatalf("Should have expected request types")
	}
}

func TestMultiConcurrency(t *testing.T) {
	sampleS3RQ := `{"replay":[
    [
    {"key":"testobject-4", "bucket":"not","op":"put","size":14},
    {"key":"testobject-1", "bucket":"not","op":"put","size":11},
    {"key":"testobject-2", "bucket":"not","op":"put","size":12},
    {"key":"testobject-3", "bucket":"not","op":"put","size":13},
    {"key":"testobject-0", "bucket":"not","op":"put","size":10},
    {"key":"testobject-1", "bucket":"not","op":"get","size":11},
    {"key":"testobject-2", "bucket":"not","op":"get","size":12},
    {"key":"testobject-3", "bucket":"not","op":"get","size":13},
    {"key":"testobject-1", "bucket":"not","op":"delete","size":11},
    {"key":"testobject-2", "bucket":"not","op":"delete","size":12},
    {"key":"testobject-4", "bucket":"not","op":"get","size":14},
    {"key":"testobject-3", "bucket":"not","op":"delete","size":13},
    {"key":"testobject-4", "bucket":"not","op":"delete","size":13},
    {"key":"testobject-0", "bucket":"not","op":"get","size":10},
    {"key":"testobject-0", "bucket":"not","op":"delete","size":10}
    ]
    ]}`
	// use default value to load initS3TesterHelper
	h := initS3TesterHelper(t, "")
	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleS3RQ))

	h.args.concurrency = 10
	results := h.runTester(t)

	defer h.Shutdown()

	if h.Size() != 15 {
		t.Fatalf("Should received 15 requests. Had %v requests", h.Size())
	}
	orderOps := []string{"PUT", "GET", "DELETE"}
	actualM := make(map[string][]string, 5)

	objectNames := map[string]int{"testobject-0": 0, "testobject-1": 0, "testobject-2": 0, "testobject-3": 0, "testobject-4": 0}

	for k, _ := range objectNames {
		actualM[k] = []string{}
	}

	for i := 0; i < 15; i++ {
		index := strings.LastIndex(h.Request(i).URL.Path, `/`)
		objName := h.Request(i).URL.Path[index+1:]
		if v, ok := actualM[objName]; ok {
			actualM[objName] = append(v, h.Request(i).Method)
		} else {
			t.Fatalf("objName should be an one of expectedObjectNames got %v", objName)
		}
	}

	// Make sure operations occured in the correct order for each test-object
	for _, v := range actualM {
		for b := 0; b < len(v); b++ {
			if v[b] != orderOps[b] {
				t.Fatalf("Ops of the same key must occur in order, got %v, but expected %v", v[b], orderOps[b])
			}
		}
	}

	if results.CummulativeResult.sumObjSize != 60+(bodyLength*5) {
		t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", 75+bodyLength, results.CummulativeResult.sumObjSize)
	}
}

func TestPutMetaReplay(t *testing.T) {
	sampleS3RQ := `{"replay":[
    [
    {"key":"testobject-0-0", "bucket":"not","op":"updatemeta","size":12},
    {"key":"testobject-0-1", "bucket":"not","op":"updatemeta","size":24}
    ]
    ]}`
	// use default value to load initS3TesterHelper
	h := initS3TesterHelper(t, "")
	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleS3RQ))

	h.args.concurrency = 1
	h.runTester(t)

	defer h.Shutdown()

	if h.Size() != 2 {
		t.Fatalf("Should received 2 requests. Had %v requests", h.Size())
	}

	if h.Request(0).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(0).Method)
	}
	if h.Request(0).Header.Get("x-amz-meta-kkkkkk") != strings.Repeat("v", 6) {
		t.Fatalf("Wrong metadata header received: %s", h.Request(0).Header.Get("x-amz-meta-kkkkkk"))
	}

	if h.Request(1).Method != "PUT" {
		t.Fatalf("Wrong request type issued. Expected PUT but got %s", h.Request(0).Method)
	}
	if h.Request(1).Header.Get("x-amz-meta-kkkkkkkkkkkk") != strings.Repeat("v", 12) {
		t.Fatalf("Wrong metadata header received: %s", h.Request(0).Header.Get("x-amz-meta-kkkkkkkkkkkk"))
	}

}

func TestMultiPutReplay(t *testing.T) {
	sampleS3RQ := `{"replay":[
    [
    {"key":"testobject-0-4", "bucket":"not","op":"put","size":0},
    {"key":"testobject-3-1", "bucket":"not","op":"put","size":10},
    {"key":"testobject-1-2", "bucket":"not","op":"put","size":10},
    {"key":"testobject-2-3", "bucket":"not","op":"put","size":10},
    {"key":"testobject-3-0", "bucket":"not","op":"put","size":1},
    {"key":"testobject-7-1", "bucket":"not","op":"put","size":2},
    {"key":"testobject-6-2", "bucket":"not","op":"put","size":3},
    {"key":"testobject-15-3", "bucket":"not","op":"put","size":4},
    {"key":"testobject-9-1", "bucket":"not","op":"put","size":5},
    {"key":"testobject-5-2", "bucket":"not","op":"put","size":6},
    {"key":"testobject-1-4", "bucket":"not","op":"put","size":7},
    {"key":"testobject-3-3", "bucket":"not","op":"put","size":8},
    {"key":"testobject-2-4", "bucket":"not","op":"put","size":9},
    {"key":"testobject-1-0", "bucket":"not","op":"put","size":10},
    {"key":"testobject-0-0", "bucket":"not","op":"put","size":11}
    ]
    ]}`
	// use default value to load initS3TesterHelper
	h := initS3TesterHelper(t, "")
	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleS3RQ))

	h.args.concurrency = 10
	results := h.runTester(t)

	defer h.Shutdown()

	if h.Size() != 15 {
		t.Fatalf("Should received 15 requests. Had %v requests", h.Size())
	}
	if results.CummulativeResult.sumObjSize != 96 {
		t.Fatalf("sumObjSize is wrong size. Expected 6, but got %d", results.CummulativeResult.sumObjSize)
	}

}

func TestMixedPutGet(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":50},
											  {"operationType":"get","ratio":50}]
											}`
	h := initS3TesterHelper(t, "")

	h.args.nrequests = &intFlag{value: 10, set: true}

	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleMixedWorkload))

	h.args.concurrency = 1
	h.args.osize = 100
	h.args.bucketname = "not"

	results := h.runTester(t)

	if h.Size() != 10 {
		t.Fatalf("Should received 10 requests. Had %v requests", h.Size())
	}

	if results.CummulativeResult.sumObjSize != 500+(5*bodyLength) {
		t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", 500+(5*bodyLength), results.CummulativeResult.sumObjSize)
	}

	defer h.Shutdown()
}

func TestMixedPutGetLargeMultiCon(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":100}]
											}`
	h := initS3TesterHelper(t, "")

	h.args.nrequests = &intFlag{value: 250, set: true}

	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleMixedWorkload))

	h.args.concurrency = 5
	h.args.osize = 30
	h.args.bucketname = "not"

	results := h.runTester(t)

	if h.Size() != 250 {
		t.Fatalf("Should received 250 requests. Had %v requests", h.Size())
	}

	if results.CummulativeResult.sumObjSize != (250 * 30) {
		t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", (125 * 30), results.CummulativeResult.sumObjSize)
	}

	if results.CummulativeResult.Failcount != 0 {
		t.Fatalf("Test should have no failures. %d failures.", results.CummulativeResult.Failcount)
	}

	defer h.Shutdown()
}

func TestMixedPutGetLargeMultiCon2(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":50},
											  {"operationType":"get","ratio":50}]
											}`
	h := initS3TesterHelper(t, "")

	h.args.nrequests = &intFlag{value: 250, set: true}

	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleMixedWorkload))

	h.args.concurrency = 5
	h.args.osize = 30
	h.args.bucketname = "not"

	results := h.runTester(t)

	if h.Size() != 250 {
		t.Fatalf("Should received 250 requests. Had %v requests", h.Size())
	}

	if results.CummulativeResult.sumObjSize != (125*30)+(125*bodyLength) {
		t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", (125*30)+(125*bodyLength), results.CummulativeResult.sumObjSize)
	}

	if results.CummulativeResult.Failcount != 0 {
		t.Fatalf("Test should have no failures. %d failures.", results.CummulativeResult.Failcount)
	}

	defer h.Shutdown()
}

func TestMixedPutGetDeleteSmall(t *testing.T) {
	sampleMixedWorkload := `{"mixedWorkload":[{"operationType":"put","ratio":25},
											  {"operationType":"get","ratio":25},
											  {"operationType":"updatemeta","ratio":25},
											  {"operationType":"delete","ratio":25}]
											}`
	h := initS3TesterHelper(t, "")

	h.args.nrequests = &intFlag{value: 4, set: true}

	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleMixedWorkload))

	h.args.concurrency = 1
	h.args.osize = 30
	h.args.bucketname = "not"

	results := h.runTester(t)

	if h.Size() != 4 {
		t.Fatalf("Should received 4 requests. Had %v requests", h.Size())
	}

	if results.CummulativeResult.sumObjSize != (30 + bodyLength) {
		t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", (30 + bodyLength), results.CummulativeResult.sumObjSize)
	}

	if results.CummulativeResult.Failcount != 0 {
		t.Fatalf("Test should have no failures. %d failures.", results.CummulativeResult.Failcount)
	}
	reqs := []string{"PUT", "GET", "PUT", "DELETE"}

	for i := 0; i < 4; i++ {
		if h.Request(i).Method != reqs[i] {
			t.Fatalf("Expected %v method, but got %v", reqs[i], h.Request(i).Method)
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
	h := initS3TesterHelper(t, "")

	h.args.nrequests = &intFlag{value: 100, set: true}

	h.args.jsonDecoder = json.NewDecoder(strings.NewReader(sampleMixedWorkload))

	h.args.concurrency = 4
	h.args.osize = 30
	h.args.bucketname = "not"

	results := h.runTester(t)

	if h.Size() != 100 {
		t.Fatalf("Should received 100 requests. Had %v requests", h.Size())
	}

	if results.CummulativeResult.sumObjSize != (25*30)+(25*bodyLength) {
		t.Fatalf("sumObjSize is wrong size. Expected %d, but got %d", (25*30)+(25*bodyLength), results.CummulativeResult.sumObjSize)
	}

	if results.CummulativeResult.Failcount != 0 {
		t.Fatalf("Test should have no failures. %d failures.", results.CummulativeResult.Failcount)
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
	requests := 1000000
	h := initMultiS3TesterHelper(nil, op, endpoints)
	defer h.ShutdownMultiServer()
	nrequest := intFlag{value: requests, set: true}
	h.args.nrequests = &nrequest
	h.args.concurrency = 1000
	h.args.osize = 5
	setValidAccessKeyEnv()
	runtest(h.args)
}
