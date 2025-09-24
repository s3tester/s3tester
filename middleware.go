package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"strings"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

var (
	_ middleware.BuildMiddleware       = (*addCustomHeader)(nil)
	_ middleware.BuildMiddleware       = (*addHeaders)(nil)
	_ middleware.BuildMiddleware       = (*addQuery)(nil)
	_ middleware.DeserializeMiddleware = (*printResponseHeaders)(nil)
	_ middleware.DeserializeMiddleware = (*debugErrorResponse)(nil)
)

type addCustomHeader struct {
	header, val string
}

func (m *addCustomHeader) ID() string {
	return "addCustomHeader-" + m.header
}

func (m *addCustomHeader) HandleBuild(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
	out middleware.BuildOutput, metadata middleware.Metadata, err error,
) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return out, metadata, fmt.Errorf("unrecognized transport type %T", in.Request)
	}

	req.Header.Set(m.header, m.val)

	return next.HandleBuild(ctx, in)
}

func AddCustomHeader(header, val string) func(*middleware.Stack) error {
	return func(s *middleware.Stack) error {
		return s.Build.Add(&addCustomHeader{
			header: header,
			val:    val,
		}, middleware.After) // must be called last as User-Agent is populated first by the SDK
	}
}

type addHeaders struct {
	headers headerFlags
}

func (m *addHeaders) ID() string {
	return "addHeaders"
}

func (m *addHeaders) HandleBuild(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
	out middleware.BuildOutput, metadata middleware.Metadata, err error,
) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return out, metadata, fmt.Errorf("unrecognized transport type %T", in.Request)
	}

	userAgent := userAgentString + req.Header.Get("User-Agent")
	req.Header.Set("User-Agent", userAgent)

	for k, v := range m.headers {
		req.Header.Set(k, v)
	}

	return next.HandleBuild(ctx, in)
}

func AddHeaders(headers headerFlags) func(*middleware.Stack) error {
	return func(s *middleware.Stack) error {
		return s.Build.Add(&addHeaders{
			headers: headers,
		}, middleware.After) // must be called last as User-Agent is populated first by the SDK
	}
}

type addQuery struct {
	query string
}

func (m *addQuery) ID() string {
	return "addQuery"
}

func (m *addQuery) HandleBuild(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
	out middleware.BuildOutput, metadata middleware.Metadata, err error,
) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return out, metadata, fmt.Errorf("unrecognized transport type %T", in.Request)
	}

	if m.query != "" {
		q := req.URL.Query()
		values, err := url.ParseQuery(m.query)
		if err != nil {
			log.Fatalf("Unable to parse query params: %v", err)
		}

		for k, v := range values {
			for _, s := range v {
				q.Add(k, s)
			}
		}
		req.URL.RawQuery = q.Encode()
	}

	return next.HandleBuild(ctx, in)
}

func AddQuery(query string) func(*middleware.Stack) error {
	return func(s *middleware.Stack) error {
		return s.Build.Add(&addQuery{
			query: query,
		}, middleware.After)
	}
}

type printResponseHeaders struct {
	headers []string
}

func (m *printResponseHeaders) ID() string {
	return "printResponseHeaders"
}

func (m *printResponseHeaders) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (
	out middleware.DeserializeOutput, metadata middleware.Metadata, err error,
) {
	// Call next middleware first to get the response
	out, metadata, err = next.HandleDeserialize(ctx, in)

	resp, ok := out.RawResponse.(*smithyhttp.Response)
	if !ok || resp == nil {
		// No HTTP response available, nothing to do
		return out, metadata, err
	}

	// Only print headers if status code is NOT 2xx
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Println("Response Header Report:")
		for _, header := range m.headers {
			// Use http.Header.Get or Values to get all values for the header
			values := resp.Header.Values(header)
			if len(values) > 0 {
				fmt.Fprintf(os.Stderr, "\t%s : %v\n", header, values)
			}
		}
	}

	return out, metadata, err
}

// AddPrintResponseHeaders returns a middleware stack option that adds the printResponseHeaders middleware
func AddPrintResponseHeaders(envVar string) func(*middleware.Stack) error {
	headersEnv := os.Getenv(envVar)
	if len(headersEnv) == 0 {
		// No headers to print, no middleware needed
		return func(s *middleware.Stack) error { return nil }
	}

	var printedHeaders []string
	for _, header := range strings.Split(headersEnv, ";") {
		if h := strings.TrimSpace(header); len(h) > 0 {
			printedHeaders = append(printedHeaders, h)
		}
	}
	if len(printedHeaders) == 0 {
		return func(s *middleware.Stack) error { return nil }
	}

	return func(s *middleware.Stack) error {
		return s.Deserialize.Add(&printResponseHeaders{
			headers: printedHeaders,
		}, middleware.After)
	}
}

type debugErrorResponse struct{}

func (m *debugErrorResponse) ID() string {
	return "debugErrorResponse"
}

func (m *debugErrorResponse) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (
	out middleware.DeserializeOutput, metadata middleware.Metadata, err error,
) {
	// Access the HTTP response before calling next to avoid consuming the body too early
	resp, ok := out.RawResponse.(*smithyhttp.Response)
	if !ok || resp == nil {
		// No HTTP response, just continue
		return next.HandleDeserialize(ctx, in)
	}

	// Read and buffer the body only if status code is not 2xx
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var buf bytes.Buffer
		// Read the body
		if resp.Body != nil {
			_, err := io.Copy(&buf, resp.Body)
			if err != nil {
				log.Printf("failed to read error response body: %v", err)
			}
			// Close original body
			resp.Body.Close()
		}

		// Replace the body with a new ReadCloser so downstream can read it again
		resp.Body = io.NopCloser(bytes.NewReader(buf.Bytes()))

		// Log the error response body (remove newlines)
		log.Printf("request %v %v not successful: %v %v",
			resp.Request.Method,
			resp.Request.URL.EscapedPath(),
			resp.StatusCode,
			strings.ReplaceAll(buf.String(), "\n", ""),
		)
	}

	// Call next middleware with the (possibly replaced) response
	return next.HandleDeserialize(ctx, in)
}

// AddDebugErrorResponseMiddleware returns a middleware stack option that adds the debug error response logger
func AddDebugErrorResponseMiddleware(enabled bool) func(*middleware.Stack) error {
	if !enabled {
		return func(s *middleware.Stack) error { return nil }
	}
	return func(s *middleware.Stack) error {
		return s.Deserialize.Add(&debugErrorResponse{}, middleware.Before)
	}
}
