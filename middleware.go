package main

import (
	"context"
	"fmt"
	"strings"
	"testing"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

type addCustomHeader struct {
	header, val string
}

type unsignedPayloadMiddleware struct{}

type captureHost struct {
	testbench *testing.T
	expect    string
}

var (
	_ middleware.BuildMiddleware    = (*addCustomHeader)(nil)
	_ middleware.BuildMiddleware    = (*unsignedPayloadMiddleware)(nil)
	_ middleware.FinalizeMiddleware = (*removeAwsChunked)(nil)
)

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

func (m *unsignedPayloadMiddleware) ID() string {
	return "UnsignedPayloadMiddleware"
}

func (m *unsignedPayloadMiddleware) HandleBuild(
	ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
) (
	out middleware.BuildOutput, metadata middleware.Metadata, err error,
) {
	// Assign via context as assigning it in the header will cause
	// SignatureDoesNotMatch errors due to signing
	ctx = v4.SetPayloadHash(ctx, "UNSIGNED-PAYLOAD")

	return next.HandleBuild(ctx, in)
}

func UnsignedPayloadMiddleware() func(*middleware.Stack) error {
	return func(s *middleware.Stack) error {
		return s.Build.Add(&unsignedPayloadMiddleware{}, middleware.After)
	}
}

func (m *captureHost) ID() string {
	return "CaptureHost"
}

func (m *captureHost) HandleFinalize(
	ctx context.Context,
	in middleware.FinalizeInput,
	next middleware.FinalizeHandler,
) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
	m.testbench.Helper()

	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		m.testbench.Logf("Middleware CaptureHost: unable to cast request to *smithyhttp.Request")
		return next.HandleFinalize(ctx, in)
	}

	if req.Host != m.expect {
		m.testbench.Errorf("Unexpected Host: got %s, expected %s", req.Host, m.expect)
	}

	return next.HandleFinalize(ctx, in)
}

func CaptureHost(t *testing.T, tc string) func(*middleware.Stack) error {
	return func(s *middleware.Stack) error {
		return s.Finalize.Add(&captureHost{testbench: t, expect: tc}, middleware.After)
	}
}

type removeAwsChunked struct{}

func (m *removeAwsChunked) ID() string {
	return "RemoveAwsChunked"
}

func (m *removeAwsChunked) HandleFinalize(
	ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return next.HandleFinalize(ctx, in)
	}

	ce := req.Header.Get("Content-Encoding")
	if ce == "" {
		// No Content-Encoding header, nothing to do
		return next.HandleFinalize(ctx, in)
	}

	encodings := strings.Split(ce, ",")
	filtered := make([]string, 0, len(encodings))
	for _, enc := range encodings {
		enc = strings.TrimSpace(enc)
		if strings.ToLower(enc) != "aws-chunked" {
			filtered = append(filtered, enc)
		}
	}

	if len(filtered) == 0 {
		req.Header.Del("Content-Encoding")
	} else {
		req.Header.Set("Content-Encoding", strings.Join(filtered, ", "))
	}

	return next.HandleFinalize(ctx, in)
}

func RemoveAwsChunked() func(*middleware.Stack) error {
	return func(s *middleware.Stack) error {
		// Must be added before the Signing middleware to prevent overwriting
		// as the SDK will repeatedly set aws-chunked in Build and Finalize stages
		return s.Finalize.Insert(&removeAwsChunked{}, "Signing", middleware.Before)
	}
}
