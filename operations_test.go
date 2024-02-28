package main

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// MockS3Client is used for mocking Amazon S3 for testing purposes
type MockS3Client struct {
	s3iface.S3API
	S3OpHandler func(interface{}) interface{}
}

// NewMockS3Client creates a new mock S3 client; suitable for use in unit tests
func NewMockS3Client(handler func(interface{}) interface{}) *MockS3Client {
	return &MockS3Client{S3OpHandler: handler}
}

func (m *MockS3Client) PutObjectWithContext(ctx context.Context, in *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	m.S3OpHandler(in)

	return &s3.PutObjectOutput{}, nil
}

func TestPutOp(t *testing.T) {
	var numBytes int64 = 100
	md5 := "+M5KlcqLv/LqWGVzA4hI/A=="

	handler := func(in interface{}) interface{} {
		i := in.(*s3.PutObjectInput)

		if *i.Bucket != "b" {
			t.Errorf("Expected bucket: %s but got: %s", "b", *i.Bucket)
		}

		if *i.Key != "k1" {
			t.Errorf("Expected key: %s but got: %s", "k1", *i.Key)
		}

		if *i.ContentLength != numBytes {
			t.Errorf("Expected object size: %d but got: %d", numBytes, *i.ContentLength)
		}

		if *i.ContentMD5 != md5 {
			t.Errorf("Expected content md5: %s, but got %s", md5, *i.ContentMD5)
		}

		data, err := io.ReadAll(i.Body)

		if err != nil {
			t.Errorf("Failed to read %d expected bytes from objects. Error: %v", *i.ContentLength, err)
		}

		if int64(len(data)) != *i.ContentLength {
			t.Fatalf("Failed to read %d expected bytes from objects. Error: %v", *i.ContentLength, err)
		}

		return in
	}

	svc := NewMockS3Client(handler)

	Put(context.Background(), svc, "b", "k1", "", numBytes, map[string]*string{})
}

func TestPutWithTagsOp(t *testing.T) {
	var numBytes int64 = 394
	tags := "tag1=blue&tag2=red"

	handler := func(in interface{}) interface{} {
		i := in.(*s3.PutObjectInput)

		if *i.Bucket != "b" {
			t.Fatalf("Expected bucket: %s but got: %s", "b", *i.Bucket)
		}

		if *i.Key != "k1" {
			t.Fatalf("Expected key: %s but got: %s", "k1", *i.Key)
		}

		if *i.ContentLength != numBytes {
			t.Fatalf("Expected object size: %d but got: %d", numBytes, *i.ContentLength)
		}

		data, err := io.ReadAll(i.Body)

		if err != nil {
			t.Fatalf("Failed to read %d expected bytes from objects. Error: %v", *i.ContentLength, err)
		}

		if int64(len(data)) != *i.ContentLength {
			t.Fatalf("Failed to read %d expected bytes from objects. Error: %v", *i.ContentLength, err)
		}

		if *i.Tagging != tags {
			t.Fatalf("Expected tags: %s but got: %s", tags, *i.Tagging)
		}

		return in
	}

	svc := NewMockS3Client(handler)

	err := Put(context.Background(), svc, "b", "k1", tags, numBytes, map[string]*string{})

	if err != nil {
		t.Fatalf("Failed PUT operation with error: %v", err)
	}
}

func (m *MockS3Client) PutObjectTagging(in *s3.PutObjectTaggingInput) (*s3.PutObjectTaggingOutput, error) {
	m.S3OpHandler(in)

	return &s3.PutObjectTaggingOutput{}, nil
}

func TestPutTaggingOp(t *testing.T) {
	tags := "tag1=blue&tag2=red"

	handler := func(in interface{}) interface{} {
		i := in.(*s3.PutObjectTaggingInput)

		if *i.Bucket != "b" {
			t.Fatalf("Expected bucket: %s but got: %s", "b", *i.Bucket)
		}

		if *i.Key != "k1" {
			t.Fatalf("Expected key: %s but got: %s", "k1", *i.Key)
		}

		if i.Tagging.String() != parseTags(tags).String() {
			t.Fatalf("Expected tags: %s but got: %s", tags, *i.Tagging)
		}

		return in
	}

	svc := NewMockS3Client(handler)

	err := PutTagging(svc, "b", "k1", tags)

	if err != nil {
		t.Fatalf("Failed PUT with tags operation with error: %v", err)
	}
}

func (m *MockS3Client) CopyObject(in *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	m.S3OpHandler(in)

	return &s3.CopyObjectOutput{}, nil
}

func TestUpdateMetadataOp(t *testing.T) {
	v1, v2, v3 := "val1", "val2", "val3"
	metadata := map[string]*string{"attribute1": &v1, "attribute2": &v2, "attribute3": &v3}

	handler := func(in interface{}) interface{} {
		i := in.(*s3.CopyObjectInput)

		if *i.Bucket != "b" {
			t.Fatalf("Expected bucket: %s but got: %s", "b", *i.Bucket)
		}

		if *i.Key != "k1" {
			t.Fatalf("Expected key: %s but got: %s", "k1", *i.Key)
		}

		if *i.MetadataDirective != directiveReplace {
			t.Fatalf("For metadata updates metadata directive must be REPLACE but got: %s", *i.MetadataDirective)
		}

		for k, v := range metadata {
			if *i.Metadata[k] != *v {
				t.Fatalf("Expected meta key/value pair: %s %s but got: %s %s", k, *v, k, *i.Metadata[k])
			}
		}

		return in
	}

	svc := NewMockS3Client(handler)

	err := UpdateMetadata(svc, "b", "k1", metadata)

	if err != nil {
		t.Fatalf("Failed update metadata operation with error: %v", err)
	}
}

func TestCopyOperation(t *testing.T) {
	metadataVal1, metadataVal2 := "val1", "val2"
	testData := []struct {
		copySourceBucket  string
		destinationBucket string
		objectKey         string
		copySource        string
		taggingDirective  string
		tags              string
		metadataDirective string
		metadata          map[string]*string
	}{
		// No tag, No metadata
		{copySourceBucket: "cs", destinationBucket: "b", objectKey: "k", copySource: "cs/k",
			taggingDirective: directiveCopy, tags: "", metadataDirective: directiveCopy, metadata: nil},

		// Only metadata
		{copySourceBucket: "cs", destinationBucket: "b", objectKey: "k", copySource: "cs/k",
			taggingDirective: directiveCopy, tags: "",
			metadataDirective: directiveReplace, metadata: map[string]*string{"key1": &metadataVal1, "key2": &metadataVal2}},

		// Only tag
		{copySourceBucket: "cs", destinationBucket: "b", objectKey: "k", copySource: "cs/k",
			taggingDirective: directiveReplace, tags: "tag1=blue&tag2=red", metadataDirective: directiveCopy, metadata: nil},

		// Both tag and metadata
		{copySourceBucket: "cs", destinationBucket: "b", objectKey: "k", copySource: "cs/k",
			taggingDirective: directiveReplace, tags: "tag1=blue&tag2=red",
			metadataDirective: directiveReplace, metadata: map[string]*string{"key1": &metadataVal1, "key2": &metadataVal2}},
	}

	for i, test := range testData {
		testcase := fmt.Sprintf("%d/tag: %s, metadata: %s", i, test.taggingDirective, test.metadataDirective)
		t.Run(testcase, func(t *testing.T) {
			handler := func(in interface{}) interface{} {
				i := in.(*s3.CopyObjectInput)
				if *i.CopySource != test.copySource {
					t.Errorf("Expected copySource: %s but got: %s", test.copySource, *i.CopySource)
				}

				if *i.Bucket != test.destinationBucket {
					t.Errorf("Expected bucket: %s but got: %s", test.destinationBucket, *i.Bucket)
				}

				if *i.Key != test.objectKey {
					t.Errorf("Expected key: %s but got: %s", test.objectKey, *i.Key)
				}

				if *i.TaggingDirective != test.taggingDirective {
					t.Errorf("Expected taggingDirective: %s but got: %s", test.taggingDirective, *i.TaggingDirective)
				}
				if *i.Tagging != test.tags {
					t.Errorf("Expected tags: %s but got: %s", test.tags, *i.Tagging)
				}

				if *i.MetadataDirective != test.metadataDirective {
					t.Errorf("Expected metadataDirective: %s but got: %s", test.metadataDirective, *i.MetadataDirective)
				}
				for k, v := range test.metadata {
					if *i.Metadata[k] != *v {
						t.Errorf("Expected meta key/value pair: %s %s but got: %s %s", k, *v, k, *i.Metadata[k])
					}
				}

				return in
			}

			svc := NewMockS3Client(handler)

			err := Copy(svc, test.copySourceBucket, test.destinationBucket, test.objectKey,
				test.tags, test.taggingDirective, test.metadata, test.metadataDirective)

			if err != nil {
				t.Errorf("Failed COPY operation with error: %v", err)
			}

		})
	}
}
func (m *MockS3Client) HeadObject(in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	m.S3OpHandler(in)

	return &s3.HeadObjectOutput{}, nil
}

func TestHeadObjectOp(t *testing.T) {
	handler := func(in interface{}) interface{} {
		i := in.(*s3.HeadObjectInput)

		if *i.Bucket != "b" {
			t.Fatalf("Expected bucket: %s but got: %s", "b", *i.Bucket)
		}

		if *i.Key != "k1" {
			t.Fatalf("Expected key: %s but got: %s", "k1", *i.Key)
		}

		return in
	}

	svc := NewMockS3Client(handler)

	err := Head(svc, "b", "k1")

	if err != nil {
		t.Fatalf("Failed HEAD operation with error: %v", err)
	}
}

func (m *MockS3Client) DeleteObject(in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	m.S3OpHandler(in)

	return &s3.DeleteObjectOutput{}, nil
}

func TestDeleteObjectOp(t *testing.T) {
	handler := func(in interface{}) interface{} {
		i := in.(*s3.DeleteObjectInput)

		if *i.Bucket != "b" {
			t.Fatalf("Expected bucket: %s but got: %s", "b", *i.Bucket)
		}

		if *i.Key != "k1" {
			t.Fatalf("Expected key: %s but got: %s", "k1", *i.Key)
		}

		return in
	}

	svc := NewMockS3Client(handler)

	err := Delete(svc, "b", "k1")

	if err != nil {
		t.Fatalf("Failed DELETE operation with error: %v", err)
	}
}

func TestParseMetadataString(t *testing.T) {
	validString := []string{"k1=v1", "ColfaX3.0-#@our=blue", "a=0&b=1&b=3&fajng47q^($%_02hg=()**#!%CLAr3"}

	for _, m := range validString {
		parseMetadataString(m)
	}
}

func (m *MockS3Client) CreateMultipartUploadWithContext(ctx context.Context, in *s3.CreateMultipartUploadInput, opts ...request.Option) (*s3.CreateMultipartUploadOutput, error) {
	m.S3OpHandler(in)
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String("VXBsb2FkIElEIGZvciA2aWWpbmcncyBteS1tb3ZpZS5tMnRzIHVwbG9hZA")}, nil
}

func (m *MockS3Client) UploadPartWithContext(ctx context.Context, in *s3.UploadPartInput, opts ...request.Option) (*s3.UploadPartOutput, error) {
	m.S3OpHandler(in)

	eTag := ""
	return &s3.UploadPartOutput{ETag: &eTag}, nil
}

func (m *MockS3Client) CompleteMultipartUploadWithContext(ctx context.Context, in *s3.CompleteMultipartUploadInput, opts ...request.Option) (*s3.CompleteMultipartUploadOutput, error) {
	m.S3OpHandler(in)

	return &s3.CompleteMultipartUploadOutput{}, nil
}

func TestMultipartPutWithTagging(t *testing.T) {
	tags := "tag1=blue&tag2=red"

	handler := func(in interface{}) interface{} {
		fmt.Print(in)
		i, ok := in.(*s3.CreateMultipartUploadInput)
		if !ok {
			return in
		}

		if *i.Bucket != "b" {
			t.Fatalf("Expected bucket: %s but got: %s", "b", *i.Bucket)
		}

		if *i.Key != "k1" {
			t.Fatalf("Expected key: %s but got: %s", "k1", *i.Key)
		}

		if *i.Tagging != tags {
			t.Fatalf("Expected tags: %s but got: %s", tags, *i.Tagging)
		}

		if len(i.Metadata) != 0 {
			t.Fatalf("Expected empty metadata but got metadata with length: %d", len(i.Metadata))
		}

		return in
	}

	svc := NewMockS3Client(handler)

	mockSysInterruptHandler := NewSyscallParams(*NewParameters())

	err := MultipartPut(context.Background(), svc, "b", "k1", 1, 1, tags, map[string]*string{}, mockSysInterruptHandler)

	if err != nil {
		t.Fatalf("Failed MultipartPut operation with error: %v", err)
	}
}
