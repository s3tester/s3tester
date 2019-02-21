package main

import (
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type mockS3Client struct {
	s3iface.S3API
	S3OpHandler func(interface{}) interface{}
}

func NewMockS3Client(handler func(interface{}) interface{}) *mockS3Client {
	return &mockS3Client{S3OpHandler: handler}
}

func (this *mockS3Client) PutObject(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	this.S3OpHandler(in)

	return &s3.PutObjectOutput{}, nil
}

func TestPutOp(t *testing.T) {
	var numBytes int64 = 100

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

		data, err := ioutil.ReadAll(i.Body)

		if err != nil {
			t.Fatalf("Failed to read %d expected bytes from objects. Error: %v", *i.ContentLength, err)
		}

		if int64(len(data)) != *i.ContentLength {
			t.Fatalf("Failed to read %d expected bytes from objects. Error: %v", *i.ContentLength, err)
		}

		return in
	}

	svc := NewMockS3Client(handler)

	Put(svc, "b", "k1", "", s3.StorageClassStandard, numBytes, map[string]*string{})
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

		data, err := ioutil.ReadAll(i.Body)

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

	err := Put(svc, "b", "k1", tags, s3.StorageClassStandard, numBytes, map[string]*string{})

	if err != nil {
		t.Fatalf("Failed PUT operation with error: %v", err)
	}
}

func (this *mockS3Client) PutObjectTagging(in *s3.PutObjectTaggingInput) (*s3.PutObjectTaggingOutput, error) {
	this.S3OpHandler(in)

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

func (this *mockS3Client) CopyObject(in *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	this.S3OpHandler(in)

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

		if *i.MetadataDirective != "REPLACE" {
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

func (this *mockS3Client) HeadObject(in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	this.S3OpHandler(in)

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

func (this *mockS3Client) DeleteObject(in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	this.S3OpHandler(in)

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
