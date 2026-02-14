package main

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	directiveCopy    = "COPY"
	directiveReplace = "REPLACE"
)

var (
	rangeStartExp = regexp.MustCompile(`=(\d+)`)
)

type S3API interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	PutObjectTagging(context.Context, *s3.PutObjectTaggingInput, ...func(*s3.Options)) (*s3.PutObjectTaggingOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	RestoreObject(context.Context, *s3.RestoreObjectInput, ...func(*s3.Options)) (*s3.RestoreObjectOutput, error)
}

// Options sends an http 'options' request to the specific endpoint
func Options(client *http.Client, endpoint string) error {
	req, err := http.NewRequest("OPTIONS", endpoint+"/", nil)
	if err != nil {
		log.Printf("Creating OPTIONS request failed: %v", err)
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("OPTIONS request failed: %v", err)
		return err
	}
	if resp != nil {
		defer resp.Body.Close()
	}

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		log.Printf("OPTIONS request failed to read body: %v", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Options failed with status code: %d", resp.StatusCode)
	}

	return nil
}

// Put performs an S3 PUT to the given bucket and key using the supplied configuration
func Put(ctx context.Context, svc S3API, bucket, key, tagging string, size int64, metadata map[string]string) error {
	obj := NewDummyReader(size, key)

	contentMD5, err := encodeMD5(obj)
	if err != nil {
		return fmt.Errorf("Calculating MD5 failed for multipart object bucket: %s, key: %s, err: %v", bucket, key, err)
	}

	params := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		ContentLength: &size,
		ContentMD5:    aws.String(contentMD5),
		Body:          obj,
		Metadata:      metadata,
	}

	if tagging != "" {
		params.Tagging = aws.String(tagging)
	}

	_, err = svc.PutObject(ctx, params)

	return err
}

// Copy performs an S3 PUT-Copy which copies the S3 object specified by the supplied key from copySourceBucket into destinationBucket
func Copy(ctx context.Context, svc S3API, copySourceBucket string, destinationBucket string, objectKey string,
	tagging string, taggingDirective string, metadata map[string]string, metadataDirective string) error {

	var taggingDirectiveEnum types.TaggingDirective
	switch strings.ToUpper(taggingDirective) {
	case "COPY":
		taggingDirectiveEnum = types.TaggingDirectiveCopy
	case "REPLACE":
		taggingDirectiveEnum = types.TaggingDirectiveReplace
	default:
		return fmt.Errorf("invalid tagging directive: %s", taggingDirective)
	}

	var metadataDirectiveEnum types.MetadataDirective
	switch strings.ToUpper(metadataDirective) {
	case "COPY":
		metadataDirectiveEnum = types.MetadataDirectiveCopy
	case "REPLACE":
		metadataDirectiveEnum = types.MetadataDirectiveReplace
	default:
		return fmt.Errorf("invalid metadata directive: %s", metadataDirective)
	}

	params := &s3.CopyObjectInput{
		Bucket:            aws.String(destinationBucket),
		Key:               aws.String(objectKey),
		CopySource:        aws.String(fmt.Sprintf("%s/%s", copySourceBucket, objectKey)),
		Tagging:           aws.String(tagging),
		TaggingDirective:  taggingDirectiveEnum,
		Metadata:          metadata,
		MetadataDirective: metadataDirectiveEnum,
	}

	_, err := svc.CopyObject(ctx, params)

	return err
}

func parseTags(tags string) types.Tagging {
	var s3Tags types.Tagging
	s3Tags.TagSet = make([]types.Tag, 0)

	if tags != "" {
		// tags are supplied like so: tag1=value2&tag2=value&...
		pairs := strings.Split(tags, "&")
		for index := range pairs {
			keyvalue := strings.Split(pairs[index], "=")
			if len(keyvalue) != 2 {
				log.Fatal("Invalid tagging string supplied. Must be formatted like: 'tag1=value1&tag2=value2...'")
			}
			t := types.Tag{
				Key:   aws.String(keyvalue[0]),
				Value: aws.String(keyvalue[1]),
			}
			s3Tags.TagSet = append(s3Tags.TagSet, t)
		}
	}

	return s3Tags
}

// PutTagging updates the S3 tagset for the given S3 object specified by the supplied bucket and key
func PutTagging(ctx context.Context, svc S3API, bucket, key, tagging string) error {
	tags := parseTags(tagging)

	params := &s3.PutObjectTaggingInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		Tagging: &tags,
	}
	_, err := svc.PutObjectTagging(ctx, params)

	return err
}

// UpdateMetadata updates the S3 user metadata for the given S3 object specified by the supplied bucket and key
func UpdateMetadata(ctx context.Context, svc S3API, bucket string, key string, metadata map[string]string) error {
	return Copy(ctx, svc, bucket, bucket, key, "", directiveCopy, metadata, directiveReplace)
}

// encodeHash calculates a Hash (MD5/SHA-256) for data read from the reader and encodes it
func encodeHash(r *DummyReader, h hash.Hash, encode func(src []byte) string) (hash string, err error) {
	if _, err = r.Seek(0, io.SeekStart); err != nil {
		return
	}

	defer func() {
		_, err = r.Seek(0, io.SeekStart)
	}()

	if _, err = io.Copy(h, r); err != nil {
		return
	}

	hash = encode(h.Sum(nil))
	return
}

// encodeMD5 calculates an MD5 hash for data read from the reader and encodes it
func encodeMD5(r *DummyReader) (string, error) {
	return encodeHash(r, md5.New(), base64.StdEncoding.EncodeToString)
}

// encodeSHA256 calculates a SHA-256 hash for data read from the reader and encodes it
func encodeSHA256(r *DummyReader) (string, error) {
	return encodeHash(r, sha256.New(), hex.EncodeToString)
}

// MultipartPut initiates an S3 multipart upload for the given S3 object specified by the supplied bucket and key
func MultipartPut(ctx context.Context, svc S3API, bucket, key string, size, partSize int64, tagging string, metadata map[string]string, sysInterruptHandler SyscallHandler) error {
	// Because the object is uploaded in parts we need to generate part sized objects.
	obj := NewDummyReader(partSize, key)

	params := &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		Metadata: metadata,
	}

	if tagging != "" {
		params.Tagging = aws.String(tagging)
	}

	numParts := int64(math.Ceil(float64(size) / float64(partSize)))

	// this is for if the last part won't be the same size
	lastobj := obj
	if numParts != size/partSize {
		lastobj = NewDummyReader(size-partSize*(numParts-1), key)
	}

	output, err := svc.CreateMultipartUpload(ctx, params)
	if err != nil {
		return err
	}

	uploadID := output.UploadId

	// log in-progress multipart upload so it can be aborted if a system interrupt occurs
	sysInterruptHandler.addMultipartUpload(key, bucket, *uploadID)

	defer func() {
		if err != nil {
			aparams := &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: uploadID,
			}
			if _, aerr := svc.AbortMultipartUpload(ctx, aparams); aerr != nil {
				log.Printf("Failed to abort multipart upload %v/%v %v: %v", bucket, key, uploadID, aerr)
				return
			}
		}
		sysInterruptHandler.doneMultipartUpload(key, bucket, *uploadID)
	}()

	contentMD5, err := encodeMD5(obj)
	if err != nil {
		return fmt.Errorf("Calculating MD5 failed for multipart object bucket: %s, key: %s, err: %v", bucket, key, err)
	}
	uparams := &s3.UploadPartInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		ContentLength: &partSize,
		ContentMD5:    aws.String(contentMD5),
		Body:          obj,
		UploadId:      uploadID,
	}

	contentSHA256, err := encodeSHA256(obj)
	if err != nil {
		return fmt.Errorf("Calculating SHA-256 failed for multipart object bucket: %s, key: %s, err: %v", bucket, key, err)
	}

	partdata := make([]types.CompletedPart, 0, numParts)
	for partnum := int64(1); partnum <= numParts-1; partnum++ {
		// In a more realistic scenario we would want to upload parts concurrently, but concurrency is already one of the test
		// options... might want to figure out if/how this should consider the concurrency option before adding concurrency here.
		uparams.PartNumber = aws.Int32(int32(partnum))

		var uoutput *s3.UploadPartOutput
		if uoutput, err = svc.UploadPart(ctx, uparams, func(o *s3.Options) {
			o.APIOptions = append(o.APIOptions, AddCustomHeader("X-Amz-Content-Sha256", contentSHA256))
		}); err != nil {
			return err
		}

		part := types.CompletedPart{}
		part.PartNumber = aws.Int32(int32(partnum))
		part.ETag = uoutput.ETag
		partdata = append(partdata, part)
		// We have to reset the object, since it is re-used for all
		// parts and its size is set to be equal to that of a single part.
		// If we don't reset the offset the second part read will get an EOF and have
		// a zero byte body.
		if _, err = obj.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("Resetting object failed while uploading parts %v/%v %v: %v", bucket, key, uploadID, err)
		}
	}

	uparams.Body = lastobj
	uparams.ContentLength = aws.Int64(lastobj.Size())
	uparams.PartNumber = aws.Int32(int32(numParts))

	if contentMD5, err = encodeMD5(lastobj); err != nil {
		return fmt.Errorf("Calculating MD5 failed for the last part of multipart object bucket: %s, key: %s, err: %v", bucket, key, err)
	}
	uparams.ContentMD5 = aws.String(contentMD5)

	if contentSHA256, err = encodeSHA256(lastobj); err != nil {
		return fmt.Errorf("Calculating SHA-256 failed for the last part of multipart object bucket: %s, key: %s, err: %v", bucket, key, err)
	}

	uoutput, err := svc.UploadPart(ctx, uparams, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, AddCustomHeader("X-Amz-Content-Sha256", contentSHA256))
	})
	if err != nil {
		return err
	}
	part := types.CompletedPart{}
	part.PartNumber = aws.Int32(int32(numParts))
	part.ETag = uoutput.ETag
	partdata = append(partdata, part)

	cparams := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: partdata,
		},
	}

	_, err = svc.CompleteMultipartUpload(ctx, cparams)
	return err
}

// Get performs an S3 GET for the S3 object specified by the supplied bucket and key
func Get(ctx context.Context, svc S3API, bucket, key, byteRange string, size int64, verify int, partSize int64) (int64, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	if byteRange != "" {
		params.Range = aws.String(byteRange)
	}

	out, err := identityGetObject(ctx, svc, params, verify, partSize, size)
	if err != nil {
		return 0, err
	}

	return *out.ContentLength, err
}

// Head performs an S3 HEAD for the S3 object specified by the supplied bucket and key
func Head(ctx context.Context, svc S3API, bucket, key string) error {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err := svc.HeadObject(ctx, params)

	return err
}

// Delete performs an S3 delete for the S3 object specified by the supplied bucket and key
func Delete(ctx context.Context, svc S3API, bucket, key string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	_, err := svc.DeleteObject(ctx, params)

	return err
}

func parseMetadataString(metaString string) map[string]string {
	meta := make(map[string]string)
	if metaString != "" {
		// metadata is supplied like so: key1=value2&key2=value2&...
		pairs := strings.Split(metaString, "&")
		for index := range pairs {
			keyvalue := strings.Split(pairs[index], "=")
			if len(keyvalue) != 2 {
				log.Fatalf("Invalid metadata string supplied: %s. Format must be: 'key1=value1&key2=value2...'", metaString)
			}
			meta[keyvalue[0]] = keyvalue[1]
		}
	}
	return meta
}

// identityGetObject retrieves objects from an Amazon S3 HTTP interface
func identityGetObject(ctx context.Context, c S3API, input *s3.GetObjectInput, verify int, partsize int64, size int64) (output *s3.GetObjectOutput, err error) {
	output, err = c.GetObject(ctx, input, func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, AddCustomHeader("Accept-Encoding", "identity"))
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		io.Copy(io.Discard, output.Body)
		output.Body.Close()
	}()

	if verify == 0 {
		_, err = io.Copy(io.Discard, output.Body)
		if err != nil {
			err = fmt.Errorf("error while reading body of %s/%s: %w", aws.ToString(input.Bucket), aws.ToString(input.Key), err)
		}
	} else {
		err = verifyGetData(output.Body, input, verify, partsize, size, *output.ContentLength)
	}
	return output, err
}

func verifyGetData(body io.ReadCloser, input *s3.GetObjectInput, verify int, partsize int64, size int64, contentLength int64) error {
	key := []byte(aws.ToString(input.Key))
	buffer := make([]byte, 1024)
	index := 0
	var read int
	var readError error
	keylen := len(key)

	// check that the response length matches the expected object size before reading
	if size != contentLength {
		return fmt.Errorf("expected data with length=%d, but retrieved data with length=%d", size, contentLength)
	}

	// get the starting index for range reads
	rangeHeader := ""
	if input.Range != nil {
		rangeHeader = aws.ToString(input.Range)
	}
	if rangeHeader != "" {
		var err error
		index, err = parseRange(rangeHeader)
		if err != nil {
			return err
		}
		if verify == 2 {
			index = index % int(partsize)
		}
	}

	// keep reading until we reach EOF (or some other error)
	for readError == nil {
		read, readError = body.Read(buffer)
		for i := 0; i < read; i++ {
			//deal with the retrieved data that comes from multipartput data, which repeat every partsize bytes
			if verify == 2 && int64(index) == partsize {
				index = 0
			}

			// Due to the performance optimizations for generating object data in generateDataFromKey the offset when validating the data
			// on the read path needs to be modulo the block size passed to generateDataFromKey. This is because the keys can get cut off
			// at block boundaries and start at the first character at the beginning of a new block. So for a key "abcd" with a block size
			// of 3, and a 9 byte object we get "abc|abc|abc" (| are block boundaries) instead of fully repeating keys "abcdabcda" which was the previous behavior.
			//
			// This uses a modulo optimization for powers of 2. To ge the modulo some value x if x is a power of two you can use
			// val & (x-1). In this case we are taking modulo objectDataBlockSize.
			//
			// We can further optimize this call by dealing with larger blocks as opposed to single characters but it's probably not worth it right now
			// since this is a special non-performance path that validates all data read.
			offset := (index & (objectDataBlockSize - 1)) % keylen

			if buffer[i] != key[offset] {
				return errors.New("Retrieved data different from expected")
			}
			index++
		}
	}

	if readError != io.EOF {
		return readError
	}
	return nil
}

// parseRange extracts the start of range (required) from an HTTP Range header
func parseRange(s string) (int, error) {
	matches := rangeStartExp.FindStringSubmatch(s)
	if len(matches) != 2 {
		return 0, fmt.Errorf("Range %q does not match required format", s)
	}
	return strconv.Atoi(matches[1])
}

// RestoreObject restores an archived copy of an object back into Amazon S3
func RestoreObject(ctx context.Context, svc S3API, bucket string, key string, tier string, days int64) error {
	var tierType types.Tier
	switch strings.ToLower(tier) {
	case "standard":
		tierType = types.TierStandard
	case "bulk":
		tierType = types.TierBulk
	case "expedited":
		tierType = types.TierExpedited
	default:
		return errors.New("invalid tier value given when restoring object: " + tier)
	}

	params := &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		RestoreRequest: &types.RestoreRequest{
			Days:                 aws.Int32(int32(days)),
			GlacierJobParameters: &types.GlacierJobParameters{Tier: tierType},
		},
	}

	_, err := svc.RestoreObject(ctx, params)

	return err
}

// DispatchOperation performs an S3 request based on the supplied arguments
func DispatchOperation(ctx context.Context, svc S3API, client *http.Client, op, keyName string, args *Parameters, r *Result, randMax int64, sysInterruptHandler SyscallHandler, debug bool) error {
	var err error

	switch op {
	case "options":
		err = Options(client, r.Endpoint)
	case "put":
		if err = Put(ctx, svc, args.Bucket, keyName, args.Tagging, int64(args.Size), parseMetadataString(args.Metadata)); err == nil {
			r.TotalObjectSize += int64(args.Size)
		}
	case "puttagging":
		err = PutTagging(ctx, svc, args.Bucket, keyName, args.Tagging)
	case "updatemeta":
		err = UpdateMetadata(ctx, svc, args.Bucket, keyName, parseMetadataString(args.Metadata))
	case "multipartput":
		if err = MultipartPut(ctx, svc, args.Bucket, keyName, int64(args.Size), int64(args.PartSize), args.Tagging, parseMetadataString(args.Metadata), sysInterruptHandler); err == nil {
			r.TotalObjectSize += int64(args.Size)
		}
	case "get":
		var retrievedBytes int64
		if retrievedBytes, err = Get(ctx, svc, args.Bucket, keyName, args.Range, int64(args.Size), args.Verify, int64(args.PartSize)); err == nil {
			r.TotalObjectSize += retrievedBytes
		}
	case "head":
		err = Head(ctx, svc, args.Bucket, keyName)
	case "delete":
		err = Delete(ctx, svc, args.Bucket, keyName)
	case "randget":
		var objnum int64
		if randMax <= 0 {
			objnum = 0
		} else {
			objnum = rand.Int63n(randMax)
		}

		key := args.Prefix + "-" + strconv.FormatInt(objnum, 10)
		var retrievedBytes int64
		if retrievedBytes, err = Get(ctx, svc, args.Bucket, key, args.Range, int64(args.Size), args.Verify, int64(args.PartSize)); err == nil {
			r.TotalObjectSize += retrievedBytes
		}
	case "restore":
		err = RestoreObject(ctx, svc, args.Bucket, keyName, args.Tier, args.Days)
	case "copy":
		err = Copy(ctx, svc, args.CopySourceBucket, args.Bucket, keyName, args.Tagging, args.TaggingDirective,
			parseMetadataString(args.Metadata), args.MetadataDirective)
	}
	return err
}
