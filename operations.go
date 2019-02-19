package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

func Options(hclient *http.Client, endpoint string) error {
	req, err := http.NewRequest("OPTIONS", endpoint+"/", nil)
	if err != nil {
		log.Print("Creating OPTIONS request failed:", err)
		return err
	}

	resp, err := hclient.Do(req)
	if err != nil {
		log.Print("OPTIONS request failed:", err)
		return err
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	return nil
}

func Put(svc s3iface.S3API, bucket, key, tagging, storageClass string, size int64, metadata map[string]*string) error {
	obj := NewDummyReader(size, key)

	params := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		ContentLength: &size,
		Body:          obj,
		StorageClass:  &storageClass,
		Metadata:      metadata,
	}

	if tagging != "" {
		params.SetTagging(tagging)
	}

	_, err := svc.PutObject(params)

	return err
}

func parseTags(tags string) s3.Tagging {
	var s3Tags s3.Tagging
	s3Tags.TagSet = make([]*s3.Tag, 0)

	if tags != "" {
		// tags are supplied like so: tag1=value2&tag2=value&...
		pairs := strings.Split(tags, "&")
		for index := range pairs {
			keyvalue := strings.Split(pairs[index], "=")
			if len(keyvalue) != 2 {
				log.Fatal("Invalid tagging string supplied. Must be formatted like: 'tag1=value1&tage2=value2...'")
			}
			t := s3.Tag{
				Key:   aws.String(keyvalue[0]),
				Value: aws.String(keyvalue[1]),
			}
			s3Tags.TagSet = append(s3Tags.TagSet, &t)
		}
	}

	return s3Tags
}

func PutTagging(svc s3iface.S3API, bucket, key, tagging string) error {
	tags := parseTags(tagging)

	params := &s3.PutObjectTaggingInput{
		Bucket:  aws.String(bucket),
		Key:     aws.String(key),
		Tagging: &tags,
	}
	_, err := svc.PutObjectTagging(params)

	return err
}

func UpdateMetadata(svc s3iface.S3API, bucket, key string, metadata map[string]*string) error {
	params := &s3.CopyObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		CopySource:        aws.String(bucket + "/" + key),
		MetadataDirective: aws.String("REPLACE"),
		Metadata:          metadata,
	}
	_, err := svc.CopyObject(params)

	return err
}

func MultipartPut(svc s3iface.S3API, bucket, key, storageClass string, size, partSize int64, metadata map[string]*string) error {
	// Because the object is uploaded in parts we need to generate part sized objects.
	obj := NewDummyReader(partSize, key)

	params := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		StorageClass: &storageClass,
		Metadata:     metadata,
	}

	numparts := int64(math.Ceil(float64(size) / float64(partSize)))

	// this is for if the last part won't be the same size
	lastobj := obj
	if numparts != size/partSize {
		lastobj = NewDummyReader(size-partSize*(numparts-1), key)
	}

	var output *s3.CreateMultipartUploadOutput
	output, err := svc.CreateMultipartUpload(params)

	if err == nil {
		uploadId := output.UploadId
		partdata := make([]*s3.CompletedPart, 0, numparts)

		uparams := &s3.UploadPartInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(key),
			ContentLength: &partSize,
			Body:          obj,
			UploadId:      uploadId,
		}

		for partnum := int64(1); partnum <= numparts-1; partnum++ {
			/* In a more realistic scenario we would want to upload parts concurrently,
			   but concurrency is already one of the test options... might want to figure out
			   if/how this should consider the concurrency option before adding concurrency here.
			*/
			uparams.SetPartNumber(partnum)

			var uoutput *s3.UploadPartOutput
			uoutput, err = svc.UploadPart(uparams)
			if err != nil {
				break
			}
			part := &s3.CompletedPart{}
			part.SetPartNumber(partnum)
			part.SetETag(*uoutput.ETag)
			partdata = append(partdata, part)
			// We have to reset the object, since it is re-used for all
			// parts and its size is set to be equal to that of a single part.
			// If we don't reset the offset the second part read will get an EOF and have
			// a zero byte body.
			obj.Seek(0, io.SeekStart)
		}

		// Don't upload the last part if we had any part upload failures.
		if err == nil {
			uparams.SetBody(lastobj)
			uparams.SetContentLength(lastobj.Size())
			uparams.SetPartNumber(numparts)
			var uoutput *s3.UploadPartOutput
			uoutput, err = svc.UploadPart(uparams)
			if err == nil {
				part := &s3.CompletedPart{}
				part.SetPartNumber(numparts)
				part.SetETag(*uoutput.ETag)
				partdata = append(partdata, part)
			}
		}

		if err != nil {
			aparams := &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: uploadId,
			}
			svc.AbortMultipartUpload(aparams)
		} else {
			cparams := &s3.CompleteMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: uploadId,
			}

			cpartdata := &s3.CompletedMultipartUpload{Parts: partdata}
			cparams.SetMultipartUpload(cpartdata)

			_, err = svc.CompleteMultipartUpload(cparams)
		}
	}

	return err

}

func Get(svc s3iface.S3API, bucket, key, byteRange string, verify int, partSize int64) (int64, error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(byteRange),
	}

	out, err := identityGetObject(svc, params, verify, partSize)
	if err != nil {
		return 0, err
	}

	return *out.ContentLength, err
}

func Head(svc s3iface.S3API, bucket, key string) error {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	_, err := svc.HeadObject(params)

	return err
}

func Delete(svc s3iface.S3API, bucket, key string) error {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	_, err := svc.DeleteObject(params)

	return err
}

func parseMetadataString(metaString string) map[string]*string {
	meta := make(map[string]*string)
	if metaString != "" {
		// metadata is supplied like so: key1=value2&key2=value2&...
		pairs := strings.Split(metaString, "&")
		for index := range pairs {
			keyvalue := strings.Split(pairs[index], "=")
			if len(keyvalue) != 2 {
				log.Fatalf("Invalid metadata string supplied: %s. Format must be: 'key1=value1&key2=value2...'", metaString)
			}
			meta[keyvalue[0]] = &keyvalue[1]
		}
	}
	return meta
}

// Retrieves objects from Amazon S3.
func identityGetObject(c s3iface.S3API, input *s3.GetObjectInput, verify int, partsize int64) (output *s3.GetObjectOutput, err error) {
	req, out := c.GetObjectRequest(input)
	output = out
	req.HTTPRequest.Header.Set("Accept-Encoding", "identity")
	err = req.Send()
	if err == nil && req.HTTPResponse.Body != nil {
		if verify == 0 {
			_, err = io.Copy(ioutil.Discard, req.HTTPResponse.Body)
			if err != nil {
				err = fmt.Errorf("Error while reading body of %s/%s. %v", *input.Bucket, *input.Key, err)
			}
		} else {
			key := []byte(*input.Key)
			buffer := make([]byte, 1024)
			index := 0
			var read int
			var readError error = nil
			keylen := len(key)
			// keep reading until we reach EOF (or some other error)
		loop:
			for readError == nil {
				read, readError = req.HTTPResponse.Body.Read(buffer)
				for i := 0; i < read; i++ {
					//deal with the retrieved data that comes from multipartput data, which repeat every partsize bytes
					if verify == 2 && int64(index) == partsize {
						index = 0
					}

					// Due to the performance optimizations for generating object data in generateDataFromKey the offset when validating the data
					// on the read path needs to be modulo the block size passed to generateDataFromKey. This is because the keys can get cut off
					// at block boundaries and start at the first character at the beginning of a new block. So for a key "abcd" with a block size
					// of 3, and a 9 byte object we get "abc|abc|abc" (| are block boundaries) instead of fully repeating keys "abcdabcda" which was the previous behaviour.
					//
					// This uses a modulo optimization for powers of 2. To ge the modulo some value x if x is a power of two you can use
					// val & (x-1). In this case we are taking modulo objectDataBlockSize.
					//
					// We can further optimize this call by dealing with larger blocks as opposed to single characters but it's probably not worth it right now
					// since this is a special non-performance path that validates all data read.
					offset := (index & (objectDataBlockSize - 1)) % keylen

					if buffer[i] != key[offset] {
						readError = errors.New("Retrieved data different from expected")
						break loop
					}
					index++
				}
			}

			if readError != io.EOF {
				err = readError
			}
		}
		req.HTTPResponse.Body.Close()
	}
	return
}

func RestoreObject(svc s3iface.S3API, bucket string, key string, tier string, days int64) error {
	params := &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		RestoreRequest: &s3.RestoreRequest{
			Days:                 &days,
			GlacierJobParameters: &s3.GlacierJobParameters{Tier: aws.String(strings.Title(strings.ToLower(tier)))},
		},
	}

	_, err := svc.RestoreObject(params)

	return err
}

func DispatchOperation(svc s3iface.S3API, hclient *http.Client, op, keyName string, args *parameters, r *result, randMax int64) error {
	var err error

	sc := s3.StorageClassStandard
	if args.reducedRedundancy {
		sc = s3.StorageClassReducedRedundancy
	}

	switch op {
	case "options":
		if err = Options(hclient, r.Endpoint); err != nil {
			r.Failcount++
		}
	case "put":
		if err = Put(svc, args.bucketname, keyName, args.tagging, sc, args.osize, parseMetadataString(args.metadata)); err == nil {
			r.sumObjSize += args.osize
		}
	case "puttagging":
		err = PutTagging(svc, args.bucketname, keyName, args.tagging)
	case "updatemeta":
		err = UpdateMetadata(svc, args.bucketname, keyName, parseMetadataString(args.metadata))
	case "multipartput":
		if err = MultipartPut(svc, args.bucketname, keyName, sc, args.osize, args.partsize, parseMetadataString(args.metadata)); err == nil {
			r.sumObjSize += args.osize
		}
	case "get":
		var retrievedBytes int64
		if retrievedBytes, err = Get(svc, args.bucketname, keyName, args.objrange, args.verify, args.partsize); err == nil {
			r.sumObjSize += retrievedBytes
		}
	case "head":
		err = Head(svc, args.bucketname, keyName)
	case "delete":
		err = Delete(svc, args.bucketname, keyName)
	case "randget":
		var objnum int64
		if randMax <= 0 {
			objnum = 0
		} else {
			objnum = rand.Int63n(randMax)
		}

		key := args.objectprefix + "-" + strconv.FormatInt(objnum, 10)
		var retrievedBytes int64
		if retrievedBytes, err = Get(svc, args.bucketname, key, args.objrange, args.verify, args.partsize); err == nil {
			r.sumObjSize += retrievedBytes
		}
	case "restore":
		err = RestoreObject(svc, args.bucketname, keyName, args.tier, args.days)
	}
	return err
}
