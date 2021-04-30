package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
)

// For performance reasons we need to generate data in blocks as opposed to one character at a time. This is especially true
// for large objects.
//
// This MUST be a power of two to allow for fast modulo optimizations.
const objectDataBlockSize = 4096

// DummyReader implements the io.ReadSeeker interface, returning data that simply repeats the value of 'data' until the size is met
type DummyReader struct {
	size   int64
	offset int64
	data   *bytes.Reader
}

// NewDummyReader constructs an io.ReadSeeker compatible instance that will repeat the 'seed' string until 'size' is met
func NewDummyReader(size int64, seed string) *DummyReader {
	d := DummyReader{size: size}
	data := generateDataFromKey(seed, objectDataBlockSize)
	d.data = bytes.NewReader(data)

	return &d
}

// Size returns the total size of the data that DummyReader represents
func (r *DummyReader) Size() int64 {
	return r.size
}

// Read reads the data in the given buffer and returns the number of bytes read
func (r *DummyReader) Read(p []byte) (n int, err error) {
	dataLength := r.data.Size()

	if dataLength == 0 {
		n, err = 0, errors.New("Data needs to be set before reading")
		return
	}

	if r.offset >= r.size {
		n, err = 0, io.EOF
		return
	}

	bufferLength := len(p)
	read := int(r.size - r.offset)
	if bufferLength < read {
		read = bufferLength
	}

	// This code runs very frequently when doing large object puts so we need to keep it fast and cheap.
	// We try to do that here by reading in blocks and using copy to move larger pieces of memory in a single
	// call as opposed to the naive approach of copying one byte in each iteration.
	bytesTransferred := 0
	for i := 0; i < read; i += bytesTransferred {
		bytesTransferred, err = r.data.Read(p[i:read])
		if err != nil && err != io.EOF {
			return 0, err
		}

		if r.data.Len() == 0 {
			r.data.Seek(0, io.SeekStart)
		}
	}

	r.offset += int64(read)

	return read, nil
}

// Seek updates the offset of the DummyReader based on a given offset and position
func (r *DummyReader) Seek(offset int64, whence int) (int64, error) {
	updateDummyDataOffset := func() error {
		if r.data != nil {
			_, err := r.data.Seek(r.offset%r.data.Size(), io.SeekStart)
			return err
		}
		return nil
	}

	switch whence {
	case io.SeekStart:
		if offset >= 0 && offset <= r.size {
			r.offset = offset
			err := updateDummyDataOffset()
			return r.offset, err
		}
		return r.offset, fmt.Errorf("SeekStart: Cannot seek past start or end of file. offset: %d, size: %d", offset, r.size)
	case io.SeekCurrent:
		off := offset + r.offset
		if off >= 0 && off <= r.size {
			r.offset = off
			err := updateDummyDataOffset()
			return off, err
		}
		return r.offset, fmt.Errorf("SeekCurrent: Cannot seek past start or end of file. offset: %d, size: %d", off, r.size)
	case io.SeekEnd:
		off := r.size - offset
		if off >= 0 && off <= r.size {
			r.offset = off
			err := updateDummyDataOffset()
			return off, err
		}
		return r.offset, fmt.Errorf("SeekEnd: Cannot seek past start or end of file. offset: %d, size: %d", off, r.size)
	}
	return 0, errors.New("Invalid value of whence")
}

// generateDataFromKey is an efficient way to generate data for objects we write to s3. Ideally
// this data is different for each object. This generates a block of data based
// on the key passed in.
func generateDataFromKey(key string, numBytes int) []byte {
	keylen := len(key)

	if keylen >= numBytes {
		return []byte(key[:numBytes])
	}

	data := make([]byte, 0, numBytes)

	repeat := numBytes / keylen
	data = append(data, []byte(strings.Repeat(key, repeat))...)

	// Generate the remaining substring < keylen
	remainder := key[:numBytes%keylen]
	data = append(data, []byte(remainder)...)

	return data
}
