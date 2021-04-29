package main

import (
	"io"
	"testing"
)

func TestRead(t *testing.T) {
	data := "hello"
	d := NewDummyReader(int64(len(data)), data)

	// The length of the buffer indicates how many bytes we want back from calling Read
	buff := make([]byte, len(data))

	bytesRead, err := d.Read(buff)

	if err != nil {
		t.Fatalf("expected no error but got %s", err)
	}

	if bytesRead != len(data) {
		t.Fatalf("expected to read %d bytes but got %d", len(data), bytesRead)
	}

	if string(buff[:bytesRead]) != data {
		t.Fatalf("expected to read %s bytes but got %s", data, string(buff))
	}

	// Try to read more data than available.
	buff = make([]byte, len(data)+1)

	d.Seek(0, io.SeekStart)
	bytesRead, err = d.Read(buff)

	if err != nil {
		t.Fatalf("expected nil but got %s", err)
	}

	if bytesRead != len(data) {
		t.Fatalf("expected to read %d bytes but got %d", len(data), bytesRead)
	}

	if string(buff[:bytesRead]) != data {
		t.Fatalf("expected to read %s bytes but got %s", data, string(buff))
	}
}

// TestReadEOF ensures we get EOF when reading past the end.
func TestReadEOF(t *testing.T) {
	data := "end"
	d := NewDummyReader(int64(len(data)), data)

	// The length of the buffer indicates how many bytes we want back from calling Read
	buff := make([]byte, len(data))

	bytesRead, err := d.Read(buff)
	bytesRead, err = d.Read(buff)

	if err != io.EOF && bytesRead != 0 {
		t.Fatalf("expected no error but got %s", err)
	}
}

// TestReadMultipleBlocks reads multiple blocks of Data
func TestReadMultipleBlocks(t *testing.T) {
	data := "block"
	d := NewDummyReader(2*int64(len(data)), data)

	// The length of the buffer indicates how many bytes we want back from calling Read
	buff := make([]byte, 2*len(data))

	bytesRead, err := d.Read(buff)

	if err != nil {
		t.Fatalf("expected no error but got %s", err)
	}

	if bytesRead != 2*len(data) {
		t.Fatalf("expected to read %d bytes but got %d", len(data), bytesRead)
	}

	if string(buff[:bytesRead]) != "blockblock" {
		t.Fatalf("expected to read %s bytes but got %s", data, string(buff))
	}

}

// TestReadMultipleUnalignedBlocks reads multiple unaligned blocks of Data
func TestReadMultipleUnalignedBlocks(t *testing.T) {
	data := "abc"
	d := NewDummyReader(3*int64(len(data)), data)

	// The length of the buffer indicates how many bytes we want back from calling Read
	buff := make([]byte, 2)
	res := make([]byte, 0, 9)

	for i := 0; i < 5; i++ {
		bytesRead, err := d.Read(buff)
		res = append(res, buff[:bytesRead]...)

		if err != nil {
			t.Fatalf("expected no error but got %s", err)
		}
	}

	if string(res) != "abcabcabc" {
		t.Fatalf("expected to read %s bytes but got %s", "abcabcabc", string(res))
	}
}

func TestGenerateData(t *testing.T) {
	dataBlock := string(generateDataFromKey("abc", 0))
	expected := ""
	if dataBlock != expected {
		t.Fatalf("expected %s but got %s", expected, dataBlock)
	}

	dataBlock = string(generateDataFromKey("hello", 1))
	expected = "h"
	if dataBlock != expected {
		t.Fatalf("expected %s but got %s", expected, dataBlock)
	}

	dataBlock = string(generateDataFromKey("turkey", 6))
	expected = "turkey"
	if dataBlock != expected {
		t.Fatalf("expected %s but got %s", expected, dataBlock)
	}

	dataBlock = string(generateDataFromKey("cran", 10))
	expected = "crancrancr"
	if dataBlock != expected {
		t.Fatalf("expected %s but got %s", expected, dataBlock)
	}
}

func BenchmarkGenerateData(b *testing.B) {
	for n := 0; n < b.N; n++ {
		generateDataFromKey("object-1-key", 4096)
	}
}

// BenchmarkReadData reads 1MiB on every iteration
func BenchmarkReadData(b *testing.B) {
	var size int64 = 1024 * 1024

	d := NewDummyReader(size, "test-object-1meg")
	buff := make([]byte, size)
	for n := 0; n < b.N; n++ {
		d.Read(buff)
		d.Seek(0, io.SeekStart)
	}
}
