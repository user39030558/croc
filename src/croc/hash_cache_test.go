package croc

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/utils"
)

func testHashCacheClient(t *testing.T) (*Client, *int) {
	t.Helper()
	calls := 0
	client := &Client{
		hashCache: &fileHashCache{dir: filepath.Join(t.TempDir(), "hash-cache")},
		stop:      newStop(nil),
	}
	client.stop.hash = func(name, algorithm string, showProgress ...bool) ([]byte, error) {
		calls++
		return utils.HashFile(name, algorithm, false)
	}
	return client, &calls
}

func TestSourceHashCacheReuseAndInvalidation(t *testing.T) {
	client, calls := testHashCacheClient(t)
	name := filepath.Join(t.TempDir(), "source.txt")
	initialTime := time.Unix(1_700_000_000, 123_000_000)
	if err := os.WriteFile(name, []byte("first-content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(name, initialTime, initialTime); err != nil {
		t.Fatal(err)
	}

	first, hit, err := client.hashSourceFile(name, "xxhash", false)
	if err != nil {
		t.Fatal(err)
	}
	if hit || *calls != 1 {
		t.Fatalf("first hash = (hit %v, calls %d), want (false, 1)", hit, *calls)
	}
	second, hit, err := client.hashSourceFile(name, "xxhash", false)
	if err != nil {
		t.Fatal(err)
	}
	if !hit || *calls != 1 || !bytes.Equal(first, second) {
		t.Fatalf("cached hash = (hit %v, calls %d, hash %x), want cached %x", hit, *calls, second, first)
	}

	if err := os.WriteFile(name, []byte("other-content"), 0o600); err != nil {
		t.Fatal(err)
	}
	changedTime := initialTime.Add(2 * time.Second)
	if err := os.Chtimes(name, changedTime, changedTime); err != nil {
		t.Fatal(err)
	}
	changed, hit, err := client.hashSourceFile(name, "xxhash", false)
	if err != nil {
		t.Fatal(err)
	}
	if hit || *calls != 2 {
		t.Fatalf("changed hash = (hit %v, calls %d), want (false, 2)", hit, *calls)
	}
	if bytes.Equal(first, changed) {
		t.Fatal("changed contents reused the old hash")
	}
}

func TestInterruptedReceiverFileIsNotMarkedVerified(t *testing.T) {
	client, calls := testHashCacheClient(t)
	directory := t.TempDir()
	destination := filepath.Join(directory, "received.bin")
	expectedFile := filepath.Join(directory, "expected.bin")
	expectedContents := []byte("complete-file-contents")
	partialContents := make([]byte, len(expectedContents))
	copy(partialContents, []byte("interrupted"))
	if err := os.WriteFile(destination, partialContents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(expectedFile, expectedContents, 0o600); err != nil {
		t.Fatal(err)
	}
	expected, err := utils.HashFile(expectedFile, "xxhash", false)
	if err != nil {
		t.Fatal(err)
	}

	actual, hit, err := client.hashReceiverFile(destination, "xxhash", expected, false)
	if err != nil {
		t.Fatal(err)
	}
	if hit || bytes.Equal(actual, expected) {
		t.Fatalf("partial receiver file = (hit %v, hash %x), want an uncached mismatch", hit, actual)
	}
	signature, err := signatureForFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := client.hashCache.lookup(signature, hashCacheVerifiedFile, "xxhash"); ok {
		t.Fatal("interrupted file was recorded as verified")
	}

	if err := os.WriteFile(destination, expectedContents, 0o600); err != nil {
		t.Fatal(err)
	}
	actual, hit, err = client.hashReceiverFile(destination, "xxhash", expected, false)
	if err != nil {
		t.Fatal(err)
	}
	if hit || !bytes.Equal(actual, expected) {
		t.Fatalf("completed receiver file = (hit %v, hash %x), want verified hash %x", hit, actual, expected)
	}
	client.rememberVerifiedReceiverFile(destination, "xxhash", expected)

	client.stop.hash = func(string, string, ...bool) ([]byte, error) {
		return nil, errors.New("file should not be rehashed")
	}
	actual, hit, err = client.hashReceiverFile(destination, "xxhash", expected, false)
	if err != nil {
		t.Fatal(err)
	}
	if !hit || !bytes.Equal(actual, expected) {
		t.Fatalf("verified receiver cache = (hit %v, hash %x), want cached %x", hit, actual, expected)
	}
	if *calls != 2 {
		t.Fatalf("receiver hash calls = %d, want 2", *calls)
	}
}
