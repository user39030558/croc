package croc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	log "github.com/schollz/croc/v11/src/logger"
)

const (
	fileHashCacheVersion  = 1
	hashCacheSource       = "source"
	hashCacheVerifiedFile = "verified-receiver"
)

type fileHashSignature struct {
	canonicalPath string
	size          int64
	modTime       int64
	mode          uint32
}

func signatureForFile(name string) (fileHashSignature, error) {
	absolute, err := filepath.Abs(name)
	if err != nil {
		return fileHashSignature{}, err
	}
	absolute = filepath.Clean(absolute)
	if runtime.GOOS == "windows" {
		absolute = strings.ToLower(absolute)
	}
	info, err := os.Lstat(name)
	if err != nil {
		return fileHashSignature{}, err
	}
	return fileHashSignature{
		canonicalPath: absolute,
		size:          info.Size(),
		modTime:       info.ModTime().UnixNano(),
		mode:          uint32(info.Mode()),
	}, nil
}

func (signature fileHashSignature) equal(other fileHashSignature) bool {
	return signature.canonicalPath == other.canonicalPath &&
		signature.size == other.size &&
		signature.modTime == other.modTime &&
		signature.mode == other.mode
}

type fileHashCacheRecord struct {
	Version   int    `json:"version"`
	Purpose   string `json:"purpose"`
	Algorithm string `json:"algorithm"`
	Size      int64  `json:"size"`
	ModTime   int64  `json:"modTime"`
	Mode      uint32 `json:"mode"`
	Hash      []byte `json:"hash"`
}

type fileHashCache struct {
	dir string
	mu  sync.Mutex
}

func defaultFileHashCache() *fileHashCache {
	if configured := strings.TrimSpace(os.Getenv("CROC_HASH_CACHE_DIR")); configured != "" {
		return &fileHashCache{dir: configured}
	}
	dir, err := os.UserCacheDir()
	if err != nil {
		return nil
	}
	return &fileHashCache{dir: filepath.Join(dir, "croc", "hashes-v1")}
}

func (cache *fileHashCache) recordPath(signature fileHashSignature, purpose, algorithm string) string {
	key := sha256.Sum256([]byte(purpose + "\x00" + algorithm + "\x00" + signature.canonicalPath))
	return filepath.Join(cache.dir, hex.EncodeToString(key[:])+".json")
}

func (cache *fileHashCache) lookup(signature fileHashSignature, purpose, algorithm string) ([]byte, bool) {
	if cache == nil || cache.dir == "" {
		return nil, false
	}
	contents, err := os.ReadFile(cache.recordPath(signature, purpose, algorithm))
	if err != nil {
		return nil, false
	}
	var record fileHashCacheRecord
	if json.Unmarshal(contents, &record) != nil ||
		record.Version != fileHashCacheVersion ||
		record.Purpose != purpose ||
		record.Algorithm != algorithm ||
		record.Size != signature.size ||
		record.ModTime != signature.modTime ||
		record.Mode != signature.mode ||
		len(record.Hash) == 0 || len(record.Hash) > sha256.Size*2 {
		return nil, false
	}
	return append([]byte(nil), record.Hash...), true
}

func (cache *fileHashCache) store(signature fileHashSignature, purpose, algorithm string, hash []byte) error {
	if cache == nil || cache.dir == "" || len(hash) == 0 {
		return nil
	}
	record := fileHashCacheRecord{
		Version:   fileHashCacheVersion,
		Purpose:   purpose,
		Algorithm: algorithm,
		Size:      signature.size,
		ModTime:   signature.modTime,
		Mode:      signature.mode,
		Hash:      append([]byte(nil), hash...),
	}
	contents, err := json.Marshal(record)
	if err != nil {
		return err
	}
	contents = append(contents, '\n')

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if err := os.MkdirAll(cache.dir, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(cache.dir, ".hash-*.tmp")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if _, err = temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	target := cache.recordPath(signature, purpose, algorithm)
	if err = os.Rename(temporaryName, target); err == nil {
		return nil
	}
	// Windows cannot always atomically replace an existing file. Losing a cache
	// entry is safe: the next run simply hashes the file again.
	if removeErr := os.Remove(target); removeErr != nil && !os.IsNotExist(removeErr) {
		return err
	}
	return os.Rename(temporaryName, target)
}

func (c *Client) hashSourceFile(name, algorithm string, showProgress bool) ([]byte, bool, error) {
	before, signatureErr := signatureForFile(name)
	if signatureErr == nil {
		if cached, ok := c.hashCache.lookup(before, hashCacheSource, algorithm); ok {
			return cached, true, nil
		}
	}
	actual, err := c.hashFile(name, algorithm, showProgress)
	if err != nil {
		return nil, false, err
	}
	after, afterErr := signatureForFile(name)
	if signatureErr == nil && afterErr == nil && before.equal(after) {
		if err := c.hashCache.store(after, hashCacheSource, algorithm, actual); err != nil {
			log.Tracef("storing source hash cache entry: %v", err)
		}
	}
	return actual, false, nil
}

func (c *Client) hashReceiverFile(name, algorithm string, expected []byte, showProgress bool) ([]byte, bool, error) {
	signature, signatureErr := signatureForFile(name)
	if signatureErr == nil {
		if cached, ok := c.hashCache.lookup(signature, hashCacheVerifiedFile, algorithm); ok && bytes.Equal(cached, expected) {
			return cached, true, nil
		}
	}
	actual, err := c.hashFile(name, algorithm, showProgress)
	return actual, false, err
}

func (c *Client) rememberVerifiedReceiverFile(name, algorithm string, expected []byte) {
	signature, err := signatureForFile(name)
	if err != nil {
		log.Tracef("stat verified receiver file for hash cache: %v", err)
		return
	}
	if err := c.hashCache.store(signature, hashCacheVerifiedFile, algorithm, expected); err != nil {
		log.Tracef("storing verified receiver hash cache entry: %v", err)
	}
}
