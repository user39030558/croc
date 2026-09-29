package croc

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/models"
)

func newControlPipe(t *testing.T) (*comm.Comm, *comm.Comm) {
	t.Helper()
	local, remote := net.Pipe()
	localComm, remoteComm := comm.New(local), comm.New(remote)
	t.Cleanup(func() {
		localComm.Close()
		remoteComm.Close()
	})
	return localComm, remoteComm
}

func receiveMessage(t *testing.T, conn *comm.Comm) <-chan message.Message {
	t.Helper()
	received := make(chan message.Message, 1)
	go func() {
		data, err := conn.Receive()
		if err != nil {
			close(received)
			return
		}
		m, err := message.Decode(nil, data)
		if err != nil {
			close(received)
			return
		}
		received <- m
	}()
	return received
}

func TestCheckLaneChunkRejectsUnrequestedChunks(t *testing.T) {
	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	fileSize := 3*chunkSize + 10
	state := &fileTransferState{
		chunkRanges:       []int64{chunkSize, chunkSize, 2},
		receivedPositions: map[int64]struct{}{},
	}
	full := make([]byte, chunkSize)

	if err := checkLaneChunk(state, fileSize, chunkSize, full); err != nil {
		t.Fatalf("requested chunk rejected: %v", err)
	}
	cases := map[string]struct {
		position int64
		payload  []byte
	}{
		"outside lane ranges": {0, full},
		"past end of file":    {fileSize, full[:1]},
		"misaligned":          {chunkSize + 1, full},
		"short chunk":         {chunkSize, full[:10]},
		"tail too long":       {3 * chunkSize, full},
	}
	for name, tc := range cases {
		if err := checkLaneChunk(state, fileSize, tc.position, tc.payload); err == nil {
			t.Errorf("%s: chunk at %d accepted", name, tc.position)
		}
	}

	state.receivedPositions[chunkSize] = struct{}{}
	if err := checkLaneChunk(state, fileSize, chunkSize, full); err == nil {
		t.Fatal("duplicate chunk accepted")
	}

	wholeFile := &fileTransferState{receivedPositions: map[int64]struct{}{}}
	if err := checkLaneChunk(wholeFile, fileSize, 3*chunkSize, full[:10]); err != nil {
		t.Fatalf("tail chunk of a whole-file lane rejected: %v", err)
	}
}

func TestParallelFileRetriesAreBounded(t *testing.T) {
	local, remote := newControlPipe(t)
	client := &Client{
		FilesToTransfer: []FileInfo{{Name: "changing.log"}},
		conn:            []*comm.Comm{local},
	}
	for attempt := range maxParallelFileRetries {
		if err := client.recordParallelFileRetry(0); err != nil {
			t.Fatalf("retry %d refused: %v", attempt+1, err)
		}
	}
	if !client.parallelFileRetried(0) {
		t.Fatal("retried file should be requested in full")
	}

	peer := receiveMessage(t, remote)
	err := client.recordParallelFileRetry(0)
	if err == nil || !strings.Contains(err.Error(), "changing.log") {
		t.Fatalf("retry limit error = %v", err)
	}
	m, ok := <-peer
	if !ok || m.Type != message.TypeError || !strings.Contains(m.Message, "changing.log") {
		t.Fatalf("peer was not told about the failure: %+v", m)
	}
}

func TestParallelRetryRequestsNonZeroChunks(t *testing.T) {
	t.Chdir(t.TempDir())
	chunkSize := models.TCP_BUFFER_SIZE / 2
	// One zero chunk followed by a non-zero chunk whose contents are corrupt.
	contents := make([]byte, 2*chunkSize)
	for index := chunkSize; index < len(contents); index++ {
		contents[index] = 0xAB
	}
	if err := os.WriteFile("data.bin", contents, 0o644); err != nil {
		t.Fatal(err)
	}
	client := &Client{
		FilesToTransfer: []FileInfo{{Name: "data.bin", FolderRemote: ".", Size: int64(len(contents))}},
		Options:         Options{HashAlgorithm: "xxhash"},
	}
	t.Cleanup(client.closeReceiveFilesystem)

	resumed, err := client.recipientOpenFileState(0, true)
	if err != nil {
		t.Fatal(err)
	}
	_ = resumed.file.Close()
	if resumed.chunkCount != 1 {
		t.Fatalf("resume requested %d chunks, want only the zero chunk", resumed.chunkCount)
	}

	retried, err := client.recipientOpenFileState(0, false)
	if err != nil {
		t.Fatal(err)
	}
	_ = retried.file.Close()
	if len(retried.chunkRanges) != 0 || retried.chunkCount != 2 {
		t.Fatalf("retry requested ranges %v (%d chunks), want the whole file", retried.chunkRanges, retried.chunkCount)
	}
}

func TestParallelSenderFailsWhenSourceShrinks(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.WriteFile("shrunk.bin", []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open("shrunk.bin")
	if err != nil {
		t.Fatal(err)
	}
	control, remoteControl := newControlPipe(t)
	data, _ := newControlPipe(t)
	client := &Client{
		FilesToTransfer: []FileInfo{{Name: "shrunk.bin", Size: models.TCP_BUFFER_SIZE / 2}},
		conn:            []*comm.Comm{control},
	}
	client.stop = &stop{ctx: context.Background()}
	attempt := &transferAttemptState{errc: make(chan error, 1)}
	state := &fileTransferState{file: file}

	peer := receiveMessage(t, remoteControl)
	client.sendParallelFileData(state, data, attempt)

	select {
	case err := <-attempt.errc:
		if !strings.Contains(err.Error(), "changed during the transfer") {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
		t.Fatal("short read was not reported")
	}
	m, ok := <-peer
	if !ok || m.Type != message.TypeError {
		t.Fatalf("peer was not told about the short read: %+v", m)
	}
}
