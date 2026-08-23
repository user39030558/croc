package croc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/denisbrodbeck/machineid"
	log "github.com/schollz/logger"
	"github.com/schollz/progressbar/v3"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/compress"
	"github.com/schollz/croc/v11/src/crypt"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/termui"
	"github.com/schollz/croc/v11/src/utils"
)

const aggregateProgressFileThreshold = 32

type parallelProgressFile struct {
	index int
	name  string
}

func newParallelFileVerifier(chunkRanges []int64, fileSize int64, algorithm string) (hash.Hash, error) {
	if utils.ChunkRangesBytes(chunkRanges, fileSize, models.TCP_BUFFER_SIZE/2) != fileSize {
		return nil, nil
	}
	verifier, supported, err := utils.NewStreamingHash(algorithm)
	if err != nil || !supported {
		return nil, err
	}
	return verifier, nil
}

func (c *Client) parallelAggregateProgress() bool {
	return len(c.FilesToTransfer) > aggregateProgressFileThreshold
}

func shortParallelProgressName(name string) string {
	const maxRunes = 32
	runes := []rune(name)
	if len(runes) <= maxRunes {
		return name
	}
	return string(runes[:maxRunes-3]) + "..."
}

func (c *Client) setParallelActiveFile(connectionIndex, fileIndex int) {
	if !c.parallelAggregateProgress() {
		return
	}
	c.progressMu.Lock()
	if c.parallelActive == nil {
		c.parallelActive = make(map[int]parallelProgressFile)
	}
	c.parallelActive[connectionIndex] = parallelProgressFile{
		index: fileIndex,
		name:  c.FilesToTransfer[fileIndex].Name,
	}
	c.parallelDirty = true
	c.progressMu.Unlock()
}

func (c *Client) clearParallelActiveFile(connectionIndex, fileIndex int) {
	if !c.parallelAggregateProgress() {
		return
	}
	c.progressMu.Lock()
	if active, ok := c.parallelActive[connectionIndex]; ok && active.index == fileIndex {
		delete(c.parallelActive, connectionIndex)
		c.parallelDirty = true
	}
	c.progressMu.Unlock()
}

func (c *Client) clearParallelActiveFiles() {
	c.progressMu.Lock()
	if len(c.parallelActive) > 0 {
		clear(c.parallelActive)
		c.parallelDirty = true
	}
	c.progressMu.Unlock()
}

func (c *Client) markParallelProgressFinished(fileIndex int) {
	if !c.parallelAggregateProgress() {
		return
	}
	c.progressMu.Lock()
	for _, active := range c.parallelActive {
		if active.index == fileIndex {
			c.parallelProgressN++
			c.parallelDirty = true
			break
		}
	}
	c.progressMu.Unlock()
}

func (c *Client) parallelProgressDescriptionLocked() string {
	fileLabel := "files"
	if c.parallelProgressN == 1 {
		fileLabel = "file"
	}
	description := fmt.Sprintf("Sent %d %s", c.parallelProgressN, fileLabel)
	if len(c.parallelActive) == 0 {
		return description
	}
	connections := make([]int, 0, len(c.parallelActive))
	for connectionIndex := range c.parallelActive {
		connections = append(connections, connectionIndex)
	}
	sort.Ints(connections)
	active := c.parallelActive[connections[0]]
	description += " | Active: " + shortParallelProgressName(active.name)
	if additional := len(connections) - 1; additional > 0 {
		description += fmt.Sprintf(" (+%d)", additional)
	}
	return description
}

func (c *Client) refreshParallelProgressLocked() {
	if c.parallelBar == nil || !c.parallelDirty {
		return
	}
	c.parallelBar.Describe(c.parallelProgressDescriptionLocked())
	c.parallelDirty = false
}

func (c *Client) addFileProgress(bar *progressbar.ProgressBar, bytes int64) {
	c.progressMu.Lock()
	if bar == c.parallelBar {
		c.refreshParallelProgressLocked()
	}
	_ = bar.Add64(bytes)
	c.progressMu.Unlock()
}

func (c *Client) initializeParallelProgress(indices []int) {
	if !c.parallelAggregateProgress() {
		return
	}
	var total int64
	for _, index := range indices {
		total += c.FilesToTransfer[index].Size
	}
	if total <= 0 {
		total = 1
	}
	c.progressMu.Lock()
	if c.parallelBar == nil {
		description := fmt.Sprintf("Sent 0 files | Preparing %d files", len(indices))
		c.parallelBar = c.newAggregateProgressBar(total, description)
		c.parallelDirty = true
	}
	c.progressMu.Unlock()
}

func (c *Client) getParallelProgressBar() *progressbar.ProgressBar {
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	if c.parallelBar == nil {
		indices := make([]int, len(c.FilesToTransfer))
		for index := range indices {
			indices[index] = index
		}
		var total int64
		for _, fileInfo := range c.FilesToTransfer {
			total += fileInfo.Size
		}
		if total <= 0 {
			total = 1
		}
		c.parallelBar = c.newAggregateProgressBar(total, fmt.Sprintf("Transferring %d files", len(indices)))
		c.parallelDirty = true
	}
	return c.parallelBar
}

func (c *Client) finishParallelProgress() {
	c.progressMu.Lock()
	if c.parallelBar != nil {
		c.refreshParallelProgressLocked()
		_ = c.parallelBar.Finish()
	}
	c.progressMu.Unlock()
}

func (q *fileIndexQueue) hasPending() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.next < len(q.indices)
}

func (c *Client) parallelFilesEnabled() bool {
	return c.peerParallelFiles && len(c.FilesToTransfer) > 1 &&
		len(c.Options.RelayPorts) > 1 && !c.Options.Stdout && !c.Options.SendingText
}

func (c *Client) closeParallelFileStates() {
	c.fileTransferMu.Lock()
	states := make([]*fileTransferState, 0, len(c.receiveTransfers)+len(c.senderTransfers))
	for _, state := range c.receiveTransfers {
		states = append(states, state)
	}
	for _, state := range c.senderTransfers {
		states = append(states, state)
	}
	c.receiveTransfers = make(map[int]*fileTransferState)
	c.senderTransfers = make(map[int]*fileTransferState)
	c.fileTransferMu.Unlock()
	c.clearParallelActiveFiles()
	for _, state := range states {
		state.mu.Lock()
		if state.file == nil || state.closed {
			state.mu.Unlock()
			continue
		}
		state.closed = true
		err := state.file.Close()
		state.mu.Unlock()
		if err != nil && !errors.Is(err, os.ErrClosed) {
			log.Tracef("closing parallel transfer file: %v", err)
		}
	}
}

func (c *Client) isFileFinished(index int) bool {
	c.fileTransferMu.Lock()
	defer c.fileTransferMu.Unlock()
	_, ok := c.FilesHasFinished[index]
	return ok
}

func (c *Client) markFileFinished(index int) {
	c.fileTransferMu.Lock()
	_, alreadyFinished := c.FilesHasFinished[index]
	c.FilesHasFinished[index] = struct{}{}
	c.fileTransferMu.Unlock()
	if !alreadyFinished {
		c.markParallelProgressFinished(index)
	}
}

// prepareRecipientFile performs the existing-file, overwrite, rename, resume,
// and empty-file decisions for one queue entry. It does no network I/O.
func (c *Client) prepareRecipientFile(index int) (bool, error) {
	if c.isFileFinished(index) {
		return false, nil
	}
	fileInfo := c.FilesToTransfer[index]
	root, err := c.receiveFilesystem()
	if err != nil {
		return false, err
	}
	relativePath := path.Join(fileInfo.FolderRemote, fileInfo.Name)
	recipientFileInfo, errRecipientFile := root.Lstat(relativePath)
	var errHash error
	var fileHash []byte
	if errRecipientFile == nil && recipientFileInfo.Size() == fileInfo.Size {
		fileHash, _, errHash = c.hashReceiverFile(relativePath, c.Options.HashAlgorithm, fileInfo.Hash, !c.Options.SendingText)
	}
	if fileInfo.Size == 0 || fileInfo.Symlink != "" {
		if err := c.createEmptyFileAndFinish(fileInfo, index); err != nil {
			return false, err
		}
		c.numberOfTransferredFiles++
		c.markFileFinished(index)
		return false, nil
	}

	if !bytes.Equal(fileHash, fileInfo.Hash) {
		log.Debugf("hashes are not equal %x != %x", fileHash, fileInfo.Hash)
		if errHash == nil && errRecipientFile == nil &&
			!strings.HasPrefix(fileInfo.Name, "croc-stdin-") &&
			!c.Options.SendingText && c.Options.Rename {
			newName := utils.UnusedFilename(fileInfo.FolderRemote, fileInfo.Name)
			output, colorEnabled := termui.Output(os.Stderr)
			fmt.Fprintf(output, "Receiving %s as %s\n", quotedFilename(fileInfo.Name, colorEnabled), quotedFilename(newName, colorEnabled))
			c.FilesToTransfer[index].Name = newName
			fileInfo.Name = newName
		}
		if errHash == nil && !c.Options.Overwrite && !c.Options.Rename &&
			errRecipientFile == nil && !strings.HasPrefix(fileInfo.Name, "croc-stdin-") &&
			!c.Options.SendingText {
			missingRanges := utils.MissingChunks(relativePath, fileInfo.Size, models.TCP_BUFFER_SIZE/2)
			missingBytes := utils.ChunkRangesBytes(missingRanges, fileInfo.Size, models.TCP_BUFFER_SIZE/2)
			percentDone := 100 - float64(missingBytes)/float64(fileInfo.Size)*100
			action := "Overwrite"
			promptDetail := ""
			promptSpacing := " "
			if percentDone < 99 {
				action = "Resume"
				promptDetail = fmt.Sprintf(" (%2.1f%%)", percentDone)
				promptSpacing = "   "
			}
			output, colorEnabled := termui.Output(os.Stderr)
			styledAction := termui.Warning(action, colorEnabled)
			if action == "Resume" {
				styledAction = action
			}
			fmt.Fprintf(output, "\n%s %s%s? %s%s(use --overwrite to omit) ",
				styledAction, quotedFilename(relativePath, colorEnabled), promptDetail,
				termui.PromptChoices("(y/N)", colorEnabled), promptSpacing)
			choice, _ := utils.GetInput("")
			choice = strings.ToLower(choice)
			if choice != "y" && choice != "yes" {
				fmt.Fprintf(output, "Skipping %s\n", quotedFilename(relativePath, colorEnabled))
				c.markFileFinished(index)
				return false, nil
			}
		}
	} else {
		log.Debugf("hashes are equal %x == %x", fileHash, fileInfo.Hash)
		c.numberOfUnchangedFiles++
		if !fileInfo.ModTime.IsZero() {
			if err := root.Chtimes(relativePath, fileInfo.ModTime, fileInfo.ModTime); err != nil {
				log.Warnf("chtimes %v: %v", fileInfo.ModTime, err)
			}
		}
		c.rememberVerifiedReceiverFile(relativePath, c.Options.HashAlgorithm, fileInfo.Hash)
		c.markFileFinished(index)
		return false, nil
	}
	if errHash != nil {
		log.Debug(errHash)
	}
	c.numberOfTransferredFiles++
	newFolder, _ := filepath.Split(fileInfo.FolderRemote)
	if newFolder != c.LastFolder && !c.Options.SendingText && newFolder != "./" {
		output, colorEnabled := termui.Output(os.Stderr)
		fmt.Fprintf(output, "\r%s\n", termui.Filename(newFolder, colorEnabled))
	}
	c.LastFolder = newFolder
	return true, nil
}

func (c *Client) initializeParallelFileTransfers() error {
	c.fileTransferMu.Lock()
	if c.parallelFilesInitialized {
		c.fileTransferMu.Unlock()
		return nil
	}
	c.parallelFilesInitialized = true
	c.parallelFileMode = true
	c.fileTransferMu.Unlock()

	pending := make([]int, 0, len(c.FilesToTransfer))
	for index := range c.FilesToTransfer {
		needsTransfer, err := c.prepareRecipientFile(index)
		if err != nil {
			return err
		}
		if needsTransfer {
			pending = append(pending, index)
		}
	}
	c.fileQueue.reset(pending)
	c.initializeParallelProgress(pending)
	c.Step3RecipientRequestFile = true
	c.markTransferStarted()
	if len(pending) == 0 {
		return c.finishParallelReceiverIfDone()
	}
	workers := len(c.Options.RelayPorts)
	if workers > len(pending) {
		workers = len(pending)
	}
	for connectionIndex := 0; connectionIndex < workers; connectionIndex++ {
		if err := c.startParallelReceiverFile(connectionIndex); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) makeFileBar(index int, chunkRanges []int64) *progressbar.ProgressBar {
	if c.parallelAggregateProgress() {
		bar := c.getParallelProgressBar()
		fileInfo := c.FilesToTransfer[index]
		byteToDo := utils.ChunkRangesBytes(chunkRanges, fileInfo.Size, models.TCP_BUFFER_SIZE/2)
		if bytesDone := fileInfo.Size - byteToDo; byteToDo > 0 && bytesDone > 0 {
			c.addFileProgress(bar, bytesDone)
		}
		return bar
	}
	fileInfo := c.FilesToTransfer[index]
	description := fmt.Sprintf("%-*s", c.longestFilename, fileInfo.Name)
	folder, _ := filepath.Split(fileInfo.FolderRemote)
	if folder == "./" {
		description = fileInfo.Name
	} else if !c.Options.IsSender {
		description = " " + description
	}
	c.progressMu.Lock()
	bar := c.newProgressBar(fileInfo.Size, formatDescription(description), 100*time.Millisecond)
	byteToDo := utils.ChunkRangesBytes(chunkRanges, fileInfo.Size, models.TCP_BUFFER_SIZE/2)
	if bytesDone := fileInfo.Size - byteToDo; byteToDo > 0 && bytesDone > 0 {
		_ = bar.Add64(bytesDone)
	}
	c.progressMu.Unlock()
	return bar
}

func (c *Client) startParallelReceiverFile(connectionIndex int) error {
	index, ok := c.fileQueue.pop()
	if !ok {
		return c.finishParallelReceiverIfDone()
	}
	state, err := c.recipientInitializeFileState(index)
	if err != nil {
		return err
	}
	state.bar = c.makeFileBar(index, state.chunkRanges)
	state.dataConnectionIndex = connectionIndex
	c.fileTransferMu.Lock()
	if _, busy := c.receiveTransfers[connectionIndex]; busy {
		c.fileTransferMu.Unlock()
		_ = state.file.Close()
		return fmt.Errorf("data connection %d already has a file", connectionIndex)
	}
	c.receiveTransfers[connectionIndex] = state
	c.fileTransferMu.Unlock()
	c.setParallelActiveFile(connectionIndex, index)

	machineID, _ := machineid.ID()
	request, err := json.Marshal(RemoteFileRequest{
		CurrentFileChunkRanges:    state.chunkRanges,
		FilesToTransferCurrentNum: index,
		DataConnectionIndex:       connectionIndex,
		MachineID:                 machineID,
		ReconnectVersion:          c.reconnectVersion,
		Features:                  []string{perFileCompressionFeature, parallelFilesFeature},
	})
	if err != nil {
		return err
	}
	log.Debugf("requesting file %d on data connection %d with %d chunks", index, connectionIndex, state.chunkCount)
	return c.sendControl(message.Message{Type: message.TypeRecipientReady, Bytes: request})
}

func (c *Client) finishParallelReceiverIfDone() error {
	if c.fileQueue.hasPending() {
		return nil
	}
	c.fileTransferMu.Lock()
	done := len(c.receiveTransfers) == 0 && !c.SuccessfulTransfer
	if done {
		c.SuccessfulTransfer = true
	}
	c.fileTransferMu.Unlock()
	if !done {
		return nil
	}
	c.finishParallelProgress()
	return c.sendControl(message.Message{Type: message.TypeFinished})
}

func (c *Client) parallelReceiveState(connectionIndex int) (*fileTransferState, bool) {
	c.fileTransferMu.Lock()
	defer c.fileTransferMu.Unlock()
	if !c.parallelFileMode {
		return nil, false
	}
	state := c.receiveTransfers[connectionIndex]
	return state, true
}

func hashOpenFile(file *os.File, size int64, algorithm string) ([]byte, error) {
	reader := io.NewSectionReader(file, 0, size)
	switch algorithm {
	case "imohash":
		return utils.IMOHashReader(reader, nil)
	case "md5":
		return utils.MD5HashReader(reader, nil)
	case "xxhash":
		return utils.XXHashReader(reader, nil)
	case "highway":
		return utils.HighwayHashReader(reader, nil)
	default:
		return nil, fmt.Errorf("unsupported hash algorithm %q", algorithm)
	}
}

func (c *Client) receiveParallelData(state *fileTransferState, data []byte) (bool, error) {
	if state == nil {
		return false, fmt.Errorf("received data on an idle parallel-file connection")
	}
	if len(data) < 8 {
		return false, fmt.Errorf("parallel-file data frame is too short: %d bytes", len(data))
	}
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return false, fmt.Errorf("received data on an idle parallel-file connection")
	}
	position := int64(binary.LittleEndian.Uint64(data[:8]))
	fileSize := c.FilesToTransfer[state.fileIndex].Size
	if position < 0 || position > fileSize || int64(len(data[8:])) > fileSize-position {
		state.mu.Unlock()
		return false, fmt.Errorf("invalid data range for file %d: offset %d, length %d", state.fileIndex, position, len(data[8:]))
	}
	if _, err := state.file.WriteAt(data[8:], position); err != nil {
		state.mu.Unlock()
		return false, err
	}
	state.totalSent += int64(len(data[8:]))
	state.totalChunks++
	if state.verifier != nil {
		if position != state.verifyPosition {
			// A reordered or duplicated stream must use the full-file verifier
			// after all writes are complete.
			state.verifier = nil
		} else {
			_, _ = state.verifier.Write(data[8:])
			state.verifyPosition += int64(len(data[8:]))
		}
	}
	c.mutex.Lock()
	c.TotalSent += int64(len(data[8:]))
	c.TotalChunksTransferred++
	c.mutex.Unlock()
	c.addFileProgress(state.bar, int64(len(data[8:])))
	if state.totalChunks != state.chunkCount {
		state.mu.Unlock()
		return false, nil
	}
	state.closed = true
	var actualHash []byte
	var err error
	if state.verifier != nil && state.verifyPosition == fileSize {
		actualHash = state.verifier.Sum(nil)
	} else {
		actualHash, err = hashOpenFile(state.file, fileSize, c.Options.HashAlgorithm)
	}
	closeErr := state.file.Close()
	if err != nil {
		state.mu.Unlock()
		return false, err
	}
	if closeErr != nil {
		state.mu.Unlock()
		return false, closeErr
	}
	if !bytes.Equal(actualHash, c.FilesToTransfer[state.fileIndex].Hash) {
		state.retry = true
		log.Warnf("hash mismatch for %s; queueing missing chunks again", state.path)
	} else {
		c.markFileFinished(state.fileIndex)
		fileInfo := c.FilesToTransfer[state.fileIndex]
		if !fileInfo.ModTime.IsZero() {
			root, rootErr := c.receiveFilesystem()
			if rootErr != nil {
				state.mu.Unlock()
				return false, rootErr
			}
			if err := root.Chtimes(state.path, fileInfo.ModTime, fileInfo.ModTime); err != nil {
				log.Warnf("chtimes %v: %v", fileInfo.ModTime, err)
			}
		}
		c.rememberVerifiedReceiverFile(state.path, c.Options.HashAlgorithm, fileInfo.Hash)
	}
	state.mu.Unlock()
	if err := c.sendControl(message.Message{Type: message.TypeCloseSender, Num: state.fileIndex}); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Client) finishParallelReceiverFile(fileIndex int) error {
	c.fileTransferMu.Lock()
	var state *fileTransferState
	for connectionIndex, candidate := range c.receiveTransfers {
		if candidate.fileIndex == fileIndex {
			state = candidate
			delete(c.receiveTransfers, connectionIndex)
			break
		}
	}
	c.fileTransferMu.Unlock()
	if state == nil {
		return fmt.Errorf("acknowledgement for inactive file %d", fileIndex)
	}
	c.clearParallelActiveFile(state.dataConnectionIndex, fileIndex)
	state.mu.Lock()
	retry := state.retry
	state.mu.Unlock()
	if retry {
		c.fileQueue.push(fileIndex)
	}
	return c.startParallelReceiverFile(state.dataConnectionIndex)
}

func (c *Client) startParallelSenderFile(request RemoteFileRequest, attempt *transferAttemptState) error {
	index := request.FilesToTransferCurrentNum
	connectionIndex := request.DataConnectionIndex
	if index < 0 || index >= len(c.FilesToTransfer) {
		return fmt.Errorf("invalid requested file index %d", index)
	}
	if connectionIndex < 0 || connectionIndex >= len(c.Options.RelayPorts) {
		return fmt.Errorf("invalid data connection index %d", connectionIndex)
	}
	pathToFile := path.Join(c.FilesToTransfer[index].FolderSource, c.FilesToTransfer[index].Name)
	file, err := os.Open(pathToFile)
	if err != nil {
		return err
	}
	state := &fileTransferState{
		fileIndex:           index,
		dataConnectionIndex: connectionIndex,
		file:                file,
		chunkRanges:         request.CurrentFileChunkRanges,
		chunkCount: utils.ChunkRangesCount(request.CurrentFileChunkRanges,
			c.FilesToTransfer[index].Size, models.TCP_BUFFER_SIZE/2),
		bar: c.makeFileBar(index, request.CurrentFileChunkRanges),
	}
	c.fileTransferMu.Lock()
	if _, busy := c.senderTransfers[connectionIndex]; busy {
		c.fileTransferMu.Unlock()
		_ = file.Close()
		return fmt.Errorf("data connection %d already sending a file", connectionIndex)
	}
	for _, active := range c.senderTransfers {
		if active.fileIndex == index {
			c.fileTransferMu.Unlock()
			_ = file.Close()
			return fmt.Errorf("file %d is already being sent", index)
		}
	}
	c.parallelFileMode = true
	c.senderTransfers[connectionIndex] = state
	c.fileTransferMu.Unlock()
	c.setParallelActiveFile(connectionIndex, index)
	c.markTransferStarted()
	go c.sendParallelFileData(state, c.conn[connectionIndex+1], attempt)
	return nil
}

func (c *Client) finishParallelSenderFile(fileIndex int) error {
	c.fileTransferMu.Lock()
	var state *fileTransferState
	for connectionIndex, candidate := range c.senderTransfers {
		if candidate.fileIndex == fileIndex {
			state = candidate
			delete(c.senderTransfers, connectionIndex)
			break
		}
	}
	c.fileTransferMu.Unlock()
	if state == nil {
		return fmt.Errorf("completion for inactive file %d", fileIndex)
	}
	c.markFileFinished(fileIndex)
	c.clearParallelActiveFile(state.dataConnectionIndex, fileIndex)
	return c.sendControl(message.Message{Type: message.TypeCloseRecipient, Num: fileIndex})
}

func (c *Client) sendParallelFileData(state *fileTransferState, dataConn *comm.Comm, attempt *transferAttemptState) {
	defer func() {
		if recovered := recover(); recovered != nil {
			attempt.report(fmt.Errorf("send data panic: %v", recovered))
		}
		if err := state.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			log.Tracef("closing sent file: %v", err)
		}
	}()
	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	fileSize := c.FilesToTransfer[state.fileIndex].Size
	payload := make([]byte, 8+chunkSize)
	var encryptedBuffer []byte
	var compressedBuffer []byte
	for position := int64(0); position < fileSize; position += chunkSize {
		if err := c.ctxErr(); err != nil {
			return
		}
		if !utils.ChunkRangesContain(state.chunkRanges, position) {
			continue
		}
		n, readErr := state.file.ReadAt(payload[8:], position)
		if c.limiter != nil && n > 0 {
			reservation := c.limiter.ReserveN(time.Now(), n)
			time.Sleep(reservation.Delay())
		}
		if n > 0 {
			binary.LittleEndian.PutUint64(payload[:8], uint64(position))
			plain := payload[:8+n]
			var dataToSend []byte
			var err error
			if c.fileUsesCompression(state.fileIndex) {
				compressedBuffer = compress.CompressTo(compressedBuffer, plain)
				dataToSend, err = crypt.EncryptAEADTo(encryptedBuffer, compressedBuffer, c.dataAEAD)
			} else {
				dataToSend, err = crypt.EncryptAEADTo(encryptedBuffer, plain, c.dataAEAD)
			}
			if err != nil {
				attempt.report(err)
				return
			}
			encryptedBuffer = dataToSend
			if err := dataConn.Send(dataToSend); err != nil {
				if c.ctxErr() == nil {
					attempt.report(transferDisconnectError{err: err})
				}
				return
			}
			c.addFileProgress(state.bar, int64(n))
			c.mutex.Lock()
			c.TotalSent += int64(n)
			c.TotalChunksTransferred++
			c.mutex.Unlock()
		}
		if readErr != nil && readErr != io.EOF {
			attempt.report(readErr)
			return
		}
	}
}
