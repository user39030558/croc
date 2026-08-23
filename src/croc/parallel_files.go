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

const (
	aggregateProgressFileThreshold = 32
)

var hybridChunkJobTargetBytes = int64(64 << 20)

func splitParallelFileJobs(fileIndex int, chunkRanges []int64, fileSize, targetBytes int64) []parallelFileJob {
	const defaultChunkSize = int64(models.TCP_BUFFER_SIZE / 2)
	if fileSize <= 0 {
		return nil
	}
	chunkSize := defaultChunkSize
	if len(chunkRanges) > 0 && chunkRanges[0] > 0 {
		chunkSize = chunkRanges[0]
	}
	if targetBytes < chunkSize {
		targetBytes = chunkSize
	}
	targetChunks := targetBytes / chunkSize
	if targetChunks < 1 {
		targetChunks = 1
	}

	ranges := chunkRanges
	if len(ranges) == 0 {
		ranges = []int64{chunkSize, 0, (fileSize + chunkSize - 1) / chunkSize}
	}
	jobs := make([]parallelFileJob, 0, int64(utils.ChunkRangesCount(ranges, fileSize, chunkSize))/targetChunks+1)
	current := []int64{chunkSize}
	var currentChunks int64
	flush := func() {
		if currentChunks == 0 {
			return
		}
		jobs = append(jobs, parallelFileJob{fileIndex: fileIndex, chunkRanges: current})
		current = []int64{chunkSize}
		currentChunks = 0
	}
	for index := 1; index+1 < len(ranges); index += 2 {
		start, count := ranges[index], ranges[index+1]
		for count > 0 {
			capacity := targetChunks - currentChunks
			if capacity == 0 {
				flush()
				capacity = targetChunks
			}
			take := count
			if take > capacity {
				take = capacity
			}
			current = append(current, start, take)
			currentChunks += take
			start += take * chunkSize
			count -= take
			if currentChunks == targetChunks {
				flush()
			}
		}
	}
	flush()
	return jobs
}

func encodeParallelLaneAck(fileIndex, connectionIndex int, fileComplete bool) ([]byte, error) {
	return json.Marshal(parallelFileLaneAck{
		FileIndex:           fileIndex,
		DataConnectionIndex: connectionIndex,
		FileComplete:        fileComplete,
	})
}

func decodeParallelLaneAck(m message.Message) (parallelFileLaneAck, bool, error) {
	if len(m.Bytes) == 0 {
		return parallelFileLaneAck{FileIndex: m.Num}, false, nil
	}
	var ack parallelFileLaneAck
	if err := json.Unmarshal(m.Bytes, &ack); err != nil {
		return parallelFileLaneAck{}, true, err
	}
	if ack.FileIndex != m.Num {
		return parallelFileLaneAck{}, true, fmt.Errorf("parallel lane acknowledgement file mismatch: %d != %d", ack.FileIndex, m.Num)
	}
	return ack, true, nil
}

func validateParallelChunkRanges(ranges []int64, fileSize int64) error {
	if len(ranges) == 0 {
		return nil
	}
	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	if len(ranges) < 3 || len(ranges)%2 == 0 || ranges[0] != chunkSize {
		return fmt.Errorf("invalid parallel chunk-range header")
	}
	var previousLastStart int64
	for index := 1; index+1 < len(ranges); index += 2 {
		start, count := ranges[index], ranges[index+1]
		if start < 0 || start >= fileSize || start%chunkSize != 0 || count <= 0 {
			return fmt.Errorf("invalid parallel chunk range at index %d", index)
		}
		maxCount := (fileSize-start-1)/chunkSize + 1
		if count > maxCount || (index > 1 && start <= previousLastStart) {
			return fmt.Errorf("parallel chunk range exceeds or overlaps file at index %d", index)
		}
		previousLastStart = start + (count-1)*chunkSize
	}
	return nil
}

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
	return c.hybridFileMode || len(c.FilesToTransfer) > aggregateProgressFileThreshold
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
	receiveFiles := make([]*parallelReceiveFileState, 0, len(c.receiveFileStates))
	for _, state := range c.receiveTransfers {
		states = append(states, state)
	}
	for _, state := range c.receiveFileStates {
		receiveFiles = append(receiveFiles, state)
	}
	for _, state := range c.senderTransfers {
		states = append(states, state)
	}
	c.receiveTransfers = make(map[int]*fileTransferState)
	c.receiveFileStates = make(map[int]*parallelReceiveFileState)
	c.senderTransfers = make(map[int]*fileTransferState)
	c.fileTransferMu.Unlock()
	c.clearParallelActiveFiles()
	for _, state := range states {
		state.mu.Lock()
		if state.file == nil || state.closed || state.receiveFile != nil {
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
	for _, state := range receiveFiles {
		state.mu.Lock()
		if state.file == nil || state.closed {
			state.mu.Unlock()
			continue
		}
		state.closed = true
		err := state.file.Close()
		state.mu.Unlock()
		if err != nil && !errors.Is(err, os.ErrClosed) {
			log.Tracef("closing hybrid receive file: %v", err)
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
	if c.peerHybridChunks {
		c.fileTransferMu.Lock()
		c.hybridFileMode = true
		c.fileTransferMu.Unlock()
	}
	c.initializeParallelProgress(pending)
	c.Step3RecipientRequestFile = true
	c.markTransferStarted()
	if len(pending) == 0 {
		return c.finishParallelReceiverIfDone()
	}
	if c.peerHybridChunks {
		return c.initializeHybridFileTransfers(pending)
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

func (c *Client) initializeHybridFileTransfers(pending []int) error {
	jobs := make([]parallelFileJob, 0, len(pending))
	var transferMax int64
	for _, fileIndex := range pending {
		base, err := c.recipientInitializeFileState(fileIndex)
		if err != nil {
			return err
		}
		fileJobs := splitParallelFileJobs(
			fileIndex,
			base.chunkRanges,
			c.FilesToTransfer[fileIndex].Size,
			hybridChunkJobTargetBytes,
		)
		if len(fileJobs) == 0 {
			_ = base.file.Close()
			return fmt.Errorf("file %d produced no hybrid transfer jobs", fileIndex)
		}
		bar := c.makeFileBar(fileIndex, base.chunkRanges)
		verifier := base.verifier
		if len(fileJobs) > 1 {
			verifier = nil
		}
		fileState := &parallelReceiveFileState{
			fileIndex:       fileIndex,
			path:            base.path,
			file:            base.file,
			bar:             bar,
			chunkRanges:     append([]int64(nil), base.chunkRanges...),
			bytesToTransfer: utils.ChunkRangesBytes(base.chunkRanges, c.FilesToTransfer[fileIndex].Size, models.TCP_BUFFER_SIZE/2),
			chunksRemaining: base.chunkCount,
			jobsRemaining:   len(fileJobs),
			verifier:        verifier,
		}
		c.fileTransferMu.Lock()
		c.receiveFileStates[fileIndex] = fileState
		c.fileTransferMu.Unlock()
		transferMax += c.FilesToTransfer[fileIndex].Size
		jobs = append(jobs, fileJobs...)
	}
	c.hybridTransferMax = transferMax
	c.hybridQueue.reset(jobs)
	return c.scheduleHybridReceiverConnections()
}

func (c *Client) scheduleHybridReceiverConnections() error {
	workers := len(c.Options.RelayPorts)
	for connectionIndex := 0; connectionIndex < workers; connectionIndex++ {
		c.fileTransferMu.Lock()
		_, busy := c.receiveTransfers[connectionIndex]
		c.fileTransferMu.Unlock()
		if busy {
			continue
		}
		job, ok := c.hybridQueue.pop()
		if !ok {
			break
		}
		if err := c.startHybridReceiverJob(connectionIndex, job); err != nil {
			return err
		}
	}
	return c.finishParallelReceiverIfDone()
}

func (c *Client) startHybridReceiverJob(connectionIndex int, job parallelFileJob) error {
	c.fileTransferMu.Lock()
	fileState := c.receiveFileStates[job.fileIndex]
	if fileState == nil {
		c.fileTransferMu.Unlock()
		return fmt.Errorf("hybrid transfer file %d is not active", job.fileIndex)
	}
	if _, busy := c.receiveTransfers[connectionIndex]; busy {
		c.fileTransferMu.Unlock()
		return fmt.Errorf("data connection %d already has a file lane", connectionIndex)
	}
	state := &fileTransferState{
		fileIndex:           job.fileIndex,
		dataConnectionIndex: connectionIndex,
		path:                fileState.path,
		chunkRanges:         append([]int64(nil), job.chunkRanges...),
		chunkCount: utils.ChunkRangesCount(
			job.chunkRanges,
			c.FilesToTransfer[job.fileIndex].Size,
			models.TCP_BUFFER_SIZE/2,
		),
		bar:               fileState.bar,
		receiveFile:       fileState,
		receivedPositions: make(map[int64]struct{}),
	}
	c.receiveTransfers[connectionIndex] = state
	c.fileTransferMu.Unlock()
	c.setParallelActiveFile(connectionIndex, job.fileIndex)

	machineID, _ := machineid.ID()
	request, err := json.Marshal(RemoteFileRequest{
		CurrentFileChunkRanges:    state.chunkRanges,
		FilesToTransferCurrentNum: job.fileIndex,
		DataConnectionIndex:       connectionIndex,
		FileTransferBytes:         fileState.bytesToTransfer,
		TransferTotalBytes:        c.hybridTransferMax,
		MachineID:                 machineID,
		ReconnectVersion:          c.reconnectVersion,
		Features:                  []string{perFileCompressionFeature, parallelFilesFeature, hybridChunksFeature},
	})
	if err != nil {
		return err
	}
	log.Debugf("requesting hybrid file %d lane %d with %d chunks", job.fileIndex, connectionIndex, state.chunkCount)
	return c.sendControl(message.Message{Type: message.TypeRecipientReady, Bytes: request})
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

func (c *Client) makeHybridSenderBar(index int, fileTransferBytes, transferTotalBytes int64) *progressbar.ProgressBar {
	c.progressMu.Lock()
	defer c.progressMu.Unlock()
	if transferTotalBytes <= 0 {
		for _, fileInfo := range c.FilesToTransfer {
			transferTotalBytes += fileInfo.Size
		}
	}
	if transferTotalBytes <= 0 {
		transferTotalBytes = 1
	}
	if c.parallelBar == nil {
		c.parallelBar = c.newAggregateProgressBar(
			transferTotalBytes,
			fmt.Sprintf("Transferring %d files", len(c.FilesToTransfer)),
		)
		c.parallelDirty = true
	}
	if c.senderProgressFile == nil {
		c.senderProgressFile = make(map[int]struct{})
	}
	if _, initialized := c.senderProgressFile[index]; !initialized {
		c.senderProgressFile[index] = struct{}{}
		if resumedBytes := c.FilesToTransfer[index].Size - fileTransferBytes; resumedBytes > 0 {
			_ = c.parallelBar.Add64(resumedBytes)
		}
	}
	return c.parallelBar
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
	c.fileTransferMu.Lock()
	hybrid := c.hybridFileMode
	c.fileTransferMu.Unlock()
	if (hybrid && c.hybridQueue.hasPending()) || (!hybrid && c.fileQueue.hasPending()) {
		return nil
	}
	c.fileTransferMu.Lock()
	done := len(c.receiveTransfers) == 0 &&
		(!hybrid || len(c.receiveFileStates) == 0) &&
		!c.SuccessfulTransfer
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
	if state.receiveFile != nil {
		return c.receiveHybridParallelData(state, data)
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

func (c *Client) receiveHybridParallelData(state *fileTransferState, data []byte) (bool, error) {
	if len(data) < 8 {
		return false, fmt.Errorf("hybrid-file data frame is too short: %d bytes", len(data))
	}
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		return false, fmt.Errorf("received data on a completed hybrid-file lane")
	}
	position := int64(binary.LittleEndian.Uint64(data[:8]))
	payload := data[8:]
	fileSize := c.FilesToTransfer[state.fileIndex].Size
	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	if position < 0 || position >= fileSize || position%chunkSize != 0 ||
		int64(len(payload)) > fileSize-position ||
		!utils.ChunkRangesContain(state.chunkRanges, position) {
		state.mu.Unlock()
		return false, fmt.Errorf("invalid hybrid data range for file %d: offset %d, length %d", state.fileIndex, position, len(payload))
	}
	expectedBytes := chunkSize
	if remaining := fileSize - position; remaining < expectedBytes {
		expectedBytes = remaining
	}
	if int64(len(payload)) != expectedBytes {
		state.mu.Unlock()
		return false, fmt.Errorf("invalid hybrid chunk length for file %d at %d: %d != %d", state.fileIndex, position, len(payload), expectedBytes)
	}
	if _, duplicate := state.receivedPositions[position]; duplicate {
		state.mu.Unlock()
		return false, fmt.Errorf("duplicate hybrid chunk for file %d at %d", state.fileIndex, position)
	}
	if _, err := state.receiveFile.file.WriteAt(payload, position); err != nil {
		state.mu.Unlock()
		return false, err
	}
	state.receivedPositions[position] = struct{}{}
	state.totalSent += int64(len(payload))
	state.totalChunks++
	laneDone := state.totalChunks == state.chunkCount
	if state.totalChunks > state.chunkCount {
		state.mu.Unlock()
		return false, fmt.Errorf("too many chunks for hybrid file %d lane %d", state.fileIndex, state.dataConnectionIndex)
	}
	if laneDone {
		state.closed = true
	}
	state.mu.Unlock()

	fileState := state.receiveFile
	fileState.mu.Lock()
	if fileState.verifier != nil {
		if position != fileState.verifyPosition {
			fileState.verifier = nil
		} else {
			_, _ = fileState.verifier.Write(payload)
			fileState.verifyPosition += int64(len(payload))
		}
	}
	fileState.chunksRemaining--
	if fileState.chunksRemaining < 0 {
		fileState.mu.Unlock()
		return false, fmt.Errorf("too many chunks received for hybrid file %d", state.fileIndex)
	}
	verifyFile := fileState.chunksRemaining == 0
	fileState.mu.Unlock()

	c.mutex.Lock()
	c.TotalSent += int64(len(payload))
	c.TotalChunksTransferred++
	c.mutex.Unlock()
	c.addFileProgress(state.bar, int64(len(payload)))

	if verifyFile {
		if err := c.verifyHybridReceiverFile(fileState); err != nil {
			return false, err
		}
	}
	if !laneDone {
		return false, nil
	}
	fileState.mu.Lock()
	fileComplete := fileState.verified
	fileState.mu.Unlock()
	ack, err := encodeParallelLaneAck(state.fileIndex, state.dataConnectionIndex, fileComplete)
	if err != nil {
		return false, err
	}
	if err := c.sendControl(message.Message{Type: message.TypeCloseSender, Num: state.fileIndex, Bytes: ack}); err != nil {
		return false, err
	}
	return true, nil
}

func (c *Client) verifyHybridReceiverFile(state *parallelReceiveFileState) error {
	state.mu.Lock()
	if state.verifyDone {
		state.mu.Unlock()
		return nil
	}
	fileSize := c.FilesToTransfer[state.fileIndex].Size
	var actualHash []byte
	var err error
	if state.verifier != nil && state.verifyPosition == fileSize {
		actualHash = state.verifier.Sum(nil)
	} else {
		actualHash, err = hashOpenFile(state.file, fileSize, c.Options.HashAlgorithm)
	}
	if err != nil {
		state.mu.Unlock()
		return err
	}
	fileInfo := c.FilesToTransfer[state.fileIndex]
	if !bytes.Equal(actualHash, fileInfo.Hash) {
		state.retry = true
		state.verifyDone = true
		state.mu.Unlock()
		log.Warnf("hash mismatch for %s; queueing the file again", state.path)
		return nil
	}
	state.verified = true
	state.verifyDone = true
	state.closed = true
	closeErr := state.file.Close()
	state.mu.Unlock()
	if closeErr != nil {
		return closeErr
	}
	if !fileInfo.ModTime.IsZero() {
		root, rootErr := c.receiveFilesystem()
		if rootErr != nil {
			return rootErr
		}
		if err := root.Chtimes(state.path, fileInfo.ModTime, fileInfo.ModTime); err != nil {
			log.Warnf("chtimes %v: %v", fileInfo.ModTime, err)
		}
	}
	c.rememberVerifiedReceiverFile(state.path, c.Options.HashAlgorithm, fileInfo.Hash)
	c.markFileFinished(state.fileIndex)
	return nil
}

func (c *Client) finishParallelReceiverFile(m message.Message) error {
	ack, hasLane, err := decodeParallelLaneAck(m)
	if err != nil {
		return err
	}
	c.fileTransferMu.Lock()
	hybrid := c.hybridFileMode
	c.fileTransferMu.Unlock()
	if hybrid {
		if !hasLane {
			return fmt.Errorf("hybrid acknowledgement for file %d has no lane identity", m.Num)
		}
		return c.finishHybridReceiverLane(ack)
	}
	fileIndex := m.Num
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

func (c *Client) finishHybridReceiverLane(ack parallelFileLaneAck) error {
	c.fileTransferMu.Lock()
	state := c.receiveTransfers[ack.DataConnectionIndex]
	if state == nil || state.fileIndex != ack.FileIndex || state.receiveFile == nil {
		c.fileTransferMu.Unlock()
		return fmt.Errorf("acknowledgement for inactive hybrid file %d lane %d", ack.FileIndex, ack.DataConnectionIndex)
	}
	delete(c.receiveTransfers, ack.DataConnectionIndex)
	fileState := state.receiveFile
	c.fileTransferMu.Unlock()
	c.clearParallelActiveFile(ack.DataConnectionIndex, ack.FileIndex)

	fileState.mu.Lock()
	fileState.jobsRemaining--
	if fileState.jobsRemaining < 0 {
		fileState.mu.Unlock()
		return fmt.Errorf("too many lane acknowledgements for hybrid file %d", ack.FileIndex)
	}
	allAcknowledged := fileState.jobsRemaining == 0
	verifyDone := fileState.verifyDone
	verified := fileState.verified
	retry := fileState.retry
	fileState.mu.Unlock()

	if allAcknowledged {
		if !verifyDone {
			return fmt.Errorf("hybrid file %d lanes closed before verification", ack.FileIndex)
		}
		if verified {
			c.fileTransferMu.Lock()
			delete(c.receiveFileStates, ack.FileIndex)
			c.fileTransferMu.Unlock()
		} else if retry {
			if err := c.queueHybridFileRetry(fileState); err != nil {
				return err
			}
		}
	}
	return c.scheduleHybridReceiverConnections()
}

func (c *Client) queueHybridFileRetry(state *parallelReceiveFileState) error {
	fileSize := c.FilesToTransfer[state.fileIndex].Size
	jobs := splitParallelFileJobs(state.fileIndex, nil, fileSize, hybridChunkJobTargetBytes)
	if len(jobs) == 0 {
		return fmt.Errorf("file %d produced no hybrid retry jobs", state.fileIndex)
	}
	verifier, err := newParallelFileVerifier(nil, fileSize, c.Options.HashAlgorithm)
	if err != nil {
		return err
	}
	if len(jobs) > 1 {
		verifier = nil
	}
	state.mu.Lock()
	state.chunkRanges = nil
	state.bytesToTransfer = fileSize
	state.chunksRemaining = utils.ChunkRangesCount(nil, fileSize, models.TCP_BUFFER_SIZE/2)
	state.jobsRemaining = len(jobs)
	state.verifyDone = false
	state.verified = false
	state.retry = false
	state.closed = false
	state.verifier = verifier
	state.verifyPosition = 0
	state.mu.Unlock()
	c.hybridQueue.push(jobs...)
	return nil
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
	if err := validateParallelChunkRanges(request.CurrentFileChunkRanges, c.FilesToTransfer[index].Size); err != nil {
		return fmt.Errorf("invalid ranges for requested file %d: %w", index, err)
	}
	pathToFile := path.Join(c.FilesToTransfer[index].FolderSource, c.FilesToTransfer[index].Name)
	file, err := os.Open(pathToFile)
	if err != nil {
		return err
	}
	if c.peerHybridChunks {
		c.fileTransferMu.Lock()
		c.parallelFileMode = true
		c.hybridFileMode = true
		c.fileTransferMu.Unlock()
	}
	var bar *progressbar.ProgressBar
	if c.peerHybridChunks {
		bar = c.makeHybridSenderBar(index, request.FileTransferBytes, request.TransferTotalBytes)
	} else {
		bar = c.makeFileBar(index, request.CurrentFileChunkRanges)
	}
	state := &fileTransferState{
		fileIndex:           index,
		dataConnectionIndex: connectionIndex,
		file:                file,
		chunkRanges:         request.CurrentFileChunkRanges,
		chunkCount: utils.ChunkRangesCount(request.CurrentFileChunkRanges,
			c.FilesToTransfer[index].Size, models.TCP_BUFFER_SIZE/2),
		bar: bar,
	}
	c.fileTransferMu.Lock()
	if _, busy := c.senderTransfers[connectionIndex]; busy {
		c.fileTransferMu.Unlock()
		_ = file.Close()
		return fmt.Errorf("data connection %d already sending a file", connectionIndex)
	}
	if !c.peerHybridChunks {
		for _, active := range c.senderTransfers {
			if active.fileIndex == index {
				c.fileTransferMu.Unlock()
				_ = file.Close()
				return fmt.Errorf("file %d is already being sent", index)
			}
		}
	}
	c.parallelFileMode = true
	c.hybridFileMode = c.peerHybridChunks
	c.senderTransfers[connectionIndex] = state
	c.fileTransferMu.Unlock()
	c.setParallelActiveFile(connectionIndex, index)
	c.markTransferStarted()
	go c.sendParallelFileData(state, c.conn[connectionIndex+1], attempt)
	return nil
}

func (c *Client) finishParallelSenderFile(m message.Message) error {
	ack, hasLane, err := decodeParallelLaneAck(m)
	if err != nil {
		return err
	}
	fileIndex := m.Num
	c.fileTransferMu.Lock()
	var state *fileTransferState
	if hasLane {
		state = c.senderTransfers[ack.DataConnectionIndex]
		if state != nil && state.fileIndex == fileIndex {
			delete(c.senderTransfers, ack.DataConnectionIndex)
		} else {
			state = nil
		}
	} else {
		for connectionIndex, candidate := range c.senderTransfers {
			if candidate.fileIndex == fileIndex {
				state = candidate
				delete(c.senderTransfers, connectionIndex)
				break
			}
		}
	}
	c.fileTransferMu.Unlock()
	if state == nil {
		return fmt.Errorf("completion for inactive file %d", fileIndex)
	}
	if !hasLane || ack.FileComplete {
		c.markFileFinished(fileIndex)
	}
	c.clearParallelActiveFile(state.dataConnectionIndex, fileIndex)
	return c.sendControl(message.Message{Type: message.TypeCloseRecipient, Num: fileIndex, Bytes: m.Bytes})
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
	sendChunk := func(position int64) bool {
		if err := c.ctxErr(); err != nil {
			return false
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
				return false
			}
			encryptedBuffer = dataToSend
			if err := dataConn.Send(dataToSend); err != nil {
				if c.ctxErr() == nil {
					attempt.report(transferDisconnectError{err: err})
				}
				return false
			}
			c.addFileProgress(state.bar, int64(n))
			c.mutex.Lock()
			c.TotalSent += int64(n)
			c.TotalChunksTransferred++
			c.mutex.Unlock()
		}
		if readErr != nil && readErr != io.EOF {
			attempt.report(readErr)
			return false
		}
		return true
	}
	if len(state.chunkRanges) == 0 {
		for position := int64(0); position < fileSize; position += chunkSize {
			if !sendChunk(position) {
				return
			}
		}
		return
	}
	rangeChunkSize := state.chunkRanges[0]
	if rangeChunkSize <= 0 {
		attempt.report(fmt.Errorf("invalid chunk size %d for file %d", rangeChunkSize, state.fileIndex))
		return
	}
	for index := 1; index+1 < len(state.chunkRanges); index += 2 {
		start, count := state.chunkRanges[index], state.chunkRanges[index+1]
		for chunk := int64(0); chunk < count; chunk++ {
			position := start + chunk*rangeChunkSize
			if position >= fileSize {
				break
			}
			if !sendChunk(position) {
				return
			}
		}
	}
}
