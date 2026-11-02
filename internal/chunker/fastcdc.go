package chunker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"

	"github.com/mirror-sync/mirror/internal/model"
)

const (
	// DefaultMinSize is the minimum FastCDC chunk size (16 KB).
	DefaultMinSize = 16 * 1024
	// DefaultAvgSize is the target average FastCDC chunk size (64 KB).
	DefaultAvgSize = 64 * 1024
	// DefaultMaxSize is the maximum FastCDC chunk size (256 KB).
	DefaultMaxSize = 256 * 1024
)

// Options configures the FastCDC chunk size boundaries.
type Options struct {
	MinSize int
	AvgSize int
	MaxSize int
}

// DefaultOptions returns the standard FastCDC parameters (16KB min, 64KB avg, 256KB max).
func DefaultOptions() Options {
	return Options{
		MinSize: DefaultMinSize,
		AvgSize: DefaultAvgSize,
		MaxSize: DefaultMaxSize,
	}
}

// Chunker performs content-defined chunking using FastCDC and BLAKE3.
type Chunker struct {
	opts  Options
	maskS uint64 // Stricter mask used before AvgSize (more 1-bits)
	maskL uint64 // Looser mask used after AvgSize (fewer 1-bits)
}

var defaultChunker = mustNewChunker(DefaultOptions())

func mustNewChunker(opts Options) *Chunker {
	c, err := New(opts)
	if err != nil {
		panic(err)
	}
	return c
}

// New creates a new Chunker with the given Options.
// If all fields in opts are zero, DefaultOptions() are used.
func New(opts Options) (*Chunker, error) {
	if opts.MinSize == 0 && opts.AvgSize == 0 && opts.MaxSize == 0 {
		opts = DefaultOptions()
	}
	if opts.MinSize <= 0 {
		return nil, errors.New("chunker: MinSize must be > 0")
	}
	if opts.AvgSize < opts.MinSize {
		return nil, errors.New("chunker: AvgSize must be >= MinSize")
	}
	if opts.MaxSize < opts.AvgSize {
		return nil, errors.New("chunker: MaxSize must be >= AvgSize")
	}

	// Compute logarithm base 2 of AvgSize to determine the baseline mask bit count.
	avgBits := bits.Len(uint(opts.AvgSize)) - 1
	if avgBits < 2 {
		avgBits = 2
	}
	if avgBits > 30 {
		avgBits = 30
	}

	bitsS := avgBits + 1
	bitsL := avgBits - 1

	return &Chunker{
		opts:  opts,
		maskS: makeNormalizedMask(bitsS),
		maskL: makeNormalizedMask(bitsL),
	}, nil
}

// makeNormalizedMask constructs a 64-bit mask with numBits 1-bits positioned in the upper bits
// of the 64-bit word so that every masked bit depends on a deep sliding window of input bytes.
func makeNormalizedMask(numBits int) uint64 {
	if numBits <= 0 {
		return 0
	}
	if numBits >= 64 {
		return ^uint64(0)
	}
	// Place the mask bits near the top of the 64-bit gear hash word (ending at bit 62)
	// so all tested bits incorporate a sliding window of ~32..63 bytes.
	shift := 62 - numBits
	if shift < 0 {
		shift = 0
	}
	return ((uint64(1) << numBits) - 1) << shift
}

// gearTable is a deterministic 256-entry lookup table of pseudo-random 64-bit integers
// generated via SplitMix64 with a fixed seed for the rolling Gear hash.
var gearTable [256]uint64

func init() {
	state := uint64(0x243F6A8885A308D3) // Fractional part of pi
	for i := range gearTable {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		z = z ^ (z >> 31)
		gearTable[i] = z
	}
}

// ChunkReader splits r into content-defined chunks using the default FastCDC parameters
// and hashes each chunk as well as the complete stream using BLAKE3.
func ChunkReader(r io.Reader) (blocks []model.BlockInfo, fileHash [32]byte, totalSize int64, err error) {
	return defaultChunker.ChunkReader(r)
}

// ChunkFile opens the file at path and splits it into content-defined chunks using the
// default FastCDC parameters.
func ChunkFile(path string) (blocks []model.BlockInfo, fileHash [32]byte, totalSize int64, err error) {
	return defaultChunker.ChunkFile(path)
}

// ChunkFile opens the file at path and splits it into content-defined chunks using c's parameters.
func (c *Chunker) ChunkFile(path string) (blocks []model.BlockInfo, fileHash [32]byte, totalSize int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, [32]byte{}, 0, fmt.Errorf("open file %q for chunking: %w", path, err)
	}
	defer f.Close()

	return c.ChunkReader(f)
}

// ChunkReader splits r into content-defined chunks using c's FastCDC parameters
// and computes the BLAKE3 hash for each chunk and the entire stream.
func (c *Chunker) ChunkReader(r io.Reader) (blocks []model.BlockInfo, fileHash [32]byte, totalSize int64, err error) {
	buf := make([]byte, c.opts.MaxSize)
	buffered := 0
	eof := false

	fileHasher := newBlake3Hasher()
	var offset int64

	for {
		// Top up buffer to MaxSize unless EOF has already been reached.
		for buffered < c.opts.MaxSize && !eof {
			n, readErr := r.Read(buf[buffered:c.opts.MaxSize])
			if n > 0 {
				fileHasher.Write(buf[buffered : buffered+n])
				buffered += n
				totalSize += int64(n)
			}
			if readErr != nil {
				if errors.Is(readErr, io.EOF) {
					eof = true
					break
				}
				return nil, [32]byte{}, totalSize, readErr
			}
		}

		if buffered == 0 {
			break
		}

		cut := c.findCutPoint(buf[:buffered])
		chunkData := buf[:cut]
		chunkHash := HashBytes(chunkData)

		blocks = append(blocks, model.BlockInfo{
			Index:  len(blocks),
			Offset: offset,
			Size:   int32(cut),
			Hash:   chunkHash,
		})

		offset += int64(cut)
		if cut < buffered {
			copy(buf, buf[cut:buffered])
		}
		buffered -= cut
	}

	fileHash = fileHasher.Sum256()
	return blocks, fileHash, totalSize, nil
}

// findCutPoint returns the chunk length (1 <= cut <= len(buf)) using FastCDC normalized chunking.
func (c *Chunker) findCutPoint(buf []byte) int {
	n := len(buf)
	if n <= c.opts.MinSize {
		return n
	}

	normalSize := c.opts.AvgSize
	if normalSize > n {
		normalSize = n
	}

	var fp uint64
	// FastCDC sub-minimum chunk skipping: warm up the 64-byte rolling gear window
	// over the 64 bytes immediately preceding MinSize. Because the 64-bit gear hash
	// shifts left by 1 per byte (modulo 2^64), bytes older than 64 positions are
	// completely shifted out and have zero influence on fp.
	warmupStart := c.opts.MinSize - 64
	if warmupStart < 0 {
		warmupStart = 0
	}
	for i := warmupStart; i < c.opts.MinSize; i++ {
		fp = (fp << 1) + gearTable[buf[i]]
	}

	maskS := c.maskS
	for i := c.opts.MinSize; i < normalSize; i++ {
		fp = (fp << 1) + gearTable[buf[i]]
		if (fp & maskS) == 0 {
			return i + 1
		}
	}

	maskL := c.maskL
	for i := normalSize; i < n; i++ {
		fp = (fp << 1) + gearTable[buf[i]]
		if (fp & maskL) == 0 {
			return i + 1
		}
	}

	return n
}

// HashBytes computes the 32-byte BLAKE3 cryptographic hash of data.
func HashBytes(data []byte) [32]byte {
	h := newBlake3Hasher()
	h.Write(data)
	return h.Sum256()
}

// ---------------------------------------------------------------------------
// BLAKE3 Reference-Compliant Implementation (256-bit default output)
// ---------------------------------------------------------------------------

const (
	blake3BlockLen = 64
	blake3ChunkLen = 1024

	flagChunkStart = 1 << 0
	flagChunkEnd   = 1 << 1
	flagParent     = 1 << 2
	flagRoot       = 1 << 3
)

var blake3IV = [8]uint32{
	0x6A09E667, 0xBB67AE85, 0x3C6EF372, 0xA54FF53A,
	0x510E527F, 0x9B05688C, 0x1F83D9AB, 0x5BE0CD19,
}

type blake3Output struct {
	inputChainingValue [8]uint32
	blockWords         [16]uint32
	counter            uint64
	blockLen           uint32
	flags              uint32
}

func (o blake3Output) chainingValue() [8]uint32 {
	out16 := blake3Compress(o.inputChainingValue, o.blockWords, o.counter, o.blockLen, o.flags)
	return [8]uint32{
		out16[0], out16[1], out16[2], out16[3],
		out16[4], out16[5], out16[6], out16[7],
	}
}

func (o blake3Output) rootOutputBytes() [32]byte {
	words := blake3Compress(o.inputChainingValue, o.blockWords, 0, o.blockLen, o.flags|flagRoot)
	var out [32]byte
	for i := 0; i < 8; i++ {
		binary.LittleEndian.PutUint32(out[i*4:(i+1)*4], words[i])
	}
	return out
}

type blake3ChunkState struct {
	chainingValue    [8]uint32
	chunkCounter     uint64
	block            [blake3BlockLen]byte
	blockLen         uint8
	blocksCompressed uint8
	flags            uint32
}

func newBlake3ChunkState(key [8]uint32, chunkCounter uint64, flags uint32) blake3ChunkState {
	return blake3ChunkState{
		chainingValue: key,
		chunkCounter:  chunkCounter,
		flags:         flags,
	}
}

func (cs *blake3ChunkState) len() int {
	return blake3BlockLen*int(cs.blocksCompressed) + int(cs.blockLen)
}

func (cs *blake3ChunkState) startFlag() uint32 {
	if cs.blocksCompressed == 0 {
		return flagChunkStart
	}
	return 0
}

func (cs *blake3ChunkState) update(input []byte) {
	for len(input) > 0 {
		if cs.blockLen == blake3BlockLen {
			var blockWords [16]uint32
			bytesToWords(&cs.block, &blockWords)
			out16 := blake3Compress(
				cs.chainingValue,
				blockWords,
				cs.chunkCounter,
				blake3BlockLen,
				cs.flags|cs.startFlag(),
			)
			cs.chainingValue = [8]uint32{
				out16[0], out16[1], out16[2], out16[3],
				out16[4], out16[5], out16[6], out16[7],
			}
			cs.blocksCompressed++
			cs.block = [blake3BlockLen]byte{}
			cs.blockLen = 0
		}

		want := blake3BlockLen - int(cs.blockLen)
		take := len(input)
		if take > want {
			take = want
		}
		copy(cs.block[cs.blockLen:], input[:take])
		cs.blockLen += uint8(take)
		input = input[take:]
	}
}

func (cs *blake3ChunkState) output() blake3Output {
	var blockWords [16]uint32
	bytesToWords(&cs.block, &blockWords)
	return blake3Output{
		inputChainingValue: cs.chainingValue,
		blockWords:         blockWords,
		counter:            cs.chunkCounter,
		blockLen:           uint32(cs.blockLen),
		flags:              cs.flags | cs.startFlag() | flagChunkEnd,
	}
}

func parentOutput(leftChildCV, rightChildCV [8]uint32, key [8]uint32, flags uint32) blake3Output {
	var blockWords [16]uint32
	for i := 0; i < 8; i++ {
		blockWords[i] = leftChildCV[i]
		blockWords[i+8] = rightChildCV[i]
	}
	return blake3Output{
		inputChainingValue: key,
		blockWords:         blockWords,
		counter:            0,
		blockLen:           blake3BlockLen,
		flags:              flags | flagParent,
	}
}

type blake3Hasher struct {
	chunkState blake3ChunkState
	key        [8]uint32
	cvStack    [54][8]uint32
	cvStackLen uint8
	flags      uint32
}

func newBlake3Hasher() *blake3Hasher {
	return &blake3Hasher{
		chunkState: newBlake3ChunkState(blake3IV, 0, 0),
		key:        blake3IV,
	}
}

func (h *blake3Hasher) pushStack(cv [8]uint32) {
	h.cvStack[h.cvStackLen] = cv
	h.cvStackLen++
}

func (h *blake3Hasher) popStack() [8]uint32 {
	h.cvStackLen--
	return h.cvStack[h.cvStackLen]
}

func (h *blake3Hasher) addChunkChainingValue(newCV [8]uint32, totalChunks uint64) {
	for totalChunks&1 == 0 {
		newCV = parentOutput(h.popStack(), newCV, h.key, h.flags).chainingValue()
		totalChunks >>= 1
	}
	h.pushStack(newCV)
}

func (h *blake3Hasher) Write(p []byte) {
	for len(p) > 0 {
		if h.chunkState.len() == blake3ChunkLen {
			chunkCV := h.chunkState.output().chainingValue()
			totalChunks := h.chunkState.chunkCounter + 1
			h.addChunkChainingValue(chunkCV, totalChunks)
			h.chunkState = newBlake3ChunkState(h.key, totalChunks, h.flags)
		}

		want := blake3ChunkLen - h.chunkState.len()
		take := len(p)
		if take > want {
			take = want
		}
		h.chunkState.update(p[:take])
		p = p[take:]
	}
}

func (h *blake3Hasher) Sum256() [32]byte {
	output := h.chunkState.output()
	parentNodesRemaining := int(h.cvStackLen)
	for parentNodesRemaining > 0 {
		parentNodesRemaining--
		output = parentOutput(h.cvStack[parentNodesRemaining], output.chainingValue(), h.key, h.flags)
	}
	return output.rootOutputBytes()
}

func bytesToWords(b *[64]byte, w *[16]uint32) {
	for i := 0; i < 16; i++ {
		w[i] = binary.LittleEndian.Uint32(b[i*4 : (i+1)*4])
	}
}

func blake3G(s *[16]uint32, a, b, c, d int, mx, my uint32) {
	s[a] = s[a] + s[b] + mx
	s[d] = bits.RotateLeft32(s[d]^s[a], -16)
	s[c] = s[c] + s[d]
	s[b] = bits.RotateLeft32(s[b]^s[c], -12)
	s[a] = s[a] + s[b] + my
	s[d] = bits.RotateLeft32(s[d]^s[a], -8)
	s[c] = s[c] + s[d]
	s[b] = bits.RotateLeft32(s[b]^s[c], -7)
}

func blake3Round(s *[16]uint32, m *[16]uint32) {
	// Mix columns
	blake3G(s, 0, 4, 8, 12, m[0], m[1])
	blake3G(s, 1, 5, 9, 13, m[2], m[3])
	blake3G(s, 2, 6, 10, 14, m[4], m[5])
	blake3G(s, 3, 7, 11, 15, m[6], m[7])
	// Mix diagonals
	blake3G(s, 0, 5, 10, 15, m[8], m[9])
	blake3G(s, 1, 6, 11, 12, m[10], m[11])
	blake3G(s, 2, 7, 8, 13, m[12], m[13])
	blake3G(s, 3, 4, 9, 14, m[14], m[15])
}

func blake3Compress(cv [8]uint32, m [16]uint32, counter uint64, blockLen uint32, flags uint32) [16]uint32 {
	s := [16]uint32{
		cv[0], cv[1], cv[2], cv[3],
		cv[4], cv[5], cv[6], cv[7],
		blake3IV[0], blake3IV[1], blake3IV[2], blake3IV[3],
		uint32(counter), uint32(counter >> 32), blockLen, flags,
	}

	for r := 0; r < 7; r++ {
		blake3Round(&s, &m)
		if r < 6 {
			m = [16]uint32{
				m[2], m[6], m[3], m[10], m[7], m[0], m[4], m[13],
				m[1], m[11], m[12], m[5], m[9], m[14], m[15], m[8],
			}
		}
	}

	for i := 0; i < 8; i++ {
		s[i] ^= s[i+8]
		s[i+8] ^= cv[i]
	}
	return s
}
