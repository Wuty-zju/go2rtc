package opus

import (
	"errors"
	"time"
)

var errFrames = errors.New("opus: invalid or non-20ms frames")

// Split20 extracts existing 20ms coded frames without decoding or resampling.
// Single 40/60ms coded frames cannot be split losslessly by this operation.
// Frame payloads are immutable; a code-0 packet can reuse the original buffer.
func Split20(packet []byte) ([][]byte, error) {
	invalid := errFrames
	if len(packet) == 0 || UnmarshalHeader(packet).FrameSize != 20*time.Millisecond {
		return nil, invalid
	}
	data := packet[1:]
	var storage [6]int
	sizes := storage[:0]
	readSize := func() (int, bool) {
		if len(data) == 0 {
			return 0, false
		}
		n := int(data[0])
		data = data[1:]
		if n >= 252 {
			if len(data) == 0 {
				return 0, false
			}
			n += 4 * int(data[0])
			data = data[1:]
		}
		return n, true
	}
	switch packet[0] & 3 {
	case 0:
		sizes = append(sizes, len(data))
	case 1:
		if len(data)%2 != 0 {
			return nil, invalid
		}
		sizes = append(sizes, len(data)/2, len(data)/2)
	case 2:
		n, ok := readSize()
		if !ok || n > len(data) {
			return nil, invalid
		}
		sizes = append(sizes, n, len(data)-n)
	case 3:
		if len(data) == 0 {
			return nil, invalid
		}
		control := data[0]
		data = data[1:]
		count := int(control & 63)
		if count == 0 || count > 6 {
			return nil, invalid
		}
		if control&64 != 0 {
			padding := 0
			for {
				if len(data) == 0 {
					return nil, invalid
				}
				n := int(data[0])
				data = data[1:]
				if n == 255 {
					padding += 254
				} else {
					padding += n
					break
				}
			}
			if padding > len(data) {
				return nil, invalid
			}
			data = data[:len(data)-padding]
		}
		if control&128 == 0 {
			if len(data)%count != 0 {
				return nil, invalid
			}
			for range count {
				sizes = append(sizes, len(data)/count)
			}
		} else {
			total := 0
			for i := 0; i < count-1; i++ {
				n, ok := readSize()
				if !ok {
					return nil, invalid
				}
				sizes = append(sizes, n)
				total += n
			}
			if total > len(data) {
				return nil, invalid
			}
			sizes = append(sizes, len(data)-total)
		}
	}
	total := 0
	for _, n := range sizes {
		if n < 0 || n > 1275 {
			return nil, invalid
		}
		total += n
	}
	if total != len(data) {
		return nil, invalid
	}
	if packet[0]&3 == 0 {
		return [][]byte{packet}, nil
	}
	frames := make([][]byte, len(sizes))
	buffer := make([]byte, total+len(sizes))
	for i, n := range sizes {
		frame := buffer[: n+1 : n+1]
		buffer = buffer[n+1:]
		frame[0] = packet[0] & 252
		copy(frame[1:], data[:n])
		data = data[n:]
		frames[i] = frame
	}
	if len(data) != 0 {
		return nil, invalid
	}
	return frames, nil
}
