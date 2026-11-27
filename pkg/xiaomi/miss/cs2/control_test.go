package cs2

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFullControlQueueKeepsMedia(t *testing.T) {
	c, conn := newTestConn(t, true)
	command := []byte{0, 0, 0, 4, 1, 0, 0, 0}
	writeConn(t, conn, drwFrame(0, 0, bytes.Repeat(command, cap(c.channels[0].popBuf)+1)))
	require.Equal(t, []byte{magic, msgPing, 0, 0}, readConn(t, conn, 4))

	media := make([]byte, 4+hdrSize+1)
	binary.BigEndian.PutUint32(media, hdrSize+1)
	media[len(media)-1] = 42
	writeConn(t, conn, drwFrame(2, 0, media))
	_, payload, err := c.ReadPacket()
	require.NoError(t, err)
	require.Equal(t, []byte{42}, payload)
}

func TestReadCommandRejectsTruncatedHeader(t *testing.T) {
	c := &Conn{channels: [4]*dataChannel{newDataChannel(0, 1)}}
	c.channels[0].popBuf <- []byte{1}
	if _, _, err := c.ReadCommand(); err == nil {
		t.Fatal("truncated command accepted")
	}
}
