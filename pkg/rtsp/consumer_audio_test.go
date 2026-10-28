package rtsp

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type audioConn struct {
	net.Conn
	writes [][]byte
}

func (c *audioConn) Write(data []byte) (int, error) {
	c.writes = append(c.writes, append([]byte(nil), data...))
	return len(data), nil
}
func (c *audioConn) SetWriteDeadline(time.Time) error { return nil }

func TestConsumersShareAudioPacket(t *testing.T) {
	codec := &core.Codec{Name: core.CodecOpus, ClockRate: 48000, PayloadType: 96}
	packet := &core.Packet{Payload: []byte{1, 2, 3}}
	var workers sync.WaitGroup
	for range 2 {
		transport := &audioConn{}
		conn := &Conn{conn: transport, state: StatePlay}
		conn.playOK.Store(true)
		write := conn.packetWriter(codec, 0, 96)
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 100 {
				write(packet)
			}
			if len(transport.writes) != 100 || transport.writes[0][5]&0x80 == 0 {
				t.Error("outgoing audio packets must carry the marker")
			}
		}()
	}
	workers.Wait()
	if packet.Marker {
		t.Fatal("a consumer modified the shared source packet")
	}
}
