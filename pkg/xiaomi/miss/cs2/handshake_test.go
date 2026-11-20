package cs2

import (
	"bytes"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestHandshakeProbeKeepsItsDestination(t *testing.T) {
	// A reply from an earlier probe may already be waiting when a new phase starts.
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	listen := func() *net.UDPConn {
		conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		return conn
	}
	discovery, device, local := listen(), listen(), listen()
	conn := &udpConn{UDPConn: local, addr: discovery.LocalAddr().(*net.UDPAddr)}
	reply := make([]byte, 16)
	if _, err := device.WriteToUDP(reply, local.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	request := []byte{magic, msgLanSearch, 0, 0}
	if _, err := conn.WriteUntil(request, func([]byte) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if conn.addr.Port != device.LocalAddr().(*net.UDPAddr).Port {
		t.Fatal("negotiated port lost")
	}
	buf := make([]byte, len(request))
	if n, _, err := discovery.ReadFromUDP(buf); err != nil || !bytes.Equal(buf[:n], request) {
		t.Fatalf("discovery did not receive its probe: %v", err)
	}
}
