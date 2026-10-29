package rtsp

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/tcp"
	"github.com/pion/rtp"
)

type Conn struct {
	core.Connection
	core.Listener

	// public

	Backchannel bool
	Media       string
	OnClose     func() error
	PacketSize  uint16
	SessionName string
	Timeout     int
	Transport   string // custom transport support, ex. RTSP over WebSocket

	URL *url.URL

	// internal

	auth      *tcp.Auth
	conn      net.Conn
	keepalive int
	mode      core.Mode
	playOK    atomic.Bool
	playErr   error
	reader    *bufio.Reader
	sequence  int
	session   string
	uri       string

	state   State
	stateMu sync.Mutex

	udpConn []*net.UDPConn
	udpAddr []*net.UDPAddr
}

const (
	ProtoRTSP      = "RTSP/1.0"
	MethodOptions  = "OPTIONS"
	MethodSetup    = "SETUP"
	MethodTeardown = "TEARDOWN"
	MethodDescribe = "DESCRIBE"
	MethodPlay     = "PLAY"
	MethodPause    = "PAUSE"
	MethodAnnounce = "ANNOUNCE"
	MethodRecord   = "RECORD"
)

type State byte

func (s State) String() string {
	switch s {
	case StateNone:
		return "NONE"
	case StateConn:
		return "CONN"
	case StateSetup:
		return MethodSetup
	case StatePlay:
		return MethodPlay
	}
	return strconv.Itoa(int(s))
}

const (
	StateNone State = iota
	StateConn
	StateSetup
	StatePlay
)

func (c *Conn) Handle() (err error) {
	c.stateMu.Lock()
	if c.mode == core.ModeActiveProducer && c.state != StatePlay {
		c.stateMu.Unlock()
		return nil
	}
	// Each playback loop owns its transport and track snapshot. Reconnect may
	// replace these fields while the previous reader is returning from Close.
	conn, reader := c.conn, c.reader
	receivers := slices.Clone(c.Receivers)
	udp := slices.Clone(c.udpConn)
	var timeout time.Duration

	switch c.mode {
	case core.ModeActiveProducer:
		var keepaliveDT time.Duration

		if c.keepalive > 5 {
			keepaliveDT = time.Duration(c.keepalive-5) * time.Second
		} else {
			keepaliveDT = 25 * time.Second
		}

		ctx, cancel := context.WithCancel(context.Background())
		go c.handleKeepalive(ctx, conn, keepaliveDT)
		defer cancel()

		if c.Timeout == 0 {
			// polling frames from remote RTSP Server (ex Camera)
			timeout = time.Second * 5

			if len(receivers) == 0 || c.Transport == "udp" {
				// if we only send audio to camera
				// https://github.com/AlexxIT/go2rtc/issues/659
				timeout += keepaliveDT
			}
		} else {
			timeout = time.Second * time.Duration(c.Timeout)
		}

	case core.ModePassiveProducer:
		// polling frames from remote RTSP Client (ex FFmpeg)
		if c.Timeout == 0 {
			timeout = time.Second * 15
		} else {
			timeout = time.Second * time.Duration(c.Timeout)
		}

	case core.ModePassiveConsumer:
		// pushing frames to remote RTSP Client (ex VLC)
		timeout = time.Second * 60

	default:
		c.stateMu.Unlock()
		return fmt.Errorf("wrong RTSP conn mode: %d", c.mode)
	}

	c.stateMu.Unlock()
	for i, socket := range udp {
		go c.handleUDPData(socket, byte(i), receivers)
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		if err = c.handleTCPData(conn, reader, receivers); err != nil {
			return
		}
	}
}

func (c *Conn) handleKeepalive(ctx context.Context, conn net.Conn, d time.Duration) {
	ticker := time.NewTicker(d)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.stateMu.Lock()
			if c.conn != conn {
				c.stateMu.Unlock()
				return
			}
			req := &tcp.Request{Method: MethodOptions, URL: c.URL}
			err := c.WriteRequest(req)
			c.stateMu.Unlock()
			if err != nil {
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Conn) handleUDPData(conn *net.UDPConn, channel byte, receivers []*core.Receiver) {
	// TODO: handle timeouts and drop TCP connection after any error

	for {
		// TP-Link Tapo camera has crazy 10000 bytes packet size
		buf := make([]byte, 10240)

		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		if err = c.handleRawPacket(channel, buf[:n], receivers); err != nil {
			return
		}
	}
}

func (c *Conn) handleTCPData(conn net.Conn, reader *bufio.Reader, receivers []*core.Receiver) error {
	// we can read:
	// 1. RTP interleaved: `$` + 1B channel number + 2B size
	// 2. RTSP response:   RTSP/1.0 200 OK
	// 3. RTSP request:    OPTIONS ...
	var buf4 []byte // `$` + 1B channel number + 2B size
	var err error

	buf4, err = reader.Peek(4)
	if err != nil {
		return err
	}

	var channel byte
	var size uint16

	if buf4[0] != '$' {
		switch string(buf4) {
		case "RTSP":
			var res *tcp.Response
			if res, err = tcp.ReadResponse(reader); err != nil {
				return err
			}
			c.Fire(res)
			// for playing backchannel only after OK response on play
			c.stateMu.Lock()
			if c.conn == conn {
				c.playOK.Store(true)
			}
			c.stateMu.Unlock()
			return nil

		case "OPTI", "TEAR", "DESC", "SETU", "PLAY", "PAUS", "RECO", "ANNO", "GET_", "SET_":
			var req *tcp.Request
			if req, err = tcp.ReadRequest(reader); err != nil {
				return err
			}
			c.Fire(req)
			if req.Method == MethodOptions {
				res := &tcp.Response{Request: req}
				c.stateMu.Lock()
				if c.conn == conn {
					err = c.WriteResponse(res)
				}
				c.stateMu.Unlock()
				if err != nil {
					return err
				}
			}
			return nil

		default:
			c.Fire("RTSP wrong input")

			for i := 0; ; i++ {
				// search next start symbol
				if _, err = reader.ReadBytes('$'); err != nil {
					return err
				}

				if channel, err = reader.ReadByte(); err != nil {
					return err
				}

				// TODO: better check maximum good channel ID
				if channel >= 20 {
					continue
				}

				buf4 = make([]byte, 2)
				if _, err = io.ReadFull(reader, buf4); err != nil {
					return err
				}

				// check if size good for RTP
				size = binary.BigEndian.Uint16(buf4)
				if size <= 1500 {
					break
				}

				// 10 tries to find good packet
				if i >= 10 {
					return fmt.Errorf("RTSP wrong input")
				}
			}
		}
	} else {
		// hope that the odd channels are always RTCP
		channel = buf4[1]

		// get data size
		size = binary.BigEndian.Uint16(buf4[2:])

		// skip 4 bytes from reader.Peek
		if _, err = reader.Discard(4); err != nil {
			return err
		}
	}

	// init memory for data
	buf := make([]byte, size)
	if _, err = io.ReadFull(reader, buf); err != nil {
		return err
	}

	c.Recv += int(size)

	return c.handleRawPacket(channel, buf, receivers)
}

func (c *Conn) handleRawPacket(channel byte, buf []byte, receivers []*core.Receiver) error {
	if channel&1 == 0 {
		packet := &rtp.Packet{}
		if err := packet.Unmarshal(buf); err != nil {
			return err
		}

		for _, receiver := range receivers {
			if receiver.ID == channel {
				receiver.WriteRTP(packet)
				break
			}
		}
	} else {
		msg := &RTCP{Channel: channel}

		if err := msg.Header.Unmarshal(buf); err != nil {
			return nil
		}

		//var err error
		//msg.Packets, err = rtcp.Unmarshal(buf)
		//if err != nil {
		//	return nil
		//}

		c.Fire(msg)
	}

	return nil
}

func (c *Conn) WriteRequest(req *tcp.Request) error {
	if req.Proto == "" {
		req.Proto = ProtoRTSP
	}

	if req.Header == nil {
		req.Header = make(map[string][]string)
	}

	c.sequence++
	// important to send case sensitive CSeq
	// https://github.com/AlexxIT/go2rtc/issues/7
	req.Header["CSeq"] = []string{strconv.Itoa(c.sequence)}

	c.auth.Write(req)

	if c.session != "" {
		req.Header.Set("Session", c.session)
	}

	if req.Body != nil {
		val := strconv.Itoa(len(req.Body))
		req.Header.Set("Content-Length", val)
	}

	c.Fire(req)

	if err := c.conn.SetWriteDeadline(time.Now().Add(Timeout)); err != nil {
		return err
	}

	return req.Write(c.conn)
}

func (c *Conn) ReadRequest() (*tcp.Request, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(Timeout)); err != nil {
		return nil, err
	}
	return tcp.ReadRequest(c.reader)
}

func (c *Conn) WriteResponse(res *tcp.Response) error {
	if res.Proto == "" {
		res.Proto = ProtoRTSP
	}

	if res.Status == "" {
		res.Status = "200 OK"
	}

	if res.Header == nil {
		res.Header = make(map[string][]string)
	}

	if res.Request != nil && res.Request.Header != nil {
		seq := res.Request.Header.Get("CSeq")
		if seq != "" {
			res.Header.Set("CSeq", seq)
		}
	}

	if c.session != "" {
		if res.Request != nil && res.Request.Method == MethodSetup {
			res.Header.Set("Session", c.session+";timeout=60")
		} else {
			res.Header.Set("Session", c.session)
		}
	}

	if res.Body != nil {
		val := strconv.Itoa(len(res.Body))
		res.Header.Set("Content-Length", val)
	}

	c.Fire(res)

	if err := c.conn.SetWriteDeadline(time.Now().Add(Timeout)); err != nil {
		return err
	}

	return res.Write(c.conn)
}

func (c *Conn) ReadResponse() (*tcp.Response, error) {
	if err := c.conn.SetReadDeadline(time.Now().Add(Timeout)); err != nil {
		return nil, err
	}
	return tcp.ReadResponse(c.reader)
}
