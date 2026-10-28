package core

import (
	"sync"

	"github.com/pion/rtp"
)

//type Packet struct {
//	Payload     []byte
//	Timestamp   uint32 // PTS if DTS == 0 else DTS
//	Composition uint32 // CTS = PTS-DTS (for support B-frames)
//	Sequence    uint16
//}

type Packet = rtp.Packet

// HandlerFunc - process input packets (just like http.HandlerFunc)
type HandlerFunc func(packet *Packet)

// Filter - a decorator for any HandlerFunc
type Filter func(handler HandlerFunc) HandlerFunc

// Node - Receiver or Sender or Filter (transform)
type Node struct {
	Codec  *Codec
	Input  HandlerFunc
	Output HandlerFunc

	id     uint32
	childs []*Node
	parent *Node

	mu sync.Mutex
}

func (n *Node) WithParent(parent *Node) *Node {
	parent.AppendChild(n)
	return n
}

func (n *Node) AppendChild(child *Node) {
	n.mu.Lock()
	// Readers keep a snapshot while invoking callbacks without the graph lock.
	childs := make([]*Node, len(n.childs)+1)
	copy(childs, n.childs)
	childs[len(n.childs)] = child
	child.mu.Lock()
	child.parent = n
	child.mu.Unlock()
	n.childs = childs
	n.mu.Unlock()
}

func (n *Node) RemoveChild(child *Node) {
	n.mu.Lock()
	for i, ch := range n.childs {
		if ch == child {
			childs := make([]*Node, len(n.childs)-1)
			copy(childs, n.childs[:i])
			copy(childs[i:], n.childs[i+1:])
			n.childs = childs
			break
		}
	}
	n.mu.Unlock()
}

func (n *Node) Close() {
	n.mu.Lock()
	parent, childs := n.parent, n.childs
	n.parent = nil
	n.mu.Unlock()
	if parent != nil {
		parent.RemoveChild(n)
		parent.mu.Lock()
		orphan := len(parent.childs) == 0 && parent.parent != nil
		parent.mu.Unlock()
		if orphan {
			parent.Close()
		}
	} else {
		for _, child := range childs {
			child.Close()
		}
	}
}

func MoveNode(dst, src *Node) {
	if dst == src {
		return
	}
	src.mu.Lock()
	childs := src.childs
	src.childs = nil
	src.mu.Unlock()

	dst.mu.Lock()
	// Preserve existing subscribers and skip children closed during the move.
	merged := make([]*Node, len(dst.childs), len(dst.childs)+len(childs))
	copy(merged, dst.childs)
	for _, child := range childs {
		child.mu.Lock()
		if child.parent == src {
			child.parent = dst
			merged = append(merged, child)
		}
		child.mu.Unlock()
	}
	dst.childs = merged
	dst.mu.Unlock()
}
