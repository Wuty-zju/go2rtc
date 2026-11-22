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

	id       uint32
	childs   []*Node
	parent   *Node
	onClose  func()
	keyframe func()

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
	parent, childs, onClose := n.parent, n.childs, n.onClose
	n.parent, n.onClose = nil, nil
	n.mu.Unlock()
	if onClose != nil {
		onClose()
	}
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

// OnKeyframe sets the handler that asks the source of this node for a keyframe.
// The handler is called from RequestKeyframe and must not block.
func (n *Node) OnKeyframe(handler func()) {
	n.mu.Lock()
	n.keyframe = handler
	n.mu.Unlock()
}

// RequestKeyframe calls the keyframe handler of this node or of the nearest
// parent that has one. Without a handler it does nothing.
func (n *Node) RequestKeyframe() {
	for n != nil {
		n.mu.Lock()
		handler, parent := n.keyframe, n.parent
		n.mu.Unlock()

		if handler != nil {
			handler()
			return
		}
		n = parent
	}
}
