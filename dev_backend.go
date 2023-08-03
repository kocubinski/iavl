package iavl

import (
	"bytes"
	"fmt"
)

var _ NodeBackend = (*MapDB)(nil)

type MapDB struct {
	nodes map[[12]byte]*Node
	add   []*Node
	del   []*Node

	// simulation
	walBuf *bytes.Buffer
}

func NewMapDB() *MapDB {
	return &MapDB{
		nodes: make(map[[12]byte]*Node),
	}
}

func (m *MapDB) QueueNode(node *Node) error {
	// var nk [12]byte
	// copy(nk[:], node.nodeKey.GetKey())
	// m.nodes[nk] = node
	m.add = append(m.add, node)
	return nil
}

func (m *MapDB) QueueOrphan(node *Node) error {
	// var nk [12]byte
	// copy(nk[:], node.nodeKey.GetKey())
	// delete(m.nodes, nk)
	m.del = append(m.del, node)
	return nil
}

func (m *MapDB) Commit(version int64) error {
	for _, node := range m.add {
		var nk [12]byte
		copy(nk[:], node.nodeKey.GetKey())
		m.nodes[nk] = node
	}
	for _, node := range m.del {
		var nk [12]byte
		copy(nk[:], node.nodeKey.GetKey())
		delete(m.nodes, nk)
	}
	m.add = nil
	m.del = nil
	return nil
}

func (m *MapDB) GetNode(nodeKey []byte) (*Node, error) {
	var nk [12]byte
	copy(nk[:], nodeKey)
	n, ok := m.nodes[nk]
	if !ok {
		return nil, fmt.Errorf("MapDB: node not found")
	}
	return n, nil
}

var _ NodeBackend = (*NopBackend)(nil)

type NopBackend struct{}

func (n NopBackend) QueueNode(*Node) error { return nil }

func (n NopBackend) QueueOrphan(*Node) error { return nil }

func (n NopBackend) Commit(int64) error { return nil }

func (n NopBackend) GetNode([]byte) (*Node, error) { return nil, nil }
