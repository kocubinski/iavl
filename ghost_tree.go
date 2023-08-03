package iavl

import "bytes"

// GhostNode is an element of a GhostTree and optionally a LinkedList representing its LRU position.
type GhostNode struct {
	Node

	ghost bool
}

// GhostTree is an AVL tree which stages nodes in memory for insertion into a backing database.
// The expectation is that periodically the ghost nodes will be written to disk and the ghost
// bit cleared.
// An LRU cache is maintained for all living and ghost nodes, but the ghost nodes may never be
// evicted.  No matter how hard you try, you can't get rid of a ghost.
type GhostTree struct {
	root    *GhostNode
	mutants []*GhostNode

	lruHead *GhostNode
	lruTail *GhostNode
}

func (t *GhostTree) addMutant(node *GhostNode) {
	t.mutants = append(t.mutants, node)
}

func (t *GhostTree) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (t *GhostTree) Set(key []byte, value []byte) (updated bool, err error) {
	if t.root == nil {
		t.root = &GhostNode{Node: Node{key: key, value: value, ghost: true}}
		return false, nil
	}
	return t.set(t.root, key, value)
}

func (t *GhostTree) set(node *GhostNode, key []byte, value []byte) (updated bool, err error) {
	if node.isLeaf() {
		switch bytes.Compare(key, node.key) {
		case 0:
			node.value = value
			node.ghost = true
			return true, nil
		case -1:
		case 1:
		}
		return false, nil
	}

	return false, nil
}
