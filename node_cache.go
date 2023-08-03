package iavl

import (
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"
)

type nodeCacheKey [12]byte

type NodeCache interface {
	Add(nodeCacheKey, *Node)
	Remove(nodeCacheKey)
	Size() int
	Get(nodeCacheKey) (*Node, bool)
}

var _ NodeCache = (*NodeLruCache)(nil)

type NodeLruCache struct {
	cache *lru.Cache[nodeCacheKey, *Node]
}

func (c *NodeLruCache) Add(nk nodeCacheKey, node *Node) {
	c.cache.Add(nk, node)
}

func (c *NodeLruCache) Get(nk nodeCacheKey) (*Node, bool) {
	return c.cache.Get(nk)
}

func (c *NodeLruCache) Remove(nk nodeCacheKey) {
	c.cache.Remove(nk)
}

func (c *NodeLruCache) Size() int {
	return c.cache.Len()
}

func NewNodeLruCache(size int) *NodeLruCache {
	cache, err := lru.New[nodeCacheKey, *Node](size)
	if err != nil {
		panic(err)
	}
	return &NodeLruCache{
		cache: cache,
	}
}

var _ NodeCache = (*NodeMapCache)(nil)

type NodeMapCache struct {
	cache map[nodeCacheKey]*Node
}

func (c *NodeMapCache) Add(nk nodeCacheKey, node *Node) {
	c.cache[nk] = node
}

func (c *NodeMapCache) Get(nk nodeCacheKey) (*Node, bool) {
	node, ok := c.cache[nk]
	return node, ok
}

func (c *NodeMapCache) Remove(nk nodeCacheKey) {
	delete(c.cache, nk)
}

func (c *NodeMapCache) Size() int {
	return len(c.cache)
}

func (t *ImmutableTree) PushFront(node *Node) {
	//if node.onEvict == nil {
	//	panic("node.onEvict is nil")
	//}

	if t.lruHead == nil {
		t.lruHead = node
		t.lruTail = node
		t.lruLength = 1
		return
	}

	if t.lruHead == node {
		return
	}

	node.prev = t.lruHead
	t.lruHead.next = node
	t.lruHead = node
	t.lruLength++

	// prune
	if t.lruLength > t.lruSize {
		n := t.lruTail
		i := 0
		for ; n != nil && i <= 1_000; n = n.next {
			// never evict ghost nodes
			if !n.ghost {
				break
			}
			i++
			if i == 1_000 {
				fmt.Printf("prune loop exceeded 1_000 iterations, this should never happen\n")
			}
		}
		if n.prev != nil {
			n.prev.next = n.next
		}
		if n.next != nil {
			n.next.prev = n.prev
		}
		if n.onEvict == nil {
			panic("node.onEvict is nil in PushFront")
		}
		n.onEvict()
		if t.lruTail == n {
			t.lruTail = n.next
		}
		t.lruLength--
	}
}

func (t *ImmutableTree) MoveToFront(node *Node) {
	if node.onEvict == nil {
		panic("node.onEvict is nil")
	}
	if node.prev == nil && node.next == nil {
		panic("node.prev and node.next are nil")
	}

	if t.lruHead == node {
		return
	}

	// not tail
	if node != t.lruTail {
		node.prev.next = node.next
	} else {
		t.lruTail = node.next
		t.lruTail.prev = nil
	}

	node.next.prev = node.prev
	t.lruHead.next = node
	node.prev = t.lruHead
	node.next = nil
	t.lruHead = node
}

func (t *ImmutableTree) Unlink(node *Node) {
	if node.onEvict == nil {
		panic("node.onEvict is nil")
	}

	if t.lruHead == node {
		t.lruHead = node.prev
	}

	if t.lruTail == node {
		t.lruTail = node.next
	}

	if node.prev != nil {
		node.prev.next = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	}

	node.prev = nil
	node.next = nil
	// unsure if this callback is needed on remove
	node.onEvict()
	t.lruLength--
}

// TODO: this func may need parent and isLeftNode args to construct onEvict callback
func (t *ImmutableTree) NewGhostNode(key []byte, value []byte) *Node {
	node := &Node{
		key:           key,
		value:         value,
		subtreeHeight: 0,
		size:          1,
		ghost:         true,
	}

	t.PushFront(node)
	return node
}
