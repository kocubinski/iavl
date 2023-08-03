package iavl

import (
	"container/list"
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

type stdLru struct {
	l        *list.List
	capacity int
}

func newStdLru(capacity int) *stdLru {
	return &stdLru{
		l:        list.New(),
		capacity: capacity,
	}
}

func (lru *stdLru) PushFront(node *Node) {
	node.lruElement = lru.l.PushFront(node)
	if lru.capacity > -1 && lru.l.Len() > lru.capacity {
		back := lru.l.Back()
		lru.l.Remove(back)
		backNode := back.Value.(*Node)
		backNode.onEvict(backNode)
	}
}

func (lru *stdLru) MoveToFront(node *Node) {
	if node.lruElement == nil {
		panic("node not in lru")
	}
	lru.l.MoveToFront(node.lruElement)
}

func (lru *stdLru) Remove(node *Node) {
	if node.lruElement == nil {
		panic("node not in lru")
	}
	lru.l.Remove(node.lruElement)
}

type treeLru struct {
	head     *Node
	tail     *Node
	length   int
	capacity int
}

func (lru *treeLru) PushFront(node *Node) {
	if node.nodeKey != nil && node.nodeKey.String() == "(164528, 98)" {
		fmt.Printf("lru: seen! PushFront: %s ghost=%t\n", node.nodeKey, node.ghost)
	}

	if lru.head == nil {
		fmt.Printf("lru: PushFront: new head is %s ghost=%t\n", node.nodeKey, node.ghost)
		lru.head = node
		lru.tail = node
		// ensure prev is nil
		lru.tail.prev = nil
		lru.length = 1
		return
	}

	if lru.head == node {
		return
	}

	node.prev = lru.head
	lru.head.next = node
	lru.head = node
	// ensure next is nil at head
	lru.head.next = nil
	lru.length++

	// prune. capacity -1 means unbounded
	if lru.capacity > -1 && lru.length > lru.capacity {
		if lru.tail.onEvict == nil {
			panic("lru.tail.onEvict is nil")
		}
		lru.tail.onEvict(lru.tail)
		lru.tail = lru.tail.next
		lru.tail.prev = nil
		lru.length--
	}
}

func (lru *treeLru) MoveToFront(node *Node) {
	if node.nodeKey != nil && node.nodeKey.String() == "(164528, 98)" {
		fmt.Printf("lru: seen! PushFront: %s ghost=%t\n", node.nodeKey, node.ghost)
	}
	if node.prev == nil && node.next == nil {
		panic("node.prev and node.next are nil")
	}

	if lru.head == node {
		return
	}

	// not tail
	if node != lru.tail {
		if node.prev == nil {
			panic(fmt.Sprintf("node.prev is nil for %s", node.nodeKey))
		}
		node.prev.next = node.next
	} else {
		lru.tail = node.next
		lru.tail.prev = nil
	}

	node.next.prev = node.prev
	lru.head.next = node
	node.prev = lru.head
	node.next = nil
	lru.head = node
}

func (lru *treeLru) Unlink(node *Node) {
	if lru.head == node {
		lru.head = node.prev
	}

	if lru.tail == node {
		lru.tail = node.next
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
	node.onEvict(node)
	lru.length--
}

// PushFront encapsulates access to the LRU cache, both the main and ghost caches.
func (t *ImmutableTree) PushFront(node *Node) {
	if node.ghost {
		t.stdGhostLru.PushFront(node)
	} else {
		t.stdLru.PushFront(node)
	}
}

func (t *ImmutableTree) MoveToFront(node *Node) {
	if node.ghost {
		t.stdGhostLru.MoveToFront(node)
	} else {
		t.stdLru.MoveToFront(node)
	}
}

func (t *ImmutableTree) Unlink(node *Node) {
	if node.ghost {
		t.stdGhostLru.Remove(node)
	} else {
		t.stdLru.Remove(node)
	}
}

func (t *ImmutableTree) StdMergeGhosts() {
	maxGhosts := t.stdLru.capacity / 2
	if t.stdGhostLru.l.Len() == 0 {
		return
	}
	newLru := newStdLru(t.stdLru.capacity)

	lastPick := 0
	g := t.stdGhostLru.l.Front()
	m := t.stdLru.l.Front()
	var n *Node
	next := func() {
		if g == nil && m == nil {
			n = nil
			return
		}
		// look ahead
		if g == nil {
			n = m.Value.(*Node)
			m = m.Next()
			return
		}
		if m == nil {
			n = g.Value.(*Node)
			n.ghost = false
			g = g.Next()
			return
		}
		if lastPick == 0 {
			n = m.Value.(*Node)
			m = m.Next()
			lastPick = 1
		} else {
			n = g.Value.(*Node)
			n.ghost = false
			g = g.Next()
			lastPick = 0
		}
	}

	ghostCount := 0
	for next(); n != nil; next() {
		if n.ghost {
			if ghostCount > maxGhosts {
				n.onEvict(n)
			} else {
				newLru.PushFront(n)
			}
			n.ghost = false
			ghostCount++
		} else {
			newLru.PushFront(n)
		}
	}

	t.stdLru = newLru
	t.stdGhostLru = newStdLru(t.stdGhostLru.capacity)
}

func (t *ImmutableTree) MergeGhosts() {
	maxGhosts := t.lru.capacity / 3
	if t.ghostLru.length == 0 {
		return
	}
	newLru := &treeLru{capacity: t.lru.capacity}

	// merging iterator
	lastPick := 0
	g := t.ghostLru.head
	m := t.lru.head
	var n *Node
	next := func() {
		if g == nil && m == nil {
			n = nil
			return
		}
		// look ahead
		if g == nil {
			n = m
			m = m.prev
			return
		}
		if m == nil {
			n = g
			n.ghost = true
			g = g.prev
			return
		}
		if lastPick == 0 {
			n = g
			n.ghost = true
			g = g.prev
			lastPick = 1
		} else {
			n = m
			m = m.prev
			lastPick = 0
		}
	}

	i := 0
	var ghostCount int
	for next(); n != nil; next() {
		if n.ghost {
			if ghostCount > maxGhosts {
				n.onEvict(n)
			} else {
				newLru.PushFront(n)
				n.next = nil
			}
			n.ghost = false
			ghostCount++
		} else {
			newLru.PushFront(n)
			n.next = nil
		}
		i++
		if i%100_000_000 == 0 {
			fmt.Printf("i: %d\n", i)
		}
	}

	t.lru = newLru
	t.ghostLru = &treeLru{capacity: t.ghostLru.capacity}
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

func (node *Node) SetEvictLeft() {
	// node is the parent
	node.leftNode.onEvict = func(e *Node) {
		// if the child is still the same, remove the ref
		if e == nil && e == node.leftNode {
			node.leftNode = nil
		}
	}
}

func (node *Node) SetEvictRight() {
	// node is the parent
	node.rightNode.onEvict = func(e *Node) {
		if e == nil && e == node.rightNode {
			node.rightNode = nil
		}
	}
}
