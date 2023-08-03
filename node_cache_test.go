package iavl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCache(t *testing.T) {
	lru := &treeLru{capacity: 10}
	tree := &ImmutableTree{lru: lru}

	assertPointers := func(expectedCount int) {
		count := 0
		for n := lru.tail; n != nil; n = n.next {
			count++
			if count > expectedCount {
				t.Fatal("expectedCount exceeded")
			}
		}
		require.Equal(t, expectedCount, count)
	}
	purge := func() {
		for n := lru.tail; n != nil; {
			next := n.next
			n.prev = nil
			n.next = nil
			n = next
		}
		lru.head = nil
		lru.tail = nil
		lru.length = 0
	}

	n1 := &Node{key: []byte("1"), onEvict: func() {
		fmt.Println("evict 1")
	}}
	n2 := &Node{key: []byte("2"), onEvict: func() {
		fmt.Println("evict 2")
	}}
	n3 := &Node{key: []byte("3"), onEvict: func() {
		fmt.Println("evict 3")
	}}
	n4 := &Node{key: []byte("4"), onEvict: func() {
		fmt.Println("evict 4")
	}}
	n5 := &Node{key: []byte("5"), onEvict: func() {
		fmt.Println("evict 5")
	}}
	tree.PushFront(n1)
	tree.PushFront(n2)
	tree.PushFront(n3)
	tree.PushFront(n4)
	tree.PushFront(n5)
	assertPointers(5)
	require.Equal(t, 5, lru.length)
	require.Equal(t, n5, lru.head)
	require.Equal(t, n1, lru.tail)

	tree.Unlink(n3)
	require.Equal(t, 4, lru.length)
	assertPointers(4)

	tree.PushFront(n3)
	assertPointers(5)
	require.Equal(t, 5, lru.length)
	require.Equal(t, n3, lru.head)

	tree.MoveToFront(n2)
	require.Equal(t, 5, lru.length)
	require.Equal(t, n2, lru.head)
	assertPointers(5)

	tree.MoveToFront(lru.tail)
	assertPointers(5)

	tree.MoveToFront(lru.head)
	assertPointers(5)

	// round 2
	purge()

	tree.PushFront(n1)
	tree.PushFront(n2)
	tree.PushFront(n3)
	tree.PushFront(n4)
	tree.PushFront(n5)
	assertPointers(5)

	tree.MoveToFront(n2)
	assertPointers(5)

	tree.MoveToFront(n4)
	assertPointers(5)
}
