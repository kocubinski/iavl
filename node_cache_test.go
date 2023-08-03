package iavl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeCache(t *testing.T) {
	tree := &ImmutableTree{lruSize: 10}

	assertPointers := func(expectedCount int) {
		count := 0
		for n := tree.lruTail; n != nil; n = n.next {
			count++
			if count > expectedCount {
				t.Fatal("expectedCount exceeded")
			}
		}
		require.Equal(t, expectedCount, count)
	}
	purge := func() {
		for n := tree.lruTail; n != nil; {
			next := n.next
			n.prev = nil
			n.next = nil
			n = next
		}
		tree.lruHead = nil
		tree.lruTail = nil
		tree.lruLength = 0
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
	require.Equal(t, 5, tree.lruLength)
	require.Equal(t, n5, tree.lruHead)
	require.Equal(t, n1, tree.lruTail)

	tree.Unlink(n3)
	require.Equal(t, 4, tree.lruLength)
	assertPointers(4)

	tree.PushFront(n3)
	assertPointers(5)
	require.Equal(t, 5, tree.lruLength)
	require.Equal(t, n3, tree.lruHead)

	tree.MoveToFront(n2)
	require.Equal(t, 5, tree.lruLength)
	require.Equal(t, n2, tree.lruHead)
	assertPointers(5)

	tree.MoveToFront(tree.lruTail)
	assertPointers(5)

	tree.MoveToFront(tree.lruHead)
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
