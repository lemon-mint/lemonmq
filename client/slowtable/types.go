package slowtable

import (
	"sync"

	"github.com/lemon-mint/lemonmq/client/types"
)

type value struct {
	size int64
	mu   sync.RWMutex

	next *item
}

type item struct {
	key  []byte
	val  *types.Topic
	next *item
}

type Table struct {
	entries []value
	hash    func([]byte) uint64
	keysize uint64

	itempool sync.Pool
}
