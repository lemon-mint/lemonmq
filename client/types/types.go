package types

import (
	"sync"
)

type Topic struct {
	Name string

	sync.RWMutex

	subscriber []func(m *Message) (Unsubscribe bool)
}

type Message struct {
	Topic string

	TimeStamp int64
	ID        string

	Body []byte
}

func (c *Topic) Subscribe(f func(m *Message) (Unsubscribe bool)) {
	c.Lock()
	defer c.Unlock()
	for i := range c.subscriber {
		if c.subscriber[i] == nil {
			c.subscriber[i] = f
			return
		}
	}
	c.subscriber = append(c.subscriber, f)
}

func (c *Topic) Publish(m *Message) {
	c.RLock()
	defer c.RUnlock()
	for i := range c.subscriber {
		if c.subscriber[i] != nil {
			if c.subscriber[i](m) {
				c.subscriber[i] = nil
			}
		}
	}
}
