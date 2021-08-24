package types

import "sync"

type Chan struct {
	sync.RWMutex

	subscriber []chan *Message
}

func (c *Chan) Subscribe(ch chan *Message) {
	c.Lock()
	defer c.Unlock()
	for i := range c.subscriber {
		if c.subscriber[i] == nil {
			c.subscriber[i] = ch
			return
		}
	}
	c.subscriber = append(c.subscriber, ch)
}

func (c *Chan) Unsubscribe(ch chan *Message) {
	c.Lock()
	defer c.Unlock()
	for i := range c.subscriber {
		if c.subscriber[i] == ch {
			c.subscriber[i] = nil
		}
	}
}

func (c *Chan) Publish(m *Message) {
	c.RLock()
	defer c.RUnlock()
	for _, ch := range c.subscriber {
		if ch != nil {
			ch <- m
		}
	}
}
