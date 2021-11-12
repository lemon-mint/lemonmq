package client

import (
	"sync"

	"github.com/lemon-mint/lemonmq/client/slowtable"
	"github.com/lemon-mint/lemonmq/client/slowtable/nocopy"
	"github.com/lemon-mint/lemonmq/client/types"
)

type Client struct {
	subscriptions *slowtable.Table

	endpoint string

	mu    sync.Mutex
	cache map[string][]string

	conns map[string]*Conn
}

func NewClient(endpoint string) *Client {
	c := &Client{
		subscriptions: slowtable.NewTable(nil, 65536),
		endpoint:      endpoint,
		cache:         make(map[string][]string),
		conns:         make(map[string]*Conn),
	}
	return c
}

func (c *Client) Subscribe(topic string, f func(m *types.Message) (Unsubscribe bool)) error {
	// BEGIN TODO: get addr from endpoint
	c.mu.Lock()
	addr, ok := c.cache[topic]
	if !ok {
		addr = []string{c.endpoint}
		c.cache[topic] = addr
	}
	// END TODO

	conn, ok := c.conns[addr[0]]
	if !ok {
		conn = &Conn{}
		err := conn.Init(addr[0], c.subscriptions)
		if err != nil {
			c.mu.Unlock()
			return err
		}
		c.conns[addr[0]] = conn
	}
	c.mu.Unlock()
	conn.Poll()

retry:
	t, ok := c.subscriptions.GetS(topic)
	if !ok {
		c.subscriptions.CompareAndSwap(nocopy.S2B(topic), nil, &types.Topic{
			Name: topic,
		})
		goto retry
	}
	t.Subscribe(f)
	return nil
}
