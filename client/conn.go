package client

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lemon-mint/frameio"
	"github.com/lemon-mint/lemonmq/client/slowtable"
	"github.com/lemon-mint/lemonmq/client/types"
	"github.com/lemon-mint/lemonmq/packetpb"
	"google.golang.org/protobuf/proto"
)

type Conn struct {
	polling int32

	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer

	upstream *slowtable.Table

	fr frameio.FrameReader
	fw frameio.FrameWriter

	heartbeatStop chan struct{}

	mu sync.Mutex
}

func (c *Conn) Init(addr string, upstream *slowtable.Table) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}

	c.conn = conn
	c.r = bufio.NewReader(conn)
	c.w = bufio.NewWriter(conn)

	c.fr = frameio.NewFrameReader(c.r)
	c.fw = frameio.NewFrameWriter(c.w)

	c.upstream = upstream

	c.heartbeatStop = make(chan struct{})

	go func() {
		t := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-t.C:
				c.Ping()
			case <-c.heartbeatStop:
				t.Stop()
				return
			}
		}
	}()

	return nil
}

func (c *Conn) Ping() {
	p := packetpb.Packet{
		Type: packetpb.Packet_HEARTBEAT,
	}
	data, err := proto.Marshal(&p)
	if err != nil {
		return
	}
	err = c.fw.Write(data)
	if err != nil {
		c.Close()
		return
	}
	err = c.w.Flush()
	if err != nil {
		c.Close()
		return
	}
}

func (c *Conn) Poll() {
	if !atomic.CompareAndSwapInt32(&c.polling, 0, 1) {
		return
	}
	go func() {
		defer atomic.StoreInt32(&c.polling, 0)
		for {
			data, err := c.fr.Read()
			if err != nil {
				c.Close()
				return
			}
			p := packetpb.Message{}
			err = proto.Unmarshal(data, &p)
			if err != nil {
				c.Close()
				return
			}
			switch p.Type {
			case packetpb.Message_NORMAL:
				topic, ok := c.upstream.GetS(p.Topic)
				if !ok {
					continue
				}
				topic.Publish(&types.Message{
					Topic:     p.Topic,
					TimeStamp: p.Timestamp,
					ID:        p.Id,
					Body:      p.Payload,
				})
			case packetpb.Message_SERVER_DRAIN:
				c.Close()
			}
		}
	}()
}

func (c *Conn) Close() error {
	c.heartbeatStop <- struct{}{}
	c.mu.Lock()
	err := c.conn.Close()
	defer c.mu.Unlock()
	return err
}
