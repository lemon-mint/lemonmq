package lemonmq

import (
	"net"

	"github.com/lemon-mint/lemonmq/ringbuffer"
	"github.com/lemon-mint/lemonmq/slowtable"
	"github.com/lemon-mint/lemonmq/slowtable/nocopy"
	"github.com/lemon-mint/lemonmq/types"
)

type Server struct {
	ln net.Listener

	Topic *slowtable.Table

	handleConn func(net.Conn)

	msgq *ringbuffer.RingBuffer
}

func NewServer() *Server {
	msgq := ringbuffer.NewRingBuffer(65535)
	s := &Server{
		msgq: msgq,
	}
	s.Topic = slowtable.NewTable(nil, 65536)
	return s
}

func (s *Server) Serve() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) Close() {
	s.ln.Close()
}

func (s *Server) Subscribe(topic string) chan *types.Message {
retry:
	t, ok := s.Topic.GetS(topic)
	if !ok {
		s.Topic.CompareAndSwap(
			nocopy.StringToBytes(topic),
			nil,
			new(types.Chan),
		)
		goto retry
	}

	ch := make(chan *types.Message, 5)
	t.Subscribe(ch)
	return ch
}

func (s *Server) DirectPublish(topic string, msg *types.Message) {
	//retry:
	t, ok := s.Topic.GetS(topic)
	if !ok {
		return
		// s.Topic.CompareAndSwap(
		// 	nocopy.StringToBytes(topic),
		// 	nil,
		// 	new(types.Chan),
		// )
		// goto retry
	}

	t.Publish(msg)
}

func (s *Server) Publish(topic string, msg *types.Message) {
	s.msgq.EnQueue(msg)
}