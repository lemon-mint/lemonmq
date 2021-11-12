package lemonmq

import (
	"log"
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/lemon-mint/lemonmq/ringbuffer"
	"github.com/lemon-mint/lemonmq/slowtable"
	"github.com/lemon-mint/lemonmq/slowtable/nocopy"
	"github.com/lemon-mint/lemonmq/types"
)

type Server struct {
	Ln net.Listener

	Topic *slowtable.Table

	msgq *ringbuffer.RingBuffer

	ConnTimeout time.Duration

	queued int64

	delivered int64

	clients int64

	rpsCounter int64

	rps float64

	rpsStopChan chan struct{}

	workerStop int32

	workers int32
}

func NewServer(workers int32) *Server {
	msgq := ringbuffer.NewRingBuffer(65535)
	s := &Server{
		msgq: msgq,
	}
	s.Topic = slowtable.NewTable(nil, 65536)
	s.ConnTimeout = time.Second * 15
	s.workers = workers
	s.workerStop = 0
	s.rpsStopChan = make(chan struct{})
	return s
}

func (s *Server) Serve() error {
	if s.workers <= 0 {
		s.workers = int32(runtime.GOMAXPROCS(0))
	}
	for i := int32(0); i < s.workers; i++ {
		go s.worker()
	}
	go s.rpsc()

	for {
		conn, err := s.Ln.Accept()
		if err != nil {
			return err
		}
		atomic.AddInt64(&s.clients, 1)
		go s.handleConn(conn)
	}
}

func (s *Server) Close() {
	atomic.StoreInt32(&s.workerStop, 1)
	s.rpsStopChan <- struct{}{}
	for {
		if atomic.LoadInt32(&s.workers) == 0 {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	s.Ln.Close()
}

func (s *Server) Subscribe(topic string, ch chan *types.Message) chan *types.Message {
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

	t.Subscribe(ch)
	return ch
}

func (s *Server) Unsubscribe(topic string, ch chan *types.Message) {
	t, ok := s.Topic.GetS(topic)
	if !ok {
		return
	}
	t.Unsubscribe(ch)
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
	atomic.AddInt64(&s.queued, 1)
	atomic.AddInt64(&s.rpsCounter, 1)
	s.msgq.EnQueue(msg)
}

func (s *Server) worker() {
	defer atomic.AddInt32(&s.workers, -1)
	for {
		msg := s.msgq.DeQueue()
		if s.workerStop == 1 {
			return
		}
		atomic.AddInt64(&s.queued, -1)
		atomic.AddInt64(&s.delivered, 1)
		s.DirectPublish(msg.Topic, msg)
	}
}

func (s *Server) rpsc() {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			s.rps = float64(atomic.SwapInt64(&s.rpsCounter, 0)) / 10
			atomic.StoreInt64(&s.rpsCounter, 0)
			log.Printf("rps: %f, queued: %d, delivered: %d, clients: %d", s.rps, atomic.LoadInt64(&s.queued), atomic.LoadInt64(&s.delivered), atomic.LoadInt64(&s.clients))
		case <-s.rpsStopChan:
			ticker.Stop()
			return
		}
	}
}
