package lemonmq

import (
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/lemon-mint/frameio"
	"github.com/lemon-mint/lemonmq/packetpb"
	"github.com/lemon-mint/lemonmq/types"
	"github.com/valyala/bytebufferpool"
	"google.golang.org/protobuf/proto"
)

var msgPool = sync.Pool{
	New: func() interface{} {
		return &types.Message{}
	},
}

func (s *Server) handleConn(c net.Conn) {
	var err error
	defer c.Close()
	bufr := frameio.BufioPool.GetReader(c)
	bufw := frameio.BufioPool.GetWriter(c)
	defer frameio.BufioPool.PutReader(bufr)
	defer frameio.BufioPool.PutWriter(bufw)

	r, w := frameio.NewFrameReader(bufr), frameio.NewFrameWriter(bufw)
	ch := make(chan *types.Message, 512)
	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)
	var topics []string
	_ = r
	_ = w
	var data []byte
	var p packetpb.Packet
	defer func() {
		for _, topic := range topics {
			s.Unsubscribe(topic, ch)
		}
	}()
	for {
		err = c.SetReadDeadline(time.Now().Add(s.ReadTimeout))
		if err != nil {
			ConnError(c, "SetReadDeadline", err)
			return
		}
		data, err = r.Read()
		if err != nil {
			ConnError(c, "Read", err)
			return
		}
		err = proto.Unmarshal(data, &p)
		if err != nil {
			ConnError(c, "Unmarshal", err)
			return
		}
		switch p.GetType() {
		case packetpb.Packet_PUB:
			pub := p.GetPublish()
			if pub == nil {
				return
			}
			msg := msgPool.Get().(*types.Message)
			msg.Topic = pub.GetTopic()
			msg.Body = pub.GetPayload()
			msg.TimeStamp = time.Now().UnixNano()
			s.Publish(pub.GetTopic(), msg)
		case packetpb.Packet_SUB:
			sub := p.GetSubscribe()
			if sub == nil {
				return
			}
			s.Subscribe(sub.GetTopic(), ch)
			topics = append(topics, sub.GetTopic())
		case packetpb.Packet_UNSUB:
			unsub := p.GetUnsubscribe()
			if unsub == nil {
				return
			}
			s.Unsubscribe(unsub.GetTopic(), ch)
			for i, t := range topics {
				if t == unsub.Topic {
					topics = append(topics[:i], topics[i+1:]...)
					break
				}
			}
		}
	}
}

var logError = log.New(os.Stderr, "Error", log.Flags())

func ConnError(c net.Conn, data ...interface{}) {
	logError.Println(c.RemoteAddr(), data)
}
