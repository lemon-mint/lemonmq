package types

type Message struct {
	Topic string

	TimeStamp int64
	ID        string
	QOS       QOS //unused

	Body []byte
}

type QOS byte

const (
	QOS_0 QOS = iota
	QOS_1
	QOS_2
)
