package types

type MSGTYPE byte

const (
	MSG_NORMAL       = 0
	MSG_SERVER_DRAIN = 1
)

type Message struct {
	T MSGTYPE

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
