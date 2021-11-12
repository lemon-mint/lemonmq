package main

import (
	"fmt"
	"time"

	"github.com/lemon-mint/lemonmq/client"
	"github.com/lemon-mint/lemonmq/client/types"
)

func main() {
	c := client.NewClient("127.0.0.1:9999")

	c.Subscribe("a", func(m *types.Message) (Unsubscribe bool) {
		fmt.Println(string(m.Body))
		return false
	})

	p, err := c.GetPublishStream("a")
	if err != nil {
		panic(err)
	}
	for {
		p([]byte("hello"))
	}
	time.Sleep(time.Second * 10)
}
