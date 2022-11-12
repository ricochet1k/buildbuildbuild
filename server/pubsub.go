package server

import (
	"fmt"
	"sync"

	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
)

type PubSub struct {
	sync.Mutex
	subscribers map[string][]Subscriber
}

type Subscriber interface {
	HandleMessage(*Server, string, *clusterpb.Message) bool
}

type FuncSubscriber func(*Server, string, *clusterpb.Message) bool

func (f FuncSubscriber) HandleMessage(c *Server, key string, msg *clusterpb.Message) bool {
	return f(c, key, msg)
}

type RemoteSubscriber string

func (r RemoteSubscriber) HandleMessage(c *Server, key string, msg *clusterpb.Message) bool {
	err := c.SendNodeMessage(string(r), &clusterpb.NodeMessage{Publish: key, PublishMsg: msg})
	if err != nil {
		fmt.Printf("Unable to send remote publish to %v: %v\n", r, err)
		return false
	}

	return true
}

// Return false to unsubscribe
func (c *Server) SubscribeFn(key string, fn func(*Server, string, *clusterpb.Message) bool) {
	c.Subscribe(key, FuncSubscriber(fn))
}
func (c *Server) Subscribe(key string, sub Subscriber) {
	c.pubsub.Lock()
	defer c.pubsub.Unlock()
	c.pubsub.subscribers[key] = append(c.pubsub.subscribers[key], sub)
}

// Return false to unsubscribe
func (c *Server) RemoteSubscribe(node, key string, fn func(*Server, string, *clusterpb.Message) bool) {
	err := c.SendNodeMessage(node, &clusterpb.NodeMessage{Subscribe: key})
	if err != nil {
		fmt.Printf("Unable to send remote subscribe to %v: %v\n", node, err)
	}
}

func (c *Server) Unsubscribe(key string, s Subscriber) {
	c.pubsub.Lock()
	subscribers := c.pubsub.subscribers[key]
	for i, sub := range subscribers {
		if sub == s {
			c.pubsub.subscribers[key] = append(subscribers[:i], subscribers[i+1:]...)
			break
		}
	}
	c.pubsub.Unlock()
}

func (c *Server) RemoteUnsubscribe(node, key string) {
	err := c.SendNodeMessage(node, &clusterpb.NodeMessage{Unsubscribe: key})
	if err != nil {
		fmt.Printf("Unable to send remote subscribe to %v: %v\n", node, err)
	}
}

// Returns true if any subscribers are left
// Because of the lock in Publish, all handlers must use goroutines if they want to publish or subscribe
func (c *Server) Publish(key string, msg *clusterpb.Message) bool {
	c.pubsub.Lock()
	defer c.pubsub.Unlock()
	subscribers := c.pubsub.subscribers[key]

	subscribersStillAlive := subscribers[:0]
	for _, subscriber := range subscribers {
		if subscriber.HandleMessage(c, key, msg) {
			subscribersStillAlive = append(subscribersStillAlive, subscriber)
		}
	}

	c.pubsub.subscribers[key] = subscribersStillAlive

	return len(subscribersStillAlive) > 0
}
