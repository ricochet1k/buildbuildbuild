package server

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
)

//go:generate sh -c "protoc -I../proto/_include/{remote-,google}apis/ -I ../proto ../proto/cluster.proto --go_out=clusterpb --go_opt=paths=source_relative --go-grpc_out=clusterpb --go-grpc_opt=paths=source_relative"

func (c *Server) InitCluster(name, host string, port int, join string) {
	events := make(chan serf.Event, 1)

	config := serf.DefaultConfig()
	if name != "" {
		config.NodeName = name
	}
	config.MemberlistConfig.BindAddr = host
	config.MemberlistConfig.BindPort = port
	config.MemberlistConfig.AdvertiseAddr = host
	config.MemberlistConfig.AdvertisePort = port
	config.EventCh = events
	config.Tags = map[string]string{}

	if c.nodeType == clusterpb.NodeType_WORKER {
		config.Tags["worker"] = "true"
	}

	ser, err := serf.Create(config)
	if err != nil {
		panic("Failed to create serf: " + err.Error())
	}

	c.list = ser

	go c.HandleEvents(events)

	// Join an existing cluster by specifying at least one known member.
	if join != "" {
		_, err = ser.Join(strings.Split(join, ","), false)
		if err != nil {
			panic("Failed to join cluster: " + err.Error())
		}
	}

	// Ask for members of the cluster
	for _, member := range ser.Members() {
		fmt.Printf("Member: %s %s %s\n", member.Name, member.Addr, member.Tags)
	}

	c.SendState(&clusterpb.NodeState{})
}

func (c *Server) MemberByName(name string) *serf.Member {
	for _, member := range c.list.Members() {
		if member.Name == name {
			return &member
		}
	}
	return nil
}

func (c *Server) SendNodeMessage(node string, msg *clusterpb.NodeMessage) error {
	member := c.MemberByName(node)
	if member == nil {
		return fmt.Errorf("Member not found (%w)", io.EOF)
	}

	msg.From = c.list.LocalMember().Name

	bytes, err := proto.Marshal(msg)
	if err != nil {
		panic(err.Error())
	}

	if err := c.list.UserEvent("", bytes, false); err != nil {
		fmt.Printf("Unable to send remote publish to %v: %v\n", node, err)
		return err
	}

	return nil
}

func (c *Server) BroadcastNodeMessage(msg *clusterpb.NodeMessage) {
	msg.From = c.list.LocalMember().Name
	// c.list.QueueBroadcast(broadcastMessage{msg: msg})
}

func (c *Server) SendState(state *clusterpb.NodeState) {
	c.BroadcastNodeMessage(&clusterpb.NodeMessage{
		From:  c.list.LocalMember().Name,
		State: state,
	})
}

func (c *Server) HandleEvents(events <-chan serf.Event) {
	for event := range events {
		switch event := event.(type) {
		case serf.MemberEvent:
			switch event.EventType() {
			case serf.EventMemberJoin:
				for _, member := range event.Members {
					fmt.Printf("Node join: %v %v\n", member.Name, member.Tags)
				}
			case serf.EventMemberLeave:
				for _, member := range event.Members {
					fmt.Printf("Node leave: %v %v\n", member.Name, member.Tags)
				}
			case serf.EventMemberUpdate:
				for _, member := range event.Members {
					fmt.Printf("Node update: %v %v\n", member.Name, member.Tags)
				}
			case serf.EventMemberFailed:
				for _, member := range event.Members {
					fmt.Printf("Node failed: %v %v\n", member.Name, member.Tags)
				}
			case serf.EventMemberReap:
				for _, member := range event.Members {
					fmt.Printf("Node reap: %v %v\n", member.Name, member.Tags)
				}
			default:
				fmt.Printf("Unhandled MemberEvent: %v\n", event.EventType())
			}
		case serf.UserEvent:
			switch event.Name {
			// case "sub":
			// 	c.Subscribe(string(event.Payload), RemoteSubscriber())
			case "msg":
				var msg clusterpb.NodeMessage
				if err := proto.Unmarshal(event.Payload, &msg); err != nil {
					fmt.Fprintf(os.Stderr, "Bad message received: %v %q", err, event.Payload)
					return
				}

				fmt.Printf("NotifyMsg: %v %#v\n", &msg, event.Payload)
				if msg.State != nil {
					c.nodeState[msg.From] = msg.State
					if msg.State.JobsSlotsFree > 0 {
						c.RequestJob(msg.From)
					}
				}
				if msg.StartJob != nil {
					c.Subscribe("jobstatus:"+msg.StartJob.Id, RemoteSubscriber(msg.From))
					c.StartJob(msg.StartJob)
				}
				if msg.Subscribe != "" {
					c.Subscribe(msg.Subscribe, RemoteSubscriber(msg.From))
				}
				if msg.Unsubscribe != "" {
					c.Unsubscribe(msg.Unsubscribe, RemoteSubscriber(msg.From))
				}
				if msg.Publish != "" && msg.PublishMsg != nil {
					if !c.Publish(msg.Publish, msg.PublishMsg) {
						c.RemoteUnsubscribe(msg.From, msg.Publish)
					}
				}
			default:
				fmt.Printf("Unhandled user event: %v\n", event.Name)
			}
		default:
			fmt.Printf("Unhandled event %T: %v\n", event, event)
		}
	}
}
