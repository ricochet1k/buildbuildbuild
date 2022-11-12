package server

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/ricochet1k/buildbuildbuild/server/clusterpb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

//go:generate sh -c "protoc -I../proto/_include/{remote-,google}apis/ -I ../proto ../proto/cluster.proto --go_out=clusterpb --go_opt=paths=source_relative --go-grpc_out=clusterpb --go-grpc_opt=paths=source_relative"

func (c *Server) InitCluster() {
	events := make(chan serf.Event, 1)

	serfConfig := serf.DefaultConfig()
	if c.config.ClusterName != "" {
		serfConfig.NodeName = c.config.ClusterName
	}
	serfConfig.MemberlistConfig.BindAddr = c.config.ClusterHost
	serfConfig.MemberlistConfig.BindPort = c.config.ClusterPort
	serfConfig.MemberlistConfig.AdvertiseAddr = c.config.ClusterHost
	serfConfig.MemberlistConfig.AdvertisePort = c.config.ClusterPort
	serfConfig.EventCh = events
	serfConfig.Tags = map[string]string{
		"grpcPort": fmt.Sprint(c.config.Port),
	}
	logger := log.New(io.Discard, "", 0)
	// logger := log.New(logrus.New().Writer(), "", 0)
	serfConfig.Logger = logger
	serfConfig.MemberlistConfig.Logger = logger

	if c.config.Worker {
		serfConfig.Tags["worker"] = "true"
	}

	ser, err := serf.Create(serfConfig)
	if err != nil {
		panic("Failed to create serf: " + err.Error())
	}

	c.list = ser

	go c.HandleEvents(events)

	// Join an existing cluster by specifying at least one known member.
	if c.config.Join != "" {
		_, err = ser.Join(strings.Split(c.config.Join, ","), false)
		if err != nil {
			logrus.Printf("WARNING: Failed to join cluster: %v\n", err)
		}
	}

	// Ask for members of the cluster
	for _, member := range ser.Members() {
		logrus.Printf("Member: %s %s %s\n", member.Name, member.Addr, member.Tags)
	}

	c.SendState(&clusterpb.NodeState{})
}

func (c *Server) GracefulStop() {
	if c.list != nil {
		c.list.Leave()
		c.list.Shutdown()
	}
}

func (c *Server) ConnectToMember(name string) (*grpc.ClientConn, error) {
	if conn, ok := c.grpcClients[name]; ok {
		// TODO: send Ping
		if conn.GetState() == connectivity.Idle {
			conn.Connect()
			return conn, nil
		}
		if conn.GetState() == connectivity.Ready {
			return conn, nil
		}
	}

	member := c.MemberByName(name)
	if member == nil {
		return nil, fmt.Errorf("Member not found: %v", name)
	}

	addr := fmt.Sprintf("%v:%v", member.Addr, member.Tags["grpcPort"])

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logrus.Printf("Could not connect to member %q at %q: %v", name, addr, err)
		return nil, err
	}

	c.grpcClients[name] = conn

	return conn, nil
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

	if err := c.list.UserEvent("msg", bytes, false); err != nil {
		logrus.Printf("Unable to send remote publish to %v: %v\n", node, err)
		return err
	}

	return nil
}

func (c *Server) BroadcastNodeMessage(msg *clusterpb.NodeMessage) {
	msg.From = c.list.LocalMember().Name
	bytes, err := proto.Marshal(msg)
	if err != nil {
		panic(err.Error())
	}

	if err := c.list.UserEvent("msg", bytes, false); err != nil {
		logrus.Printf("Error broadcasting: %v\n", err)
	}
}

func (c *Server) SendState(state *clusterpb.NodeState) {
	logrus.Printf("Sending state %v\n", state)
	c.BroadcastNodeMessage(&clusterpb.NodeMessage{State: state})
}

func (c *Server) HandleEvents(events <-chan serf.Event) {
	defer logrus.Printf("HandleEvents bye!")
	for event := range events {
		switch event := event.(type) {
		case serf.MemberEvent:
			switch event.EventType() {
			case serf.EventMemberJoin:
				for _, member := range event.Members {
					logrus.Printf("Node join: %v %v\n", member.Name, member.Tags)
				}
			case serf.EventMemberLeave:
				for _, member := range event.Members {
					logrus.Printf("Node leave: %v %v\n", member.Name, member.Tags)
					delete(c.nodeState, member.Name)
				}
			case serf.EventMemberUpdate:
				for _, member := range event.Members {
					logrus.Printf("Node update: %v %v\n", member.Name, member.Tags)
				}
			case serf.EventMemberFailed:
				for _, member := range event.Members {
					logrus.Printf("Node failed: %v %v\n", member.Name, member.Tags)
				}
			case serf.EventMemberReap:
				for _, member := range event.Members {
					logrus.Printf("Node reap: %v %v\n", member.Name, member.Tags)
				}
			default:
				logrus.Printf("Unhandled MemberEvent: %v\n", event.EventType())
			}
		case serf.UserEvent:
			switch event.Name {
			case "msg":
				var msg clusterpb.NodeMessage
				if err := proto.Unmarshal(event.Payload, &msg); err != nil {
					fmt.Fprintf(os.Stderr, "Bad message received: %v %q", err, event.Payload)
					continue
				}
				if msg.From == c.list.LocalMember().Name {
					continue
				}

				logrus.Printf("NotifyMsg: %v\n", &msg)
				if msg.State != nil {
					c.nodeState[msg.From] = msg.State
					if msg.State.JobsSlotsFree > 0 {
						go c.RequestJob(msg.From)
					}
				}
				// if msg.StartJob != nil {
				// 	c.Subscribe("jobstatus:"+msg.StartJob.Id, RemoteSubscriber(msg.From))
				// 	c.StartJob(msg.StartJob)
				// }
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
				logrus.Printf("Unhandled user event: %v\n", event.Name)
			}
		case *serf.Query:
			switch event.Name {
			case "wherejob":
				if _, ok := c.jobsRunning.Load(string(event.Payload)); ok {
					logrus.Printf("I have job %q!\n", string(event.Payload))
					event.Respond(nil)
				} else {
					logrus.Printf("I don't have job %q\n", string(event.Payload))
				}
			default:
				logrus.Printf("Unhandled query %q: %v\n", event.Name, event)
			}
		default:
			logrus.Printf("Unhandled event %T: %v\n", event, event)
		}
	}
}
