package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "auction/proto"
)

type Bidder struct {
	id     int64
	client pb.AuctionClient
}

type AuctionState struct {
	highestBid      int64
	highestBidderId string
	endTime         time.Time
}

type Node struct {
	pb.UnimplementedAuctionServer
	pb.UnimplementedReplicaManagerServer

	mu    sync.Mutex
	state *AuctionState

	id            int64
	clock         int64
	port          int64
	leader        int64
	peers         map[int]pb.ReplicaManagerClient
	auctionOnline bool
}

var ports = []string{
	"5001",
	"5002",
	"5003",
}

const auctionDuration = time.Second * 10

// Replicates the auction state to a client
// send to all RM that isn't leader on every state change
func (n *Node) Replicate(ctx context.Context, s *pb.AuctionState) (*pb.Acknowledgement, error) {

	n.tick(s.Lamport)

	n.mu.Lock()
	if n.state == nil {
		n.state = &AuctionState{}
		n.auctionOnline = true
	}

	// overwrite auction state
	as := n.state
	as.highestBid = s.HighestBid
	as.highestBidderId = s.HighestBidderId
	as.endTime = time.UnixMilli(s.EndTime)
	n.mu.Unlock()

	return &pb.Acknowledgement{Outcome: pb.Outcome_Success}, nil
}

// Method that should be called every second to the current leader
// if returnd nil asume leader is dead
func (n *Node) Heartbeat(ctx context.Context, _ *pb.Nothing) (*pb.Nothing, error) {
	return &pb.Nothing{}, nil
}

func (n *Node) HeartbeatHandler(ctx context.Context) {
	// start ticker
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n.isLeader() {
				continue
			}

			n.mu.Lock()
			leader := n.peers[int(n.leader)]
			n.mu.Unlock()

			heartbeatCtx, cancel := context.WithTimeout(ctx, time.Second)
			_, err := leader.Heartbeat(heartbeatCtx, &pb.Nothing{})
			cancel()

			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
				n.getLeader()
			}
		case <-ctx.Done():
			return
		}
	}
}

// handles bids from client
// assumes it will only be called if this node is the leader
func (n *Node) Bid(ctx context.Context, req *pb.BidRequest) (*pb.Acknowledgement, error) {
	lamport := n.tick(req.Lamport)

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.id != n.leader {
		return &pb.Acknowledgement{Outcome: pb.Outcome_Exception}, nil
	}

	if n.state == nil {
		n.state = n.createAuction(req)

		for i, peer := range n.peers {
			ack, err := peer.Replicate(ctx, &pb.AuctionState{
				HighestBidderId: n.state.highestBidderId,
				HighestBid:      n.state.highestBid,
				EndTime:         n.state.endTime.UnixMilli(),
				Lamport:         lamport,
			})

			if err != nil || ack.Outcome != pb.Outcome_Success {
				log.Printf("Could not replicate to node %d: %v", i, err)
			}
		}

		return &pb.Acknowledgement{Outcome: pb.Outcome_Success}, nil
	}

	if !n.auctionOnline {
		return &pb.Acknowledgement{Outcome: pb.Outcome_Fail}, nil
	}

	if req.Amount > n.state.highestBid {
		n.state.highestBid = req.Amount
		n.state.highestBidderId = req.ClientUuid

		// Replicate the state with other RMs
		for i, peer := range n.peers {
			ack, err := peer.Replicate(ctx, &pb.AuctionState{
				HighestBidderId: n.state.highestBidderId,
				HighestBid:      n.state.highestBid,
				EndTime:         n.state.endTime.UnixMilli(),
				Lamport:         lamport,
			})

			if err != nil || ack.Outcome != pb.Outcome_Success {
				log.Printf("Could not replicate to node %d: %v", i, err)
			}
		}
		return &pb.Acknowledgement{Outcome: pb.Outcome_Success}, nil
	}
	return &pb.Acknowledgement{Outcome: pb.Outcome_Fail}, nil
}

func (n *Node) ConfirmWin(ctx context.Context, _ *pb.Nothing) (*pb.AuctionState, error) {
	lamport := n.tick(0)

	n.mu.Lock()
	defer n.mu.Unlock()

	s := n.state
	if s == nil {
		return &pb.AuctionState{Lamport: lamport}, nil
	}
	return &pb.AuctionState{
		HighestBidderId: s.highestBidderId,
		HighestBid:      s.highestBid,
		EndTime:         s.endTime.UnixMilli(),
		Lamport:         lamport,
	}, nil
}

func (n *Node) createAuction(req *pb.BidRequest) *AuctionState {
	n.auctionOnline = true
	return &AuctionState{
		highestBidderId: req.ClientUuid,
		highestBid:      req.Amount,
		endTime:         time.Now().Add(auctionDuration),
	}
}

func (n *Node) isLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.id == n.leader
}

func (n *Node) getLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if int(n.leader) < len(ports) {
		n.leader++
		log.Printf("New leader is %d", n.leader)
		return
	}
	log.Fatalf("Couldn't find new leader because all nodes have failed. Shutting down.")
}

// tick clock
func (n *Node) tick(receivedClock int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clock = max(n.clock, receivedClock) + 1
	return n.clock
}

func (n *Node) auctionHandler() {
	// start ticker
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.auctionOnline && time.Now().After(n.state.endTime) {
			n.auctionOnline = false
		}
		n.mu.Unlock()
	}
}

func main() {
	// create flag with command to log in on port
	port := flag.Int("port", 0, "port of this node (5001, 5002 or 5003)")
	flag.Parse()

	if *port == 0 {
		fmt.Fprintln(os.Stderr, "usage: node -port 5001")
		os.Exit(1)
	}

	n := &Node{
		id:     int64(*port - 5000),
		clock:  0,
		port:   int64(*port),
		leader: 1,
		peers:  make(map[int]pb.ReplicaManagerClient),
	}

	// Setup listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("could not listen %d: %v", *port, err)
	}

	log.Printf("Connected on port %d", *port)

	s := grpc.NewServer()
	pb.RegisterReplicaManagerServer(s, n)
	pb.RegisterAuctionServer(s, n)
	go s.Serve(listener)

	// create peer connections
	for i := 1; i <= 3; i++ {
		if strconv.Itoa(*port) == ports[i-1] {
			continue
		}

		// keep trying to connect until it works
		for {
			conn, err := grpc.NewClient("localhost:"+ports[i-1], grpc.WithTransportCredentials(insecure.NewCredentials()))
			// brakes out of the loop once conected
			if err == nil {
				n.peers[i] = pb.NewReplicaManagerClient(conn)
				break
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// open HeartBeats on all peers
	go n.HeartbeatHandler(ctx)
	go n.auctionHandler()

	select {}
}