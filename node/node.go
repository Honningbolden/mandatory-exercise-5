package main

import (
)

type Bidder struct {
	id 			int64
	client 		pb.AuctionServiceClient
}

type AuctionState struct {
	highestBid    int32
    highestBidder *Bidder
	highestBid 	  int64
    bidders       map[int64]*Bidder
    endTime       time.Time
    seenRequests  map[int64]bool
}

type Node struct {
	pb.UnimplementedAuctionServiceServer
    pb.UnimplementedReplicaManegerServiceServer

	mu       		sync.Mutex
	state	*AuctionState

	id 		 		int64
	clock    		int64
	port     		int64
	leader   		int64
	isLeader 		bool
	peers    		map[int]pb.ReplicaManegerServiceClient
}

var ports = []string{
    "localhost:5001",
    "localhost:5002",
    "localhost:5003",
}

const auctionDuration = 1000

func (n *Node) Heartbeat(ctx context.Context, _ *pb.Nothing) (*pb.Nothing, error) {
	return &pb.Nothing{}, nil
}

func (n *Node)Replicate(ctx context.Context, s *pb.AuctionState) (*pb.Acknowledgement, error) {
	n.mu.Lock()
    defer n.mu.Unlock()

    n.clock = tick(s.Lamport)

	// overite auction state
    n.state = s 
    return &pb.Acknowledgement{Output: "ok"}, nil
}

func (n *Node) tick(receivedClock int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clock = max(n.clock, receivedClock) + 1
	return n.clock
}

func main() {
	// create flag with command to log in on port
	
	// go through each port list and create a peerconection with each port
		// if it cant connect keep going untill it will

}