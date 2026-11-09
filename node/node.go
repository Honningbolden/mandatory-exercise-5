package main

import (
)

type Node struct {
	pb.UnimplementedAuctionServiceServer
	id int64
	clock    int64
	port     int
	leader   int64
	peers    map[int]pb.AuctionServiceClient
	mu       sync.Mutex
}

var nodes = []string{
    "localhost:5001",
    "localhost:5002",
    "localhost:5003",
}

func (n *Node) Heartbeat(ctx context.Context, _ pb.Nothing) () {

}

func main() {
	
	// create flag with command to log in on port
	
	// go through each port list and create a peerconection with each

}