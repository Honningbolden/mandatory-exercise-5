package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "auction/proto"
)

type Client struct {
	clock      int64
	mu         sync.Mutex
	uuid       uuid.UUID
	highestBid int64
	lastReqNum int64
	servers    []pb.AuctionClient
	serverIdx  int
}

type BidId struct {
	clientUuid uuid.UUID
	reqNum     int64
	timestamp  int64
}

func (c *Client) tick(receivedClock int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clock = max(c.clock, receivedClock) + 1
	return c.clock
}

func (c *Client) makeBid(ctx context.Context, amount int64) (*pb.Acknowledgement, error) {
	if len(c.servers) == 0 {
		return nil, fmt.Errorf("No auction servers are configured")
	}

	// Update highestBid
	c.mu.Lock()
	c.highestBid = max(c.highestBid, amount)
	c.lastReqNum++
	c.clock++
	bid := &pb.BidRequest{
		ClientUuid:   c.uuid.String(),
		ClientReqNum: c.lastReqNum,
		Lamport:      c.clock,
		Amount:       int64(amount),
	}
	c.mu.Unlock()

	// Make BidRequest
	for attempt := 0; attempt < 3*len(c.servers); attempt++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		reply, err := c.servers[c.serverIdx].Bid(timeoutCtx, bid)
		cancel()

		if err == nil && reply.Outcome != pb.Outcome_Exception {
			return reply, nil
		}
		log.Printf("Server %d could not take the big (err=%v), trying next", c.serverIdx, err)
		c.serverIdx = (c.serverIdx + 1) % len(c.servers)
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("All servers are dead. Can't make bid. Closing.")
}

func (c *Client) getResult(ctx context.Context) (*pb.AuctionState, error) {
	for attempt := 0; attempt < 3*len(c.servers); attempt++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		state, err := c.servers[c.serverIdx].ConfirmWin(timeoutCtx, &pb.Nothing{})
		cancel()

		if err == nil {
			c.tick(state.Lamport)
			return state, nil
		}
		c.serverIdx = (c.serverIdx + 1) % len(c.servers)
		time.Sleep(500 * time.Millisecond)
	}
	return nil, fmt.Errorf("All servers are dead. Can't get result. Closing.")
}

func (c *Client) parseWinner(winnerId string) string {
	if winnerId == c.uuid.String() {
		return "you"
	}
	return winnerId
}

func main() {
	portList := flag.String("ports", "", "comma-separated ports of the server nodes")
	flag.Parse()

	if *portList == "" {
		fmt.Fprintln(os.Stderr, "usage: client -ports 5001,5002,5003")
		os.Exit(1)
	}

	// Parse portList into ports array
	var ports []int
	for _, p := range strings.Split(*portList, ",") {
		port, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Fatalf("invalid port %q: %v", p, err)
			return
		}
		ports = append(ports, port)
	}

	// Sort ports in ascending order (always pick lowest as leader)
	slices.Sort(ports)

	// Initialize client
	client := &Client{
		clock:      1,
		uuid:       uuid.New(),
		highestBid: 0,
		lastReqNum: 0,
		servers:    make([]pb.AuctionClient, len(ports)),
		serverIdx:  0,
	}

	// Establish connections to all servers
	for i, p := range ports {
		conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", p), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("client for:%d: %v", p, err)
		}
		client.servers[i] = pb.NewAuctionClient(conn)
	}

	ctx := context.Background()

	for {
		state, err := client.getResult(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if state.EndTime != 0 && time.Now().UnixMilli() >= state.EndTime { // Endtime 0 means there are no bids yet
			fmt.Printf("Auction over. Winner is %s with %d\n", client.parseWinner(state.HighestBidderId), state.HighestBid)
			return
		}

		bidAmount := max(state.HighestBid-5+rand.Int64N(20), 0) // Make some bids too low, and make all bids above greater than or equal to 0
		reply, err := client.makeBid(ctx, bidAmount)
		if err != nil {
			log.Fatal(err)
		}
		switch reply.Outcome {
		case pb.Outcome_Success:
			fmt.Printf("Bid %d: you're the highest bidder.\n", bidAmount)
		case pb.Outcome_Fail:
			fmt.Printf("Bid %d: you're bid was too low and wasn't accepted, or the auction has ended.\n", bidAmount)
		}
		time.Sleep(time.Duration(500+rand.IntN(1000)) * time.Millisecond)
	}

}
