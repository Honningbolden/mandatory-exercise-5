package auction

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
	pb.UnimplementedAuctionServer

	clock      int64
	mu         sync.Mutex
	uuid       uuid.UUID
	highestBid int64
	lastReqNum int64
	servers    map[int]pb.AuctionServer
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

func (c *Client) makeBid(ctx context.Context, amount int64) (*pb.BidReply, error) {
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
	for {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Second)

		reply, err := c.servers[c.serverIdx].Bid(timeoutCtx, bid)

		isTimedout := timeoutCtx.Err() == context.DeadlineExceeded
		cancel()

		if err == nil {
			return reply, nil
		}
		if !isTimedout {
			return nil, err
		}
		if c.serverIdx+1 > len(c.servers) {
			return nil, fmt.Errorf("All servers are dead. Can't make bid. Closing down.")
		}
		c.serverIdx++
	}
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
		servers:    make(map[int]pb.AuctionServer),
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

	var auctionEndtime int64
	var auctionHighestBid int64

	for {
		if (auctionEndtime > time.Now().Unix()) {
			
		}
		time.Sleep(time.Duration(min(rand.IntN(5000), 1000)) * time.Millisecond)

		bidAmount := rand.Int64N(client.highestBid) + client.highestBid/2
		reply, err := client.makeBid(ctx, bidAmount)
		if err != nil {
			log.Fatal(err)
		}
		switch reply.Outcome {
			case pb.Outcome_Success {
				fmt.Println("You're the highest bidder.")
			}
			case pb.Outcome_Fail {
				fmt.Println("You're big was too low.")
			}
		}
		auctionHighestBid = reply.HighestBid
		auctionTimeleft = reply.TimeLeft

		fmt.Println(reply)
	}

}
