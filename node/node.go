package main

import (
	"context"
	"log"
	"time"

	"golang.org/x/tools/go/analysis/passes/nilfunc"

	pb "auction/proto"
)

type Bidder struct {
	id 			int64
	client 		pb.AuctionServiceClient
}

type AuctionState struct {
	highestBid    int64
    highestBidderId int64
    endTime       time.Time
}

type Node struct {
	pb.UnimplementedAuctionServiceServer
    pb.UnimplementedReplicaManagerServiceServer

	mu       		sync.Mutex
	state			*AuctionState

	id 		 		int64
	clock    		int64
	port     		int64
	leader   		int64
	peers    		map[int]pb.ReplicaManagerServiceClient
}

var ports = []string{
    "5001",
    "5002",
    "5003",
}

const auctionDuration = time.Second * 10

// Replicates the auction state to a client
// send to all RM that isn't leader on every state change
func (n *Node)Replicate(ctx context.Context, s *pb.AuctionState) (*pb.Acknowledgement, error) {

	n.clock = tick(s.Lamport)
	if n.state = nil {
		n.createAuction(req)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	// overwrite auction state
	as := n.state
	as.highestBid = s.HighestBid
	as.highestBidder = s.HighestBidderId
	as.endTime = s.end_time
	return &pb.Acknowledgement{Output: "Sucess"}, nil
}

// Method that should be called every second to the current leader
// if returnd nil asume leader is dead
func (n *Node) Heartbeat(ctx context.Context, _ *pb.Nothing) (*pb.Nothing, error) {
	return &pb.Nothing{}, nil
}

func (n *Node) HeartbeatHandler(ctx context.Context, RM pb.ReplicaManagerClient) {
	// start ticker
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			leader := n.peers[int(n.leader)]

			heartbeatCtx, cancel := context.WithTimeout(ctx, time.Second)
			_, err := leader.Heartbeat(heartbeatCtx, &pb.Nothing{})
			cancel()

			if err != nil {
				log.Printf("Heartbeat failed: %v", err)
				getLeader();
			}
		case <-ctx.Done():
			return
		}
	}
} 

func (n *Node) Bid(ctx context.Context, req *pb.BidRequest) (*pb.Acknowledgement) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.clock = n.tick(req.Lamport)

	if n.AuctionState = nil {
		n.state = createAuction(req)
		return &pb.Acknowledgement{Output: "Success"}
	}

	if (req.Amount < int64(n.state.highestBid)) {
		n.state.highestBid = int64(req.Amount)
		n.state.highestBidder = int64(req.ClientUuid)

		for i:= 1; i <= 3; i++ {
			peers[i].Replicate(&pb.AuctionState{
				highest_bidder_id: n.state.highestBidderId;
				highest_bid: n.state.highestBid;
				end_time: n.state.endTime;
				req.Lamport: req.Lamport;
			})
		}
		return &pb.Acknowledgement{Output: "Success"}
	}
	return &pb.Acknowledgement{Output: "Fail"}
}

// 
func (n *Node) createAuction(req *pb.BidRequest) (AuctionState) {
	return &AuctionState{
		highestBidderId: req.clientUuid,
		highestBid: req.Amount,
		endTime: time.Now() + auctionDuration,
		Lamport: req.Lamport,
	}
}

func (n *Node) isLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	return n.id == n.leader
}

func (n *Node) getLeader() {
	if (n.leader + 1 < len(n.peers)) {
		n.leader++
	}
	log.Printf("Couldn't find new leader because all nodes have failed. Shutting down.")
	return;
}

func isAucOnline() bool {
	return n.state != nil
}

// tick clock
func (n *Node) tick(receivedClock int64) int64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.clock = max(n.clock, receivedClock) + 1
	return n.clock
}

func (n *Node) auctionHandler (as AuctionState) (return) {
	// start ticker
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	s := n.state

	for {
		select {
		case <-ticker.C:
			if s = nil {
				continue
			}

			if time.now() = s.endtime
		}
	}
}

func main() {
	// create flag with command to log in on port
	portList := flag.String("ports", "", "comma-separated ports of the server nodes")
	flag.Parse()

	// TODO: should not set all ports fix later 
	if *portList == "" {
		fmt.Fprintln(os.Stderr, "usage: node -port 5001,5002,5003")
		os.Exit(1)
	}

	for _, p := range strings.Split(*portList, ",") {
		port, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Fatalf("invalid port %q: %v", p, err)
		}
	}

	n := &Node{
		id: strconv.Atoi(port - 5000)
		clock: 0
		port: port
		leader: 1
		peers: make(map[int64]pb.ReplicaManagerClient)
	}

	// Setup listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("could not listen %d: %v", *port, err)
	}

	log.Printf("Connected on port %d", *port)

	s := grpc.NewServer()
	pb.RegisterReplicaManegerServiceServer(s, node)
	pb.RegiseterAuctionServiceServer(s, node)
	go grpcServer.Serve(listener)

	// create peer connections
	for i := 1; i <= 3; i++ {
		if (port == ports[i]) {
			continue;
		}

		// keep trying to connect until it works
		for {
			conn, err := grpc.NewClient(ports[i - 1], grpc.WithTransportCredentials(insecure.NewCredentials()))
			// brakes out of the loop once conected 
			if err != nil {
				break
			}
		}
	}
	
	

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// open HeartBeats on all peers 
	for i := 1; i <= 3; i++ { 
		if (n.isLeader()) {
			go n.HeartbeatHandler(ctx, n.peers[i])
		}
	}
}