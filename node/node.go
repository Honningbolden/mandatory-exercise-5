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
	state			*AuctionState

	id 		 		int64
	clock    		int64
	port     		int64
	leader   		int64
	isLeader 		bool
	peers    		map[int]pb.ReplicaManegerServiceClient
}

var ports = []string{
    "5001",
    "5002",
    "5003",
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

	// create peer conection
	for i := 1; i <= 3; i++ {
		if (port == ports[i]) {
			continue;
		}

		// keep trying to connect untill it works
		for {
			conn, err := grpc.NewClient(ports[i - 1], grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err = nil {
				break
			}
		}

		n.peers[i] = pb.NewReplicaManagerClient(conn)
	}
		// keep trying if they arnt open yet

}