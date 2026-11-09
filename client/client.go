package auction

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	clock     int64
	uuid      uuid.UUID
	requestId int
	ports     []int
}

func main() {
	portList := flag.String("ports", "", "comma-separated ports of the server nodes")
	flag.Parse()

	if *portList == "" {
		fmt.Fprintln(os.Stderr, "usage: client -ports 5001,5002,5003")
		os.Exit(1)
	}

	for _, p := range strings.Split(*portList, ",") {
		port, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			log.Fatalf("invalid port %q: %v", p, err)
		}

	}

	client := &Client{
		clock:     1,
		uuid:      uuid.New(),
		requestId: 0,
		ports:     ports,
	}

	conn, err := grpc.NewClient(fmt.Springf("localhost:%d", p), grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("client for:%d: %v", p, err)
	}
}
