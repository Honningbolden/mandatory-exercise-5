## Run
Start the nodes, each in its own terminal (5001 is the first leader):

```
go run node/node.go -port 5001
go run node/node.go -port 5002
go run node/node.go -port 5003
```

Start one or more clients, each in its own terminal:

```
go run client/client.go -ports 5001,5002,5003
```

The auction lasts 20 seconds from the first bid. To test a crash, kill node 5001 with Ctrl+C while the clients are bidding.

Logs of our test runs are in `logs/`: `A` shows what happens if node 1 crashes, `C` shows what happens if none of the nodes crash.
