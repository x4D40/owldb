# OwlDB

### Development
1. run the initial node `go run main.go -node node1`
1. add a node to the cluster `go run main.go -node node2 -port 4009 -sport 4010 -join localhost:4008`
    1. `-node node2` gives friendly name to the node
    1. `-port 4009` the port the clients will connect too
    1. `-sport 4010` the sync port to communicate membership requests
    1. `-join localhost:4008` join this node to `node 1`