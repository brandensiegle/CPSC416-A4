// Version 1.0
//
// Starter code for key-value front-end that supports three API calls
// over rpc:
// - get(key)
// - put(key,val)
// - testset(key,testval,newval)
//
// Usage: go run kv-front-end.go [client ip:port] [kv-node ip:port] [r]
//
// - [client ip:port] : address that clients use to connect to the front-end node
// - [kv-node ip:port] : address that kv nodes use to connect to the front-end node
// - [r] : replication factor for keys

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strings"
)

// args in get(args)
type GetArgs struct {
	Key string // key to look up
}

// args in put(args)
type PutArgs struct {
	Key string // key to associate value with
	Val string // value
}

// args in testset(args)
type TestSetArgs struct {
	Key     string // key to test
	TestVal string // value to test against actual value
	NewVal  string // value to use if testval equals to actual value
}

// Reply from service for all three API calls above.
type ValReply struct {
	Val string // value; depends on the call
}

type KeyValService int

// GET
func (kvs *KeyValService) Get(args *GetArgs, reply *ValReply) error {
	// Check if the issued get command is a CMD operation.
	if args.Key[0:3] == "CMD" {
		// Split by spaces.
		tokens := strings.Fields(args.Key)
		if len(tokens) == 3 {
			if tokens[1] == "get-replicas-of" {
				// get("CMD get-replicas-of k")
				key := tokens[2]
				fmt.Println("get(CMD " + tokens[1] + " " + key + ")")

				// TODO
				reply.Val = "" // No replicas for this key in the system.
				return nil
			} else if tokens[1] == "kill-replica" {
				// get("CMD kill-replica id")
				replicaId := tokens[2]
				fmt.Println("get(CMD " + tokens[1] + " " + replicaId + ")")

				// TODO
				reply.Val = "false" // No replicas, so can't kill a replica.
				return nil
			}
		} else if len(tokens) == 4 {
			if tokens[1] == "kill-replicas-of" {
				// get("CMD kill-replicas-of k x")
				key := tokens[2]
				nReplicas := tokens[3]
				fmt.Println("get(CMD " + tokens[1] + " " + key + " " + nReplicas + ")")

				// TODO
				reply.Val = "0" // No replicas, so killed 0 of them.
				return nil
			}
		}
	}

	// TODO: do some stuff here.
	reply.Val = "DRAGONS"
	return nil
}

// PUT
func (kvs *KeyValService) Put(args *PutArgs, reply *ValReply) error {
	// TODO: do the other stuff here.
	reply.Val = "DRAGONS"
	return nil
}

// TESTSET
func (kvs *KeyValService) TestSet(args *TestSetArgs, reply *ValReply) error {
	// TODO: do here the other stuff
	reply.Val = "DRAGONS"
	return nil
}

// Main server loop.
func main() {
	// Parse args.
	usage := fmt.Sprintf("Usage: %s [client ip:port] [kv-node ip:port] [r]\n",
		os.Args[0])
	if len(os.Args) != 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	clientsIpPort := os.Args[1]
	// kvnodesIpPort := os.Args[2]
	// replicationFactor := os.Args[2]

	// TODO: do here the stuff other

	// Setup key-value store and register service.
	kvservice := new(KeyValService)
	rpc.Register(kvservice)
	l, e := net.Listen("tcp", clientsIpPort)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	for {
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}
}