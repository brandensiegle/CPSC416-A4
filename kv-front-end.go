// Code for key-value front-end that supports three API calls over rpc:
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
	"strconv"
	"strings"
	"sync"
)

type MapVal struct {
	value string // the underlying value representation
}

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

type kvnReply struct {
	Val string
}

type Empty struct {
	Val string
}

type kvNodeItem struct {
	kvNodeConn rpc.Client
	nodeID     string
	nextKVNode *kvNodeItem
}

type MapMessage struct {
	Map map[string]*MapVal
}

type mapReply struct {
	Map map[string]*MapVal
}

type KeyValService int
type KeyValNode int

var firstNode *kvNodeItem
var lastNode *kvNodeItem

var addKVConnMutex *sync.Mutex
var replicationFactor int
var err error
var numNodes int // number of nodes in system
var kvNodeStore *rpc.Client

func getFromKVNodes(key string) string {

	return "STRING"
}

func putToKVNodes(key string, value string) string {
	// put in replicationFactor nodes
	node := firstNode
	var rep int

	if numNodes < replicationFactor {
		rep = numNodes
	} else {
		rep = replicationFactor
	}

	for i := 0; i < rep; i++ {
		err := node.kvNodeConn.Call("",1,1) // TODO, make RPC call to KV node
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		node = node.nextKVNode
	}
	return ""
}

func testSetKVNodes(key string, value string, testVal string) string {
	node := firstNode

	for node != lastNode || node == lastNode {
		// call kvnTestSet in kv-node with testArgs
		node = node.nextKVNode
	}
	return "STRING"
}

// GET
func (kvs *KeyValService) Get(args *GetArgs, reply *ValReply) error {
	// Check if the issued get command is a CMD operation.

	//var kvnVal kvnReply
	//var dur Empty

	if args.Key[0:3] == "CMD" {
		// Split by spaces.
		tokens := strings.Fields(args.Key)
		if len(tokens) == 3 {
			if tokens[1] == "get-replicas-of" {
				// get("CMD get-replicas-of k")
				key := tokens[2]
				fmt.Println("get(CMD " + tokens[1] + " " + key + ")")

				
				// TODO
				//stringOfNodes := ""
				//node := firstNode
				//stringOfNodes = firstNode.nodeID
				//node = firstNode.nextKVNode
				

				//reply.Val = stringOfNodes // No replicas for this key in the system.
				return nil
			} else if tokens[1] == "kill-replica" {
				// get("CMD kill-replica id")
				replicaId := tokens[2]
				fmt.Println("get(CMD " + tokens[1] + " " + replicaId + ")")

				// TODO

				//err := lastNode.kvNodeConn.Call("KeyValNode.getNodeID", dur, &kvnVal)
				//checkError(err)

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
	putToKVNodes(args.Key, args.Val)
	reply.Val = "DRAGONS~"
	return nil
}

// TESTSET
func (kvs *KeyValService) TestSet(args *TestSetArgs, reply *ValReply) error {
	testSetKVNodes(args.Key, args.NewVal, args.TestVal)

	reply.Val = "DRAGONS"
	return nil
}

func replicateNodes() {
	// replicate keys/values when node dies
	// onto next node

	var mapResponse mapReply
	mapSend := MapMessage{
		Map: nil,
	}
	var empty Empty
	var kvr kvnReply

	node := firstNode

	for i := 0; i < replicationFactor; i++ {
		if i==0 {
			node.kvNodeConn.Call("keyValNode.getMap", empty, &mapResponse)
			mapSend.Map = mapResponse.Map
			node = node.nextKVNode
		} else if  i == (replicationFactor-1) {
			node.kvNodeConn.Call("keyValNode.putMap", mapSend, &kvr)
		} else {
			node = node.nextKVNode
		}
	}

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
	kvnodesIpPort := os.Args[2]
	replicationFactor, err = strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	firstNode = nil
	lastNode = nil

	kvNodeListen, err := net.Listen("tcp", kvnodesIpPort)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	clientService := new(KeyValService)
	rpc.Register(clientService)
	clientListen, err := net.Listen("tcp", clientsIpPort)
	if err != nil {
		log.Fatal("listen error:", err)
	}

	//accept the clients
	go func() {
		println("Waiting For Clients")
		for {
			clientConn, _ := clientListen.Accept()
			go rpc.ServeConn(clientConn)
			print("Client Connection Accepted")
		}
	}()

	//accept the kv nodes
	for {
		println("Waiting For KVNodes")

		kvConn, err := kvNodeListen.Accept()
		if err != nil {
			fmt.Println("Error on 183: ", err)
			os.Exit(-1)
		}

		
		// address of kv node
		//nodeAddr := kvConn.RemoteAddr().String()
		
		var cmd []byte
    	fmt.Fscan(kvConn, &cmd)

    	//fmt.Println("Message:", string(cmd))

    	println(string(cmd))

		kvNode, err := rpc.Dial("tcp", string(cmd))
		checkError(err)
		kvNodeStore = kvNode



		go handleNewKVNode(kvNodeStore)
		print("KVNode Connection Accepted")
	}

}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		os.Exit(1)
	}
}

func handleNewKVNode(kvNodeStore *rpc.Client) {
	//setup Connection between FE and KVNode
	//create new list item
	addKVConnMutex.Lock()
		

	var newNode *kvNodeItem
	var kvnVal kvnReply
	dur := Empty{
		Val: " ",
	}

	if lastNode == nil {
		newNode = &kvNodeItem{
			kvNodeConn: *kvNodeStore,
			nodeID:     "",
			nextKVNode: nil,
		}

		firstNode = newNode
		lastNode = newNode
	} else {
		newNode = &kvNodeItem{
			kvNodeConn: *kvNodeStore,
			nodeID:     "",
			nextKVNode: nil,
		}

		lastNode.nextKVNode = newNode
		lastNode = newNode
	}
	addKVConnMutex.Unlock()

	err := lastNode.kvNodeConn.Call("keyValNode.getNodeID", dur, &kvnVal)
	checkError(err)

	newNode.nodeID = kvnVal.Val
	numNodes++

	return
}
