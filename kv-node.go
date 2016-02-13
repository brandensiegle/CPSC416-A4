// Code for kv-node
//
// Usage: go run kv-node.go [local ip] [front-end ip:port] [id]
//
// - [local ip] : local IP to use when connecting to front-end
// - [front-end ip:port] : address of the front-end node
// - [id] : a unique string identifier for this kv node (no spaces)

package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	//"strings"
	"sync"
)

// Map implementing the key-value store.
var kvmap map[string]*MapVal

// Value in the key-val store.
type MapVal struct {
	value string // the underlying value representation
}

// Mutex for accessing kvmap from different goroutines safely.
var mapMutex *sync.Mutex

var myAddress string
var frontEndAdress string
var myID string

// Reserved value in the service that is used to indicate that the key
// is unavailable: used in return values to clients and internally.
const unavail string = "unavailable"



type TestSetArgs struct {
	Key     string // key to test
	TestVal string // value to test against actual value
	NewVal  string // value to use if testval equals to actual value
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

//reply
type kvnReply struct {
	Val string
}

type MapMessage struct {
	Map map[string]*MapVal
}

type mapReply struct {
	Map map[string]*MapVal
}

//rpc service
type KeyValNode int

// Lookup a key, and if it's used for the first time, then initialize its value.
func lookupKey(key string) *MapVal {
	// lookup key in store
	val := kvmap[key]
	if val == nil {
		// key used for the first time: create and initialize a MapVal
		// instance to associate with a key
		val = &MapVal{
			value: "",
		}
		kvmap[key] = val
	}
	return val
}

func (kvn *KeyValNode) getNodeID(arg string, reply *kvnReply) error {
	reply.Val = myID

	return nil
}

func (kvn *KeyValNode) getFromKVN(getArgs *GetArgs, reply *kvnReply) error {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	val := lookupKey(getArgs.Key)
	reply.Val = val.value


	return nil
}

func (kvn *KeyValNode) putToKVN(putArgs *PutArgs, reply *kvnReply) error {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	val := lookupKey(putArgs.Key)
	val.value = putArgs.Val
	return nil
}

func (kvn *KeyValNode) kvnTestSet(args *TestSetArgs, reply *kvnReply) error {
	// Acquire mutex for exclusive access to kvmap.
	mapMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer mapMutex.Unlock()

	val := lookupKey(args.Key)

	// Execute the testset.
	if val.value == args.TestVal {
		val.value = args.NewVal
	}

	// reply.Val = val.value
	return nil
}

func (kvn *KeyValNode) getMap(arg, string, reply *mapReply) error {
	// Acquire mutex for exclusive access to kvmap.
	mapMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer mapMutex.Unlock()

	reply.Map = kvmap

	return nil
}

func (kvn *KeyValNode) putMap(newMap *MapMessage, reply *kvnReply) error {
	// Acquire mutex for exclusive access to kvmap.
	mapMutex.Lock()
	// Defer mutex unlock to (any) function exit.
	defer mapMutex.Unlock()

	kvmap = newMap.Map

	reply.Val = "Success"

	return nil
}

func (kvn *KeyValNode) killNode(arg string, reply *kvnReply) error {

	os.Exit(-1)

	return nil
}




func main() {
	// Parse args.
	usage := fmt.Sprintf("Usage: %s [local ip] [front-end ip:port] [id]\n",
		os.Args[0])
	if len(os.Args) != 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	myAddress = os.Args[1]
	frontEndAdress = os.Args[2]
	myID = os.Args[3]


	keyValNode := new(KeyValNode)
	rpc.Register(keyValNode)

	//connect to front end
	conn, err := net.Dial("tcp", frontEndAdress)
	if err != nil {
		fmt.Println("Error on connect: ", err)
		os.Exit(-1)
	}

	conn.Close()

	l, e := net.Listen("tcp", myAddress)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	for{
		conn, _ := l.Accept()
		go rpc.ServeConn(conn)
	}

}
