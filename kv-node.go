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
	//"log"
	"net"
	//"net/rpc"
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

type FrontEndCommand struct {
	Command string
	Key     string
	Value   string
	TestVal string
}

type FrontEndReply struct {
	Message string
}

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

func putToKVN(key string, value string) error {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	val := lookupKey(key)
	val.value = value
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

	//connect to front end
	conn, err := net.Dial("tcp", frontEndAdress)
	if err != nil {
		fmt.Println("Error on connect: ", err)
		os.Exit(-1)
	}

	var buf [1024]byte
	num, err := conn.Read(buf[:])
	if err != nil {
		fmt.Println("Error on read: ", err)
		os.Exit(-1)
	}

	if string(buf[0:num]) == "Success" {
		println(string(buf[0:num]))
	}

	_, werr := conn.Write([]byte(myID))
	if werr != nil {
		fmt.Println("Error on write: ", werr)
		os.Exit(-1)
	}

	//for{
	//wait for command

	//parse and execute command

	//return to front end

	//}

}
