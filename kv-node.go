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
	//"net"
	//"net/rpc"
	"os"
	//"strings"
)

// Map implementing the key-value store.
var kvmap map[string]*MapVal

// Mutex for accessing kvmap from different goroutines safely.
var mapMutex *sync.Mutex

// Reserved value in the service that is used to indicate that the key
// is unavailable: used in return values to clients and internally.
const unavail string = "unavailable"

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

func main(){
	// Parse args.
	usage := fmt.Sprintf("Usage: %s [local ip] [front-end ip:port] [id]\n",
		os.Args[0])
	if len(os.Args) != 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	

}