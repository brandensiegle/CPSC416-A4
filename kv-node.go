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

func main(){
	// Parse args.
	usage := fmt.Sprintf("Usage: %s [local ip] [front-end ip:port] [id]\n",
		os.Args[0])
	if len(os.Args) != 4 {
		fmt.Printf(usage)
		os.Exit(1)
	}

	

}