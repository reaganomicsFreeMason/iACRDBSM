package main

import (
	"fmt"
	repl "iACRDBSM/db-engine/repl"
	tcpserver "iACRDBSM/db-engine/tcpserver"
	"os"
)

// Entry point for iACRDBSM. To use a local repl for testing purposes, execute as "go run main.go local"
func main() {
	args := os.Args[1:]
	if len(args) > 0 {
		if args[0] == "local" {
			repl.REPL()
		} else {
			fmt.Println("Invalid command line argument")
			os.Exit(1)
		}
	} else {
		tcpserver.RunServer()
	}
}
