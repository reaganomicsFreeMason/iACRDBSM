package main

import (
	"fmt"
	"iACRDBSM/db-engine/parser"
	"iACRDBSM/db-engine/repl"
	"iACRDBSM/db-engine/tcpserver"
	"os"
)

// Entry point for iACRDBSM. To use a local repl for testing purposes, execute as "go run main.go local"
func main() {
	// Initialize SQL parser
	initParserErr := parser.InitParser()
	if initParserErr != nil {
		fmt.Println("The parser generator crashed")
		os.Exit(1)
	}

	// Process command line argumets
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
