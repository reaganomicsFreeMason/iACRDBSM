package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

const (
	// TODO: These will be the server network information eventually
	connHost = "localhost"
	connPort = "3333"
	connType = "tcp"
)

func main() {
	conn, err := net.Dial(connType, connHost+":"+connPort)

	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Some problem connecting.")
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		os.Exit(1)
	}

	for {
		// Simple REPL that sends SQL strings to the server
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("\n iARCDBSM: ")
		text, _ := reader.ReadString('\n')
		fmt.Fprintf(conn, text+"\n")
		message, err := bufio.NewReader(conn).ReadString('\r')
		if err != nil {
			fmt.Println("Issue reading server response " + err.Error())
		} else {
			fmt.Print(message)
		}
	}
}
