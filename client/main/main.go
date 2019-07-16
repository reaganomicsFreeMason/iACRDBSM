package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

const (
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
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("iARCDBSM: ")
		text, _ := reader.ReadString('\n')
		fmt.Fprintf(conn, text+"\n")
		message, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print("Message from server: " + message)
	}
}
