package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	// TODO: Change IP address to server IP eventually
	conn, _ := net.Dial("tcp", "205.189.0.129")
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("iARCDBSM: ")
		text, _ := reader.ReadString('\n')
		fmt.Fprintf(conn, text+"\n")
		message, _ := bufio.NewReader(conn).ReadString('\n')
		fmt.Print("Message from server: " + message)
	}
}
