package tcpserver

import (
	"bufio"
	"errors"
	"fmt"
	core "iACRDBSM/db-engine/core"
	"net"
	"os"
)

const (
	connHost = "localhost"
	connPort = "3333"
	connType = "tcp"
)

// RunServer - Process sql commands over the network via TCP
func RunServer() {
	//Handle connections
	ln, err := net.Listen(connType, connHost+":"+connPort)

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	//Close listener once function returns
	defer ln.Close()

	fmt.Println("Listening on " + connHost + ":" + connPort)

	//Accept client connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting incoming connection:", err.Error())
			os.Exit(1)
		}
		quit := make(chan int)
		go handleClient(conn, quit)
	}
}

// TODO: Properly handle client killing process, so we don't
// have to restart server every time.
func handleClient(conn net.Conn, c chan int) {
	for {
		// Read input string from client
		sqlstr, err := readInputFromCon(conn)
		if err != nil {
			// Error reading string from socket
			fmt.Fprintf(conn, err.Error())
		} else {
			if sqlstr == "exit\n" {
				break
			}
			// Process SQL string
			result, err := core.ProcessSQLString(sqlstr)
			if err != nil {
				fmt.Fprintln(conn, err.Error()+"\r")
			} else {
				// This is really bad, and we should probably not check
				// that the end of the result string is a \r on the client side
				fmt.Fprintln(conn, result+"\r")
				fmt.Println("SERVER RESULT")
				fmt.Println(result)
			}
		}
	}
	fmt.Fprintf(conn, "closed"+"\r")
	c <- 3
}

func readInputFromCon(conn net.Conn) (string, error) {
	//Read input string from client
	message, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		readErr := errors.New("Error reading message from client: " + err.Error())
		return "", readErr
	}
	return message, nil
}
