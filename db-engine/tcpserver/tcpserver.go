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

		handleClient(conn)
	}
}

// TODO: Properly handle client killing process, so we don't
// have to restart server every time.
func handleClient(conn net.Conn) {
	for {
		// Read input string from client
		sqlstr, err := readInputFromCon(conn)
		if err != nil {
			// Error reading string from socket
			fmt.Fprintf(conn, err.Error())
		} else {
			// Process SQL string
			result, err := core.ProcessSQLString(sqlstr)
			if err != nil {
				fmt.Fprintln(conn, err.Error())
			} else {
				fmt.Fprintln(conn, "Result: "+result)
			}
		}
	}
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
