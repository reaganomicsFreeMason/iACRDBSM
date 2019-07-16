package main

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/xwb1989/sqlparser"
)

const (
	connHost = "localhost"
	connPort = "3333"
	connType = "tcp"
)

func main() {
	ln, err := net.Listen(connType, connHost+":"+connPort)

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

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

func handleClient(conn net.Conn) {
	for {
		//Read input string from client
		sqlString := readInput(conn)
		//Parse input string into an AST
		ast := parseInput(sqlString)
		_ = ast
		print("generated ast!")
		os.Exit(1)
	}
}

func readInput(conn net.Conn) string {
	//Read input string from client
	message, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error reading message from client:", err.Error())
		os.Exit(1)
	}
	return message
}

func parseInput(sqlString string) sqlparser.Statement {
	stmt, err := sqlparser.Parse(sqlString)
	if err != nil {
		fmt.Println("Error parsing SQL statemnt: ", err)
		os.Exit(1)
	}
	return stmt
}
