package repl

import (
	"bufio"
	"fmt"
	core "iACRDBSM/db-engine/core"
	"os"
)

//REPL - Simple Read-Eval-Print-Loop for testing
func REPL() {
	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Print("iACRDBSM: ")
		text, _ := reader.ReadString('\n')
		result, err := core.ProcessSQLString(text)
		if err != nil {
			// TODO: Probably need better error handling than this..
			fmt.Println("Error: " + err.Error())
		} else {
			fmt.Println("Result: " + result)
		}
	}
}
