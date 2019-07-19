package core

/* These tests verify states of the datastore after sequences
   of SQL instructions
*/

import (
	"iACRDBSM/db-engine/parser"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

func setup() {
	parser.InitParser()
}

func ExecuteAndShow(t *testing.T, cmdString string, tableName string) {
	t.Log("QUERY INPUT")
	t.Log(cmdString)
	t.Log()

	result, err := ProcessSQLString(cmdString)
	if err != nil {
		t.Log("Error Processing: " + err.Error())
	} else {
		// Display query output
		t.Log("QUERY RESULT OUTPUT")
		t.Log(result)
		t.Log()

		// Display specified table
		result, err := ProcessSQLString("SELECT ALL, FROM " + tableName)
		if err != nil {
			t.Log("Error dumping table: " + err.Error())
		} else {
			t.Log("FULL TABLE OUTPUT")
			t.Log(result)
		}
	}
}

func TestSelect(t *testing.T) {
	ExecuteAndShow(t, "CREATE TABLE tablename (c1 string,)", "tablename")
}
