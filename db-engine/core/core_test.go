package core

/* These tests verify the state of the datastore after sequences
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
	result, err := ProcessSQLString(cmdString)
	if err != nil {
		t.Log("Error Processing: " + err.Error())
	} else {
		t.Log("QUERY RESULT OUTPUT\n")
		t.Log(result)
		t.Log("FULL TABLE OUTPUT\n")
		// TODO: Probably want this to display all tables in database
		result, _ := ProcessSQLString("SELECT ALL, FROM " + tableName)
	}
}

func TestSelect(t *testing.T) {
	ExecuteAndShow("CREATE TABLE tablename (c1 string,)", "tablename")
	v2 = "INSERT INTO tablename (c1,) VALUES (hello,)", "tablename")
	v3 = "SELECT c1, FROM tablename"

}
