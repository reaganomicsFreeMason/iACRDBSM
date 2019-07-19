package codegen

/* All these tests do is print out the instructions generated.
   They will all fail because we force the test logs to print
   By asserting that 1 == 0 in PrintInsns.
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

func PrintInsns(t *testing.T, cmdString string) {
	ast, err := parser.ParseInput(cmdString)
	if err != nil {
		t.Log("Parse Error: " + err.Error())
	}
	insns, err := GenByteCode(ast)
	if err != nil {
		t.Log("GenByteCode Error: " + err.Error())
	}
	for _, i := range insns {
		t.Log(i.GetOpName())
	}
}

func TestSelect(t *testing.T) {
	PrintInsns(t, "SELECT c1, c2, c3, FROM t1, WHERE col1 = 3, col2 = 2,")
}

func TestMakeTable(t *testing.T) {
	PrintInsns(t, "CREATE TABLE t1 (col1 int, col2 string, col3 float,)")
}

func TestInsert(t *testing.T) {
	PrintInsns(t, "INSERT INTO t1 (col1, col2, col3,) VALUES (v1, v2, v3,)")
}

func TestUpdate(t *testing.T) {
	PrintInsns(t, "UPDATE tablename SET col1 = 1, col2 = 2, col3 = 3, WHERE col1 = 3,")
}

func TestQuerySeq(t *testing.T) {
	PrintInsns(t, "CREATE TABLE tablename (c1 string,)")
	t.Log("\n")
	PrintInsns(t, "INSERT INTO tablename (c1,) VALUES (hello,)")
	t.Log("\n")
	PrintInsns(t, "SELECT c1, FROM tablename,")
}

func TestDelete(t *testing.T) {
	PrintInsns(t, "DELETE FROM t WHERE c = 4,")
}
