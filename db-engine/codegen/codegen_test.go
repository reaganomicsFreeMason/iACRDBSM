package codegen

/* All these tests do is print out the instructions generated.
   They will all fail because we force the test logs to print
   By asserting that 1 == 0 in PrintInsns.
*/

import (
	"iACRDBSM/db-engine/parser"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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
		assert.Equal(t, 1, 0)
	}
	insns, _ := GenByteCode(ast)
	for _, i := range insns {
		t.Log(i.GetOpName())
	}
	assert.Equal(t, 1, 0)
}

func TestSelect(t *testing.T) {
	PrintInsns(t, "SELECT c1, c2, c3, FROM t1, WHERE col1 = v1, col2 = v2,")
}

func TestMakeTable(t *testing.T) {
	PrintInsns(t, "CREATE TABLE t1 (col1 int, col2 string, col3 float,)")
}

func TestInsert(t *testing.T) {
	PrintInsns(t, "INSERT INTO t1 (col1, col2, col3,) VALUES (v1, v2, v3,)")
}
