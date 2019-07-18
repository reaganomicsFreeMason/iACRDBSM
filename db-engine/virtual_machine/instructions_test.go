package virtual_machine

import (
	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/datastore/key_value"
	kv "iACRDBSM/db-engine/datastore/key_value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoBad(t *testing.T) {
	// examples on how to use testing suite
	assert.Equal(t, 0, 0)
	// assert.Equal(t, 0, 1)

	theDataBase := DataBase
	theDataBase.NewTable(
		"LongLiveSanjitPart2",
		[]string{
			"col1",
			"col2",
		},
		[]string{
			"Supported-Value-Type.int",
			"Supported-Value-Type.string",
		},
	)
	theTable, _ := theDataBase.GetTable("LongLiveSanjitPart2")
	theTable.PutRow(key_value.Row{
		kv.SupportedValueTypeImpl{"Supported-Value-Type.int", 1},
		kv.SupportedValueTypeImpl{"Supported-Value-Type.string", "one"},
	})
	theTable.PutRow(key_value.Row{
		kv.SupportedValueTypeImpl{"Supported-Value-Type.int", 2},
		kv.SupportedValueTypeImpl{"Supported-Value-Type.string", "two"},
	})
	theTable.PutRow(key_value.Row{
		kv.SupportedValueTypeImpl{"Supported-Value-Type.int", 3},
		kv.SupportedValueTypeImpl{"Supported-Value-Type.string", "three"},
	})
	theTable.PutRow(key_value.Row{
		kv.SupportedValueTypeImpl{"Supported-Value-Type.int", 4},
		kv.SupportedValueTypeImpl{"Supported-Value-Type.string", "four"},
	})
	// fmt.Println(R1)
	// fmt.Println(theTable, theDataBase)
	res, err := ExecByteCode([]codegen.ByteCodeOp{
		codegen.GetTableOp{"LongLiveSanjitPart2"},
		codegen.AddRowOp{uint32(0)},
		codegen.AddRowOp{uint32(1)},
		codegen.AddColumnOp{"col1"},
		codegen.AddColumnOp{"col2"},
		codegen.FilterOp{"col2", "two"},
		codegen.UpdateTableOp{[]string{"col2"}, []string{"three"}},
		codegen.AddRowOp{uint32(0)},
		codegen.AddRowOp{uint32(1)},
		codegen.AddColumnOp{"col1"},
		codegen.AddColumnOp{"col2"},

		// fix repeated thing
	})
	t.Log("error: \n", err)
	t.Log("\n" + res)
}
