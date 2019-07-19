package virtual_machine

import (
	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/datastore/key_value"
	kv "iACRDBSM/db-engine/datastore/key_value"
	"testing"
)

func TestNoBad(t *testing.T) {
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

	res, _ := ExecByteCode([]codegen.ByteCodeOp{
		codegen.MakeTableOp{
			"Potato",
			[]string{"type"},
			[]string{"string"},
		},
		codegen.GetTableOp{"Potato"},
		codegen.AddColumnOp{"type"},
		codegen.InsertOp{
			[]string{"type"},
			[]string{"fattening"},
		},
		codegen.AddRowOp{uint32(0)},
	})
	t.Log("hello")
	t.Log("\n" + res)
	res, _ = ExecByteCode([]codegen.ByteCodeOp{
		codegen.DisplayOp{},
	})
	t.Log("hello")
	t.Log("\n" + res)
}

func TestMultipleClientsReadOnly(t *testing.T) {
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

}
