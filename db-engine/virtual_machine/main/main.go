package main

import (
	"fmt"
	"iACRDBSM/db-engine/datastore/key_value"
	kv "iACRDBSM/db-engine/datastore/key_value"
	vm "iACRDBSM/db-engine/virtual_machine"
)

func main() {
	theDataBase := vm.DataBase
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
	fmt.Println(vm.R1)
	fmt.Println(theTable, theDataBase)
}
