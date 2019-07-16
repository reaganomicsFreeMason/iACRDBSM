package main

// Main for testing datastore operations

import (
	"fmt"
	"iACRDBSM/db-engine/datastore/key_value"
)

func main() {
	testDB := key_value.NewDataBase()
	// fmt.Println(testDB)
	testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string"},
	)
	// fmt.Println(*testDB)
	// fmt.Println(testDB.GetTable("LongLiveSanjit"))
	testDB.NewTable(
		"LongLiveSanjit2",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string"},
	)
	testDB.DeleteTable("LongLiveSanjit2")
	// fmt.Println(testDB.GetTable("LongLiveSanjit2"))
	sanjitTable, _ := testDB.GetTable("LongLiveSanjit")
	sanjitTable.PutColumn("LindaGong", "Supported-Value-Type.int")
	// fmt.Println(testDB.GetTable("LongLiveSanjit"))
	sanjitTable.PutRow(key_value.Row{
		key_value.SupportedValueTypeImpl{"Supported-Value-Type.int", 1},
		key_value.SupportedValueTypeImpl{"Supported-Value-Type.float", 1.3},
		key_value.SupportedValueTypeImpl{"Supported-Value-Type.string", "sanjawanja"},
		key_value.SupportedValueTypeImpl{"Supported-Value-Type.int", 2},
	})
	table, _ := testDB.GetTable("LongLiveSanjit")
	fmt.Println(table.Rows)
	fmt.Println(table.ColumnsMap)
	sanjitTable.UpdateRow(uint64(0), "Sanjit1", key_value.SupportedValueTypeImpl{"Supported-Value-Type.int", 5})
	table, _ = testDB.GetTable("LongLiveSanjit")
	fmt.Println(table.Rows)
	fmt.Println(table.ColumnsMap)
	sanjitTable.DeleteColumn("Sanjit1")
	fmt.Println(table.Rows)
	fmt.Println(table.ColumnsMap)
	fmt.Println(table.ColumnNames)

	// test update row and delete column together and shit
}
