package datastore_test

import (
	"iACRDBSM/db-engine/datastore/key_value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTableSingleThread(t *testing.T) {
	testDB := key_value.NewDataBase()
	err := testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string"},
	)
	assert.NoError(t, err, "table should be created without issue")

	dt, err := testDB.GetTable("LongLiveSanjit")
	assert.NoError(t, err)
	assert.NotNil(t, dt)

	err = testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string"},
	)
	assert.Error(t, err, "table should already exist")

}

// func main() {
// 	// fmt.Println(testDB)

// 	// fmt.Println(*testDB)
// 	// fmt.Println(testDB.GetTable("LongLiveSanjit"))
// 	testDB.NewTable(
// 		"LongLiveSanjit2",
// 		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
// 		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string"},
// 	)
// 	testDB.DeleteTable("LongLiveSanjit2")
// 	// fmt.Println(testDB.GetTable("LongLiveSanjit2"))
// 	sanjitTable, _ := testDB.GetTable("LongLiveSanjit")
// 	sanjitTable.PutColumn("LindaGong", "Supported-Value-Type.int")
// 	// fmt.Println(testDB.GetTable("LongLiveSanjit"))
// 	sanjitTable.PutRow(key_value.Row{
// 		key_value.SupportedValueTypeImpl{"Supported-Value-Type.int", 1},
// 		key_value.SupportedValueTypeImpl{"Supported-Value-Type.float", 1.3},
// 		key_value.SupportedValueTypeImpl{"Supported-Value-Type.string", "sanjawanja"},
// 		key_value.SupportedValueTypeImpl{"Supported-Value-Type.int", 2},
// 	})
// 	table, _ := testDB.GetTable("LongLiveSanjit")
// 	fmt.Println(table.Rows)
// 	fmt.Println(table.columnnames)
// 	sanjitTable.UpdateRow(uint64(0), "Sanjit1", key_value.SupportedValueTypeImpl{"Supported-Value-Type.int", 5})
// 	table, _ = testDB.GetTable("LongLiveSanjit")
// 	fmt.Println(table.Rows)
// 	fmt.Println(table.columnnames)
// 	sanjitTable.DeleteColumn("Sanjit1")
// 	fmt.Println(table.Rows)
// 	fmt.Println(table.columnnames)
// 	fmt.Println(table.ColumnNames)

// 	// test update row and delete column together and shit
// }
