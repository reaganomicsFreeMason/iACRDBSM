package datastore_test

import (
	"iACRDBSM/db-engine/datastore/key_value"
	"testing"

	"github.com/stretchr/testify/assert"
)

//TODO(lgong) test column typing

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

	assert.Equal(t, dt.GetAllColumnNames(), []string{"Sanjit1", "Sanjit2", "Sanjit3"})

	err = testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1"},
		[]string{"Supported-Value-Type.int"},
	)
	assert.Error(t, err, "table should already exist")
}

func TestNewTableDifferentLengthInputsThrowsError(t *testing.T) {
	testDB := key_value.NewDataBase()

	err := testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
		[]string{"Supported-Value-Type.int"},
	)
	assert.Error(t, err, "Table should not have been able to be created")

	err = testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float"},
	)
	assert.Error(t, err, "table should not have been able to be created")

}

func TestGetTableAndDeleteTableSingleThread(t *testing.T) {
	testDB := key_value.NewDataBase()
	testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string"},
	)

	err := testDB.DeleteTable("LongLiveSanjit")

	assert.NoError(t, err)

	dt, err := testDB.GetTable("LongLiveSanjit")
	assert.Nil(t, dt)
	assert.Error(t, err)

	dt, err = testDB.GetTable("LongLiveSanjit2")
	assert.Nil(t, dt)
	assert.Error(t, err, "table should not exist")
}

func TestPutColumnSingleThread(t *testing.T) {
	testDB := key_value.NewDataBase()

	testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string"},
	)

	dt, _ := testDB.GetTable("LongLiveSanjit")
	dt.PutColumn("LindaGong", "Supported-Value-Type.int")
	assert.Equal(t, dt.GetAllColumnNames(), []string{"Sanjit1", "Sanjit2", "Sanjit3", "LindaGong"})
}

func TestPutRowSingleThread(t *testing.T) {
	testDB := key_value.NewDataBase()
	testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3", "LindaGong"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string", "Supported-Value-Type.int"},
	)
	dt, _ := testDB.GetTable("LongLiveSanjit")

	dt.PutRow(key_value.Row{
		key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.int", Value: 1},
		key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.float", Value: 1.3},
		key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.string", Value: "sanjawanja"},
		key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.int", Value: 2},
	})

	row, err := dt.GetRow(0)
	assert.NoError(t, err)

	rowVals := []interface{}{}
	for _, obj := range row {
		rowVals = append(rowVals, obj.GetValue())
	}

	assert.Equal(t, []interface{}{1, 1.3, "sanjawanja", 2}, rowVals)
}

func TestUpdateRowSingleThread(t *testing.T) {
	testDB := key_value.NewDataBase()
	testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3", "LindaGong"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string", "Supported-Value-Type.int"},
	)
	dt, _ := testDB.GetTable("LongLiveSanjit")

	dt.PutRow(key_value.Row{
		key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.int", Value: 1},
		key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.float", Value: 1.3},
		key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.string", Value: "sanjawanja"},
		key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.int", Value: 2},
	})

	dt.UpdateRow(uint64(0), "Sanjit1", key_value.SupportedValueTypeImpl{Name: "Supported-Value-Type.int", Value: 5})

	row, err := dt.GetRow(0)
	assert.NoError(t, err)

	rowVals := []interface{}{}
	for _, obj := range row {
		rowVals = append(rowVals, obj.GetValue())
	}

	assert.NotEqual(t, []interface{}{1, 1.3, "sanjawanja", 2}, rowVals)
	assert.NotEqual(t, []interface{}{1, 1.3, "Sanjit1", 2}, rowVals)

}

func TestDeleteColumnSingleThread(t *testing.T) {
	testDB := key_value.NewDataBase()
	testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"},
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string"},
	)
	dt, err := testDB.GetTable("LongLiveSanjit")
	assert.NoError(t, err)
	assert.NotNil(t, dt)

	err = dt.DeleteColumn("Sanjit1")

	assert.NoError(t, err)
	assert.Equal(t, dt.GetAllColumnNames(), []string{"Sanjit2", "Sanjit3"})

}
