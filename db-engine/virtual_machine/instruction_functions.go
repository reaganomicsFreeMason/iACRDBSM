package virtual_machine

import ( // fucking shit go is dumb

	"errors"
	"fmt"
	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/datastore/key_value"
	"strconv"
)

const (
	R1          = codegen.R1
	R2          = codegen.R2
	R3          = codegen.R3
	R4          = codegen.R4
	TABLE_REG   = codegen.R5
	COLUMNS_REG = codegen.R6
	ROWS_REG    = codegen.R7
	ALL_ROWS    = -1 // signifies that all of the rows are being loaded into the register
)

type Register *interface{}

var (
	numRegisters = 20
	Registers    = make([]Register, numRegisters) // each one is an empty register for now
	DataBase     = key_value.NewDataBase()
	// _            = DataBase.NewTable(
	// 	"TestTable",
	// 	[]string{"a", "b", "c"},
	// 	[]string{"Supported-Value-Type.int",
	// 		"Supported-Value-Type.float",
	// 		"Supported-Value-Type.string"},
	// )
)

// func loadInstruction(instruction codegen.LoadValOp) error {
// 	idx := instruction.idx
// 	value := instruction.value
// 	if idx <= 0 {
// 		return errors.New("This is register is like your brain: non-existent.")
// 	} else if value == nil {
// 		return errors.New("Giving a stupid fucking null value, moron. ")
// 	}
// 	Registers[idx] = &value
// 	return nil
// }

// returns whether or not was successful
func getTable(instruction codegen.GetTableOp) error {
	tableName := instruction.Tablename
	tableAddress, err := DataBase.GetTable(tableName)
	if err != nil {
		return err
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter
	var asInter2 interface{}
	asInter2 = ALL_ROWS
	Registers[ROWS_REG] = &asInter2 // initialize the rows reg to have a pointer that says all rows
	// load the special all value into the ROWS

	return nil
}

func addColumn(instruction codegen.AddColumnOp) error {
	columnName := instruction.Colname
	if Registers[COLUMNS_REG] == nil {
		var asInter interface{}
		asInter = map[string]bool{} // change these to sets
		Registers[COLUMNS_REG] = &asInter
	}
	setOfPointers := (*(Registers[COLUMNS_REG])).(map[string]bool) // list of column names
	setOfPointers[columnName] = true
	var asInter2 interface{}
	asInter2 = setOfPointers
	Registers[COLUMNS_REG] = &asInter2
	return nil
}

// TODO: fix bug
func addRow(instruction codegen.AddRowOp) error {
	rowInd := instruction.Idx
	if Registers[ROWS_REG] == nil {
		var asInter interface{}
		asInter = map[uint32]bool{}
		Registers[ROWS_REG] = &asInter
	}
	setOfPointers := *(Registers[ROWS_REG]) // set of pointers to indices
	if setOfPointers == ALL_ROWS {
		setOfPointers = map[uint32]bool{}
	}
	newSet := (setOfPointers).(map[uint32]bool) // list of column names
	newSet[rowInd] = true
	var asInter2 interface{}
	asInter2 = newSet
	// setOfPointers[&rowInd] = true
	Registers[ROWS_REG] = &asInter2
	return nil
}

func clear() error {
	Registers[COLUMNS_REG] = nil
	Registers[ROWS_REG] = nil
	return nil
}

// TODO format so it looks better
func display() string { // return the display string
	// assume, for now, everything is valid in the registers
	res := ""
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	columnNames := tableAddress.ColumnNames

	setOfQueriedColumns := map[string]bool{}
	goodIndices := map[uint32]bool{}
	for colNamePointer := range (*(Registers[COLUMNS_REG])).(map[string]bool) {
		setOfQueriedColumns[colNamePointer] = true // CHANGE VAR NA<ME
	}

	// print the columns
	for i, columnName := range columnNames {
		if _, found := setOfQueriedColumns[columnName]; found {
			goodIndices[uint32(i)] = true
			res += " " + columnName + " "
		}
	}
	res += "\n" // new line as a conclusion

	// TODO: error handle THIS SHIT THIS IS NASTY
	if *Registers[ROWS_REG] != ALL_ROWS {
		for rowIndPointer := range (*(Registers[ROWS_REG])).(map[uint32]bool) {
			row, _ := tableAddress.GetRow(uint64(rowIndPointer))
			for i, elem := range row {
				if _, found := goodIndices[uint32(i)]; found {
					asValue := elem.(key_value.SupportedValueType)
					res += " " + supValToString(asValue) + " "
				}
			}
			res += "\n" // new row
		}
	} else {
		for i := range tableAddress.Rows {
			rowIndPointer := i
			row, _ := tableAddress.GetRow(uint64(rowIndPointer))
			for i, elem := range row {
				if _, found := goodIndices[uint32(i)]; found {
					asValue := elem.(key_value.SupportedValueType)
					res += " " + supValToString(asValue) + " "
				}
			}
			res += "\n" // new row
		}
	}
	return res[1 : len(res)-1] // ignore the first whitespace character and the last new line char.
}

func filter(instruction codegen.FilterOp) error {
	// value is a string; convert it to the supportedValue

	colName := instruction.ColName
	valueName := instruction.ValName
	val := makeSupportedVal(colName, valueName)

	setOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	columnInfoMap, _ := tableAddress.GetColumn(colName)
	goodIndices := columnInfoMap.Values[val] // set of the valid indices

	if *(Registers[ROWS_REG]) == ALL_ROWS {
		for index := range goodIndices {
			addRow(codegen.AddRowOp{uint32(index)})
		}
		return nil
	}

	newSetOfPointers := map[uint32]bool{}
	for formerAddress := range setOfPointers.(map[uint32]bool) {
		if _, found := goodIndices[uint64(formerAddress)]; found {
			newSetOfPointers[formerAddress] = true
		}
	}
	var asInter interface{}
	asInter = newSetOfPointers
	Registers[ROWS_REG] = &asInter
	return nil
}

func insert(instruction codegen.InsertOp) error {
	// first, make a map from the colNames to the values
	colNameToValue := map[string]string{}
	colNames, valNames := instruction.ColNames, instruction.ValNames
	numNamesGiven := len(colNames)
	for i := 0; i < numNamesGiven; i++ {
		colName := colNames[i]
		valName := valNames[i]
		colNameToValue[colName] = valName
	}
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table // need this soon
	tableColNamesOfficial := tableAddress.ColumnNames
	rowToInsert := make(key_value.Row, len(tableColNamesOfficial)) // just give everything a null value for now
	for i, tableColName := range tableColNamesOfficial {
		if tableColName == "" {
			continue
		} else if val, found := colNameToValue[tableColName]; !found {
			continue
		} else {
			rowToInsert[i] = makeSupportedVal(tableColName, val)
		}
	}
	return tableAddress.PutRow(rowToInsert) // should be nil?
}

func makeTable(instruction codegen.MakeTableOp) error {
	colNames := instruction.ColNames
	colTypes := instruction.ColTypes
	tableName := instruction.TableName
	tableTypes := make([]string, len(colNames)) // just do it pre made
	for i := 0; i < len(colNames); i++ {
		colType := colTypes[i]
		tableType := normalToTableType(colType)
		tableTypes[i] = tableType
		// do some error handling here later
	}
	return DataBase.NewTable(tableName, colNames, colTypes)
}

// Straightforward instruction

func deleteTable(instruction codegen.DeleteTableOp) error {
	tableName := instruction.TableName
	return DataBase.DeleteTable(tableName)
}

// DELETE is going to look at registers and delete anything in the rows register

func deleteRows() error {
	setOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	for indAddress := range setOfPointers.(map[uint32]bool) {
		index := indAddress
		tableAddress.DeleteRow(uint64(index))
		// error handling TODO
	}
	return nil
}

// DELETE is going to look at registers and delete anything in the cols register

func deleteCols() error {
	setOfPointers := *(Registers[COLUMNS_REG]) // list of pointers to indices
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	for colNameAddress := range setOfPointers.(map[string]bool) {
		colName := colNameAddress
		tableAddress.DeleteColumn(colName)
		// error handling TODO
	}
	return nil

}

func updateTable(instruction codegen.UpdateTableOp) error {
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colNamesToChange := instruction.ColNames
	newVals := instruction.NewVals
	// UpdateRow(rowIndex uint64, colName string, newValue SupportedValueType)

	setOfPointers := *(Registers[ROWS_REG])
	for indAddress := range setOfPointers.(map[uint32]bool) {
		index := indAddress
		for i := 0; i < len(colNamesToChange); i++ {
			colName := colNamesToChange[i]
			approproVal := makeSupportedVal(colName, newVals[i])
			tableAddress.UpdateRow(uint64(index), colName, approproVal)
		}
	}
	return nil
}

func makeSupportedVal(colName, valName string) key_value.SupportedValueType {
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colType := tableAddress.ColumnsMap[colName].Type
	var asInterface interface{}
	switch colType {
	case "Supported-Value-Type.int":
		asInterface, _ = strconv.Atoi(valName)
	case "Supported-Value-Type.float":
		asInterface, _ = strconv.ParseFloat(valName, 32)
	case "Supported-Value-Type.string":
		asInterface = valName
	}
	return key_value.SupportedValueTypeImpl{colType, asInterface}
}

func supValToString(asValue key_value.SupportedValueType) string {
	switch asValue.GetName() {
	case "Supported-Value-Type.int":
		return strconv.Itoa(asValue.GetValue().(int))
	case "Supported-Value-Type.float":
		return fmt.Sprintf("%f", asValue.GetValue().(float32))
	case "Supported-Value-Type.string":
		return asValue.GetValue().(string)
	}
	return ""
}

// TODO replace GetRedIndex is now replaced with the valid register named; put
// them in later.
// TODO HELLA FUCKING ERROR HANDLING

func ExecByteCode(instructions []codegen.ByteCodeOp) (string, error) {
	// FOR TESTING RID OF THIS LATER!!!!!~
	// tableAddress, _ := DataBase.GetTable("TestTable")
	// var asInter interface{}
	// asInter = *tableAddress
	// Registers[TABLE_REG] = &asInter
	// END stuff to rid later
	for _, instruction := range instructions {
		instName := instruction.GetOpName()
		switch instName {
		case "GetTableOp":
			getTable(instruction.(codegen.GetTableOp))
		case "AddColumnOp":
			addColumn(instruction.(codegen.AddColumnOp))
		case "AddRowOp":
			addRow(instruction.(codegen.AddRowOp))
		case "FilterOp":
			filter(instruction.(codegen.FilterOp))
		default:
			return "", errors.New("Bad instruction shit face")
		}
	}
	res := display()
	clear()
	return res, nil
}

func normalToTableType(colType string) string {
	switch colType {
	case "int":
		return "Supported-Value-Type.int"
	case "float":
		return "Supported-Value-Type.float"
	case "string":
		return "Supported-Value-Type.string"
	}
	return ""
}

// CHANGE SETS TO BE VALUES RATHER THAN REFERENCES
