package virtual_machine

import ( // fucking shit go is dumb

	"errors"
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
		asInter = []*string{}
		Registers[COLUMNS_REG] = &asInter
	}
	listOfPointers := *(Registers[COLUMNS_REG]) // list of column names
	var asInter2 interface{}
	asInter2 = append(listOfPointers.([]*string), &columnName)
	Registers[COLUMNS_REG] = &asInter2
	return nil
}

func addRow(instruction codegen.AddRowOp) error {
	rowInd := instruction.Idx
	if Registers[ROWS_REG] == nil {
		var asInter interface{}
		asInter = []*uint32{}
		Registers[ROWS_REG] = &asInter
	}
	listOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
	var asInter2 interface{}
	asInter2 = append(listOfPointers.([]*uint32), &rowInd)
	Registers[ROWS_REG] = &asInter2
	return nil
}

func clear() error {
	Registers[COLUMNS_REG] = nil
	Registers[ROWS_REG] = nil
	return nil
}

func display() string { // return the display string
	// assume, for now, everything is valid in the registers
	res := ""
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	columnNames := tableAddress.ColumnNames

	setOfQueriedColumns := map[string]bool{}
	goodIndices := map[uint32]bool{}
	for _, colNamePointer := range (*(Registers[COLUMNS_REG])).([]*string) {
		setOfQueriedColumns[*colNamePointer] = true
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
	for _, rowIndPointer := range (*(Registers[ROWS_REG])).([]*uint32) {
		row, _ := tableAddress.GetRow(uint64(*rowIndPointer))
		for i, elem := range row {
			if _, found := goodIndices[uint32(i)]; found {
				res += " " + elem.(string) + " "
			}
		}
		res += "\n" // new row
	}
	return res[1 : len(res)-1] // ignore the first whitespace character and the last new line char.
}

func filter(instruction codegen.FilterOp) error {
	// value is a string; convert it to the supportedValue

	colName := instruction.ColName
	valueName := instruction.ValName
	val := makeSupportedVal(colName, valueName)

	listOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
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

	newListOfPointers := make([]*uint32, 0)
	for _, formerAddress := range listOfPointers.([]*uint32) {
		if _, found := goodIndices[uint64(*formerAddress)]; found {
			newListOfPointers = append(newListOfPointers, formerAddress)
		}
	}
	var asInter interface{}
	asInter = newListOfPointers
	Registers[ROWS_REG] = &asInter
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

// TODO replace GetRedIndex is now replaced with the valid register named; put
// them in later.
// TODO HELLA FUCKING ERROR HANDLING

func ExecByteCode(instructions []codegen.ByteCodeOp) (string, error) {
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
	clear()
	return display(), nil
}
