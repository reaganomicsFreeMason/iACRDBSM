package virtual_machine

import ( // fucking shit go is dumb

	"errors"
	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/datastore/key_value"
	"sort"
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
	_            = DataBase.NewTable(
		"TestTable",
		[]string{"a", "b", "c"},
		[]string{"Supported-Value-Type.int",
			"Supported-Value-Type.float",
			"Supported-Value-Type.string"},
	)
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

//Adds columns to the registers to filter them
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

//Adds rows to the registers to filter them
func addRow(instruction codegen.AddRowOp) error {
	rowInd := instruction.Idx
	if Registers[ROWS_REG] == nil {
		var asInter interface{}
		asInter = []*uint32{}
		Registers[ROWS_REG] = &asInter
	}
	listOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
	if listOfPointers == ALL_ROWS {
		listOfPointers = []*uint32{}
	}
	var asInter2 interface{}
	asInter2 = append(listOfPointers.([]*uint32), &rowInd)
	Registers[ROWS_REG] = &asInter2
	return nil
}

//Empties the registers
func clear() error {
	Registers[COLUMNS_REG] = nil
	Registers[ROWS_REG] = nil
	return nil
}

func display() string { // return the display string
	// assume, for now, everything is valid in the registers
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	columnNames := tableAddress.GetAllColumnNames()
	res := ""
	//columnHeader := []string{}
	//data := [][]string{}
	//mytable := tablewriter.NewWriter(os.Stdout)

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

	// TODO: error handle this
	if *Registers[ROWS_REG] != ALL_ROWS {
		keys := []uint32{}
		for rowIndPointer := range (*(Registers[ROWS_REG])).(map[uint32]bool) {
			keys = append(keys, rowIndPointer)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		for _, rowIndPointer := range keys {
			row, _ := tableAddress.GetRow(uint64(rowIndPointer))
			for i, elem := range row {
				if _, found := goodIndices[uint32(i)]; found {
					asValue := elem.(key_value.SupportedValueType)
					res += " " + key_value.SupValToString(asValue) + " "
				}
			}
			res += "\n" // new row
		}
	} else {
		res += tableAddress.GetAllRowNames(goodIndices)
	}
	return res[1 : len(res)-1]
}

//Filters columns
func filter(instruction codegen.FilterOp) error {
	// value is a string; convert it to the supportedValue

	colName := instruction.ColName
	valueName := instruction.ValName
	val, err := makeSupportedVal(colName, valueName)
	if err != nil {
		return err
	}
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

//Inserts a row to the table
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
	tableColNamesOfficial := tableAddress.GetAllColumnNames()
	rowToInsert := make(key_value.Row, len(tableColNamesOfficial)) // just give everything a null value for now
	for i, tableColName := range tableColNamesOfficial {
		if tableColName == "" {
			continue
		} else if val, found := colNameToValue[tableColName]; !found {
			continue
		} else {
			rowToInsert[i], _ = makeSupportedVal(tableColName, val)
		}
	}
	// fmt.Println("THISROW", rowToInsert)
	tableAddress.PutRow(rowToInsert) // should be nil?
	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter
	// fmt.Println(*(Registers[TABLE_REG]), "table")
	return nil
	// UPDATE THE REGISTER WITH THE CORRECT ADDRESS
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
	return DataBase.NewTable(tableName, colNames, tableTypes)
}

// Straightforward instruction

func deleteTable(instruction codegen.DeleteTableOp) error {
	tableName := instruction.TableName
	return DataBase.DeleteTable(tableName)
}

// DELETE is going to look at registers and delete anything in the rows register

func deleteRows() error {
	listOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	for _, indAddress := range listOfPointers.([]*uint32) {
		index := *indAddress
		tableAddress.DeleteRow(uint64(index))
		// error handling TODO
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter
	return nil
}

// DELETE is going to look at registers and delete anything in the cols register

func deleteCols() error {
	listOfPointers := *(Registers[COLUMNS_REG]) // list of pointers to indices
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	for _, colNameAddress := range listOfPointers.([]*string) {
		colName := *colNameAddress
		tableAddress.DeleteColumn(colName)
		// error handling TODO
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter
	return nil

}

func deleteColFromTable(instruction codegen.DeleteColFromTableOp) error {
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colName := instruction.ColName
	tableAddress.DeleteColumn(colName)
	return nil
}

func updateTable(instruction codegen.UpdateTableOp) error {
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colNameToChange := instruction.ColName
	newVal := instruction.NewVal
	// UpdateRow(rowIndex uint64, colName string, newValue SupportedValueType)
	approproVal, err := makeSupportedVal(colNameToChange, newVal)
	if err != nil {
		return err
	}

	setOfPointers := *(Registers[ROWS_REG])
	for indAddress := range setOfPointers.(map[uint32]bool) {
		index := indAddress
		tableAddress.UpdateRow(uint64(index), colNameToChange, approproVal)
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter
	return nil
}

//Adds a column to the table
func insertColumn(instruction codegen.InsertColumnOp) error {
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colName := instruction.ColName
	colType := normalToTableType(instruction.ColType)
	return tableAddress.PutColumn(colName, colType)

}

func makeSupportedVal(colName, valName string) (key_value.SupportedValueType, error) {
	// fmt.Println(colName, valName)
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colType, err := tableAddress.GetColumnType(colName)
	if err != nil {
		return nil, err
	}
	// fmt.Println(colType)
	var asInterface interface{}
	switch colType {
	case "Supported-Value-Type.int":
		asInterface, _ = strconv.Atoi(valName)
	case "Supported-Value-Type.float":
		asInterface, _ = strconv.ParseFloat(valName, 32)
	case "Supported-Value-Type.string":
		asInterface = valName
	}
	return key_value.SupportedValueTypeImpl{colType, asInterface}, nil
}

// TODO replace GetRedIndex is now replaced with the valid register named; put
// them in later.
// TODO ERROR HANDLING

func ExecByteCode(instructions []codegen.ByteCodeOp) (string, error) {
	// FOR TESTING RID OF THIS LATER!!!!!~
	tableAddress, _ := DataBase.GetTable("TestTable")
	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter
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
		case "InsertOp":
			insert(instruction.(codegen.InsertOp))
		case "MakeTableOp":
			makeTable(instruction.(codegen.MakeTableOp))
		case "DeleteTableOp":
			deleteTable(instruction.(codegen.DeleteTableOp))
		case "UpdateTableOp":
			updateTable(instruction.(codegen.UpdateTableOp))
		case "DeleteRowsOp":
			deleteRows()
		case "DeleteColsOp":
			deleteCols()
		case "DeleteColFromTableOp":
			deleteColFromTable(instruction.(codegen.DeleteColFromTableOp))
		case "InsertColumnOp":
			insertColumn(instruction.(codegen.InsertColumnOp))
		case "DisplayOp":
			res := display()
			clear()
			return res, nil
		default:
			return "", errors.New("Bad instruction shit face")
		}
	}
	return "", nil
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
