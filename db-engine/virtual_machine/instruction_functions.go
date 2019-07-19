package virtual_machine

import ( // fucking shit go is dumb

	"errors"
	"fmt"
	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/datastore/key_value"
	"sort"
	"strconv"
	"sync"
	// "github.com/olekukonko/tablewriter"
)

const (
	TABLE_NAME_REG = codegen.R1
	TABLE_REG      = codegen.R2
	COLUMNS_REG    = codegen.R3
	ROWS_REG       = codegen.R4
	ALL_ROWS       = -1 // signifies that all of the rows are being loaded into the register
)

type Register *interface{}

var (
	numRegisters = 20
	packetSize   = 4
	Registers    = make([]Register, numRegisters) // each one is an empty register for now
	DataBase     = key_value.NewDataBase()

	numPackets     = numRegisters / packetSize      // floor division
	packetIndexSet = make(map[int]bool, numPackets) // map
	packetLock     = sync.Mutex{}
)

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
	var nameAsInter interface{}
	nameAsInter = tableName
	Registers[TABLE_NAME_REG] = &nameAsInter
	return nil
}

func addColumn(instruction codegen.AddColumnOp) error {
	columnName := instruction.Colname
	if Registers[COLUMNS_REG] == nil {
		var asInter interface{}
		asInter = map[string]bool{}
		Registers[COLUMNS_REG] = &asInter
	}
	listOfPointers := (*(Registers[COLUMNS_REG])).(map[string]bool) // list of column names
	listOfPointers[columnName] = true
	var asInter2 interface{}
	asInter2 = listOfPointers
	Registers[COLUMNS_REG] = &asInter2
	return nil
}

func addRow(instruction codegen.AddRowOp) error {
	rowInd := instruction.Idx
	if Registers[ROWS_REG] == nil {
		var asInter interface{}
		asInter = map[uint32]bool{}
		Registers[ROWS_REG] = &asInter
	}
	listOfPointers := (*(Registers[ROWS_REG])) // list of pointers to indices
	if listOfPointers == ALL_ROWS {
		listOfPointers = map[uint32]bool{}
	}
	asSetPoint := listOfPointers.(map[uint32]bool)
	asSetPoint[rowInd] = true
	var asInter2 interface{}
	asInter2 = asSetPoint
	Registers[ROWS_REG] = &asInter2
	return nil
}

func clear() error {
	Registers[COLUMNS_REG] = nil
	Registers[ROWS_REG] = nil
	Registers[TABLE_REG] = nil
	Registers[TABLE_NAME_REG] = nil
	return nil
}

func display() string { // return the display string
	// assume, for now, everything is valid in the registers
	res := ""
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	columnNames := tableAddress.GetAllColumnNames()
	columnHeader := []string{}
	data := [][]string{}
	// mytable := tablewriter.NewWriter(os.Stdout)

	setOfQueriedColumns := map[string]bool{}
	goodIndices := map[uint32]bool{}
	for colNamePointer := range (*(Registers[COLUMNS_REG])).(map[string]bool) {
		setOfQueriedColumns[colNamePointer] = true
	}

	// print the columns
	for i, columnName := range columnNames {
		if _, found := setOfQueriedColumns[columnName]; found {
			goodIndices[uint32(i)] = true
			res += " " + columnName + " "
			// columnHeader = append(columnHeader, tablewriter.Title(columnName))
		}
	}
	res += "\n" // new line as a conclusion

	// TODO: error handle THIS SHIT THIS IS NASTY
	if *Registers[ROWS_REG] != ALL_ROWS {
		keys := []uint32{}
		for rowIndPointer := range (*(Registers[ROWS_REG])).(map[uint32]bool) {
			keys = append(keys, rowIndPointer)
		}
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		for _, rowIndPointer := range keys {
			retRow := []string{}
			row, _ := tableAddress.GetRow(uint64(rowIndPointer))
			for i, elem := range row {
				if _, found := goodIndices[uint32(i)]; found {
					asValue := elem.(key_value.SupportedValueType)
					res += " " + supValToString(asValue) + " "
					retRow = append(retRow, supValToString(asValue))
				}
			}
			res += "\n" // new row
			data = append(data, retRow)
		}
	} else {
		numRows := tableAddress.GetNumRows()
		for i := 0; i < numRows; i++ {
			rowIndPointer := &i
			row, _ := tableAddress.GetRow(uint64(*rowIndPointer))
			retRow := []string{}
			for i, elem := range row {
				if _, found := goodIndices[uint32(i)]; found {
					asValue := elem.(key_value.SupportedValueType)
					res += " " + supValToString(asValue) + " "
					retRow = append(retRow, supValToString(asValue))
				}
			}
			res += "\n" // new row
			data = append(data, retRow)
		}
	}
	if len(columnHeader) > 0 {
		if len(data) > 0 && len(data[0]) > 0 {
			// mytable.SetHeader(columnHeader)
		} else {
			data = append(data, columnHeader)
		}
		// mytable.AppendBulk(data)
		// mytable.Render()
	}
	return res[1 : len(res)-1] // ignore the first whitespace character and the last new line char.
}

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

	newListOfPointers := make(map[uint32]bool, 0)
	for formerAddress := range listOfPointers.(map[uint32]bool) {
		if _, found := goodIndices[uint64(formerAddress)]; found {
			newListOfPointers[formerAddress] = true
		}
	}
	var asInter interface{}
	asInter = newListOfPointers
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
	tableName := (*Registers[TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)
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
	for indAddress := range listOfPointers.(map[uint32]bool) {
		index := indAddress
		tableAddress.DeleteRow(uint64(index))
		// error handling TODO
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter

	tableName := (*Registers[TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

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

	tableName := (*Registers[TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

	return nil

}

func deleteColFromTable(instruction codegen.DeleteColFromTableOp) error {
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colName := instruction.ColName
	tableAddress.DeleteColumn(colName)

	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter

	tableName := (*Registers[TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

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

	tableName := (*Registers[TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

	return nil
}

func insertColumn(instruction codegen.InsertColumnOp) error {
	table := (*(Registers[TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colName := instruction.ColName
	colType := normalToTableType(instruction.ColType)

	var asInter interface{}
	asInter = *tableAddress
	Registers[TABLE_REG] = &asInter

	tableName := (*Registers[TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

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
		case "ClearOp":
			clear()
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
