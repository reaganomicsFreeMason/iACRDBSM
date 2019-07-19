package virtual_machine

import (
	"errors"
	"fmt"
	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/datastore/key_value"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	numPackets     = numRegisters / packetSize // floor division
	packetIndexSet = makePacketIndexSet()      // map
	packetLock     = sync.Mutex{}
	cond           = sync.NewCond(&packetLock)
)

func makePacketIndexSet() map[int]bool {
	res := make(map[int]bool, numPackets)
	for i := 0; i < numPackets; i++ {
		res[i] = true
	}
	return res
}

// returns whether or not was successful
func getTable(instruction codegen.GetTableOp, startIndex int) error {
	tableName := instruction.Tablename
	tableAddress, err := DataBase.GetTable(tableName)
	if err != nil {
		return err
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[startIndex+TABLE_REG] = &asInter
	var asInter2 interface{}
	asInter2 = ALL_ROWS
	Registers[startIndex+ROWS_REG] = &asInter2 // initialize the rows reg to have a pointer that says all rows
	// load the special all value into the ROWS
	var nameAsInter interface{}
	nameAsInter = tableName
	Registers[startIndex+TABLE_NAME_REG] = &nameAsInter
	return nil
}

func addColumn(instruction codegen.AddColumnOp, startIndex int) error {
	columnName := instruction.Colname
	if Registers[startIndex+COLUMNS_REG] == nil {
		var asInter interface{}
		asInter = map[string]bool{}
		Registers[startIndex+COLUMNS_REG] = &asInter
	}
	listOfPointers := (*(Registers[startIndex+COLUMNS_REG])).(map[string]bool) // list of column names
	listOfPointers[columnName] = true
	var asInter2 interface{}
	asInter2 = listOfPointers
	Registers[startIndex+COLUMNS_REG] = &asInter2
	return nil
}

func addAllColumns(instruction codegen.AddAllColumnsOp, startIndex int) error {
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colNames := tableAddress.GetAllColumnNames()
	for _, colName := range colNames {
		addColumn(codegen.AddColumnOp{colName}, startIndex)
	}
	return nil
}

func addRow(instruction codegen.AddRowOp, startIndex int) error {
	rowInd := instruction.Idx
	if Registers[startIndex+ROWS_REG] == nil {
		var asInter interface{}
		asInter = map[uint32]bool{}
		Registers[startIndex+ROWS_REG] = &asInter
	}
	listOfPointers := (*(Registers[startIndex+ROWS_REG])) // list of pointers to indices
	if listOfPointers == ALL_ROWS {
		listOfPointers = map[uint32]bool{}
	}
	asSetPoint := listOfPointers.(map[uint32]bool)
	asSetPoint[rowInd] = true
	var asInter2 interface{}
	asInter2 = asSetPoint
	Registers[startIndex+ROWS_REG] = &asInter2
	return nil
}

func clear(startIndex int) error {
	Registers[startIndex+COLUMNS_REG] = nil
	Registers[startIndex+ROWS_REG] = nil
	Registers[startIndex+TABLE_REG] = nil
	Registers[startIndex+TABLE_NAME_REG] = nil
	return nil
}

func separator(length int) string {
	ret := "+"
	for i := 0; i < length; i++ {
		ret += strings.Repeat(string("-"), 15) + "+"
	}
	return ret
}

func addCell(content string) string {
	size := len(content)
	border := 15 - size
	if border > 0 {
		left := int(math.Ceil(float64(border / 2)))
		right := border - left
		return "|" + strings.Repeat(string(" "), left) + content + strings.Repeat(string(" "), right)
	}
	if border < 0 {
		return "|" + content[:25] + "..." + content[size-3:]
	}
	return "|" + content
}

func display(startIndex int) string { // return the display string
	// assume, for now, everything is valid in the registers
	res := ""
	retTableLen := 0
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	columnNames := tableAddress.GetAllColumnNames()

	setOfQueriedColumns := map[string]bool{}
	goodIndices := map[uint32]bool{}
	for colNamePointer := range (*(Registers[startIndex+COLUMNS_REG])).(map[string]bool) {
		setOfQueriedColumns[colNamePointer] = true
	}

	// print the columns
	for i, columnName := range columnNames {
		if _, found := setOfQueriedColumns[columnName]; found {
			goodIndices[uint32(i)] = true
			retTableLen++
			res += addCell(columnName)
		}
	}
	newLine := separator(retTableLen)
	res += "|" + "\n" + newLine + "\n" // new line as a conclusion

	// TODO: error handle
	if *Registers[startIndex+ROWS_REG] != ALL_ROWS {
		keys := []uint32{}
		for rowIndPointer := range (*(Registers[startIndex+ROWS_REG])).(map[uint32]bool) {
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
					res += addCell(supValToString(asValue))
				}
			}
			res += "|" + "\n" // row separator
		}
	} else {
		numRows := tableAddress.GetNumRows()
		for i := 0; i < numRows; i++ {
			rowIndPointer := &i
			row, _ := tableAddress.GetRow(uint64(*rowIndPointer))
			for i, elem := range row {
				if _, found := goodIndices[uint32(i)]; found {
					asValue := elem.(key_value.SupportedValueType)
					res += addCell(supValToString(asValue))
				}
			}
			res += "|" + "\n" // new row
		}
	}
	res += newLine
	return res
}

func filter(instruction codegen.FilterOp, startIndex int) error {
	// value is a string; convert it to the supportedValue

	colName := instruction.ColName
	valueName := instruction.ValName
	val, err := makeSupportedVal(colName, valueName, startIndex)
	if err != nil {
		return err
	}
	listOfPointers := *(Registers[startIndex+ROWS_REG]) // list of pointers to indices
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	columnInfoMap, _ := tableAddress.GetColumn(colName)
	goodIndices := columnInfoMap.Values[val] // set of the valid indices

	if *(Registers[startIndex+ROWS_REG]) == ALL_ROWS {
		for index := range goodIndices {
			addRow(codegen.AddRowOp{uint32(index)}, startIndex)
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
	Registers[startIndex+ROWS_REG] = &asInter
	return nil
}

func insert(instruction codegen.InsertOp, startIndex int) error {
	// first, make a map from the colNames to the values
	colNameToValue := map[string]string{}
	colNames, valNames := instruction.ColNames, instruction.ValNames
	numNamesGiven := len(colNames)
	for i := 0; i < numNamesGiven; i++ {
		colName := colNames[i]
		valName := valNames[i]
		colNameToValue[colName] = valName
	}
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table // need this soon
	tableColNamesOfficial := tableAddress.GetAllColumnNames()
	rowToInsert := make(key_value.Row, len(tableColNamesOfficial)) // just give everything a null value for now
	for i, tableColName := range tableColNamesOfficial {
		if tableColName == "" {
			continue
		} else if val, found := colNameToValue[tableColName]; !found {
			continue
		} else {
			rowToInsert[i], _ = makeSupportedVal(tableColName, val, startIndex)
		}
	}
	// fmt.Println("THISROW", rowToInsert)
	tableAddress.PutRow(rowToInsert) // should be nil?
	var asInter interface{}
	asInter = *tableAddress
	Registers[startIndex+TABLE_REG] = &asInter
	tableName := (*Registers[startIndex+TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)
	// fmt.Println(*(Registers[startIndex + TABLE_REG]), "table")
	return nil
	// UPDATE THE REGISTER WITH THE CORRECT ADDRESS
}

func makeTable(instruction codegen.MakeTableOp, startIndex int) error {
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

func deleteTable(instruction codegen.DeleteTableOp, startIndex int) error {
	tableName := instruction.TableName
	return DataBase.DeleteTable(tableName)
}

// DELETE is going to look at registers and delete anything in the rows register

func deleteRows(startIndex int) error {
	listOfPointers := *(Registers[startIndex+ROWS_REG]) // list of pointers to indices
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	if listOfPointers == ALL_ROWS {
		numRows := tableAddress.GetNumRows()
		for indAddress := 0; indAddress < numRows; indAddress++ {
			index := indAddress
			tableAddress.DeleteRow(uint64(index))
			// error handling TODO
		}
	} else {
		for indAddress := range listOfPointers.(map[uint32]bool) {
			index := indAddress
			tableAddress.DeleteRow(uint64(index))
			// error handling TODO
		}
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[startIndex+TABLE_REG] = &asInter

	tableName := (*Registers[startIndex+TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

	return nil
}

// DELETE is going to look at registers and delete anything in the cols register

func deleteCols(startIndex int) error {
	listOfPointers := *(Registers[startIndex+COLUMNS_REG]) // list of pointers to indices
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	for _, colNameAddress := range listOfPointers.([]*string) {
		colName := *colNameAddress
		tableAddress.DeleteColumn(colName)
		// error handling TODO
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[startIndex+TABLE_REG] = &asInter

	tableName := (*Registers[startIndex+TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

	return nil

}

func deleteColFromTable(instruction codegen.DeleteColFromTableOp, startIndex int) error {
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colName := instruction.ColName
	tableAddress.DeleteColumn(colName)

	var asInter interface{}
	asInter = *tableAddress
	Registers[startIndex+TABLE_REG] = &asInter

	tableName := (*Registers[startIndex+TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

	return nil
}

func updateTable(instruction codegen.UpdateTableOp, startIndex int) error {
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colNameToChange := instruction.ColName
	newVal := instruction.NewVal
	// UpdateRow(rowIndex uint64, colName string, newValue SupportedValueType)
	approproVal, err := makeSupportedVal(colNameToChange, newVal, startIndex)
	if err != nil {
		return err
	}

	setOfPointers := *(Registers[startIndex+ROWS_REG])
	for indAddress := range setOfPointers.(map[uint32]bool) {
		index := indAddress
		tableAddress.UpdateRow(uint64(index), colNameToChange, approproVal)
	}
	var asInter interface{}
	asInter = *tableAddress
	Registers[startIndex+TABLE_REG] = &asInter

	tableName := (*Registers[startIndex+TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

	return nil
}

func insertColumn(instruction codegen.InsertColumnOp, startIndex int) error {
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
	tableAddress := &table
	colName := instruction.ColName
	colType := normalToTableType(instruction.ColType)

	var asInter interface{}
	asInter = *tableAddress
	Registers[startIndex+TABLE_REG] = &asInter

	tableName := (*Registers[startIndex+TABLE_NAME_REG]).(string)
	DataBase.SetPointer(tableName, tableAddress)

	return tableAddress.PutColumn(colName, colType)

}

func makeSupportedVal(colName, valName string, startIndex int) (key_value.SupportedValueType, error) {
	// fmt.Println(colName, valName)
	table := (*(Registers[startIndex+TABLE_REG])).(key_value.DataTable)
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
// TODO  ERROR HANDLING

func ExecByteCode(instructions []codegen.ByteCodeOp) (string, error) {
	var err error
	putBack := getToken()
	startIndex := putBack * packetSize
	for _, instruction := range instructions {
		instName := instruction.GetOpName()
		switch instName {
		case "GetTableOp":
			err = getTable(instruction.(codegen.GetTableOp), startIndex)
			if err != nil {
				clear(startIndex)
				return "", err
			}
		case "AddAllColumnsOp":
			err := addAllColumns(instruction.(codegen.AddAllColumnsOp), startIndex)
			if err != nil {
				clear(startIndex)
				return "", err
			}
		case "AddColumnOp":
			err = addColumn(instruction.(codegen.AddColumnOp), startIndex)
		case "AddRowOp":
			err = addRow(instruction.(codegen.AddRowOp), startIndex)
		case "FilterOp":
			err = filter(instruction.(codegen.FilterOp), startIndex)
		case "InsertOp":
			err = insert(instruction.(codegen.InsertOp), startIndex)
		case "MakeTableOp":
			err = makeTable(instruction.(codegen.MakeTableOp), startIndex)
		case "DeleteTableOp":
			err = deleteTable(instruction.(codegen.DeleteTableOp), startIndex)
		case "UpdateTableOp":
			err = updateTable(instruction.(codegen.UpdateTableOp), startIndex)
		case "DeleteRowsOp":
			err = deleteRows(startIndex)
		case "DeleteColsOp":
			err = deleteCols(startIndex)
		case "DeleteColFromTableOp":
			err = deleteColFromTable(instruction.(codegen.DeleteColFromTableOp), startIndex)
		case "InsertColumnOp":
			err = insertColumn(instruction.(codegen.InsertColumnOp), startIndex)
			insertColumn(instruction.(codegen.InsertColumnOp), startIndex)
		case "ClearOp":
			clear(startIndex)
		case "DisplayOp":
			res := display(startIndex)
			clear(startIndex)
			putToken(putBack)
			return res, nil
		default:
			return "", errors.New("Invalid Instruction")
		}
		if err != nil {
			clear(startIndex)
			putToken(putBack)
			return "", err
		}
	}
	putToken(putBack)
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

// MUTEX stuffs

func getToken() int {
	cond.L.Lock()
	for len(packetIndexSet) == 0 {
		cond.Wait()
	}
	var toReturn int
	for key := range packetIndexSet {
		toReturn = key
		break
	}
	delete(packetIndexSet, toReturn)
	cond.L.Unlock()
	return toReturn
}

func putToken(startIndex int) { // error will panic for us thanks to sync package
	cond.L.Lock()
	if _, found := packetIndexSet[startIndex]; found {
		panic(errors.New("Inconsistent packet index set state!!!"))
	}
	packetIndexSet[startIndex] = true
	cond.L.Unlock()
}
