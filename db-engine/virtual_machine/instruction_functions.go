package virtual_machine

import ( // fucking shit go is dumb
	"errors"

	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/datastore/key_value"
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
	numRegisters = 6
	Registers    = make([]Register, numRegisters) // each one is an empty register for now
	DataBase     = key_value.NewDataBase()
)

func loadInstruction(instruction codegen.LoadValOp) error {
	idx := instruction.idx
	value := instruction.value
	if idx <= 0 {
		return errors.New("This is register is like your brain: non-existent.")
	} else if value == nil {
		return errors.New("Giving a stupid fucking null value, moron. ")
	}
	Registers[idx] = &value
	return nil
}

// returns whether or not was successful
func getTableInstruction(instruction codegen.GetTableOp) error {
	tableName := instruction.Name
	tableAddress, err := DataBase.GetTable(tableName)
	if err != nil {
		return err
	}
	Registers[TABLE_REG] = tableAddress
	Registers[ROWS_REG] = &ALL_ROWS // initialize the rows reg to have a pointer that says all rows
	// load the special all value into the ROWS

	return nil
}

func addColumn(instruction codegen.AddColumnOpp) error {
	columnName := instruction.colname
	if Registers[regInd] == nil {
		Registers[regInd] = &(make([]*string{}))
	}
	listOfPointers := *(Registers[COLUMNS_REG]) // list of column names
	tableAddress := Registers[TABLE_REG]
	Registers[regInd] = &(append(listOfPointers, &columnName))
	return nil
}

func addRow(instruction codegen.AddRowOpp) error {
	rowInd := instruction.rowInd
	if Registers[regInd] == nil {
		Registers[regInd] = &(make([]*uint32{}))
	}
	listOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
	tableAddress := Registers[TABLE_REG]
	Registers[regInd] = &(append(listOfPointers, &rowInd))
	return nil
}

func clear() error {
	Registers[COLUMNS_REG] = nil
	Registers[ROWS_REG] = nil
}

func display() string { // return the display string
	// assume, for now, everything is valid in the registers
	res := ""
	tableAddress = Registers[TABLE_REG]
	columnNames := tableAddress.ColumnNames

	setOfQueriedColumns := map[string]bool{}
	goodIndices := map[string]uint32{}
	for _, colNamePointer := range *(Registers[COLUMNS_REG]) {
		setOfQueriedColumns[*colNamePointer] = true
	}

	// print the columns
	for i, columnName := range columnNames {
		if _, found := setOfQueriedColumns[columnName]; found {
			goodIndices[i] = true
			res += " " + columnName + " "
		}
	}
	res += "\n" // new line as a conclusion
	for _, rowIndPointer := range *(Registers[ROWS_REG]) {
		row := tableAddress.GetRow(*rowIndPointer)
		for i, elem := range row {
			if _, found := goodIndices[i]; found {
				res += " " + elem + " "
			}
		}
		res += "\n" // new row
	}
	return res[1 : len(res)-1] // ignore the first whitespace character and the last new line char.
}

func filter(instruction codegen.FilterOp) error {
	colName := instruction.colname
	value := instruction.value
	listOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
	tableAddress := Registers[TABLE_REG]
	newIndices := []uint32{}
	columnInfoMap := tableAddress.GetColumn(colName)
	goodIndices := columnInfoMap.Values[value] // set of the valid indices

	if *(Registers[ROWS_REG]) == ALL_ROWS {
		for index, _ := range goodIndices {
			addRow(codegen.AddRowOp{index})
		}
		return nil
	}

	newListOfPointers := make([]*uint32, 0, len(listOfPointers))
	for _, formerAddress := range listOfPointers {
		if _, found := goodIndices[*formerAddress]; found {
			newListOfPointers = append(newListOfPointers, formerAddress)
		}
	}
	Registers[ROWS_REG] = &newListOfPointers
	return nil
}

// TODO replace GetRedIndex is now replaced with the valid register named; put
// them in later.
