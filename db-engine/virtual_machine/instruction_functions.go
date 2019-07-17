package virtual_machine

import ( // fucking shit go is dumb
	"errors"
	"fmt"

	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/datastore/key_value"
)

const (
	R0          = codegen.R0
	R1          = codegen.R1
	R2          = codegen.R2
	R3          = codegen.R3
	R4          = codegen.R4
	TABLE_REG   = codegen.R5
	COLUMNS_REG = codegen.R6
	ROWS_REG    = codegen.R7
)

type Register *interface{}

var (
	numRegisters = 6
	Registers    = make([]Register, numRegisters) // each one is an empty register for now
	DataBase     = key_value.NewDataBase()
)

func loadInstruction(instruction codegen.LoadValOp) error {
	idx := instruction.idx
	value := insturction.value
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

func display() error {
	// assume, for now, everything is valid in the registers
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
			fmt.Print(" " + columnName + " ")
		}
	}
	fmt.Println("") // new line as a conclusion
	for _, rowIndPointer := range *(Registers[ROWS_REG]) {
		row := tableAddress.GetRow(*rowIndPointer)
		for i, elem := range row {
			if _, found := goodIndices[i]; found {
				fmt.Print(" " + elem + " ")
			}
		}
		fmt.Println("") // new row
	}
	return nil
}

func where(instruction codegen.WhereOp) {
	colName = instruction.colname
	value := instruction.value
	listOfPointers := *(Registers[ROWS_REG]) // list of pointers to indices
	tableAddress := Registers[TABLE_REG]
	newIndices := []uint32{}
	columnInfoMap = tableAddress.GetColumn(colName)
	goodIndices := 

}

// TODO replace GetRedIndex is now replaced with the valid register named; put
// them in later.
