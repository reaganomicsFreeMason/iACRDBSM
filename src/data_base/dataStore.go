package data_base

import(
	. "./set"
)

type DataBase tables map[string]dataTable // map from name of the table to the table itself 
type row []interface{}

type dataTable {
	indexMap map[featuresToIndices]bool, // set type data structure(maps item to True since set not in go) 
	table row[],
}

type featuresToIndices {
	featureName string
	featureValueToIndices map[interface{}]indexSet
}

func NewDataBase() {
	return DataBase
}