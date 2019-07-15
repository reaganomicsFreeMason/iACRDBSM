package data_base


type DataBase {
	tables []DataTable,
}

type dataTable {
	// TODO(someone): make internal set package?
	indexMap map[featuresToIndices]bool, // set type data structure(maps item to True since set not in go) 
	table row[],
}

type featuresToIndices {
	featureName string
	featureValueToIndices map[interface{}] 
}

