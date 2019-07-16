package types 

type supportedValueType interface{
	getName() string
	getValue() interface{}
}

type supportedInt struct{
	Value int
}

type (i *supportedInt) getName() string {
	return "int"
}

type (i *supportedInt) getValue() string {
	return i.Value
}

type supportedFloat struct{
	value float32
}

type (f *supportedFloat) getName() string {
	return "float"
}

type (f *supportedFloat) getValue() string {
	return f.Value
}

type supportedString struct{
	value string
}

type (s *supportedString) getName() string {
	return "string"
}

type (s *supportedString) getValue() string {
	return s.Value
}
