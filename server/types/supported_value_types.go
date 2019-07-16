package types

type supportedValueType interface {
	getName() string
	getValue() interface{}
}

type supportedInt struct {
	Value int
}

func (i *supportedInt) getName() string {
	return "int"
}

func (i *supportedInt) getValue() string {
	return i.Value
}

type supportedFloat struct {
	Value float32
}

func (f *supportedFloat) getName() string {
	return "float"
}

func (f *supportedFloat) getValue() string {
	return f.Value
}

type supportedString struct {
	Value string
}

func (s *supportedString) getName() string {
	return "string"
}

func (s *supportedString) getValue() string {
	return s.Value
}
