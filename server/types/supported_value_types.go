package types

// var (
// 	valueInfoInitialCap   = 10
// 	NameToInfoConstructor = map[string]func() map[supportedValueType]valueToRowMap{
// 		"Supported-Value-Type.int": func() map[supportedValueType]valueToRowMap {
// 			res := make(map[*SupportedFloat]valueToRowMap, valueInfoInitialCap)
// 			return res
// 		},
// 		"Supported-Value-Type.float": func() map[SupportedFloat]valueToRowMap {
// 			return make(map[SupportedFloat]valueToRowMap, valueInfoInitialCap)
// 		},
// 		"Supported-Value-Type.string": func() map[SupportedString]valueToRowMap {
// 			return make(map[SupportedString]valueToRowMap, valueInfoInitialCap)
// 		},
// 	}
// )

func CreateSupportedIntMap() map[supportedValueType]valueToRowMap {
	ret := make(map[SupportedInt]valueToRowMap)
	return ret
}

type SupportedValueType interface {
	getName() string
	getValue() interface{}
}

type SupportedInt struct {
	Value int
}

func (i *supportedInt) getName() string {
	return "Supported-Value-Type.int"
}

func (i *supportedInt) getValue() int {
	return i.Value
}

type SupportedFloat struct {
	Value float32
}

func (f *supportedFloat) getName() string {
	return "Supported-Value-Type.float"
}

func (f *supportedFloat) getValue() float32 {
	return f.Value
}

type SupportedString struct {
	Value string
}

func (s *supportedString) getName() string {
	return "Supported-Value-Type.string"
}

func (s *supportedString) getValue() string {
	return s.Value
}
