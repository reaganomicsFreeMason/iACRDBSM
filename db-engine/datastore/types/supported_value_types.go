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

// func CreateSupportedIntMap() map[SupportedValueType]valueToRowMap {
// 	ret := make(map[SupportedInt]valueToRowMap)
// 	return ret
// }

type SupportedValueType interface {
	getName() string
	getValue() interface{}
}

type SupportedInt struct {
	Value int
}

func (i *SupportedInt) getName() string {
	return "Supported-Value-Type.int"
}

func (i *SupportedInt) getValue() int {
	return i.Value
}

type SupportedFloat struct {
	Value float32
}

func (f *SupportedFloat) getName() string {
	return "Supported-Value-Type.float"
}

func (f *SupportedFloat) getValue() float32 {
	return f.Value
}

type SupportedString struct {
	Value string
}

func (s *SupportedString) getName() string {
	return "Supported-Value-Type.string"
}

func (s *SupportedString) getValue() string {
	return s.Value
}
