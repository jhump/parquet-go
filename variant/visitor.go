package variant

// Visitor accepts values decoded from a variant.
//
// The interface has typed methods for visiting simple "leaf" values.
// All of these methods have names that start with "Visit".
//
// There are also methods that delineate the start and end of
// composite values. There are two types of composite values:
//
//  1. Arrays: The visitor's BeginArray will be called to indicate
//     the start of the array. Then zero or more additional calls,
//     to visit the elements of the array, followed by a call to
//     EndArray.
//  2. Objects: The visitor's BeginObject will be called to indicate
//     the start of the object. Then there will be a call to ObjectField
//     for each field in the object. Another method call will be made
//     after ObjectField, before the next call to ObjectField, to visit
//     the value of that field. Finally, after all fields have been
//     visited, EndObject is called.
//
// When composite values contain other composite values, the calls to
// BeginArray and EndArray or BeginObject and EndObject nest. A visitor
// implementation will need to keep track of the nesting, as well as
// keep track of field names between the call to ObjectField and the
// call(s) to visit the field's value.
type Visitor interface {
	VisitNull() error
	VisitBool(bool) error
	VisitInt8(int8) error
	VisitInt16(int16) error
	VisitInt32(int32) error
	VisitInt64(int64) error
	VisitFloat32(float32) error
	VisitFloat64(float64) error
	VisitDecimal4(Decimal4) error
	VisitDecimal8(Decimal8) error
	VisitDecimal16(Decimal16) error
	VisitDate(Date) error
	VisitTime(Time) error
	VisitTimestamp(Timestamp) error
	VisitBytes([]byte) error
	VisitString(string) error
	VisitUUID(UUID) error

	// BeginArray indicates that the variant is an array value. Other
	// Visit* methods will be called, zero or more times, for each
	// element in the array, followed by a call to EndArray.
	BeginArray() error
	// EndArray is the closing bookend of a call to BeginArray. It
	// indicates that all elements have been visited.
	EndArray() error

	// BeginObject indicates that the variant is an object value. Calls
	// will subsequently alternate between ObjectField and then Other
	// Visit* methods, for each field in the object, followed by a call
	// to EndObject.
	BeginObject() error
	// ObjectField indicates the name of the field whose value is next
	// to be visited.
	ObjectField(name string) error
	// EndObject is the closing bookend of a call to BeginObject. It
	// indicates that all fields have been visited.
	EndObject() error
}

// Visit interprets src as a variant value and invokes the relevant
// methods of visitor. Using an *encoder as the visitor can be
// used to encode an arbitrary Go type as a variant.
func Visit(src any, visitor Visitor) error {
	// TODO
	return nil
}
