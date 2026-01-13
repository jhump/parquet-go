package variant

import (
	"errors"
	"github.com/google/uuid"
)

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
	VisitUUID(uuid.UUID) error

	// BeginArray indicates that the variant is an array value. Other
	// Visit* methods will be called, zero or more times, for each
	// element in the array, followed by a call to EndArray.
	//
	// The given size may be -1 to indicate that the size is not known.
	// Otherwise, it indicates the number of elements in the array.
	// EndArray will be called visiting that many values.
	BeginArray(sizeHint int) error
	// EndArray is the closing bookend of a call to BeginArray. It
	// indicates that all elements have been visited.
	EndArray() error

	// BeginObject indicates that the variant is an object value. Calls
	// will subsequently alternate between ObjectField and then Other
	// Visit* methods, for each field in the object, followed by a call
	// to EndObject.
	//
	// The given size may be -1 to indicate that the size is not known.
	// Otherwise, it indicates the number of fields in the object.
	// EndObject will be called visiting that many field values.
	BeginObject(sizeHint int) error
	// ObjectField indicates the name of the field whose value is next
	// to be visited.
	ObjectField(name string) error
	// EndObject is the closing bookend of a call to BeginObject. It
	// indicates that all fields have been visited.
	EndObject() error
}

// SimpleVisitor is a base visitor implementation that constructs a Shredded
// value for all leaf values and passes to the given HandleValue function
// Visit calls for non-leaf values delegate to the relevant Handle* function.
// If any of the given functions is nil, the visitor will simply return nil
// for that visit method, so it can also serve as a no-op visitor. Because
// the zero value is a no-op, it can be embedded in a struct to implement
// Visitor and only override implement the methods of interest.
type SimpleVisitor struct {
	// Visits a leaf value.
	HandleValue func(Shredded) error

	HandleBeginArray  func(sizeHint int) error
	HandleEndArray    func() error
	HandleBeginObject func(sizeHint int) error
	HandleObjectField func(name string) error
	HandleEndObject   func() error
}

var _ Visitor = (*SimpleVisitor)(nil)

func (s *SimpleVisitor) VisitNull() error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfNull())
}

func (s *SimpleVisitor) VisitBool(b bool) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfBool(b))
}

func (s *SimpleVisitor) VisitInt8(i int8) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfInt8(i))
}

func (s *SimpleVisitor) VisitInt16(i int16) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfInt16(i))
}

func (s *SimpleVisitor) VisitInt32(i int32) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfInt32(i))
}

func (s *SimpleVisitor) VisitInt64(i int64) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfInt64(i))
}

func (s *SimpleVisitor) VisitFloat32(f float32) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfFloat32(f))
}

func (s *SimpleVisitor) VisitFloat64(f float64) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfFloat64(f))
}

func (s *SimpleVisitor) VisitDecimal4(decimal4 Decimal4) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfDecimal4(decimal4))
}

func (s *SimpleVisitor) VisitDecimal8(decimal8 Decimal8) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfDecimal8(decimal8))
}

func (s *SimpleVisitor) VisitDecimal16(decimal16 Decimal16) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfDecimal16(decimal16))
}

func (s *SimpleVisitor) VisitDate(date Date) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfDate(date))
}

func (s *SimpleVisitor) VisitTime(time Time) error {
	if s.HandleValue == nil {
		return nil
	}
	t, ok := ShreddedValueOfTime(time)
	if !ok {
		return errors.New("encountered invalid time value")
	}
	return s.HandleValue(t)
}

func (s *SimpleVisitor) VisitTimestamp(timestamp Timestamp) error {
	if s.HandleValue == nil {
		return nil
	}
	ts, ok := ShreddedValueOfTimestamp(timestamp)
	if !ok {
		return errors.New("encountered invalid timestamp value")
	}
	return s.HandleValue(ts)
}

func (s *SimpleVisitor) VisitBytes(bytes []byte) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfBytes(bytes))
}

func (s *SimpleVisitor) VisitString(str string) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfString(str))
}

func (s *SimpleVisitor) VisitUUID(u uuid.UUID) error {
	if s.HandleValue == nil {
		return nil
	}
	return s.HandleValue(ShreddedValueOfUUID(u))
}

func (s *SimpleVisitor) BeginArray(sizeHint int) error {
	if s.HandleBeginArray == nil {
		return nil
	}
	return s.HandleBeginArray(sizeHint)
}

func (s *SimpleVisitor) EndArray() error {
	if s.HandleEndArray == nil {
		return nil
	}
	return s.HandleEndArray()
}

func (s *SimpleVisitor) BeginObject(sizeHint int) error {
	if s.HandleBeginObject == nil {
		return nil
	}
	return s.HandleBeginObject(sizeHint)
}

func (s *SimpleVisitor) ObjectField(name string) error {
	if s.HandleObjectField == nil {
		return nil
	}
	return s.HandleObjectField(name)
}

func (s *SimpleVisitor) EndObject() error {
	if s.HandleEndObject == nil {
		return nil
	}
	return s.HandleEndObject()
}
