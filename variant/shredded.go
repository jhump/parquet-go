package variant

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

// Shredded represents a shredded value, which is a strongly
// typed variant value that is not encoded. Object and array
// values may be partially encoded since only elements of certain
// types and fields of certain names and types will remain
// un-encoded.
//
// The zero value is a null variant value, which is equivalent
// to a Go nil.
type Shredded struct {
	// primary value; or length for string, bytes, array, and object kinds
	v1 uint64

	// extra data for value (for 128-bit vals like UUIDs and decimals)
	v2 uint64

	// used only for string, bytes, array, and object kinds
	// points to an array of byte for the former 2, array of Data for array,
	// or array of FieldData for object
	p *byte

	// scale for decimal values
	s uint8

	kind Kind

	// Note: This type is 26 bytes on 64-bit platforms (aligned
	// size of 32 bytes!). That seems okay for passing around by
	// value since it's only one int-sized-word larger than a
	// slice. But it would likely be useful to benchmark and see
	// if pointers would be better (trading off more allocations
	// and GC work for less copying of data all over stack frames).
}

// ShreddedValueOf returns a Shredded that represents the given val. Returns
// false if val is not a supported type. The supported types follow:
//   - nil
//   - bool
//   - int8, int16, int32, int64
//   - float32, float64
//   - []byte
//   - string
//   - variant.Decimal4, variant.Decimal8, variant.Decimal16
//   - variant.Date, variant.Time, variant.Timestamp
//   - variant.UUID
//   - []variant.Data
//   - []variant.FieldData
//
// The lattermost two are used for array and object values, respectively.
//
// Time values currently must have AdjustedToUTC set to false a unit of
// microseconds. Timestamp values must have a unit of microseconds or
// nanoseconds. Otherwise, false is returned.
func ShreddedValueOf(val any) (Shredded, bool) {
	switch val := val.(type) {
	case nil:
		return ShreddedValueOfNull(), true
	case bool:
		return ShreddedValueOfBool(val), true
	case int8:
		return ShreddedValueOfInt8(val), true
	case int16:
		return ShreddedValueOfInt16(val), true
	case int32:
		return ShreddedValueOfInt32(val), true
	case int64:
		return ShreddedValueOfInt64(val), true
	case float64:
		return ShreddedValueOfFloat64(val), true
	case Decimal4:
		return ShreddedValueOfDecimal4(val), true
	case Decimal8:
		return ShreddedValueOfDecimal8(val), true
	case Decimal16:
		return ShreddedValueOfDecimal16(val), true
	case Date:
		return ShreddedValueOfDate(val), true
	case Timestamp:
		return ShreddedValueOfTimestamp(val)
	case float32:
		return ShreddedValueOfFloat32(val), true
	case []byte:
		return ShreddedValueOfBytes(val), true
	case string:
		return ShreddedValueOfString(val), true
	case Time:
		return ShreddedValueOfTime(val)
	case UUID:
		return ShreddedValueOfUUID(val), true
	case []FieldData:
		return ShreddedValueOfObject(val), true
	case []Data:
		return ShreddedValueOfArray(val), true
	default:
		return Shredded{}, false
	}
}

func ShreddedValueOfNull() Shredded {
	return Shredded{kind: KindNull}
}

func ShreddedValueOfBool(val bool) Shredded {
	if val {
		return Shredded{kind: KindBooleanTrue}
	}
	return Shredded{kind: KindBooleanFalse}
}

func ShreddedValueOfInt8(val int8) Shredded {
	return Shredded{kind: KindInt8, v1: uint64(val)}
}

func ShreddedValueOfInt16(val int16) Shredded {
	return Shredded{kind: KindInt16, v1: uint64(val)}
}

func ShreddedValueOfInt32(val int32) Shredded {
	return Shredded{kind: KindInt32, v1: uint64(val)}
}

func ShreddedValueOfInt64(val int64) Shredded {
	return Shredded{kind: KindInt64, v1: uint64(val)}
}

func ShreddedValueOfFloat32(val float32) Shredded {
	return Shredded{kind: KindFloat, v1: uint64(math.Float32bits(val))}
}

func ShreddedValueOfFloat64(val float64) Shredded {
	return Shredded{kind: KindDouble, v1: math.Float64bits(val)}
}

func ShreddedValueOfDecimal4(val Decimal4) Shredded {
	return Shredded{kind: KindDecimal4, v1: uint64(val.Value), s: val.Scale}
}

func ShreddedValueOfDecimal8(val Decimal8) Shredded {
	return Shredded{kind: KindDecimal8, v1: uint64(val.Value), s: val.Scale}
}

func ShreddedValueOfDecimal16(val Decimal16) Shredded {
	return Shredded{
		kind: KindDecimal16,
		v1:   binary.BigEndian.Uint64(val.Value[:8]),
		v2:   binary.BigEndian.Uint64(val.Value[8:]),
		s:    val.Scale,
	}
}

func ShreddedValueOfBytes(val []byte) Shredded {
	return Shredded{kind: KindBinary, p: &val[0], v1: uint64(len(val))}
}

func ShreddedValueOfString(val string) Shredded {
	return Shredded{kind: KindString, p: unsafe.StringData(val), v1: uint64(len(val))}
}

func ShreddedValueOfDate(val Date) Shredded {
	return Shredded{kind: KindDate, v1: uint64(val)}
}

func ShreddedValueOfTime(val Time) (Shredded, bool) {
	if val.AdjustedToUTC || val.TimeUnit != Microsecond {
		return Shredded{}, false
	}
	return Shredded{kind: KindTimeNTZ, v1: uint64(val.Value)}, true
}

func ShreddedValueOfTimestamp(val Timestamp) (Shredded, bool) {
	switch val.TimeUnit {
	case Microsecond:
		if val.AdjustedToUTC {
			return Shredded{kind: KindTimestampMicros, v1: uint64(val.Value)}, true
		}
		return Shredded{kind: KindTimestampMicrosNTZ, v1: uint64(val.Value)}, true
	case Nanosecond:
		if val.AdjustedToUTC {
			return Shredded{kind: KindTimestampNanos, v1: uint64(val.Value)}, true
		}
		return Shredded{kind: KindTimestampNanosNTZ, v1: uint64(val.Value)}, true
	default:
		return Shredded{}, false
	}
}

func ShreddedValueOfUUID(val UUID) Shredded {
	return Shredded{
		kind: KindUUID,
		v1:   binary.BigEndian.Uint64(val[:8]),
		v2:   binary.BigEndian.Uint64(val[8:]),
	}
}

func ShreddedValueOfArray(val []Data) Shredded {
	return Shredded{
		kind: KindArray,
		p:    (*byte)(unsafe.Pointer(unsafe.SliceData(val))),
		v1:   uint64(len(val)),
	}
}

func ShreddedValueOfObject(val []FieldData) Shredded {
	return Shredded{
		kind: KindObject,
		p:    (*byte)(unsafe.Pointer(unsafe.SliceData(val))),
		v1:   uint64(len(val)),
	}
}

func (s Shredded) Kind() Kind {
	return s.kind
}

func (s Shredded) Interface() any {
	switch s.kind {
	case KindNull:
		return nil
	case KindBooleanTrue:
		return true
	case KindBooleanFalse:
		return false
	case KindInt8:
		return int8(s.v1)
	case KindInt16:
		return int16(s.v1)
	case KindInt32:
		return int32(s.v1)
	case KindInt64:
		return int64(s.v1)
	case KindDouble:
		return math.Float64frombits(s.v1)
	case KindDecimal4:
		return Decimal4{Value: int32(s.v1), Scale: s.s}
	case KindDecimal8:
		return Decimal8{Value: int64(s.v1), Scale: s.s}
	case KindDecimal16:
		var val [16]byte
		binary.BigEndian.AppendUint64(val[:], s.v1)
		binary.BigEndian.AppendUint64(val[8:], s.v2)
		return Decimal16{Value: val, Scale: s.s}
	case KindDate:
		return Date(s.v1)
	case KindTimestampMicros:
		return Timestamp{TimeUnit: Microsecond, AdjustedToUTC: true, Value: int64(s.v1)}
	case KindTimestampMicrosNTZ:
		return Timestamp{TimeUnit: Microsecond, AdjustedToUTC: false, Value: int64(s.v1)}
	case KindFloat:
		return math.Float32frombits(uint32(s.v1))
	case KindBinary:
		return unsafe.Slice(s.p, s.v1)
	case KindString:
		return unsafe.String(s.p, s.v1)
	case KindTimeNTZ:
		return Time{TimeUnit: Microsecond, AdjustedToUTC: false, Value: int64(s.v1)}
	case KindTimestampNanos:
		return Timestamp{TimeUnit: Nanosecond, AdjustedToUTC: true, Value: int64(s.v1)}
	case KindTimestampNanosNTZ:
		return Timestamp{TimeUnit: Nanosecond, AdjustedToUTC: false, Value: int64(s.v1)}
	case KindUUID:
		var val UUID
		binary.BigEndian.AppendUint64(val[:], s.v1)
		binary.BigEndian.AppendUint64(val[8:], s.v2)
		return val
	case KindArray:
		ptr := (*Data)(unsafe.Pointer(s.p))
		return unsafe.Slice(ptr, s.v1)
	case KindObject:
		ptr := (*FieldData)(unsafe.Pointer(s.p))
		return unsafe.Slice(ptr, s.v1)
	default:
		// TODO: panic?
		return nil
	}
}

func (s Shredded) NullValue() (any, bool) {
	// This method is here for completeness. But it seems of
	// little or no value...
	return nil, s.kind == KindNull
}

func (s Shredded) BoolValue() (val bool, ok bool) {
	switch s.kind {
	case KindBooleanTrue:
		return true, true
	case KindBooleanFalse:
		return false, true
	default:
		return false, false
	}
}

func (s Shredded) Int8Value() (int8, bool) {
	if s.kind != KindInt8 {
		return 0, false
	}
	return int8(s.v1), true
}

func (s Shredded) Int16Value() (int16, bool) {
	if s.kind != KindInt16 {
		return 0, false
	}
	return int16(s.v1), true
}

func (s Shredded) Int32Value() (int32, bool) {
	if s.kind != KindInt32 {
		return 0, false
	}
	return int32(s.v1), true
}

func (s Shredded) Int64Value() (int64, bool) {
	if s.kind != KindInt64 {
		return 0, false
	}
	return int64(s.v1), true
}

func (s Shredded) Float64Value() (float64, bool) {
	if s.kind != KindDouble {
		return 0, false
	}
	return math.Float64frombits(s.v1), true
}

func (s Shredded) Float32Value() (float32, bool) {
	if s.kind != KindFloat {
		return 0, false
	}
	return math.Float32frombits(uint32(s.v1)), true
}

func (s Shredded) Decimal4Value() (Decimal4, bool) {
	if s.kind != KindDecimal4 {
		return Decimal4{}, false
	}
	return Decimal4{Value: int32(s.v1), Scale: s.s}, true
}

func (s Shredded) Decimal8Value() (Decimal8, bool) {
	if s.kind != KindDecimal8 {
		return Decimal8{}, false
	}
	return Decimal8{Value: int64(s.v1), Scale: s.s}, true
}

func (s Shredded) Decimal16Value() (Decimal16, bool) {
	if s.kind != KindDecimal16 {
		return Decimal16{}, false
	}
	var val [16]byte
	binary.BigEndian.AppendUint64(val[:], s.v1)
	binary.BigEndian.AppendUint64(val[8:], s.v2)
	return Decimal16{Value: val, Scale: s.s}, true
}

func (s Shredded) DateValue() (Date, bool) {
	if s.kind != KindDate {
		return 0, false
	}
	return Date(s.v1), true
}

func (s Shredded) TimeValue() (Time, bool) {
	if s.kind != KindTimeNTZ {
		return Time{}, false
	}
	return Time{TimeUnit: Microsecond, AdjustedToUTC: false, Value: int64(s.v1)}, true
}

func (s Shredded) TimestampValue() (Timestamp, bool) {
	switch s.kind {
	case KindTimestampMicros:
		return Timestamp{TimeUnit: Microsecond, AdjustedToUTC: true, Value: int64(s.v1)}, true
	case KindTimestampMicrosNTZ:
		return Timestamp{TimeUnit: Microsecond, AdjustedToUTC: false, Value: int64(s.v1)}, true
	case KindTimestampNanos:
		return Timestamp{TimeUnit: Nanosecond, AdjustedToUTC: true, Value: int64(s.v1)}, true
	case KindTimestampNanosNTZ:
		return Timestamp{TimeUnit: Nanosecond, AdjustedToUTC: false, Value: int64(s.v1)}, true
	default:
		return Timestamp{}, false
	}
}

func (s Shredded) BytesValue() ([]byte, bool) {
	if s.kind != KindBinary {
		return nil, false
	}
	return unsafe.Slice(s.p, s.v1), true
}

func (s Shredded) StringValue() (string, bool) {
	if s.kind != KindString {
		return "", false
	}
	return unsafe.String(s.p, s.v1), true
}

func (s Shredded) UUIDValue() (UUID, bool) {
	if s.kind != KindUUID {
		return UUID{}, false
	}
	var val UUID
	binary.BigEndian.AppendUint64(val[:], s.v1)
	binary.BigEndian.AppendUint64(val[8:], s.v2)
	return val, true
}

func (s Shredded) ArrayValue() ([]Data, bool) {
	if s.kind != KindArray {
		return nil, false
	}
	ptr := (*Data)(unsafe.Pointer(s.p))
	return unsafe.Slice(ptr, s.v1), true
}

func (s Shredded) ObjectValue() ([]FieldData, bool) {
	if s.kind != KindObject {
		return nil, false
	}
	ptr := (*FieldData)(unsafe.Pointer(s.p))
	return unsafe.Slice(ptr, s.v1), true
}

// Kind represents the kind or type of a value. Each kind constant
// represents the "Type ID" of a primitive type in the variant
// encoding spec, which can have values between 0 and 31 (5 bits).
//
// There are two additional kinds, KindArray and KindObject, which
// do not have corresponding type IDs since they are composite
// kinds, not primitive.
type Kind uint8

const (
	KindNull               = Kind(0)
	KindBooleanTrue        = Kind(1)
	KindBooleanFalse       = Kind(2)
	KindInt8               = Kind(3)
	KindInt16              = Kind(4)
	KindInt32              = Kind(5)
	KindInt64              = Kind(6)
	KindDouble             = Kind(7)
	KindDecimal4           = Kind(8)
	KindDecimal8           = Kind(9)
	KindDecimal16          = Kind(10)
	KindDate               = Kind(11)
	KindTimestampMicros    = Kind(12)
	KindTimestampMicrosNTZ = Kind(13)
	KindFloat              = Kind(14)
	KindBinary             = Kind(15)
	KindString             = Kind(16)
	KindTimeNTZ            = Kind(17)
	KindTimestampNanos     = Kind(18)
	KindTimestampNanosNTZ  = Kind(19)
	KindUUID               = Kind(20)

	KindArray  = Kind(254)
	KindObject = Kind(255)
)

func (k Kind) String() string {
	switch k {
	case KindNull:
		return "null"
	case KindBooleanTrue:
		return "boolean-true"
	case KindBooleanFalse:
		return "boolean-false"
	case KindInt8:
		return "int8"
	case KindInt16:
		return "int16"
	case KindInt32:
		return "int32"
	case KindInt64:
		return "int64"
	case KindDouble:
		return "double"
	case KindDecimal4:
		return "decimal4"
	case KindDecimal8:
		return "decimal8"
	case KindDecimal16:
		return "decimal16"
	case KindDate:
		return "date"
	case KindTimestampMicros:
		return "timestamp-micros"
	case KindTimestampMicrosNTZ:
		return "timestamp-micros-ntz"
	case KindFloat:
		return "float"
	case KindBinary:
		return "binary"
	case KindString:
		return "string"
	case KindTimeNTZ:
		return "time-ntz"
	case KindTimestampNanos:
		return "timestamp-nanos"
	case KindTimestampNanosNTZ:
		return "timestamp-nanos-ntz"
	case KindUUID:
		return "uuid"
	case KindArray:
		return "array"
	case KindObject:
		return "object"
	default:
		return fmt.Sprintf("Kind(%d)", k)
	}
}

type TimeUnit uint8

const (
	Microsecond TimeUnit = iota
	Nanosecond
)

type Timestamp struct {
	TimeUnit      TimeUnit
	AdjustedToUTC bool
	Value         int64
}

type Time Timestamp

type Date int32

type UUID [16]byte

type Decimal4 struct {
	Value int32
	Scale uint8
}

type Decimal8 struct {
	Value int64
	Scale uint8
}

type Decimal16 struct {
	Value [16]byte
	Scale uint8
}
