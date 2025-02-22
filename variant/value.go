package variant

import (
	"encoding/binary"
	"math"
	"unsafe"
)

// Value represents a variant. It contains encoded metadata and data
// and also optionally stores a representation of shredded fields.
type Value struct {
	// Metadata contains field name metadata for all data in this
	// value.
	Metadata []byte

	Data
}

// Decode decodes the value represented by v and calls the appropriate
// methods of visitor.
func (v Value) Decode(visitor Visitor) error {
	// TODO
	return nil
}

// Data represents encoded variant data. A Value may encode a composite
// that comprises multiple pieces of Data that all share the same
// metadata.
type Data struct {
	// Unshredded contains raw "unshredded" encoded data.
	Unshredded []byte

	// Shredded contains strongly-typed data for a subset of
	// fields. The field(s) represented in Shredded will not be
	// present in Unshredded. So to decode and observe all data
	// in the value, both fields must be examined.
	Shredded Shredded
}

// Shredded represents a (partially) decoded value.
type Shredded struct {
	// primary value; or length for string, bytes, array, and group kinds
	v1 uint64

	// extra data for value (for 128-bit vals like UUIDs and decimals)
	v2 uint64

	// used only for string, bytes, array, and group kinds
	// points to an array of byte for the former 2, array of Data for array,
	// or array of FieldData for group
	p *byte

	// scale for decimal values
	s uint8

	kind Kind
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
// The lattermost two are used for array and group values, respectively.
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
		return ShreddedValueOfGroup(val), true
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

func ShreddedValueOfGroup(val []FieldData) Shredded {
	return Shredded{
		kind: KindGroup,
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
	case KindGroup:
		ptr := (*FieldData)(unsafe.Pointer(s.p))
		return unsafe.Slice(ptr, s.v1)
	default:
		// TODO: panic?
		return nil
	}
}

// FieldData represents the Data for a field in a group.
type FieldData struct {
	// Name represents the field name for this element.
	Name string
	Data
}

// Kind represents the kind or type of a value. Each kind constant
// represents the "Type ID" of a primitive type in the variant
// encoding spec, which can have values between 0 and 31 (5 bits).
//
// There are two additional kinds, KindArray and KindGroup, which
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

	KindArray = Kind(254)
	KindGroup = Kind(255)
)

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
