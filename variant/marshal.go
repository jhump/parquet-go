package variant

import (
	"fmt"
	"math"
	"reflect"
	"sync"
)

var (
	strategiesMu        sync.RWMutex
	marshalStrategies   map[reflect.Type]marshalStrategy
	unmarshalStrategies map[reflect.Type]unmarshalStrategy
)

// Marshal marshals the given src by calling methods on the given visitor.
// If the visitor is, for example, an Encoder then this will effectively
// marshal src to a Value.
func Marshal(src any, visitor Visitor) error {
	if src == nil {
		return visitor.VisitNull()
	}
	return marshal(reflect.ValueOf(src), visitor)
}

func MarshalToValue(src any, opts ...EncodeOption) (Value, error) {
	enc := NewEncoder(opts...)
	if err := Marshal(src, enc); err != nil {
		return Value{}, err
	}
	return enc.Encode()
}

type marshaler func(reflect.Value, Visitor) error
type marshalStrategy struct {
	marshaler marshaler
	err       error
}

func marshal(val reflect.Value, visitor Visitor) error {
	strategiesMu.RLock()
	strategy := marshalStrategies[val.Type()]
	strategiesMu.RUnlock()
	if strategy.err != nil {
		return strategy.err
	}
	if strategy.marshaler == nil {
		strategy.marshaler, strategy.err = marshalerFor(val.Type())
		strategiesMu.Lock()
		marshalStrategies[val.Type()] = strategy
		strategiesMu.Unlock()
		if strategy.err != nil {
			return strategy.err
		}
	}
	return strategy.marshaler(val, visitor)
}

func marshalerFor(typ reflect.Type) (marshaler, error) {
	switch typ.Kind() {
	case reflect.Bool:
		return marshalBool, nil
	case reflect.Int8:
		return marshalInt8, nil
	case reflect.Int16:
		return marshalInt16, nil
	case reflect.Int32:
		return marshalInt32, nil
	case reflect.Int64, reflect.Int:
		return marshalInt64, nil
	case reflect.Uint8:
		return marshalUint8, nil
	case reflect.Uint16:
		return marshalUint16, nil
	case reflect.Uint32:
		return marshalUint32, nil
	case reflect.Uint64, reflect.Uint, reflect.Uintptr:
		return marshalUint64, nil
	case reflect.Float32:
		return marshalFloat32, nil
	case reflect.Float64:
		return marshalFloat64, nil
	case reflect.Complex64:
		return marshalComplex64, nil
	case reflect.Complex128:
		return marshalComplex128, nil
	case reflect.Array, reflect.Slice:
		isSlice := typ.Kind() == reflect.Slice
		if typ.Elem().Kind() == reflect.Uint8 {
			if isSlice {
				return marshalNilCheck(marshalBytes), nil
			}
			return marshalByteArray, nil
		}
		elem, err := marshalerFor(typ.Elem())
		if err != nil {
			return nil, err
		}
		m := marshalElements(elem)
		if isSlice {
			return marshalNilCheck(m), nil
		}
		return m, nil
	case reflect.String:
		return marshalString, nil
	case reflect.Struct:
		var numFields int
		fields := make([]marshaler, typ.NumField())
		for i := range typ.NumField() {
			field := typ.Field(i)
			if !field.IsExported() {
				// leave this marshaler nil; we won't do anything with it
				continue
			}
			var err error
			fields[i], err = marshalerFor(field.Type)
			if err != nil {
				return nil, err
			}
			numFields++
		}
		if numFields == 0 {
			return marshalEmptyStruct, nil
		}
		return marshalStruct(fields), nil
	case reflect.Map:
		if typ.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("%w: maps must have string keys", marshalTypeError(typ))
		}
		elem, err := marshalerFor(typ.Elem())
		if err != nil {
			return nil, err
		}
		return marshalMap(elem), nil
	case reflect.Pointer:
		elem, err := marshalerFor(typ.Elem())
		if err != nil {
			return nil, err
		}
		return marshalNilCheck(marshalElem(elem)), nil
	case reflect.Interface:
		// We have to invoke marshal, to dynamically look up (or create)
		// a marshaler for the value's concrete runtime type.
		return marshalNilCheck(marshalElem(marshal)), nil
	case reflect.Func, reflect.Chan, reflect.UnsafePointer:
		return marshalNilCheck(marshalInvalid), nil
	case reflect.Invalid:
		fallthrough
	default:
		return nil, marshalTypeError(typ)
	}
}

func marshalTypeError(typ reflect.Type) error {
	return fmt.Errorf("cannot marshal value of type %v", typ.Kind())
}

func marshalNilCheck(delegate marshaler) marshaler {
	return func(val reflect.Value, visitor Visitor) error {
		if val.IsNil() {
			return visitor.VisitNull()
		}
		return delegate(val, visitor)
	}
}

func marshalBool(val reflect.Value, visitor Visitor) error {
	return visitor.VisitBool(val.Bool())
}

func marshalInt8(val reflect.Value, visitor Visitor) error {
	return visitor.VisitInt8(int8(val.Int()))
}

func marshalInt16(val reflect.Value, visitor Visitor) error {
	return visitor.VisitInt16(int16(val.Int()))
}

func marshalInt32(val reflect.Value, visitor Visitor) error {
	return visitor.VisitInt32(int32(val.Int()))
}

func marshalInt64(val reflect.Value, visitor Visitor) error {
	return visitor.VisitInt64(val.Int())
}

func marshalUint8(val reflect.Value, visitor Visitor) error {
	if val.Uint() <= math.MaxInt8 {
		return visitor.VisitInt8(int8(val.Uint()))
	}
	return marshalUint16(val, visitor)
}

func marshalUint16(val reflect.Value, visitor Visitor) error {
	if val.Uint() <= math.MaxInt16 {
		return visitor.VisitInt16(int16(val.Uint()))
	}
	return marshalUint32(val, visitor)
}

func marshalUint32(val reflect.Value, visitor Visitor) error {
	if val.Uint() <= math.MaxInt32 {
		return visitor.VisitInt32(int32(val.Uint()))
	}
	return marshalUint64(val, visitor)
}

func marshalUint64(val reflect.Value, visitor Visitor) error {
	if val.Uint() <= math.MaxInt64 {
		return visitor.VisitInt64(int64(val.Uint()))
	}
	return visitor.VisitDecimal16(Decimal16{ValueLo: val.Uint()})
}

func marshalFloat32(val reflect.Value, visitor Visitor) error {
	return visitor.VisitFloat32(float32(val.Float()))
}

func marshalFloat64(val reflect.Value, visitor Visitor) error {
	return visitor.VisitFloat64(val.Float())
}

func marshalComplex64(val reflect.Value, visitor Visitor) error {
	cmp := val.Complex()
	if imag(cmp) == 0 {
		return visitor.VisitFloat32(float32(real(cmp)))
	}
	return marshalTypeError(val.Type())
}

func marshalComplex128(val reflect.Value, visitor Visitor) error {
	cmp := val.Complex()
	if imag(cmp) == 0 {
		return visitor.VisitFloat64(real(cmp))
	}
	return marshalTypeError(val.Type())
}

func marshalByteArray(val reflect.Value, visitor Visitor) error {
	if val.CanAddr() {
		return marshalBytes(val, visitor)
	}
	bytes := make([]byte, val.Len())
	for i := range val.Len() {
		bytes[i] = byte(val.Index(i).Uint())
	}
	return visitor.VisitBytes(bytes)
}

func marshalBytes(val reflect.Value, visitor Visitor) error {
	return visitor.VisitBytes(val.Bytes())
}

func marshalElements(elems marshaler) marshaler {
	return func(val reflect.Value, visitor Visitor) error {
		if err := visitor.BeginArray(val.Len()); err != nil {
			return err
		}
		for i := range val.Len() {
			if err := elems(val.Index(i), visitor); err != nil {
				return err
			}
		}
		return visitor.EndArray()
	}
}

func marshalString(val reflect.Value, visitor Visitor) error {
	return visitor.VisitString(val.String())
}

func marshalEmptyStruct(_ reflect.Value, visitor Visitor) error {
	if err := visitor.BeginObject(0); err != nil {
		return err
	}
	return visitor.EndObject()
}

func marshalStruct(fields []marshaler) marshaler {
	return func(val reflect.Value, visitor Visitor) error {
		if err := visitor.BeginObject(val.NumField()); err != nil {
			return err
		}
		t := val.Type()
		for i := range val.NumField() {
			elem := fields[i]
			if elem == nil {
				continue
			}
			if err := visitor.ObjectField(t.Field(i).Name); err != nil {
				return err
			}
			if err := elem(val.Field(i), visitor); err != nil {
				return err
			}
		}
		return visitor.EndObject()
	}
}

func marshalMap(elems marshaler) marshaler {
	return func(val reflect.Value, visitor Visitor) error {
		if err := visitor.BeginObject(val.Len()); err != nil {
			return err
		}
		iter := val.MapRange()
		for iter.Next() {
			if err := visitor.ObjectField(iter.Key().String()); err != nil {
				return err
			}
			if err := elems(iter.Value(), visitor); err != nil {
				return err
			}
		}
		return visitor.EndObject()
	}
}

func marshalElem(elem marshaler) marshaler {
	return func(val reflect.Value, visitor Visitor) error {
		return elem(val.Elem(), visitor)
	}
}

func marshalInvalid(val reflect.Value, visitor Visitor) error {
	return marshalTypeError(val.Type())
}
