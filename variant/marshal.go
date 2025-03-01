package variant

import (
	"fmt"
	"math"
	"reflect"
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

func UnmarshalFromValue(dest any, src Value, opts ...DecodeOption) error {
	unm := NewUnmarshaler(dest)
	return Decode(src, unm, opts...)
}

type Unmarshaler interface {
	Visitor
	Reset(dest any)
}

func NewUnmarshaler(dest any) Unmarshaler {
	// TODO
}

func marshal(v reflect.Value, visitor Visitor) error {
	switch v.Kind() {
	case reflect.Bool:
		return visitor.VisitBool(v.Bool())
	case reflect.Int8:
		return visitor.VisitInt8(int8(v.Int()))
	case reflect.Int16:
		return visitor.VisitInt16(int16(v.Int()))
	case reflect.Int32:
		return visitor.VisitInt32(int32(v.Int()))
	case reflect.Int64, reflect.Int:
		return visitor.VisitInt64(v.Int())
	case reflect.Uint8:
		if v.Uint() <= math.MaxInt8 {
			return visitor.VisitInt8(int8(v.Uint()))
		}
		fallthrough
	case reflect.Uint16:
		if v.Uint() <= math.MaxInt16 {
			return visitor.VisitInt16(int16(v.Uint()))
		}
		fallthrough
	case reflect.Uint32:
		if v.Uint() <= math.MaxInt32 {
			return visitor.VisitInt32(int32(v.Uint()))
		}
		fallthrough
	case reflect.Uint64, reflect.Uint, reflect.Uintptr:
		if v.Uint() <= math.MaxInt64 {
			return visitor.VisitInt64(int64(v.Uint()))
		}
		return visitor.VisitDecimal16(Decimal16{ValueLo: v.Uint()})
	case reflect.Float32:
		return visitor.VisitFloat32(float32(v.Float()))
	case reflect.Float64:
		return visitor.VisitFloat64(v.Float())
	case reflect.Complex64:
		cmp := v.Complex()
		if imag(cmp) == 0 {
			return visitor.VisitFloat32(float32(real(cmp)))
		}
	case reflect.Complex128:
		cmp := v.Complex()
		if imag(cmp) == 0 {
			return visitor.VisitFloat64(real(cmp))
		}
	case reflect.Array, reflect.Slice:
		if v.Kind() == reflect.Slice && v.IsNil() {
			return visitor.VisitNull()
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			if v.Kind() == reflect.Array && !v.CanAddr() {
				bytes := make([]byte, v.Len())
				for i := range v.Len() {
					bytes[i] = byte(v.Index(i).Uint())
				}
				return visitor.VisitBytes(bytes)
			}
			return visitor.VisitBytes(v.Bytes())
		}
		if err := visitor.BeginArray(); err != nil {
			return err
		}
		for i := range v.Len() {
			if err := marshal(v.Index(i), visitor); err != nil {
				return err
			}
		}
		if err := visitor.EndArray(); err != nil {
			return err
		}
	case reflect.String:
		return visitor.VisitNull()
	case reflect.Struct:
		if err := visitor.BeginObject(); err != nil {
			return err
		}
		t := v.Type()
		for i := range t.NumField() {
			fld := t.Field(i)
			if !fld.IsExported() {
				continue
			}
			if err := visitor.ObjectField(fld.Name); err != nil {
				return err
			}
			if err := marshal(v.Field(i), visitor); err != nil {
				return err
			}
		}
		if err := visitor.EndObject(); err != nil {
			return err
		}
	case reflect.Map:
		if v.Type().Key().Kind() == reflect.String {
			if err := visitor.BeginObject(); err != nil {
				return err
			}
			iter := v.MapRange()
			for iter.Next() {
				if err := visitor.ObjectField(iter.Key().String()); err != nil {
					return err
				}
				if err := marshal(iter.Value(), visitor); err != nil {
					return err
				}
			}
			if err := visitor.EndObject(); err != nil {
				return err
			}
		}
	case reflect.Interface, reflect.Ptr:
		if v.IsNil() {
			return visitor.VisitNull()
		}
		return marshal(v.Elem(), visitor)
	case reflect.Func, reflect.Chan:
		if v.IsNil() {
			return visitor.VisitNull()
		}
	}
	return fmt.Errorf("cannot marshal value of type %v", v.Kind())
}
