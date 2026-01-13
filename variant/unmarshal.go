package variant

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"time"

	"github.com/google/uuid"
)

var (
	typeOfAnyMap       = reflect.TypeOf(map[string]any(nil))
	typeOfAnySlice     = reflect.TypeOf([]any(nil))
	typeOfUUID         = reflect.TypeOf(uuid.UUID{})
	typeOfDate         = reflect.TypeOf(Date(0))
	typeOfTime         = reflect.TypeOf(Time{})
	typeOfTimestamp    = reflect.TypeOf(Timestamp{})
	typeOfDecimal4     = reflect.TypeOf(Decimal4{})
	typeOfDecimal8     = reflect.TypeOf(Decimal8{})
	typeOfDecimal16    = reflect.TypeOf(Decimal16{})
	typeOfTimeTime     = reflect.TypeOf(time.Time{})
	typeOfTimeDuration = reflect.TypeOf(time.Duration(0))
	typeofBigInt       = reflect.TypeOf(big.Int{})
	typeofBigRat       = reflect.TypeOf(big.Rat{})
	typeofBigFloat     = reflect.TypeOf(big.Float{})
)

func UnmarshalFromValue(dest any, src Value, opts ...DecodeOption) error {
	unm, err := NewUnmarshaler(dest)
	if err != nil {
		return err
	}
	return src.Decode(unm, opts...)
}

type Unmarshaler interface {
	Visitor
	Reset(dest any) error
}

func NewUnmarshaler(dest any) (Unmarshaler, error) {
	result := &unmarshalStack{}
	if err := result.Reset(dest); err != nil {
		return nil, err
	}
	return result, nil
}

type unmarshalStack struct {
	stack []unmarshalStackFrame
	init  unmarshalStackFrame
}

func (s *unmarshalStack) Reset(dest any) error {
	val := reflect.ValueOf(dest)
	if stackFrame := s.init; stackFrame != nil && stackFrame.tryReset(val) {
		clear(s.stack)
		s.stack = []unmarshalStackFrame{stackFrame}
		return nil
	}
	stackFrame, err := newUnmarshalStackRoot(val, s)
	if err != nil {
		return err
	}
	clear(s.stack)
	s.stack = []unmarshalStackFrame{stackFrame}
	s.init = stackFrame
	return nil
}

func (s *unmarshalStack) push(stackFrame unmarshalStackFrame) {
	stackFrame.initStack(s)
	s.stack = append(s.stack, stackFrame)
}

func (s *unmarshalStack) pop() error {
	top := s.stack[len(s.stack)-1]
	s.stack[len(s.stack)-1] = nil
	s.stack = s.stack[:len(s.stack)-1]
	if len(s.stack) > 0 {
		return s.stack[len(s.stack)-1].childPopped(top.getDest())
	}
	return nil
}

func unmarshalStackCall(stack *unmarshalStack, action func(unmarshalStackFrame) error) error {
	if len(stack.stack) == 0 {
		return errMustReset
	}
	return action(stack.stack[len(stack.stack)-1])
}

func unmarshalStackCallValue[T any](stack *unmarshalStack, val T, action func(unmarshalStackFrame, T) error) error {
	if len(stack.stack) == 0 {
		return errMustReset
	}
	return action(stack.stack[len(stack.stack)-1], val)
}

func (s *unmarshalStack) VisitNull() error {
	return unmarshalStackCall(s, unmarshalStackFrame.VisitNull)
}
func (s *unmarshalStack) VisitBool(val bool) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitBool)
}
func (s *unmarshalStack) VisitInt8(val int8) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitInt8)
}
func (s *unmarshalStack) VisitInt16(val int16) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitInt16)
}
func (s *unmarshalStack) VisitInt32(val int32) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitInt32)
}
func (s *unmarshalStack) VisitInt64(val int64) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitInt64)
}
func (s *unmarshalStack) VisitFloat32(val float32) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitFloat32)
}
func (s *unmarshalStack) VisitFloat64(val float64) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitFloat64)
}
func (s *unmarshalStack) VisitDecimal4(val Decimal4) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitDecimal4)
}
func (s *unmarshalStack) VisitDecimal8(val Decimal8) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitDecimal8)
}
func (s *unmarshalStack) VisitDecimal16(val Decimal16) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitDecimal16)
}
func (s *unmarshalStack) VisitDate(val Date) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitDate)
}
func (s *unmarshalStack) VisitTime(val Time) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitTime)
}
func (s *unmarshalStack) VisitTimestamp(val Timestamp) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitTimestamp)
}
func (s *unmarshalStack) VisitBytes(val []byte) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitBytes)
}
func (s *unmarshalStack) VisitString(val string) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitString)
}
func (s *unmarshalStack) VisitUUID(val uuid.UUID) error {
	return unmarshalStackCallValue(s, val, unmarshalStackFrame.VisitUUID)
}
func (s *unmarshalStack) BeginArray(sizeHint int) error {
	return unmarshalStackCallValue(s, sizeHint, unmarshalStackFrame.BeginArray)
}
func (s *unmarshalStack) EndArray() error {
	return unmarshalStackCall(s, unmarshalStackFrame.EndArray)
}
func (s *unmarshalStack) BeginObject(sizeHint int) error {
	return unmarshalStackCallValue(s, sizeHint, unmarshalStackFrame.BeginObject)
}
func (s *unmarshalStack) ObjectField(name string) error {
	return unmarshalStackCallValue(s, name, unmarshalStackFrame.ObjectField)
}
func (s *unmarshalStack) EndObject() error {
	return unmarshalStackCall(s, unmarshalStackFrame.EndObject)
}

func newUnmarshalStackRoot(val reflect.Value, stack *unmarshalStack) (unmarshalStackFrame, error) {
	if val.Kind() != reflect.Map && val.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("destination must be a pointer or a map, instead got %v", val.Type())
	}
	if val.IsNil() {
		return nil, errors.New("destination is nil")
	}
	factory, err := unmarshalStrategyFor(val.Type())
	if err != nil {
		return nil, err
	}
	return makeUnmarshalStackFrame(val, factory, stack), nil
}

type unmarshalStrategy struct {
	factory unmarshalFrameFactory
	err     error
}

func unmarshalStrategyFor(typ reflect.Type) (unmarshalFrameFactory, error) {
	strategiesMu.RLock()
	strategy := unmarshalStrategies[typ]
	strategiesMu.RUnlock()
	if strategy.err != nil {
		return nil, strategy.err
	}
	if strategy.factory == nil {
		strategy.factory, strategy.err = newUnmarshalStrategy(typ)
		strategiesMu.Lock()
		unmarshalStrategies[typ] = strategy
		strategiesMu.Unlock()
		if strategy.err != nil {
			return nil, strategy.err
		}
	}
	return strategy.factory, nil
}

func newUnmarshalStrategy(typ reflect.Type) (unmarshalFrameFactory, error) {
	switch typ.Kind() {
	case reflect.Bool:
		return factoryOf[unmarshalerBool], nil
	case reflect.Int8:
		return newUnmarshalerInt[int8](math.MinInt8, math.MaxInt8), nil
	case reflect.Int16:
		return newUnmarshalerInt[int16](math.MinInt16, math.MaxInt16), nil
	case reflect.Int32:
		// TODO: special-case Date
		return newUnmarshalerInt[int32](math.MinInt32, math.MaxInt32), nil
	case reflect.Int64:
		// TODO: maybe special-case time.Duration?
		return newUnmarshalerInt[int64](math.MinInt64, math.MaxInt64), nil
	case reflect.Int:
		return newUnmarshalerInt[int](math.MinInt, math.MaxInt), nil
	case reflect.Uint8:
		return newUnmarshalerUint[uint8](math.MaxUint8), nil
	case reflect.Uint16:
		return newUnmarshalerUint[uint16](math.MaxUint16), nil
	case reflect.Uint32:
		return newUnmarshalerUint[uint32](math.MaxUint32), nil
	case reflect.Uint64:
		return newUnmarshalerUint[uint64](math.MaxUint64), nil
	case reflect.Uint, reflect.Uintptr:
		return newUnmarshalerUint[uint](math.MaxUint), nil
	case reflect.Float32, reflect.Float64:
		return factoryOf[unmarshalerFloat], nil
	case reflect.Complex64, reflect.Complex128:
		return factoryOf[unmarshalerComplex], nil
	case reflect.String:
		return factoryOf[unmarshalerString], nil
	case reflect.Array, reflect.Slice:
		elem, err := unmarshalStrategyFor(typ.Elem())
		if err != nil {
			return nil, err
		}
		length := -1
		if typ.Kind() == reflect.Array {
			length = typ.Len()
		}
		factory := newUnmarshalerRepeated(elem, length)
		if typ.Elem().Kind() == reflect.Uint8 {
			return newUnmarshalerBytes(factory, length, typ == typeOfUUID), nil
		}
		return factory, nil
	case reflect.Struct:
		// TODO: special-case Time, Timestamp, Decimal4, Decimal8, and Decimal16
		// TODO: maybe special-case Value and Shredded?
		// TODO: maybe special-case time.Time, big.Int, big.Rat, and big.Float?
		fields := make([]unmarshalFrameFactory, typ.NumField())
		for i := range typ.NumField() {
			field := typ.Field(i)
			if !field.IsExported() {
				continue // leave unmarshaler for unexported fields nil
			}
			fieldUnmarshaler, err := unmarshalStrategyFor(field.Type)
			if err != nil {
				return nil, err
			}
			fields[i] = fieldUnmarshaler
		}
		return newUnmarshalerStruct(fields), nil
	case reflect.Map:
		if typ.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("can only unmarshal into a map with string keys")
		}
		elem, err := unmarshalStrategyFor(typ.Elem())
		if err != nil {
			return nil, err
		}
		return newUnmarshalerMap(elem), nil
	case reflect.Pointer:
		elem, err := unmarshalStrategyFor(typ.Elem())
		if err != nil {
			return nil, err
		}
		return newUnmarshalerPointer(elem), nil
	case reflect.Interface:
		if typ.NumMethod() > 0 {
			return nil, fmt.Errorf("cannot unmarshal into interface type that has methods")
		}
		return factoryOf[unmarshalerInterface], nil
	case reflect.UnsafePointer, reflect.Func, reflect.Chan:
		return factoryOf[unmarshalerNil], nil
	case reflect.Invalid:
		fallthrough
	default:
		return nil, fmt.Errorf("cannot unmarshal value of type %v", typ.Kind())
	}
}

type unmarshalFrameFactory func() unmarshalStackFrame

type unmarshalStackFrame interface {
	Visitor
	initStack(stack *unmarshalStack)
	reset(dest reflect.Value)
	tryReset(dest reflect.Value) bool
	getDest() reflect.Value
	childPopped(childValue reflect.Value) error
}

func makeUnmarshalStackFrame(dest reflect.Value, factory unmarshalFrameFactory, stack *unmarshalStack) unmarshalStackFrame {
	frame := factory()
	frame.initStack(stack)
	frame.reset(dest)
	return frame
}

type unmarshalConcreteStackFrame[T any] interface {
	*T
	unmarshalStackFrame
}

func factoryOf[T any, V unmarshalConcreteStackFrame[T]]() unmarshalStackFrame {
	return V(new(T))
}

type baseUnmarshalStackFrame struct {
	dest  reflect.Value
	stack *unmarshalStack
}

func (v *baseUnmarshalStackFrame) initStack(stack *unmarshalStack) {
	v.stack = stack
}

func (v *baseUnmarshalStackFrame) getDest() reflect.Value {
	return v.dest
}

func (v *baseUnmarshalStackFrame) reset(dest reflect.Value) {
	v.dest = dest
}

func (v *baseUnmarshalStackFrame) tryReset(dest reflect.Value) bool {
	if dest.Type() != v.dest.Type() {
		return false
	}
	v.dest = dest
	return true
}

func (v *baseUnmarshalStackFrame) childPopped(val reflect.Value) error {
	return fmt.Errorf("cannot process child of type %v into destination of type %v", val.Type(), v.dest.Type())
}

func (v *baseUnmarshalStackFrame) VisitNull() error {
	return unmarshalTypeError("null", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitBool(bool) error {
	return unmarshalTypeError("bool", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitInt8(int8) error {
	return unmarshalTypeError("int8", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitInt16(int16) error {
	return unmarshalTypeError("int16", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitInt32(int32) error {
	return unmarshalTypeError("int32", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitInt64(int64) error {
	return unmarshalTypeError("int64", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitFloat32(float32) error {
	return unmarshalTypeError("float32", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitFloat64(float64) error {
	return unmarshalTypeError("float64", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitDecimal4(Decimal4) error {
	return unmarshalTypeError("decimal4", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitDecimal8(Decimal8) error {
	return unmarshalTypeError("decimal8", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitDecimal16(Decimal16) error {
	return unmarshalTypeError("decimal16", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitDate(Date) error {
	return unmarshalTypeError("date", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitTime(Time) error {
	return unmarshalTypeError("time", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitTimestamp(Timestamp) error {
	return unmarshalTypeError("timestamp", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitBytes([]byte) error {
	return unmarshalTypeError("bytes", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitString(string) error {
	return unmarshalTypeError("string", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) VisitUUID(uuid.UUID) error {
	return unmarshalTypeError("uuid", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) BeginArray(int) error {
	return unmarshalTypeError("array", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) EndArray() error {
	return errCannotCallEndArray
}
func (v *baseUnmarshalStackFrame) BeginObject(int) error {
	return unmarshalTypeError("object", v.dest.Type())
}
func (v *baseUnmarshalStackFrame) ObjectField(string) error {
	return errCannotCallObjectField
}
func (v *baseUnmarshalStackFrame) EndObject() error {
	return errCannotCallEndObject
}

func unmarshalTypeError(valueType string, typ reflect.Type) error {
	return fmt.Errorf("cannot unmarshal value of type %v into destination of type %v", valueType, typ)
}

type unmarshalerBool struct {
	baseUnmarshalStackFrame
}

func (v *unmarshalerBool) VisitBool(val bool) error {
	v.dest.SetBool(val)
	return v.stack.pop()
}

type intType interface {
	int8 | int16 | int32 | int64 | int
}

type unmarshalerInt[T intType] struct {
	baseUnmarshalStackFrame
	minVal, maxVal int64
}

func newUnmarshalerInt[T intType](minVal, maxVal T) unmarshalFrameFactory {
	return func() unmarshalStackFrame {
		return &unmarshalerInt[T]{
			minVal: int64(minVal),
			maxVal: int64(maxVal),
		}
	}
}

func (v *unmarshalerInt[T]) visitInt(val int64) error {
	if val > v.maxVal || val < v.minVal {
		return fmt.Errorf("value %d out of allowed range %d-%d", val, v.minVal, v.maxVal)
	}
	v.dest.SetInt(val)
	return v.stack.pop()
}
func (v *unmarshalerInt[T]) VisitInt8(val int8) error {
	return v.visitInt(int64(val))
}
func (v *unmarshalerInt[T]) VisitInt16(val int16) error {
	return v.visitInt(int64(val))
}
func (v *unmarshalerInt[T]) VisitInt32(val int32) error {
	return v.visitInt(int64(val))
}
func (v *unmarshalerInt[T]) VisitInt64(val int64) error {
	return v.visitInt(val)
}

// TODO: also accept decimal and float values, truncating to int (error on overflow)?

type uintType interface {
	uint8 | uint16 | uint32 | uint64 | uint | uintptr
}

type unmarshalerUint[T uintType] struct {
	baseUnmarshalStackFrame
	maxVal uint64
}

func newUnmarshalerUint[T uintType](maxVal T) unmarshalFrameFactory {
	return func() unmarshalStackFrame {
		return &unmarshalerUint[T]{
			maxVal: uint64(maxVal),
		}
	}
}

func (v *unmarshalerUint[T]) visitInt(val int64) error {
	if val < 0 || uint64(val) > v.maxVal {
		return fmt.Errorf("value %d out of allowed range 0-%d", val, v.maxVal)
	}
	v.dest.SetUint(uint64(val))
	return v.stack.pop()
}
func (v *unmarshalerUint[T]) VisitInt8(val int8) error {
	return v.visitInt(int64(val))
}
func (v *unmarshalerUint[T]) VisitInt16(val int16) error {
	return v.visitInt(int64(val))
}
func (v *unmarshalerUint[T]) VisitInt32(val int32) error {
	return v.visitInt(int64(val))
}
func (v *unmarshalerUint[T]) VisitInt64(val int64) error {
	return v.visitInt(val)
}

// TODO: also accept decimal and float values, truncating to int (error on overflow)?

type unmarshalerFloat struct {
	baseUnmarshalStackFrame
}

func (v *unmarshalerFloat) visitFloat(val float64) error {
	v.dest.SetFloat(val)
	return v.stack.pop()
}
func (v *unmarshalerFloat) VisitInt8(val int8) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerFloat) VisitInt16(val int16) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerFloat) VisitInt32(val int32) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerFloat) VisitInt64(val int64) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerFloat) VisitFloat32(val float32) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerFloat) VisitDecimal4(val Decimal4) error {
	floatVal, _ := val.AsBigRat().Float64()
	return v.visitFloat(floatVal)
}
func (v *unmarshalerFloat) VisitDecimal8(val Decimal8) error {
	floatVal, _ := val.AsBigRat().Float64()
	return v.visitFloat(floatVal)
}
func (v *unmarshalerFloat) VisitDecimal16(val Decimal16) error {
	floatVal, _ := val.AsBigRat().Float64()
	return v.visitFloat(floatVal)
}

type unmarshalerComplex struct {
	baseUnmarshalStackFrame
}

func (v *unmarshalerComplex) visitFloat(val float64) error {
	v.dest.SetComplex(complex(val, 0))
	return v.stack.pop()
}
func (v *unmarshalerComplex) VisitInt8(val int8) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerComplex) VisitInt16(val int16) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerComplex) VisitInt32(val int32) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerComplex) VisitInt64(val int64) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerComplex) VisitFloat32(val float32) error {
	return v.visitFloat(float64(val))
}
func (v *unmarshalerComplex) VisitDecimal4(val Decimal4) error {
	floatVal, _ := val.AsBigRat().Float64()
	return v.visitFloat(floatVal)
}
func (v *unmarshalerComplex) VisitDecimal8(val Decimal8) error {
	floatVal, _ := val.AsBigRat().Float64()
	return v.visitFloat(floatVal)
}
func (v *unmarshalerComplex) VisitDecimal16(val Decimal16) error {
	floatVal, _ := val.AsBigRat().Float64()
	return v.visitFloat(floatVal)
}

type unmarshalerString struct {
	baseUnmarshalStackFrame
}

func (v *unmarshalerString) VisitString(val string) error {
	v.dest.SetString(val)
	return v.stack.pop()
}
func (v *unmarshalerString) VisitBytes(val []byte) error {
	v.dest.SetString(string(val))
	return v.stack.pop()
}

type delegatingUnmarshalStackFrame struct {
	baseUnmarshalStackFrame
	next func() (unmarshalStackFrame, error)
}

func delegateCall(v *delegatingUnmarshalStackFrame, action func(unmarshalStackFrame) error) error {
	next, err := v.next()
	if err != nil {
		return err
	}
	err = action(next)
	if err != nil {
		return err
	}
	v.stack.push(next)
	return nil
}

func delegateCallValue[T any](v *delegatingUnmarshalStackFrame, val T, action func(unmarshalStackFrame, T) error) error {
	next, err := v.next()
	if err != nil {
		return err
	}
	err = action(next, val)
	if err != nil {
		return err
	}
	v.stack.push(next)
	return nil
}

func (v *delegatingUnmarshalStackFrame) VisitNull() error {
	return delegateCall(v, unmarshalStackFrame.VisitNull)
}
func (v *delegatingUnmarshalStackFrame) VisitBool(val bool) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitBool)
}
func (v *delegatingUnmarshalStackFrame) VisitInt8(val int8) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitInt8)
}
func (v *delegatingUnmarshalStackFrame) VisitInt16(val int16) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitInt16)
}
func (v *delegatingUnmarshalStackFrame) VisitInt32(val int32) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitInt32)
}
func (v *delegatingUnmarshalStackFrame) VisitInt64(val int64) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitInt64)
}
func (v *delegatingUnmarshalStackFrame) VisitFloat32(val float32) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitFloat32)
}
func (v *delegatingUnmarshalStackFrame) VisitFloat64(val float64) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitFloat64)
}
func (v *delegatingUnmarshalStackFrame) VisitDecimal4(val Decimal4) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitDecimal4)
}
func (v *delegatingUnmarshalStackFrame) VisitDecimal8(val Decimal8) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitDecimal8)
}
func (v *delegatingUnmarshalStackFrame) VisitDecimal16(val Decimal16) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitDecimal16)
}
func (v *delegatingUnmarshalStackFrame) VisitDate(val Date) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitDate)
}
func (v *delegatingUnmarshalStackFrame) VisitTime(val Time) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitTime)
}
func (v *delegatingUnmarshalStackFrame) VisitTimestamp(val Timestamp) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitTimestamp)
}
func (v *delegatingUnmarshalStackFrame) VisitBytes(val []byte) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitBytes)
}
func (v *delegatingUnmarshalStackFrame) VisitString(val string) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitString)
}
func (v *delegatingUnmarshalStackFrame) VisitUUID(val uuid.UUID) error {
	return delegateCallValue(v, val, unmarshalStackFrame.VisitUUID)
}
func (v *delegatingUnmarshalStackFrame) BeginArray(sizeHint int) error {
	return delegateCallValue(v, sizeHint, unmarshalStackFrame.BeginArray)
}
func (v *delegatingUnmarshalStackFrame) BeginObject(sizeHint int) error {
	return delegateCallValue(v, sizeHint, unmarshalStackFrame.BeginObject)
}

type unmarshalComposite interface {
	unmarshalStackFrame
	setToNil() error
	begin(sizeHint int) error
}

type unmarshalerArray struct {
	baseUnmarshalStackFrame
	onActivation unmarshalComposite
}

func (v *unmarshalerArray) reset(dest reflect.Value) {
	v.baseUnmarshalStackFrame.reset(dest)
	v.onActivation.reset(dest)
}
func (v *unmarshalerArray) tryReset(dest reflect.Value) bool {
	if v.baseUnmarshalStackFrame.tryReset(dest) {
		v.onActivation.reset(dest)
		return true
	}
	return false
}
func (v *unmarshalerArray) childPopped(_ reflect.Value) error {
	return v.stack.pop()
}
func (v *unmarshalerArray) VisitNull() error {
	return v.onActivation.setToNil()
}
func (v *unmarshalerArray) BeginArray(sizeHint int) error {
	if err := v.onActivation.begin(sizeHint); err != nil {
		return err
	}
	v.stack.push(v.onActivation)
	return nil
}

type unmarshalerRepeated struct {
	delegatingUnmarshalStackFrame
	factory unmarshalFrameFactory
	limit   int // -1 if this is a slice; otherwise array length
	count   int // items added so far
	elem    unmarshalStackFrame
}

func newUnmarshalerRepeated(elem unmarshalFrameFactory, length int) unmarshalFrameFactory {
	return func() unmarshalStackFrame {
		frame := &unmarshalerRepeated{factory: elem, limit: length}
		frame.next = frame.nextChild
		return &unmarshalerArray{onActivation: frame}
	}
}

func (v *unmarshalerRepeated) reset(dest reflect.Value) {
	v.baseUnmarshalStackFrame.reset(dest)
	if v.limit == -1 {
		// reset to empty slice w/ zero capacity
		dest.Set(reflect.MakeSlice(dest.Type().Elem(), 0, 0))
	} else {
		dest.Clear()
	}
}
func (v *unmarshalerRepeated) setToNil() error {
	if v.limit != -1 {
		// can't set array to nil
		return v.baseUnmarshalStackFrame.VisitNull()
	}
	v.dest.Set(reflect.Zero(v.dest.Type()))
	return v.stack.pop()
}
func (v *unmarshalerRepeated) begin(sizeHint int) error {
	if sizeHint > 0 && v.limit == -1 {
		// pre-allocate capacity of slice
		v.dest.Set(reflect.MakeSlice(v.dest.Type().Elem(), 0, sizeHint))
	}
	return nil
}
func (v *unmarshalerRepeated) nextChild() (unmarshalStackFrame, error) {
	if v.count == v.limit {
		return nil, fmt.Errorf("out of range: can't set index %d on array with length %d", v.count, v.limit)
	}
	if v.elem == nil {
		v.elem = v.factory()
	}
	var val reflect.Value
	if v.limit == -1 {
		val = reflect.New(v.dest.Type().Elem()).Elem()
	} else {
		val = v.dest.Index(v.count)
	}
	v.elem.reset(val)
	return v.elem, nil
}
func (v *unmarshalerRepeated) childPopped(childValue reflect.Value) error {
	if v.limit == -1 {
		reflect.Append(v.dest, childValue)
	}
	v.count++
	return nil
}
func (v *unmarshalerRepeated) EndArray() error {
	return v.stack.pop()
}

type unmarshalerBytes struct {
	unmarshalStackFrame
	limit  int
	isUUID bool
}

func newUnmarshalerBytes(delegate unmarshalFrameFactory, length int, isUUID bool) unmarshalFrameFactory {
	return func() unmarshalStackFrame {
		frame := delegate()
		return &unmarshalerBytes{unmarshalStackFrame: frame, limit: length, isUUID: isUUID}
	}
}

func (v *unmarshalerBytes) VisitString(val string) error {
	if !v.isUUID {
		return v.VisitBytes([]byte(val))
	}
	uid, err := uuid.Parse(val)
	if err != nil {
		return err
	}
	return v.VisitBytes(uid[:])
}
func (v *unmarshalerBytes) VisitBytes(val []byte) error {
	dest := v.getDest()
	if v.limit != -1 {
		if len(val) > v.limit {
			return fmt.Errorf("out of range: can't set array with length %d to value with length %d", v.limit, len(val))
		}
	} else {
		dest.Set(reflect.MakeSlice(dest.Type().Elem(), len(val), len(val)))
	}
	reflect.Copy(dest, reflect.ValueOf(val))
	// tell the wrapped unmarshalerRepeated that we've got a value
	return v.childPopped(dest)
}
func (v *unmarshalerBytes) VisitUUID(val uuid.UUID) error {
	if !v.isUUID {
		return v.unmarshalStackFrame.VisitBytes(val[:])
	}
	v.getDest().Set(reflect.ValueOf(val))
	return nil
}

type unmarshalerObject struct {
	baseUnmarshalStackFrame
	onActivation unmarshalComposite
}

func (v *unmarshalerObject) reset(dest reflect.Value) {
	v.baseUnmarshalStackFrame.reset(dest)
	v.onActivation.reset(dest)
	if v.dest.Kind() == reflect.Map && !v.dest.CanSet() {
		// Can't set map to nil, so clear it instead
		v.dest.Clear()
	}
	v.dest.Set(reflect.Zero(v.dest.Type()))
}
func (v *unmarshalerObject) tryReset(dest reflect.Value) bool {
	if v.baseUnmarshalStackFrame.tryReset(dest) {
		v.onActivation.reset(dest)
		return true
	}
	return false
}
func (v *unmarshalerObject) childPopped(_ reflect.Value) error {
	return v.stack.pop()
}
func (v *unmarshalerObject) VisitNull() error {
	return v.onActivation.setToNil()
	//if !v.allowNil {
	//	return v.baseUnmarshalStackFrame.VisitNull()
	//}
	//// set to nil
	//if v.dest.CanSet() {
	//	v.dest.Set(reflect.Zero(v.dest.Type()))
	//} else {
	//	return fmt.Errorf("can't set map to nil (provide pointer to map instead)")
	//}
	//return v.stack.pop()
}
func (v *unmarshalerObject) BeginObject(sizeHint int) error {
	if err := v.onActivation.begin(sizeHint); err != nil {
		return err
	}
	//if v.allowNil && v.dest.CanSet() {
	//	// pre-allocate capacity of map
	//	v.dest.Set(reflect.MakeSlice(v.dest.Type().Elem(), 0, sizeHint))
	//}
	v.stack.push(v.onActivation)
	return nil
}

type unmarshalerStruct struct {
	delegatingUnmarshalStackFrame
	fields       map[string]unmarshalFrameFactory
	fieldsNoCase map[string]unmarshalFrameFactory
	currentField string
}

func (u *unmarshalerStruct) setToNil() error {
	//TODO implement me
	panic("implement me")
}

func (u *unmarshalerStruct) begin(sizeHint int) error {
	//TODO implement me
	panic("implement me")
}

func newUnmarshalerStruct(fields []unmarshalFrameFactory) unmarshalFrameFactory {
	return func() unmarshalStackFrame {
		frame := &unmarshalerStruct{fields: fieldIndex, fieldsNoCase: fieldNoCaseIndex}
		frame.next = frame.nextChild
		return &unmarshalerObject{onActivation: frame}
	}
}

type unmarshalerMap struct {
	delegatingUnmarshalStackFrame
	factory unmarshalFrameFactory
}

func newUnmarshalerMap(value unmarshalFrameFactory) unmarshalFrameFactory {
	return func() unmarshalStackFrame {
		frame := &unmarshalerMap{factory: value}
		frame.next = frame.nextChild
		return &unmarshalerObject{onActivation: frame}
	}
}

type unmarshalerPointer struct {
	delegatingUnmarshalStackFrame
	elem unmarshalFrameFactory
}

func newUnmarshalerPointer(elem unmarshalFrameFactory) unmarshalFrameFactory {
	return func() unmarshalStackFrame {
		frame := &unmarshalerPointer{elem: elem}
		frame.next = frame.child
		return frame
	}
}

func (v *unmarshalerPointer) child() (unmarshalStackFrame, error) {
	if v.dest.CanSet() {
		v.dest.Set(reflect.New(v.dest.Type().Elem()))
	}
	frame := v.elem()
	frame.reset(v.dest.Elem())
	return frame, nil
}

func (v *unmarshalerPointer) childPopped(_ reflect.Value) error {
	return v.stack.pop()
}

func (v *unmarshalerPointer) VisitNull() error {
	if !v.dest.CanSet() {
		v.dest.Set(reflect.Zero(v.dest.Type()))
		return v.stack.pop()
	}
	return v.delegatingUnmarshalStackFrame.VisitNull()
}

type unmarshalerInterface struct {
	baseUnmarshalStackFrame
}

func (v *unmarshalerInterface) childPopped(val reflect.Value) error {
	v.dest.Set(val)
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitNull() error {
	v.dest.Set(reflect.Zero(v.dest.Type()))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitBool(val bool) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitInt8(val int8) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitInt16(val int16) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitInt32(val int32) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitInt64(val int64) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitFloat32(val float32) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitFloat64(val float64) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitDecimal4(val Decimal4) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitDecimal8(val Decimal8) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitDecimal16(val Decimal16) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitDate(val Date) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitTime(val Time) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitTimestamp(val Timestamp) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitBytes(val []byte) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitString(val string) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) VisitUUID(val uuid.UUID) error {
	v.dest.Set(reflect.ValueOf(val))
	return v.stack.pop()
}
func (v *unmarshalerInterface) BeginArray(sizeHint int) error {
	val := reflect.New(typeOfAnySlice).Elem()
	frame := newUnmarshalerRepeated(factoryOf[unmarshalerInterface], -1)()
	frame.reset(val)
	v.stack.push(frame)
	return frame.BeginArray(sizeHint)
}
func (v *unmarshalerInterface) BeginObject(sizeHint int) error {
	var val reflect.Value
	if sizeHint >= 0 {
		val = reflect.MakeMapWithSize(typeOfAnyMap, sizeHint)
	} else {
		val = reflect.MakeMap(typeOfAnyMap)
	}
	frame := newUnmarshalerMap(factoryOf[unmarshalerInterface])()
	frame.reset(val)
	v.stack.push(frame)
	return frame.BeginObject(sizeHint)
}

type unmarshalerNil struct {
	baseUnmarshalStackFrame
}

func (v *unmarshalerNil) VisitNull() error {
	v.dest.Set(reflect.Zero(v.dest.Type()))
	return v.stack.pop()
}
