package variant

import (
	"errors"
	"unsafe"
)

var (
	errAlreadyVisited   = errors.New("invalid state: value already visited")
	errInvalidTime      = errors.New("invalid time")
	errInvalidTimestamp = errors.New("invalid timestamp")
)

// ShredOracle is a value that advises whether an Encoder should keep
// the variant value in typed/shredded form or encode to "unshredded"
// bytes.
type ShredOracle interface {
	// ShredPrimitive returns true if a value of the given kind should
	// be kept in shredded form.
	ShredPrimitive(Kind) bool
	// ShredArray returns a non-nil value if an array value should be
	// kept in shredded form. The returned value advises whether
	// elements in the array should be shredded, based on their type.
	ShredArray() ShredOracle
	// ShredGroup returns a non-nil value if a group value should be
	// kept in shredded form. The returned value is used to determine
	// which fields in the group should be shredded.
	ShredGroup() ShredFieldOracle
}

// ShredFieldOracle is a value that advises an Encoder as to which
// fields should be kept in typed/shredded form.
type ShredFieldOracle interface {
	// ShredField returns a non-nil value if the given named field
	// should be kept in shredded form. The returned value advises
	// whether the field's value should be shredded, based on its
	// type.
	ShredField(field string) ShredOracle
}

// Encoder is a visitor that encodes the visited value. After the
// variant is visited, the Encode method can be used to extract the
// encoded Value.
type Encoder struct {
	err   error
	stack []encodeStackEntry
	value Shredded
	done  bool
}

var _ Visitor = (*Encoder)(nil)

func (e *Encoder) VisitNull() error {
	return e.accept(ShreddedValueOfNull())
}

func (e *Encoder) VisitBool(val bool) error {
	return e.accept(ShreddedValueOfBool(val))
}

func (e *Encoder) VisitInt8(val int8) error {
	return e.accept(ShreddedValueOfInt8(val))
}

func (e *Encoder) VisitInt16(val int16) error {
	return e.accept(ShreddedValueOfInt16(val))
}

func (e *Encoder) VisitInt32(val int32) error {
	return e.accept(ShreddedValueOfInt32(val))
}

func (e *Encoder) VisitInt64(val int64) error {
	return e.accept(ShreddedValueOfInt64(val))
}

func (e *Encoder) VisitFloat32(val float32) error {
	return e.accept(ShreddedValueOfFloat32(val))
}

func (e *Encoder) VisitFloat64(val float64) error {
	return e.accept(ShreddedValueOfFloat64(val))
}

func (e *Encoder) VisitDecimal4(val Decimal4) error {
	return e.accept(ShreddedValueOfDecimal4(val))
}

func (e *Encoder) VisitDecimal8(val Decimal8) error {
	return e.accept(ShreddedValueOfDecimal8(val))
}

func (e *Encoder) VisitDecimal16(val Decimal16) error {
	return e.accept(ShreddedValueOfDecimal16(val))
}

func (e *Encoder) VisitDate(val Date) error {
	return e.accept(ShreddedValueOfDate(val))
}

func (e *Encoder) VisitTime(val Time) error {
	shredded, ok := ShreddedValueOfTime(val)
	if !ok {
		return errInvalidTime
	}
	return e.accept(shredded)
}

func (e *Encoder) VisitTimestamp(val Timestamp) error {
	shredded, ok := ShreddedValueOfTimestamp(val)
	if !ok {
		return errInvalidTimestamp
	}
	return e.accept(shredded)
}

func (e *Encoder) VisitBytes(val []byte) error {
	return e.accept(ShreddedValueOfBytes(val))
}

func (e *Encoder) VisitString(val string) error {
	return e.accept(ShreddedValueOfString(val))
}

func (e *Encoder) VisitUUID(val UUID) error {
	return e.accept(ShreddedValueOfUUID(val))
}

func (e *Encoder) BeginArray() error {
	return e.push(initArray)
}

func (e *Encoder) EndArray() error {
	return e.pop(mustBeArray)
}

func (e *Encoder) BeginGroup() error {
	return e.push(initGroup)
}

func (e *Encoder) GroupField(name string) error {
	if err := e.checkState(); err != nil {
		return err
	}
	top := len(e.stack) - 1
	entry := &e.stack[top]
	if !entry.isGroup {
		return e.error(errors.New("invalid state: GroupField called but not visiting a group"))
	}
	if entry.isSet {
		return e.error(errors.New("invalid state: GroupField already called"))
	}
	entry.isSet = true
	entry.pendingField = name
	return nil
}

func (e *Encoder) EndGroup() error {
	return e.pop(mustBeGroup)
}

func (e *Encoder) checkState() error {
	if e.err != nil {
		return e.err
	}
	if e.stack == nil {
		e.stack = []encodeStackEntry{{}}
	}
	return nil
}

func (e *Encoder) accept(val Shredded) error {
	if err := e.checkState(); err != nil {
		return err
	}
	top := len(e.stack) - 1
	entry := &e.stack[top]
	done, err := entry.accept(val)
	if err != nil {
		return e.error(err)
	}
	if !done {
		return nil
	}
	val, err = entry.result()
	if err != nil {
		return e.error(err)
	}
	e.stack = e.stack[:top]
	if top > 0 {
		return e.accept(val)
	}
	// done!
	e.value = val
	// set sentinel error to prevent future visit calls
	e.err = errAlreadyVisited
	e.done = true
	return nil
}

func (e *Encoder) push(init func(*encodeStackEntry)) error {
	if err := e.checkState(); err != nil {
		return err
	}
	e.stack = append(e.stack, encodeStackEntry{})
	init(&e.stack[len(e.stack)-1])
	return nil
}

func (e *Encoder) pop(check func(*encodeStackEntry) error) error {
	if err := e.checkState(); err != nil {
		return err
	}
	top := len(e.stack) - 1
	if err := check(&e.stack[top]); err != nil {
		return e.error(err)
	}
	val, err := e.stack[top].result()
	if err != nil {
		return e.error(err)
	}
	e.stack = e.stack[:top]
	return e.accept(val)
}

func (e *Encoder) error(err error) error {
	if e.err != nil {
		return e.err
	}
	e.err = err
	return err
}

func (e *Encoder) Encode(oracle ShredOracle) (Value, error) {
	if e.stack == nil && e.err == nil {
		return Value{}, errors.New("invalid state: no value ever visited")
	}
	if e.err != nil {
		return Value{}, e.err
	}
	if !e.done {
		return Value{}, errors.New("invalid state: still visiting value")
	}
	if oracle == nil {
		oracle = neverShred{}
	}
	//TODO
	return Value{}, nil
}

type encodeStackEntry struct {
	val                     Shredded
	ptr                     *byte
	len, cap                uint
	pendingField            string
	isArray, isGroup, isSet bool
}

func (e *encodeStackEntry) accept(val Shredded) (bool, error) {
	switch {
	case e.isArray:
		vals := unsafe.Slice((*Data)(unsafe.Pointer(e.ptr)), e.cap)[:e.len]
		vals = append(vals, Data{Shredded: val})
		e.ptr = (*byte)(unsafe.Pointer(&vals[0]))
		e.len = uint(len(vals))
		e.cap = uint(cap(vals))
		return false, nil
	case e.isGroup:
		if !e.isSet {
			return false, errors.New("invalid state: GroupField never called")
		}
		vals := unsafe.Slice((*FieldData)(unsafe.Pointer(e.ptr)), e.cap)[:e.len]
		vals = append(vals, FieldData{Name: e.pendingField, Data: Data{Shredded: val}})
		e.ptr = (*byte)(unsafe.Pointer(&vals[0]))
		e.len = uint(len(vals))
		e.cap = uint(cap(vals))
		e.pendingField = ""
		e.isSet = false
		return false, nil
	case e.isSet:
		return false, errors.New("invalid state: already visited value")
	default:
		e.val = val
		e.isSet = true
		return true, nil
	}
}

func (e *encodeStackEntry) result() (Shredded, error) {
	switch {
	case e.isArray:
		return Shredded{kind: KindArray, p: e.ptr, v1: uint64(e.len)}, nil
	case e.isGroup:
		if e.isSet {
			return Shredded{}, errors.New("invalid state: no field value visited")
		}
		return Shredded{kind: KindGroup, p: e.ptr, v1: uint64(e.len)}, nil
	case e.isSet:
		return e.val, nil
	default:
		return Shredded{}, errors.New("invalid state: no value visited")
	}
}

func mustBeArray(entry *encodeStackEntry) error {
	if !entry.isArray {
		return errors.New("invalid state: EndArray called but not visiting an array")
	}
	return nil
}

func initArray(entry *encodeStackEntry) {
	entry.isArray = true
}

func mustBeGroup(entry *encodeStackEntry) error {
	if !entry.isGroup {
		return errors.New("invalid state: EndGroup called but not visiting a group")
	}
	return nil
}

func initGroup(entry *encodeStackEntry) {
	entry.isGroup = true
}

type neverShred struct{}

func (n neverShred) ShredPrimitive(_ Kind) bool {
	return false
}

func (n neverShred) ShredArray() ShredOracle {
	return nil
}

func (n neverShred) ShredGroup() ShredFieldOracle {
	return nil
}
