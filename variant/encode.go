package variant

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"unsafe"

	"github.com/google/uuid"
)

var (
	// ErrAlreadyVisited may be returned by a visitor that has already been visited.
	// Many visitors are "one shot", meaning they can be visited with a single value.
	// Some one-shot visitors may be re-used only after explicitly resetting them.
	ErrAlreadyVisited = errors.New("invalid state: value already visited")

	errNoValueVisited   = errors.New("invalid state: no value visited")
	errVisitIncomplete  = errors.New("invalid state: still visiting value")
	errMustReset        = fmt.Errorf("%w; call Reset before visiting again", ErrAlreadyVisited)
	errInvalidTime      = errors.New("invalid time")
	errInvalidTimestamp = errors.New("invalid timestamp")

	errCannotCallEndArray       = errors.New("invalid state: EndArray called but not visiting an array")
	errCannotCallEndObject      = errors.New("invalid state: EndObject called but not visiting an object")
	errCannotCallObjectField    = errors.New("invalid state: ObjectField called but not visiting an object")
	errObjectFieldAlreadyCalled = errors.New("invalid state: ObjectField already called")
	errObjectFieldNeverCalled   = errors.New("invalid state: ObjectField never called")
	errNoFieldValueVisited      = errors.New("invalid state: no field value visited")
)

// Encoder is a visitor that encodes the visited value. After the
// variant is visited, the Encode method can be used to extract the
// encoded Value.
//
// An Encoder can only be used to visit a single value and then
// extract the encoded value at the end. In order to visit another
// value, Reset() must be called.
type Encoder interface {
	Visitor
	Encode() (Value, error)
	Reset()
}

// NewEncoder returns a new Encoder that behaves according to the
// given options. If no options are given, it does not sort the
// metadata dictionary and produces an entirely unshredded Value.
func NewEncoder(opts ...EncodeOption) Encoder {
	enc := &encoder{
		metadataKey: make(map[string]int),
	}
	for _, opt := range opts {
		opt.apply(enc)
	}
	if enc.oracle == nil {
		enc.oracle = neverShred{}
	}
	return enc
}

// EncodeOption is an option that customizes how an Encoder
// behaves.
type EncodeOption interface {
	apply(*encoder)
}

type encodeOptionFunc func(*encoder)

func (f encodeOptionFunc) apply(e *encoder) {
	f(e)
}

// WithSortedMetadata is an option that instructs an Encoder to
// produce a Value with sorted metadata.
func WithSortedMetadata() EncodeOption {
	return encodeOptionFunc(func(e *encoder) {
		e.sortMetadata = true
	})
}

// WithShredOracle is an option that instructs an Encoder to
// use the given oracle for deciding which fields to keep in
// shredded form.
func WithShredOracle(oracle ShredOracle) EncodeOption {
	return encodeOptionFunc(func(e *encoder) {
		e.oracle = oracle
	})
}

// ShredOracle is a value that advises whether an encoder should keep
// a variant value in typed/shredded form or encode to "unshredded"
// bytes.
type ShredOracle interface {
	// ShredPrimitive returns true if a value of the given kind should
	// be kept in shredded form.
	ShredPrimitive(Kind) bool
	// ShredArray returns a non-nil value if an array value should be
	// kept in shredded form. The returned value advises whether
	// elements in the array should be shredded, based on their type.
	ShredArray() ShredOracle
	// ShredObject returns a non-nil value if an object value should
	// be kept in shredded form. The returned value is used to
	// determine which fields in the object should be shredded.
	ShredObject() ShredFieldOracle
}

// ShredFieldOracle is a value that advises an encoder as to which
// fields in an object should be kept in typed/shredded form.
type ShredFieldOracle interface {
	// ShredField returns a non-nil value if the given named field
	// should be kept in shredded form. The returned value advises
	// whether the field's value should be shredded, based on its
	// type.
	ShredField(field string) ShredOracle
}

type encoder struct {
	oracle        ShredOracle
	metadataKey   map[string]int
	metadataIndex []string
	stack         []encodeStackEntry
	err           error
	value         Data
	metadata      []byte
	sortMetadata  bool
	encoded       bool
}

func (e *encoder) Encode() (Value, error) {
	if e.encoded {
		return Value{
			Metadata: e.metadata,
			Data:     e.value,
		}, nil
	}

	if e.stack == nil && e.err == nil {
		return Value{}, errNoValueVisited
	}
	if e.err != nil && !errors.Is(e.err, ErrAlreadyVisited) {
		return Value{}, e.err
	}
	if e.err == nil {
		return Value{}, errVisitIncomplete
	}
	if !e.sortMetadata {
		// We've already encoded everything as we wnt.
		// So we just need to compile the metadata.
		e.metadata = compileMetadata(e.metadataIndex, false)
	} else {
		var indexRemap []int
		e.metadata, indexRemap = compileSortedMetadata(e.metadataIndex)
		e.value = encodeData(nil, e.value.Shredded, e.oracle, e.metadataKey, indexRemap)
	}
	e.encoded = true
	return Value{
		Metadata: e.metadata,
		Data:     e.value,
	}, nil
}

func (e *encoder) Reset() {
	clear(e.metadataKey)
	clear(e.metadataIndex)
	e.metadataIndex = e.metadataIndex[:0]
	e.err = nil
	clear(e.stack)
	e.stack = e.stack[:0]
	e.value = Data{}
	e.metadata = nil
	e.encoded = false
}

func (e *encoder) VisitNull() error {
	return e.accept(ShreddedValueOfNull())
}

func (e *encoder) VisitBool(val bool) error {
	return e.accept(ShreddedValueOfBool(val))
}

func (e *encoder) VisitInt8(val int8) error {
	return e.accept(ShreddedValueOfInt8(val))
}

func (e *encoder) VisitInt16(val int16) error {
	return e.accept(ShreddedValueOfInt16(val))
}

func (e *encoder) VisitInt32(val int32) error {
	return e.accept(ShreddedValueOfInt32(val))
}

func (e *encoder) VisitInt64(val int64) error {
	return e.accept(ShreddedValueOfInt64(val))
}

func (e *encoder) VisitFloat32(val float32) error {
	return e.accept(ShreddedValueOfFloat32(val))
}

func (e *encoder) VisitFloat64(val float64) error {
	return e.accept(ShreddedValueOfFloat64(val))
}

func (e *encoder) VisitDecimal4(val Decimal4) error {
	return e.accept(ShreddedValueOfDecimal4(val))
}

func (e *encoder) VisitDecimal8(val Decimal8) error {
	return e.accept(ShreddedValueOfDecimal8(val))
}

func (e *encoder) VisitDecimal16(val Decimal16) error {
	return e.accept(ShreddedValueOfDecimal16(val))
}

func (e *encoder) VisitDate(val Date) error {
	return e.accept(ShreddedValueOfDate(val))
}

func (e *encoder) VisitTime(val Time) error {
	shredded, ok := ShreddedValueOfTime(val)
	if !ok {
		return errInvalidTime
	}
	return e.accept(shredded)
}

func (e *encoder) VisitTimestamp(val Timestamp) error {
	shredded, ok := ShreddedValueOfTimestamp(val)
	if !ok {
		return errInvalidTimestamp
	}
	return e.accept(shredded)
}

func (e *encoder) VisitBytes(val []byte) error {
	return e.accept(ShreddedValueOfBytes(val))
}

func (e *encoder) VisitString(val string) error {
	return e.accept(ShreddedValueOfString(val))
}

func (e *encoder) VisitUUID(val uuid.UUID) error {
	return e.accept(ShreddedValueOfUUID(val))
}

func (e *encoder) BeginArray(sizeHint int) error {
	return e.push(func(entry *encodeStackEntry) {
		entry.isArray = true
		if sizeHint >= 0 {
			entry.cap = uint(sizeHint)
		}
	})
}

func (e *encoder) EndArray() error {
	return e.pop(func(entry *encodeStackEntry) error {
		if !entry.isArray {
			return errCannotCallEndArray
		}
		return nil
	})
}

func (e *encoder) BeginObject(sizeHint int) error {
	return e.push(func(entry *encodeStackEntry) {
		entry.isObject = true
		if sizeHint >= 0 {
			entry.cap = uint(sizeHint)
		}
	})
}

func (e *encoder) ObjectField(name string) error {
	if err := e.checkState(); err != nil {
		return err
	}
	top := len(e.stack) - 1
	entry := &e.stack[top]
	if !entry.isObject {
		return errCannotCallObjectField
	}
	if entry.isSet {
		return errObjectFieldAlreadyCalled
	}
	entry.isSet = true
	entry.pendingField = name
	return nil
}

func (e *encoder) EndObject() error {
	return e.pop(func(entry *encodeStackEntry) error {
		if !entry.isObject {
			return errCannotCallEndObject
		}
		return nil
	})
}

func (e *encoder) checkState() error {
	if e.err != nil {
		return e.err
	}
	if e.stack == nil {
		e.stack = []encodeStackEntry{{encoder: e}}
	}
	return nil
}

func (e *encoder) accept(val Shredded) error {
	if err := e.checkState(); err != nil {
		return err
	}
	top := len(e.stack) - 1
	entry := &e.stack[top]
	done, err := entry.accept(val)
	if err != nil {
		return err
	}
	if !done {
		return nil
	}
	// The value has been accepted. So any errors after this
	// could leave the encoder in a corrupted state. So we must
	// use e.error(...) for any error returned so that the
	// encoder cannot subsequently be used.
	val, err = entry.result()
	if err != nil {
		return e.error(err)
	}
	e.stack = e.stack[:top]
	if top > 0 {
		return e.accept(val)
	}
	// done!
	e.value = e.toData(val)
	// set sentinel error to prevent future visit calls
	e.err = errMustReset
	return nil
}

func (e *encoder) push(init func(*encodeStackEntry)) error {
	if err := e.checkState(); err != nil {
		return err
	}
	e.stack = append(e.stack, encodeStackEntry{encoder: e})
	init(&e.stack[len(e.stack)-1])
	return nil
}

func (e *encoder) pop(check func(*encodeStackEntry) error) error {
	if err := e.checkState(); err != nil {
		return err
	}
	top := len(e.stack) - 1
	if err := check(&e.stack[top]); err != nil {
		return err
	}
	val, err := e.stack[top].result()
	if err != nil {
		return err
	}
	e.stack = e.stack[:top]
	// If we get an error at this some point, something has gone
	// wrong and internal state may be bad. So we need to call
	// e.error(...) for any error returned so that the encoder
	// cannot subsequently be used.
	return e.error(e.accept(val))
}

func (e *encoder) error(err error) error {
	if e.err != nil {
		return e.err
	}
	e.err = err
	return err
}

func (e *encoder) toData(val Shredded) Data {
	if e.sortMetadata {
		// We must defer encoding to bytes until we have all metadata
		// keys, so that we can encode correct metadata indices.
		return Data{Shredded: val}
	}
	return encodeData(nil, val, e.oracle, e.metadataKey, nil)
}

type encodeStackEntry struct {
	val                      Shredded
	ptr                      *byte
	len, cap                 uint
	pendingField             string
	encoder                  *encoder
	isArray, isObject, isSet bool
}

func (e *encodeStackEntry) accept(val Shredded) (bool, error) {
	switch {
	case e.isArray:
		var vals []Data
		if e.ptr == nil {
			if e.cap > 0 {
				// e.cap is a size hint that we'll use to pre-allocate
				vals = make([]Data, 0, e.cap)
			}
		} else {
			vals = unsafe.Slice((*Data)(unsafe.Pointer(e.ptr)), e.cap)[:e.len]
		}
		vals = append(vals, e.encoder.toData(val))
		e.ptr = (*byte)(unsafe.Pointer(&vals[0]))
		e.len = uint(len(vals))
		e.cap = uint(cap(vals))
		return false, nil
	case e.isObject:
		if !e.isSet {
			return false, errObjectFieldNeverCalled
		}
		var vals []FieldData
		if e.ptr == nil {
			if e.cap > 0 {
				// e.cap is a size hint that we'll use to pre-allocate
				vals = make([]FieldData, 0, e.cap)
			}
		} else {
			vals = unsafe.Slice((*FieldData)(unsafe.Pointer(e.ptr)), e.cap)[:e.len]
		}
		vals = append(vals, FieldData{Name: e.pendingField, Data: e.encoder.toData(val)})
		e.ptr = (*byte)(unsafe.Pointer(&vals[0]))
		e.len = uint(len(vals))
		e.cap = uint(cap(vals))
		e.pendingField = ""
		e.isSet = false
		return false, nil
	case e.isSet:
		return false, ErrAlreadyVisited
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
	case e.isObject:
		if e.isSet {
			return Shredded{}, errNoFieldValueVisited
		}
		return Shredded{kind: KindObject, p: e.ptr, v1: uint64(e.len)}, nil
	case e.isSet:
		return e.val, nil
	default:
		return Shredded{}, errNoValueVisited
	}
}

type neverShred struct{}

func (n neverShred) ShredPrimitive(_ Kind) bool {
	return false
}

func (n neverShred) ShredArray() ShredOracle {
	return nil
}

func (n neverShred) ShredObject() ShredFieldOracle {
	return nil
}

func compileMetadata(metadataIndex []string, sorted bool) []byte {
	offsetSize, appendOffset, totalSize := determineMetadataOffsetSize(metadataIndex)
	metadataBytes := make([]byte, 1, 1+(len(metadataIndex)+2)*offsetSize+totalSize)
	var sortedBit byte
	if sorted {
		sortedBit = 0b10000
	}
	// Header
	metadataBytes[0] = 1 | // version
		sortedBit | // sorted flag
		(byte(offsetSize-1) << 6) // offset size in bytes, minus one
	// Dictionary Size
	metadataBytes = appendOffset(metadataBytes, len(metadataIndex))
	// Offsets
	var offset int
	for _, name := range metadataIndex {
		metadataBytes = appendOffset(metadataBytes, offset)
		offset += len(name)
	}
	metadataBytes = appendOffset(metadataBytes, offset)
	// Bytes
	for _, name := range metadataIndex {
		stringAsBytes := unsafe.Slice(unsafe.StringData(name), len(name))
		metadataBytes = append(metadataBytes, stringAsBytes...)
	}
	return metadataBytes
}

func determineMetadataOffsetSize(metadataIndex []string) (offset int, appendOffset func([]byte, int) []byte, totalSize int) {
	for _, name := range metadataIndex {
		totalSize += len(name)
	}
	maxValue := totalSize
	if maxValue < len(metadataIndex) {
		// only possible if the empty string is one of the keys and all others have length==1
		maxValue = len(metadataIndex)
	}
	offsetSize, appendOffset := determineSize(maxValue, "metadata")
	return offsetSize, appendOffset, totalSize
}

func determineSize(maxValue int, what string) (offset int, appendOffset func([]byte, int) []byte) {
	switch {
	case maxValue > math.MaxUint32:
		panic(fmt.Sprintf("%s too large: %d bytes (max is %d)", what, maxValue, math.MaxUint32))
	case maxValue > math.MaxUint16:
		return 4, appendUint32
	case maxValue > math.MaxUint8:
		return 2, appendUint16
	default:
		return 1, appendUint8
	}
}

func appendUint32(data []byte, val int) []byte {
	data = growBy(data, 4)
	binary.LittleEndian.PutUint32(data[len(data):], uint32(val))
	return data
}

func appendUint16(data []byte, val int) []byte {
	data = growBy(data, 2)
	binary.LittleEndian.PutUint16(data[len(data):], uint16(val))
	return data
}

func appendUint8(data []byte, val int) []byte {
	return append(data, uint8(val))
}

func growBy(val []byte, numBytes int) []byte {
	requiredSize := len(val) + numBytes
	if cap(val) >= requiredSize {
		return val[:requiredSize]
	}
	newCap := cap(val) * 2
	if newCap < requiredSize {
		// either starting capacity is too small or we overflowed...
		newCap = requiredSize
	}
	newVal := make([]byte, newCap)[:requiredSize]
	copy(newVal, val)
	return newVal
}

func compileSortedMetadata(metadataIndex []string) ([]byte, []int) {
	mapping := metadataMapping{
		metadataIndex: metadataIndex,
	}
	// Double the capacity, so we have room to compute the inverse w/out allocating another slice.
	mapping.indexRemap = make([]int, len(metadataIndex)*2)
	for i := range len(metadataIndex) {
		mapping.indexRemap[i] = i
	}
	sort.Sort(&mapping)
	indexRemap := mapping.indexRemap[len(metadataIndex):]
	// The remap slice we want to return is the inverse of the one created by above sort step.
	for i := range len(metadataIndex) {
		indexRemap[mapping.indexRemap[i]] = i
	}
	return compileMetadata(metadataIndex, true), indexRemap
}

type metadataMapping struct {
	metadataIndex []string
	indexRemap    []int
}

func (m *metadataMapping) Len() int {
	return len(m.metadataIndex)
}

func (m *metadataMapping) Less(i, j int) bool {
	return m.metadataIndex[i] < m.metadataIndex[j]
}

func (m *metadataMapping) Swap(i, j int) {
	m.metadataIndex[i], m.metadataIndex[j] = m.metadataIndex[j], m.metadataIndex[i]
	m.indexRemap[i], m.indexRemap[j] = m.indexRemap[j], m.indexRemap[i]
}

type basicType uint8

const (
	basicTypePrimitive   basicType = 0
	basicTypeShortString basicType = 1
	basicTypeObject      basicType = 2
	basicTypeArray       basicType = 3
)

func (b basicType) String() string {
	switch b {
	case basicTypePrimitive:
		return "primitive"
	case basicTypeShortString:
		return "short string"
	case basicTypeObject:
		return "object"
	case basicTypeArray:
		return "array"
	default:
		return fmt.Sprintf("unknown basic type(%d)", b)
	}
}

func encodeData(baseData []byte, val Shredded, oracle ShredOracle, metadataKey map[string]int, indexRemap []int) Data {
	switch val.kind {
	case KindObject:
		ptr := (*FieldData)(unsafe.Pointer(val.p))
		fields := unsafe.Slice(ptr, val.v1)
		// This is the most complicated one because we could have a mix of
		// unshredded and shredded values.
		var typedVal Shredded
		if fieldOracle := oracle.ShredObject(); fieldOracle != nil {
			var j int
			var shreddedFields []FieldData
			for i := range fields {
				field := &fields[i]
				if valOracle := fieldOracle.ShredField(field.Name); valOracle != nil {
					if shreddedFields == nil {
						shreddedFields = make([]FieldData, 0, len(fields))
					}
					shreddedFields = append(shreddedFields, FieldData{
						Name: field.Name,
						Data: encodeData(field.Unshredded[:0], field.Shredded, valOracle, metadataKey, indexRemap),
					})
					continue
				}
				if i != j {
					fields[j] = fields[i]
				}
				j++
			}
			typedVal = ShreddedValueOfObject(shreddedFields)
			if j == 0 {
				// All fields were shredded. Nothing else to compute
				return Data{Shredded: typedVal}
			}
			// Replace fields with remaining, unshredded ones and fall through.
			fields = fields[:j]
		}
		return Data{
			Unshredded: encodeObject(baseData, fields, metadataKey, indexRemap),
			Shredded:   typedVal,
		}
	case KindArray:
		ptr := (*Data)(unsafe.Pointer(val.p))
		elems := unsafe.Slice(ptr, val.v1)
		if elemOracle := oracle.ShredArray(); elemOracle != nil {
			for i := range elems {
				elem := &elems[i]
				*elem = encodeData(elem.Unshredded[:0], elem.Shredded, elemOracle, metadataKey, indexRemap)
			}
			// The loop above modifies the elements in place. So we're done.
			return Data{Shredded: val}
		}
		return Data{Unshredded: encodeArray(baseData, elems, metadataKey, indexRemap)}
	default:
		if oracle.ShredPrimitive(val.kind) {
			return Data{Shredded: val}
		}
		str, ok := val.StringValue()
		if ok && len(str) < 64 {
			return Data{Unshredded: encodeSmallString(baseData, str)}
		}
		return Data{Unshredded: encodePrimitive(baseData, val)}
	}
}

func encodePrimitive(baseData []byte, val Shredded) []byte {
	headerByte := byte(basicTypePrimitive) | (byte(val.kind) << 2)
	// TODO: benchmark this switch. There are many cases, so it may be
	// faster to pre-compute a map of kind -> function.
	switch val.kind {
	case KindNull, KindBooleanTrue, KindBooleanFalse:
		return append(baseData, headerByte)
	case KindInt8:
		return append(baseData, headerByte, byte(val.v1))
	case KindInt16:
		baseLen := len(baseData)
		result := growBy(baseData, 3)
		result[baseLen] = headerByte
		binary.LittleEndian.PutUint16(result[baseLen+1:], uint16(val.v1))
		return result
	case KindInt32, KindDate, KindFloat:
		baseLen := len(baseData)
		result := growBy(baseData, 5)
		result[baseLen] = headerByte
		binary.LittleEndian.PutUint32(result[baseLen+1:], uint32(val.v1))
		return result
	case KindInt64, KindDouble, KindTimeNTZ,
		KindTimestampMicros, KindTimestampMicrosNTZ, KindTimestampNanos, KindTimestampNanosNTZ:
		baseLen := len(baseData)
		result := growBy(baseData, 9)
		result[baseLen] = headerByte
		binary.LittleEndian.PutUint64(result[baseLen:], val.v1)
		return result
	case KindDecimal4:
		baseLen := len(baseData)
		result := growBy(baseData, 6)
		result[baseLen] = headerByte
		result[baseLen+1] = val.s
		binary.LittleEndian.PutUint32(result[baseLen+2:], uint32(val.v1))
		return result
	case KindDecimal8:
		baseLen := len(baseData)
		result := growBy(baseData, 10)
		result[baseLen] = headerByte
		result[baseLen+1] = val.s
		binary.LittleEndian.PutUint64(result[baseLen+2:], val.v1)
		return result
	case KindDecimal16:
		baseLen := len(baseData)
		result := growBy(baseData, 18)
		result[baseLen] = headerByte
		result[baseLen+1] = val.s
		binary.LittleEndian.PutUint64(result[baseLen+2:], val.v2)
		binary.LittleEndian.PutUint64(result[baseLen+10:], val.v1)
		return result
	case KindBinary, KindString:
		baseLen := len(baseData)
		result := growBy(baseData, 5+int(val.v1))
		result[baseLen] = headerByte
		binary.LittleEndian.PutUint32(result[baseLen+1:], uint32(val.v1))
		copy(result[baseLen+5:], unsafe.Slice(val.p, val.v1))
		return result
	case KindUUID:
		baseLen := len(baseData)
		result := growBy(baseData, 17)
		result[baseLen] = headerByte
		binary.BigEndian.PutUint64(result[baseLen+1:], val.v1)
		binary.BigEndian.PutUint64(result[baseLen+9:], val.v2)
		return result
	default:
		panic(fmt.Sprintf("unknown primitive kind: %v", val.kind))
	}
}

func encodeSmallString(baseData []byte, str string) []byte {
	headerByte := byte(basicTypeShortString) | (byte(len(str)) << 2)
	result := growBy(baseData, len(str)+1)
	result[0] = headerByte
	copy(result[1:], unsafe.Slice(unsafe.StringData(str), len(str)))
	return result
}

func encodeObject(baseData []byte, fields []FieldData, metadataKey map[string]int, indexRemap []int) []byte {
	isLarge := len(fields) > 255
	// The prefix will be this size *at most*. So we go ahead and allocate this
	// much and can then shift the contents back if the actual prefix turns out
	// to be smaller. We still pay for copying (to shift contents), but we get
	// to skip an additional allocation.
	prefixEstSize := (len(fields)*2+1)*4 + 1 // offsets and header byte
	if isLarge {                             // num-fields byte(s)
		prefixEstSize += 4
	} else {
		prefixEstSize++
	}
	baseLen := len(baseData)
	data := growBy(baseData, prefixEstSize)
	intBuffer := make([]uint32, len(fields)*2+1)
	fieldIDs := intBuffer[:len(fields)]
	var maxFieldID int
	offsets := intBuffer[len(fields):]
	start := len(data)
	for i, field := range fields {
		fieldID := metadataKey[field.Name]
		if indexRemap != nil {
			fieldID = indexRemap[fieldID]
		}
		if fieldID > maxFieldID {
			maxFieldID = fieldID
		}
		fieldIDs[i] = uint32(fieldID)
		offset := len(data) - start
		if int64(offset) > math.MaxUint32 {
			panic(fmt.Sprintf("object values too large: %d or more bytes (max is %d)", offset, math.MaxUint32))
		}
		offsets[i] = uint32(offset)
		data = encodeData(data, field.Shredded, neverShred{}, metadataKey, indexRemap).Unshredded
	}
	dataSize := len(data) - start
	fieldOffsetSize, appendFieldOffset := determineSize(dataSize, "object values")
	offsets[len(offsets)] = uint32(dataSize)
	fieldIDSize, appendFieldID := determineSize(maxFieldID, "object field IDs")

	var appendNumFields func([]byte, int) []byte
	var isLargeBit byte
	if isLarge {
		appendNumFields = appendUint32
		isLargeBit = 0x40
	} else {
		appendNumFields = appendUint8
	}
	headerByte := byte(basicTypeObject) | (byte(fieldOffsetSize-1) << 2) | (byte(fieldIDSize-1) << 2) | isLargeBit
	data[baseLen] = headerByte
	prefix := data[baseLen : baseLen+1]
	appendNumFields(prefix, len(fields))
	for fieldID := range fieldIDs {
		appendFieldID(prefix, fieldID)
	}
	for offset := range offsets {
		appendFieldOffset(prefix, offset)
	}
	if len(prefix) != prefixEstSize {
		// we need to shift contents to make up for the discrepancy
		actualStart := baseLen + len(prefix)
		copy(data[actualStart:], data[start:])
		data = data[:actualStart+dataSize]
	}
	return data
}

func encodeArray(baseData []byte, elems []Data, metadataKey map[string]int, indexRemap []int) []byte {
	// TODO: This function is SO similar to encodeObject... It would be nice if
	// there were a clean and simple way to consolidate the logic but omit the
	// name/field ID handling bits.

	isLarge := len(elems) > 255
	// The prefix will be this size *at most*. So we go ahead and allocate this
	// much and can then shift the contents back if the actual prefix turns out
	// to be smaller. We still pay for copying (to shift contents), but we get
	// to skip an additional allocation.
	prefixEstSize := (len(elems)+1)*4 + 1 // offsets and header byte
	if isLarge {                          // num-elements byte(s)
		prefixEstSize += 4
	} else {
		prefixEstSize++
	}
	baseLen := len(baseData)
	data := growBy(baseData, prefixEstSize)
	offsets := make([]uint32, len(elems)+1)
	start := len(data)
	for i, elem := range elems {
		offset := len(data) - start
		if int64(offset) > math.MaxUint32 {
			panic(fmt.Sprintf("array values too large: %d or more bytes (max is %d)", offset, math.MaxUint32))
		}
		offsets[i] = uint32(offset)
		data = encodeData(data, elem.Shredded, neverShred{}, metadataKey, indexRemap).Unshredded
	}
	dataSize := len(data) - start
	fieldOffsetSize, appendFieldOffset := determineSize(dataSize, "array values")
	offsets[len(offsets)] = uint32(dataSize)

	var appendNumElems func([]byte, int) []byte
	var isLargeBit byte
	if isLarge {
		appendNumElems = appendUint32
		isLargeBit = 0b10000
	} else {
		appendNumElems = appendUint8
	}
	headerByte := byte(basicTypeArray) | (byte(fieldOffsetSize-1) << 2) | isLargeBit
	data[baseLen] = headerByte
	prefix := data[baseLen : baseLen+1]
	appendNumElems(prefix, len(elems))
	for offset := range offsets {
		appendFieldOffset(prefix, offset)
	}
	if len(prefix) != prefixEstSize {
		// we need to shift contents to make up for the discrepancy
		actualStart := baseLen + len(prefix)
		copy(data[actualStart:], data[start:])
		data = data[:actualStart+dataSize]
	}
	return data
}
