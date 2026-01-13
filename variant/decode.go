package variant

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"math"
	"strings"
	"unsafe"
)

var errMalformedMetadata = errors.New("metadata is metadata")

func decode(src Value, visitor Visitor, options decodeOptions) error {
	if len(src.Metadata) == 0 {
		return fmt.Errorf("%w: metadata cannot be empty", errMalformedMetadata)
	}
	metaHeader := src.Metadata[0]
	if version := metaHeader & 0b1111; version != 1 {
		return fmt.Errorf("metadata version is %d; only version 1 is supported", version)
	}
	metaOffsetSize := int((metaHeader >> 6 & 3) + 1)
	dict, size, err := verifyMetadata(src.Metadata[1:], metaOffsetSize)
	if err != nil {
		return err
	}
	var lookup metadataDict
	if options.noDictionaryIndex {
		lookup = dict
	} else {
		lookup = buildDictIndex(dict, size)
	}
	return decodeData(&src.Data, visitor, lookup)
}

type DecodeOption interface {
	apply(*decodeOptions)
}

type decodeOptionFunc func(*decodeOptions)

func (f decodeOptionFunc) apply(e *decodeOptions) {
	f(e)
}

func WithNoDictionaryIndex() DecodeOption {
	return decodeOptionFunc(func(o *decodeOptions) {
		o.noDictionaryIndex = true
	})
}

type decodeOptions struct {
	noDictionaryIndex bool
}

func verifyMetadata(metadata []byte, offsetSize int) (*metadataDictRaw, int, error) {
	extractInt, err := extractFuncForSize(offsetSize)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %s", errMalformedMetadata, err)
	}
	var decodeOffset func(int, []byte) (int, int, bool)
	switch offsetSize {
	case 1:
		extractInt = func(bytes []byte) int {
			return int(bytes[0])
		}
		decodeOffset = decodeUint8Offset
	case 2:
		extractInt = func(bytes []byte) int {
			return int(binary.LittleEndian.Uint16(bytes))
		}
		decodeOffset = decodeUint16Offset
	case 4:
		extractInt = func(bytes []byte) int {
			return int(binary.LittleEndian.Uint32(bytes))
		}
		decodeOffset = decodeUint32Offset
	default:
		return nil, 0, fmt.Errorf("%w: offset size %d is not supported", errMalformedMetadata, offsetSize)
	}
	if len(metadata) < offsetSize {
		return nil, 0, fmt.Errorf("%w: need %d bytes for dictionary size but only have %d", errMalformedMetadata, offsetSize, len(metadata))
	}
	dictSize := extractInt(metadata)
	offsets := metadata[offsetSize:]
	offsetsLen := offsetSize * (dictSize + 1)
	if len(offsets) < offsetsLen {
		return nil, 0, fmt.Errorf("%w: need %d bytes for offsets but only have %d", errMalformedMetadata, offsetsLen, len(offsets))
	}
	offsets = offsets[:offsetsLen]
	// We make a defensive copy so we can use unsafe.String to
	// return strings that point directly into this slice.
	data := bytes.Clone(offsets[offsetsLen:])
	var prevOffset int
	for i := range dictSize + 1 {
		offset := extractInt(offsets[i*offsetSize:])
		if i == 0 && offset != 0 {
			return nil, 0, fmt.Errorf("%w: first offset must be zero but instead was %d",
				errMalformedMetadata, offset)
		}
		if offset < prevOffset {
			return nil, 0, fmt.Errorf("%w: offset #%d (%d) must not be less than offset #%d (%d)",
				errMalformedMetadata, i+1, offset, i, prevOffset)
		}
		if offset > len(data) {
			return nil, 0, fmt.Errorf("%w: offset #%d (%d) is out of range; data only has %d bytes",
				errMalformedMetadata, i+1, offset, len(data))
		}
	}
	return &metadataDictRaw{
		offsets:      offsets,
		data:         data,
		decodeOffset: decodeOffset,
	}, dictSize, nil
}

func buildDictIndex(dict *metadataDictRaw, size int) metadataDictMap {
	index := make(metadataDictMap, size)
	for i := range size {
		name, ok := dict.lookup(i)
		if !ok {
			panic(fmt.Sprintf("metadata dictionary is corrupt: could not retrieve item #%d (out of %d)", i+1, size))
		}
		index[i] = name
	}
	return index
}

type metadataDict interface {
	lookup(int) (string, bool)
}

type metadataDictMap map[int]string

func (m metadataDictMap) lookup(index int) (string, bool) {
	val, ok := m[index]
	return val, ok
}

type metadataDictRaw struct {
	offsets []byte
	data    []byte

	decodeOffset func(int, []byte) (int, int, bool)
}

func (m *metadataDictRaw) lookup(index int) (string, bool) {
	start, end, ok := m.decodeOffset(index, m.offsets)
	if !ok {
		return "", false
	}
	return unsafe.String(unsafe.SliceData(m.data), end-start), true
}

func extractFuncForSize(size int) (func([]byte) int, error) {
	switch size {
	case 1:
		return func(bytes []byte) int {
			return int(bytes[0])
		}, nil
	case 2:
		return func(bytes []byte) int {
			return int(binary.LittleEndian.Uint16(bytes))
		}, nil
	case 4:
		return func(bytes []byte) int {
			return int(binary.LittleEndian.Uint32(bytes))
		}, nil
	default:
		return nil, fmt.Errorf("offset size %d is not supported", size)
	}
}

func decodeUint32Offset(i int, offsets []byte) (int, int, bool) {
	indexStart := i * 4
	indexMid := indexStart + 4
	indexEnd := indexMid + 4
	if indexEnd > len(offsets) {
		return 0, 0, false
	}
	start := int(binary.LittleEndian.Uint32(offsets[indexStart:indexMid]))
	end := int(binary.LittleEndian.Uint32(offsets[indexMid:indexEnd]))
	return start, end, true
}

func decodeUint16Offset(i int, offsets []byte) (int, int, bool) {
	indexStart := i * 2
	indexMid := indexStart + 2
	indexEnd := indexMid + 2
	if indexEnd > len(offsets) {
		return 0, 0, false
	}
	start := int(binary.LittleEndian.Uint16(offsets[indexStart:indexMid]))
	end := int(binary.LittleEndian.Uint16(offsets[indexMid:indexEnd]))
	return start, end, true
}

func decodeUint8Offset(i int, offsets []byte) (int, int, bool) {
	start := int(offsets[i])
	end := int(offsets[i+1])
	return start, end, true
}

func decodeData(data *Data, visitor Visitor, dict metadataDict) error {
	if len(data.Unshredded) > 0 && data.Shredded != (Shredded{}) {
		// If both are set they must be groups in order to be valid
		fields, isObject := data.Shredded.ObjectValue()
		if !isObject {
			return fmt.Errorf("only object values can contain both shredded and unshredded data; instead found shredded data for %v", data.Shredded.Kind())
		}
		if t := basicType(data.Unshredded[0] & 3); t != basicTypeObject {
			return fmt.Errorf("only object values can contain both shredded and unshredded data; instead found unshredded data for %v", t)
		}
		var decoder unshreddedObjectDecoder
		if err := decoder.init(data.Unshredded); err != nil {
			return err
		}
		if err := visitor.BeginObject(len(fields) + decoder.numElements); err != nil {
			return err
		}
		fieldsSeen := make(map[string]struct{}, len(fields))
		for _, field := range fields {
			if _, ok := fieldsSeen[field.Name]; ok {
				return fmt.Errorf("object contains duplicate field name: %v", field.Name)
			}
			fieldsSeen[field.Name] = struct{}{}
			if err := visitor.ObjectField(field.Name); err != nil {
				return err
			}
			if err := decodeData(&field.Data, visitor, dict); err != nil {
				return prefixErr(field.Name, err)
			}
		}
		if err := decoder.decode(fieldsSeen, visitor, dict); err != nil {
			return err
		}
		return visitor.EndObject()
	}

	if len(data.Unshredded) > 0 {
		return decodeUnshredded(data.Unshredded, visitor, dict)
	}
	// If neither unshredded nor shredded is set, we'll interpret that as a shredded null
	return decodeShredded(data.Shredded, visitor, dict)
}

func decodeUnshredded(data []byte, visitor Visitor, dict metadataDict) error {
	t := basicType(data[0] & 3)
	switch t {
	case basicTypeObject:
		var decoder unshreddedObjectDecoder
		if err := decoder.init(data); err != nil {
			return err
		}
		if err := visitor.BeginObject(decoder.numElements); err != nil {
			return err
		}
		if err := decoder.decode(make(map[string]struct{}), visitor, dict); err != nil {
			return err
		}
		return visitor.EndObject()
	case basicTypeArray:
		var decoder unshreddedArrayDecoder
		if err := decoder.init(data); err != nil {
			return err
		}
		if err := visitor.BeginArray(decoder.numElements); err != nil {
			return err
		}
		if err := decoder.decode(visitor, dict); err != nil {
			return err
		}
		return visitor.EndArray()
	case basicTypeShortString:
		length := int((data[0] >> 2) & 0x3f)
		data = data[1:]
		if len(data) < length {
			return fmt.Errorf("string value indicates length of %d but only %d bytes available", length, len(data))
		}
		// We copy the data, instead of using unsafe.String, because the source
		// is exported Data.Unshredded field, which could be mutated and corrupt
		// a string that was built using unsafe.
		return visitor.VisitString(string(data[:length]))
	case basicTypePrimitive:
		kind := Kind((data[0] >> 2) & 0x3f)
		return decodeUnshreddedPrimitive(data[1:], kind, visitor, dict)
	default:
		return fmt.Errorf("value indicates unknown basic type: %d", t)
	}
}

func decodeShredded(s Shredded, visitor Visitor, dict metadataDict) error {
	switch s.Kind() {
	case KindNull:
		return visitor.VisitNull()
	case KindBooleanTrue:
		return visitor.VisitBool(true)
	case KindBooleanFalse:
		return visitor.VisitBool(false)
	case KindInt8:
		return visitor.VisitInt8(int8(s.v1))
	case KindInt16:
		return visitor.VisitInt16(int16(s.v1))
	case KindInt32:
		return visitor.VisitInt32(int32(s.v1))
	case KindInt64:
		return visitor.VisitInt64(int64(s.v1))
	case KindDouble:
		return visitor.VisitFloat64(math.Float64frombits(s.v1))
	case KindDecimal4:
		return visitor.VisitDecimal4(Decimal4{Value: int32(s.v1), Scale: s.s})
	case KindDecimal8:
		return visitor.VisitDecimal8(Decimal8{Value: int64(s.v1), Scale: s.s})
	case KindDecimal16:
		return visitor.VisitDecimal16(Decimal16{ValueHi: int64(s.v1), ValueLo: s.v2, Scale: s.s})
	case KindDate:
		return visitor.VisitDate(Date(s.v1))
	case KindTimestampMicros:
		return visitor.VisitTimestamp(Timestamp{TimeUnit: Microsecond, AdjustedToUTC: true, Value: int64(s.v1)})
	case KindTimestampMicrosNTZ:
		return visitor.VisitTimestamp(Timestamp{TimeUnit: Microsecond, AdjustedToUTC: false, Value: int64(s.v1)})
	case KindFloat:
		return visitor.VisitFloat32(math.Float32frombits(uint32(s.v1)))
	case KindBinary:
		return visitor.VisitBytes(unsafe.Slice(s.p, s.v1))
	case KindString:
		return visitor.VisitString(unsafe.String(s.p, s.v1))
	case KindTimeNTZ:
		return visitor.VisitTime(Time{TimeUnit: Microsecond, AdjustedToUTC: false, Value: int64(s.v1)})
	case KindTimestampNanos:
		return visitor.VisitTimestamp(Timestamp{TimeUnit: Nanosecond, AdjustedToUTC: true, Value: int64(s.v1)})
	case KindTimestampNanosNTZ:
		return visitor.VisitTimestamp(Timestamp{TimeUnit: Nanosecond, AdjustedToUTC: false, Value: int64(s.v1)})
	case KindUUID:
		var val uuid.UUID
		binary.BigEndian.AppendUint64(val[:], s.v1)
		binary.BigEndian.AppendUint64(val[8:], s.v2)
		return visitor.VisitUUID(val)
	case KindArray:
		ptr := (*Data)(unsafe.Pointer(s.p))
		elems := unsafe.Slice(ptr, s.v1)
		if err := visitor.BeginArray(len(elems)); err != nil {
			return err
		}
		for i := range elems {
			if err := decodeData(&elems[i], visitor, dict); err != nil {
				return prefixErr(fmt.Sprintf("[%d]", i), err)
			}
		}
		return visitor.EndArray()
	case KindObject:
		ptr := (*FieldData)(unsafe.Pointer(s.p))
		fields := unsafe.Slice(ptr, s.v1)
		fieldsSeen := make(map[string]struct{}, len(fields))
		if err := visitor.BeginObject(len(fields)); err != nil {
			return err
		}
		for _, field := range fields {
			if _, ok := fieldsSeen[field.Name]; ok {
				return fmt.Errorf("object contains duplicate field name: %v", field.Name)
			}
			fieldsSeen[field.Name] = struct{}{}
			if err := visitor.ObjectField(field.Name); err != nil {
				return err
			}
			if err := decodeData(&field.Data, visitor, dict); err != nil {
				return prefixErr(field.Name, err)
			}
		}
		return visitor.EndObject()
	default:
		return fmt.Errorf("shredded value has unknown kind: %d", s.Kind())
	}

}

type unshreddedObjectDecoder struct {
	data                                 []byte
	offsetSize, fieldIDSize, numElements int
	extractOffset, extractFieldID        func([]byte) int
}

func (d *unshreddedObjectDecoder) init(data []byte) error {
	d.offsetSize = int((data[0]>>2)&3) + 1
	var err error
	d.extractOffset, err = extractFuncForSize(d.offsetSize)
	if err != nil {
		return err
	}
	d.fieldIDSize = int((data[0]>>4)&3) + 1
	d.extractFieldID, err = extractFuncForSize(d.fieldIDSize)
	if err != nil {
		return err
	}
	var numElementsSize int
	if (data[0] & 0x40) == 0 {
		numElementsSize = 1
	} else {
		numElementsSize = 4
	}
	extractNumElements, err := extractFuncForSize(numElementsSize)
	if err != nil {
		return err
	}
	data = data[1:]
	if len(data) < numElementsSize {
		return fmt.Errorf("need %d bytes to read the number of elements but only %d bytes available",
			numElementsSize, len(data))
	}
	d.numElements = extractNumElements(data)
	d.data = data[numElementsSize:]
	return nil
}

func (d *unshreddedObjectDecoder) decode(fieldsSeen map[string]struct{}, visitor Visitor, dict metadataDict) error {
	data := d.data
	offsetSize, fieldIDSize, numElements := d.offsetSize, d.fieldIDSize, d.numElements
	extractOffset, extractFieldID := d.extractOffset, d.extractFieldID

	fieldIDsLength := numElements * fieldIDSize
	offsetsLength := (numElements + 1) * offsetSize
	totalLength := fieldIDsLength + offsetsLength
	if len(data) < totalLength {
		return fmt.Errorf("field IDs and offsets for %d fields need %d bytes but only %d bytes available",
			numElements, totalLength, len(data))
	}
	fieldIDs := data[:fieldIDsLength]
	offsets := data[fieldIDsLength:totalLength]
	end := extractOffset(offsets[numElements*offsetSize:])
	data = data[totalLength:]
	if len(data) < end {
		return fmt.Errorf("field offsets indicate data has %d bytes but only %d bytes available",
			end, len(data))
	}
	data = data[:end]
	for i := range numElements {
		fieldID := extractFieldID(fieldIDs[i*fieldIDSize:])
		fieldName, ok := dict.lookup(fieldID)
		if !ok {
			return fmt.Errorf("field #%d indicates unknown field ID: %d", i+1, fieldID)
		}
		if _, ok := fieldsSeen[fieldName]; ok {
			return fmt.Errorf("object contains duplicate field name: %v", fieldName)
		}
		fieldsSeen[fieldName] = struct{}{}
		if err := visitor.ObjectField(fieldName); err != nil {
			return err
		}
		offset := extractOffset(offsets[i*offsetSize:])
		if err := decodeUnshredded(data[offset:], visitor, dict); err != nil {
			return prefixErr(fieldName, err)
		}
	}
	return nil
}

type unshreddedArrayDecoder struct {
	data                    []byte
	offsetSize, numElements int
	extractOffset           func([]byte) int
}

func (d *unshreddedArrayDecoder) init(data []byte) error {
	d.offsetSize = int((data[0]>>2)&3) + 1
	var err error
	d.extractOffset, err = extractFuncForSize(d.offsetSize)
	if err != nil {
		return err
	}
	var numElementsSize int
	if (data[0] & 0x10) == 0 {
		numElementsSize = 1
	} else {
		numElementsSize = 4
	}
	extractNumElements, err := extractFuncForSize(numElementsSize)
	if err != nil {
		return err
	}
	data = data[1:]
	if len(data) < numElementsSize {
		return fmt.Errorf("need %d bytes to read the number of elements but only %d bytes available",
			numElementsSize, len(data))
	}
	d.numElements = extractNumElements(data)
	d.data = data[numElementsSize:]
	return nil
}

func (d *unshreddedArrayDecoder) decode(visitor Visitor, dict metadataDict) error {
	data := d.data
	offsetSize, numElements := d.offsetSize, d.numElements
	extractOffset := d.extractOffset

	length := (numElements + 1) * offsetSize
	if len(data) < length {
		return fmt.Errorf("offsets for %d elements need %d bytes but only %d bytes available",
			numElements, length, len(data))
	}
	offsets := data[:length]
	end := extractOffset(offsets[numElements*offsetSize:])
	data = data[length:]
	if len(data) < end {
		return fmt.Errorf("array offsets indicate data has %d bytes but only %d bytes available",
			end, len(data))
	}
	data = data[:end]
	for i := range numElements {
		offset := extractOffset(offsets[i*offsetSize:])
		if err := decodeUnshredded(data[offset:], visitor, dict); err != nil {
			return prefixErr(fmt.Sprintf("[%d]", i), err)
		}
	}
	return nil
}

func decodeUnshreddedPrimitive(data []byte, kind Kind, visitor Visitor, dict metadataDict) error {
	switch kind {
	case KindNull:
		return visitor.VisitNull()
	case KindBooleanTrue:
		return visitor.VisitBool(true)
	case KindBooleanFalse:
		return visitor.VisitBool(false)
	case KindInt8:
		if len(data) < 1 {
			return fmt.Errorf("int8 value must be 1 byte but only %d bytes available", len(data))
		}
		return visitor.VisitInt8(int8(data[0]))
	case KindInt16:
		if len(data) < 2 {
			return fmt.Errorf("int16 value must be 2 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitInt16(int16(binary.LittleEndian.Uint16(data)))
	case KindInt32:
		if len(data) < 4 {
			return fmt.Errorf("int32 value must be 4 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitInt32(int32(binary.LittleEndian.Uint32(data)))
	case KindInt64:
		if len(data) < 8 {
			return fmt.Errorf("int64 value must be 8 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitInt64(int64(binary.LittleEndian.Uint64(data)))
	case KindFloat:
		if len(data) < 4 {
			return fmt.Errorf("float value must be 4 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitFloat32(math.Float32frombits(binary.LittleEndian.Uint32(data)))
	case KindDouble:
		if len(data) < 8 {
			return fmt.Errorf("double value must be 8 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitFloat64(math.Float64frombits(binary.LittleEndian.Uint64(data)))
	case KindDecimal4:
		if len(data) < 5 {
			return fmt.Errorf("decimal4 value must be 5 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitDecimal4(Decimal4{
			Scale: data[0],
			Value: int32(binary.LittleEndian.Uint32(data[1:])),
		})
	case KindDecimal8:
		if len(data) < 9 {
			return fmt.Errorf("decimal8 value must be 9 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitDecimal8(Decimal8{
			Scale: data[0],
			Value: int64(binary.LittleEndian.Uint64(data[1:])),
		})
	case KindDecimal16:
		if len(data) < 17 {
			return fmt.Errorf("decimal16 value must be 17 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitDecimal16(Decimal16{
			Scale:   data[0],
			ValueLo: binary.LittleEndian.Uint64(data[1:]),
			ValueHi: int64(binary.LittleEndian.Uint64(data[9:])),
		})
	case KindDate:
		if len(data) < 4 {
			return fmt.Errorf("date value must be 4 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitDate(Date(binary.LittleEndian.Uint32(data)))
	case KindTimeNTZ:
		if len(data) < 8 {
			return fmt.Errorf("time-ntz value must be 8 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitTime(Time{
			Value:         int64(binary.LittleEndian.Uint64(data)),
			TimeUnit:      Microsecond,
			AdjustedToUTC: false,
		})
	case KindTimestampMicros:
		if len(data) < 8 {
			return fmt.Errorf("timestamp-micros value must be 8 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitTimestamp(Timestamp{
			Value:         int64(binary.LittleEndian.Uint64(data)),
			TimeUnit:      Microsecond,
			AdjustedToUTC: true,
		})
	case KindTimestampMicrosNTZ:
		if len(data) < 8 {
			return fmt.Errorf("timestamp-micros-ntz value must be 8 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitTimestamp(Timestamp{
			Value:         int64(binary.LittleEndian.Uint64(data)),
			TimeUnit:      Microsecond,
			AdjustedToUTC: false,
		})
	case KindTimestampNanos:
		if len(data) < 8 {
			return fmt.Errorf("timestamp-nanos value must be 8 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitTimestamp(Timestamp{
			Value:         int64(binary.LittleEndian.Uint64(data)),
			TimeUnit:      Nanosecond,
			AdjustedToUTC: true,
		})
	case KindTimestampNanosNTZ:
		if len(data) < 8 {
			return fmt.Errorf("timestamp-nanos-ntz value must be 8 bytes but only %d bytes available", len(data))
		}
		return visitor.VisitTimestamp(Timestamp{
			Value:         int64(binary.LittleEndian.Uint64(data)),
			TimeUnit:      Nanosecond,
			AdjustedToUTC: false,
		})
	case KindBinary:
		byteVals, err := readBytes(data)
		if err != nil {
			return err
		}
		return visitor.VisitBytes(byteVals)
	case KindString:
		byteVals, err := readBytes(data)
		if err != nil {
			return err
		}
		// We copy the data, instead of using unsafe.String, because the source
		// is exported Data.Unshredded field, which could be mutated and corrupt
		// a string that was built using unsafe.
		return visitor.VisitString(string(byteVals))
	case KindUUID:
		if len(data) < 16 {
			return fmt.Errorf("uuid value must be 16 bytes but only %d bytes available", len(data))
		}
		var id uuid.UUID
		copy(id[:], data)
		return visitor.VisitUUID(id)
	default:
		return fmt.Errorf("unknown primitive type ID: %d", kind)
	}
}

func prefixErr(prefix string, err error) error {
	var withPrefix *errorWithContextPrefix
	if errors.As(err, &withPrefix) {
		withPrefix.prefix = append(withPrefix.prefix, prefix)
		return withPrefix
	}
	return &errorWithContextPrefix{prefix: []string{prefix}, err: err}
}

type errorWithContextPrefix struct {
	prefix []string
	err    error
}

func (e *errorWithContextPrefix) Error() string {
	var buf strings.Builder
	for i := range e.prefix {
		elem := e.prefix[len(e.prefix)-1-i] // print prefixes in reverse order they were added
		if i > 0 && elem[0] != '[' {
			buf.WriteByte('.')
		}
		buf.WriteString(elem)
	}
	buf.WriteString(": ")
	buf.WriteString(e.err.Error())
	return buf.String()
}

func readBytes(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("binary value must be at least 4 bytes but only %d bytes available", len(data))
	}
	length := int(binary.BigEndian.Uint32(data))
	data = data[4:]
	if len(data) < length {
		return nil, fmt.Errorf("binary value indicates length of %d but only %d bytes available", length, len(data))
	}
	return data[:length], nil
}
