package variant

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"math"
	"strconv"
)

func DecodeJSON(r io.Reader, visitor Visitor) error {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if err := decodeJSON(dec, tok, visitor); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("invalid JSON: more than one JSON element")
		}
		return err
	}
	return nil
}

func DecodeJSONToValue(r io.Reader, opts ...EncodeOption) (Value, error) {
	enc := NewEncoder(opts...)
	if err := DecodeJSON(r, enc); err != nil {
		return Value{}, err
	}
	return enc.Encode()
}

func EncodeValueToJSON(w io.Writer, v Value, opts ...DecodeOption) error {
	enc := NewJSONEncoder(w)
	return Decode(v, enc, opts...)
}

func NewJSONEncoder(w io.Writer) Visitor {
	return &jsonEncoder{w: w, state: []jsonEncodeFrame{}}
}

func decodeJSON(dec *json.Decoder, tok json.Token, visitor Visitor) error {
	switch tok := tok.(type) {
	case bool:
		return visitor.VisitBool(tok)
	case string:
		return visitor.VisitString(tok)
	case json.Number:
		if intVal, err := tok.Int64(); err == nil {
			return visitor.VisitInt64(intVal)
		}
		fltVal, err := tok.Float64()
		if err == nil {
			return visitor.VisitFloat64(fltVal)
		}
		return fmt.Errorf("invalid JSON number %q: %w", tok, err)
	case nil:
		return visitor.VisitNull()
	case json.Delim:
		switch tok {
		case json.Delim('['):
			return decodeJSONArray(dec, visitor)
		case json.Delim('{'):
			return decodeJSONObject(dec, visitor)
		default:
			return fmt.Errorf("unexpected delimiter: %q", tok)
		}
	default:
		return fmt.Errorf("unexpected token type: %T", tok)
	}
}

func decodeJSONArray(dec *json.Decoder, visitor Visitor) error {
	if err := visitor.BeginArray(); err != nil {
		return err
	}
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if tok == json.Delim(']') {
			return visitor.EndArray()
		}
		if err := decodeJSON(dec, tok, visitor); err != nil {
			return err
		}
	}
}

func decodeJSONObject(dec *json.Decoder, visitor Visitor) error {
	if err := visitor.BeginObject(); err != nil {
		return err
	}
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		if tok == json.Delim('}') {
			return visitor.EndObject()
		}
		fieldName, ok := tok.(string)
		if !ok {
			return fmt.Errorf("expecting field name; instead got token type %T", tok)
		}
		if err := visitor.ObjectField(fieldName); err != nil {
			return err
		}
		tok, err = dec.Token()
		if err != nil {
			return err
		}
		if err := decodeJSON(dec, tok, visitor); err != nil {
			return err
		}
	}
}

type jsonEncoder struct {
	w     io.Writer
	state []jsonEncodeFrame
}

func (j *jsonEncoder) VisitNull() error {
	if err := j.checkState(); err != nil {
		return err
	}
	_, err := j.w.Write([]byte{'n', 'u', 'l', 'l'})
	return err
}

func (j *jsonEncoder) VisitBool(b bool) error {
	if err := j.checkState(); err != nil {
		return err
	}
	var err error
	if b {
		_, err = j.w.Write([]byte{'t', 'r', 'u', 'e'})
	} else {
		_, err = j.w.Write([]byte{'f', 'a', 'l', 's', 'e'})
	}
	return err
}

func (j *jsonEncoder) VisitInt8(i int8) error {
	return j.VisitInt64(int64(i))
}

func (j *jsonEncoder) VisitInt16(i int16) error {
	return j.VisitInt64(int64(i))
}

func (j *jsonEncoder) VisitInt32(i int32) error {
	return j.VisitInt64(int64(i))
}

func (j *jsonEncoder) VisitInt64(i int64) error {
	if err := j.checkState(); err != nil {
		return err
	}
	_, err := j.w.Write(([]byte)(strconv.FormatInt(i, 10)))
	return err
}

func (j *jsonEncoder) VisitFloat32(f float32) error {
	if err := j.checkState(); err != nil {
		return err
	}
	return j.writeFloat(float64(f), 32)
}

func (j *jsonEncoder) VisitFloat64(f float64) error {
	if err := j.checkState(); err != nil {
		return err
	}
	return j.writeFloat(f, 64)
}

func (j *jsonEncoder) writeFloat(f float64, bits int) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fmt.Errorf("illegal value: %v; NaN and Inf values cannot be represented in JSON", f)
	}
	_, err := j.w.Write(([]byte)(strconv.FormatFloat(f, 'g', -1, bits)))
	return err
}

func (j *jsonEncoder) VisitDecimal4(decimal4 Decimal4) error {
	if err := j.checkState(); err != nil {
		return err
	}
	//TODO implement me
	panic("implement me")
}

func (j *jsonEncoder) VisitDecimal8(decimal8 Decimal8) error {
	if err := j.checkState(); err != nil {
		return err
	}
	//TODO implement me
	panic("implement me")
}

func (j *jsonEncoder) VisitDecimal16(decimal16 Decimal16) error {
	if err := j.checkState(); err != nil {
		return err
	}
	//TODO implement me
	panic("implement me")
}

func (j *jsonEncoder) VisitDate(date Date) error {
	if err := j.checkState(); err != nil {
		return err
	}
	//TODO implement me
	panic("implement me")
}

func (j *jsonEncoder) VisitTime(time Time) error {
	if err := j.checkState(); err != nil {
		return err
	}
	//TODO implement me
	panic("implement me")
}

func (j *jsonEncoder) VisitTimestamp(timestamp Timestamp) error {
	if err := j.checkState(); err != nil {
		return err
	}
	//TODO implement me
	panic("implement me")
}

func (j *jsonEncoder) VisitBytes(bytes []byte) error {
	if err := j.checkState(); err != nil {
		return err
	}
	return j.writeString(base64.StdEncoding.EncodeToString(bytes))
}

func (j *jsonEncoder) VisitString(s string) error {
	if err := j.checkState(); err != nil {
		return err
	}
	return j.writeString(s)
}

func (j *jsonEncoder) VisitUUID(uuid uuid.UUID) error {
	if err := j.checkState(); err != nil {
		return err
	}
	return j.writeString(uuid.String())
}

func (j *jsonEncoder) BeginArray() error {
	if err := j.push(json.Delim('[')); err != nil {
		return err
	}
	_, err := j.w.Write([]byte{'['})
	return err
}

func (j *jsonEncoder) EndArray() error {
	if err := j.pop(json.Delim('['), "EndArray"); err != nil {
		return err
	}
	_, err := j.w.Write([]byte{']'})
	return err
}

func (j *jsonEncoder) BeginObject() error {
	if err := j.push(json.Delim('{')); err != nil {
		return err
	}
	_, err := j.w.Write([]byte{'{'})
	return err
}

func (j *jsonEncoder) ObjectField(name string) error {
	if err := j.checkState(); err != nil {
		return err
	}
	if len(j.state) == 0 || j.state[len(j.state)-1].start != '{' {
		return errors.New("call to ObjectField without prior call to BeginObject")
	}
	if err := j.writeString(name); err != nil {
		return err
	}
	_, err := j.w.Write([]byte{':'})
	return err
}

func (j *jsonEncoder) EndObject() error {
	if err := j.pop(json.Delim('{'), "EndObject"); err != nil {
		return err
	}
	_, err := j.w.Write([]byte{'}'})
	return err
}

func (j *jsonEncoder) checkState() error {
	if len(j.state) == 0 {
		return errors.New("already visited value")
	}
	top := j.state[len(j.state)-1]
	if top.needComma {
		if _, err := j.w.Write([]byte{','}); err != nil {
			return err
		}
	} else {
		top.needComma = true
	}
	if top.start == 0 {
		j.state = j.state[:len(j.state)-1]
	}
	return nil
}

func (j *jsonEncoder) writeString(s string) error {
	strBytes, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = j.w.Write(strBytes)
	return err
}

func (j *jsonEncoder) push(start json.Delim) error {
	if err := j.checkState(); err != nil {
		return err
	}
	if len(j.state) == 1 {
		j.state[0].start = start
		return nil
	}
	j.state = append(j.state, jsonEncodeFrame{start: start})
	return nil
}

func (j *jsonEncoder) pop(expectStart json.Delim, name string) error {
	if err := j.checkState(); err != nil {
		return err
	}
	if len(j.state) == 0 || j.state[len(j.state)-1].start != expectStart {
		return fmt.Errorf("unmatched call to %s", name)
	}
	j.state = j.state[:len(j.state)-1]
	return nil
}

type jsonEncodeFrame struct {
	start     json.Delim
	needComma bool
}
