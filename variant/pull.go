package variant

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"runtime"

	"github.com/google/uuid"
)

var (
	ErrPullerStopped = fmt.Errorf("puller stopped")

	errIterationStopped = fmt.Errorf("iteration stopped")
)

// Pull converts the “push-style” visit operation into a pull-style, accessed
// by the two functions next and stop. This is very similar to how [iter.Pull]
// and [iter.Pull2] transform an [iter.Seq] or [iter.Seq2]. A "push-style"
// iterator is a function with a yield callback. In this case, the "push-style"
// visit is a function that accepts a Visitor. A [Token] returned from the next
// function maps to a particular method call on the visitor.
//
// The next functions returns either the next token or an error. When there
// are no more tokens and the visit operation is complete, [io.EOF] will be
// returned. Once any error is returned, the visit operation is complete, and
// any subsequent calls to the next function will return the same error. If
// the returned stop function is called before the visit operation is complete,
// subsequent calls to next will return ErrPullerStopped.
//
// The caller should arrange for stop to be called when the operation is
// complete. It is okay if stop is called after the visit operation is complete
// and also okay to be called multiple times. So it is advised to defer it
// like so:
//
//	next, stop := visitor.Pull(visitOperation)
//	defer stop()
func Pull(action func(Visitor) error) (next func() (Token, error), stop func()) {
	iterator := func(yield func(Token, error) bool) {
		err := action(&iterVisitor{yield: yield})
		if errors.Is(err, errIterationStopped) {
			return
		}
		if err != nil {
			yield(Token{}, err)
		}
	}
	nextTok, stop := iter.Pull2(iterator)
	dec := &pullStream{
		next: nextTok,
		stop: stop,
	}
	// Just in case caller forgets to call stop(), we'll do it automatically
	// when the underlying pullStream is garbage collected.
	runtime.AddCleanup(dec, func(_ any) { stop() }, nil)
	return dec.nextToken, stop
}

type pullStream struct {
	next func() (Token, error, bool)
	stop func()
	err  error
}

func (p *pullStream) close() {
	p.stop()
	if p.err == nil {
		p.err = ErrPullerStopped
	}
}

func (p *pullStream) nextToken() (Token, error) {
	if p.err != nil {
		return Token{}, p.err
	}
	typ, err, ok := p.next()
	if !ok {
		p.stop()
		p.err = io.EOF
		return Token{}, p.err
	}
	if err != nil {
		p.stop()
		p.err = err
		return Token{}, p.err
	}
	return typ, nil
}

type TokenType uint8

const (
	TokenTypeInvalid = TokenType(iota)
	TokenTypeValue
	TokenTypeBeginArray
	TokenTypeEndArray
	TokenTypeBeginObject
	TokenTypeObjectField
	TokenTypeEndObject
	TokenTypeEOF
)

func (t TokenType) String() string {
	switch t {
	case TokenTypeInvalid:
		return "invalid"
	case TokenTypeValue:
		return "value"
	case TokenTypeBeginArray:
		return "begin-array"
	case TokenTypeEndArray:
		return "end-array"
	case TokenTypeBeginObject:
		return "begin-object"
	case TokenTypeObjectField:
		return "object-field"
	case TokenTypeEndObject:
		return "end-object"
	case TokenTypeEOF:
		return "eof"
	default:
		return fmt.Sprintf("unknown token type(%d)", t)
	}
}

type Token Shredded

func valToToken(val Shredded) Token {
	tok := Token(val)
	tok.kind = ^tok.kind
	return tok
}

func typeToToken(t TokenType) Token {
	return Token{kind: Kind(t)}
}

func (t Token) Type() TokenType {
	if (t.kind & 0x80) != 0 {
		return TokenTypeValue
	}
	return TokenType(t.kind)
}

func (t Token) Value() (Shredded, bool) {
	switch TokenType(t.kind) {
	case TokenTypeObjectField:
		sh := Shredded(t)
		sh.kind = KindString
		return sh, true
	case TokenTypeBeginArray, TokenTypeEndArray:
		sh := Shredded(t)
		sh.kind = KindInt64
		return sh, true
	default:
		if (t.kind & 0x80) != 0 {
			sh := Shredded(t)
			sh.kind = ^sh.kind
			return sh, true
		}
		return Shredded{}, false
	}
}

type iterVisitor struct {
	yield func(Token, error) bool
}

func (v *iterVisitor) VisitNull() error {
	if !v.yield(valToToken(ShreddedValueOfNull()), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitBool(b bool) error {
	if !v.yield(valToToken(ShreddedValueOfBool(b)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitInt8(i int8) error {
	if !v.yield(valToToken(ShreddedValueOfInt8(i)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitInt16(i int16) error {
	if !v.yield(valToToken(ShreddedValueOfInt16(i)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitInt32(i int32) error {
	if !v.yield(valToToken(ShreddedValueOfInt32(i)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitInt64(i int64) error {
	if !v.yield(valToToken(ShreddedValueOfInt64(i)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitFloat32(f float32) error {
	if !v.yield(valToToken(ShreddedValueOfFloat32(f)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitFloat64(f float64) error {
	if !v.yield(valToToken(ShreddedValueOfFloat64(f)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitDecimal4(d Decimal4) error {
	if !v.yield(valToToken(ShreddedValueOfDecimal4(d)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitDecimal8(d Decimal8) error {
	if !v.yield(valToToken(ShreddedValueOfDecimal8(d)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitDecimal16(d Decimal16) error {
	if !v.yield(valToToken(ShreddedValueOfDecimal16(d)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitDate(d Date) error {
	if !v.yield(valToToken(ShreddedValueOfDate(d)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitTime(t Time) error {
	sh, ok := ShreddedValueOfTime(t)
	if !ok {
		return errors.New("encountered invalid time value")
	}
	if !v.yield(valToToken(sh), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitTimestamp(t Timestamp) error {
	sh, ok := ShreddedValueOfTimestamp(t)
	if !ok {
		return errors.New("encountered invalid timestamp value")
	}
	if !v.yield(valToToken(sh), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitBytes(b []byte) error {
	if !v.yield(valToToken(ShreddedValueOfBytes(b)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitString(s string) error {
	if !v.yield(valToToken(ShreddedValueOfString(s)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) VisitUUID(u uuid.UUID) error {
	if !v.yield(valToToken(ShreddedValueOfUUID(u)), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) BeginArray(sizeHint int) error {
	tok := valToToken(ShreddedValueOfInt64(int64(sizeHint)))
	tok.kind = Kind(TokenTypeBeginArray)
	if !v.yield(tok, nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) EndArray() error {
	if !v.yield(typeToToken(TokenTypeEndArray), nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) BeginObject(sizeHint int) error {
	tok := valToToken(ShreddedValueOfInt64(int64(sizeHint)))
	tok.kind = Kind(TokenTypeBeginObject)
	if !v.yield(tok, nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) ObjectField(name string) error {
	tok := valToToken(ShreddedValueOfString(name))
	tok.kind = Kind(TokenTypeObjectField)
	if !v.yield(tok, nil) {
		return errIterationStopped
	}
	return nil
}

func (v *iterVisitor) EndObject() error {
	if !v.yield(typeToToken(TokenTypeEndObject), nil) {
		return errIterationStopped
	}
	return nil
}
