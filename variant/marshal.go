package variant

func Marshal(src any, visitor Visitor) error {
	// TODO
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
