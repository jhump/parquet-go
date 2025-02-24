package variant

func Decode(src Value, visitor Visitor, opts ...DecodeOption) error {

}

type DecodeOption interface {
	apply(*decoder)
}

func WithNoIndexForSortedMetadata() DecodeOption {
}

type decoder struct {
}
