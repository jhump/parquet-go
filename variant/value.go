package variant

// Value represents a variant. It contains encoded metadata and data
// and also optionally stores a representation of shredded fields.
type Value struct {
	// Metadata contains field name metadata for all data in this
	// value.
	Metadata []byte

	Data

	// TODO: This type is currently 74 bytes on 64-bit platforms
	//       (with an aligned size of 80!). Maybe methods should
	//       receive pointers instead of copying the whole thing
	//       around on the stack?
}

// Decode decodes the value represented by v and calls the appropriate
// methods of visitor.
func (v Value) Decode(visitor Visitor) error {
	// TODO
	return nil
}

// Data represents encoded variant data. A Value may encode a composite
// that comprises multiple pieces of Data that all share the same
// metadata.
//
// The Shredded field should be considered unset/absent if it represents
// a null value (which is the Shredded zero-value) and Unshredded is
// not empty. The Unshredded field should be considered unset/absent if
// it is empty.
//
// If Unshredded is not empty and Shredded is not a null value, then
// the value is an object that contains both shredded and unshredded
// fields.
type Data struct {
	// Unshredded contains raw "unshredded" encoded data.
	Unshredded []byte

	// Shredded contains strongly-typed data for a subset of
	// fields. The field(s) represented in Shredded will not be
	// present in Unshredded. So to decode and observe all data
	// in the value, both fields must be examined.
	Shredded Shredded
}

// FieldData represents the Data for a single field in an object.
type FieldData struct {
	// Name represents the field name for this element.
	Name string
	Data
}
