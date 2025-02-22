package variant

// NewDecoder returns a visitor that can, for example, be used with
// VariantValue.Decode to decode a variant value into the given destination
// value.
func NewDecoder(dest any) Visitor {
	//return &valueBuilder{current: dest}
	return nil
}

type valueBuilder struct {
	groupStack []map[string]any
	current    any
}
