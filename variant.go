package parquet

import "github.com/parquet-go/parquet-go/variant"

type variantContext struct {
	node        Node
	firstColumn int
	levels
}

func reconstructVariant(ctx variantContext, dest *variant.Value, vals []Value) error {

}

func deconstructVariant(ctx variantContext, appendTo []Value, val *variant.Value) error {

}
