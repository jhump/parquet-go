package variant

import "testing"

func TestEncoder(t *testing.T) {
	enc := &Encoder{}
	err := enc.BeginArray()
	if err != nil {
		t.Fatal(err)
	}
	err = enc.VisitString("abc")
	if err != nil {
		t.Fatal(err)
	}
	err = enc.VisitString("abc")
	if err != nil {
		t.Fatal(err)
	}
	err = enc.BeginGroup()
	if err != nil {
		t.Fatal(err)
	}
	err = enc.GroupField("foo")
	if err != nil {
		t.Fatal(err)
	}
	err = enc.BeginArray()
	if err != nil {
		t.Fatal(err)
	}
	err = enc.EndArray()
	if err != nil {
		t.Fatal(err)
	}
	err = enc.EndGroup()
	if err != nil {
		t.Fatal(err)
	}
	err = enc.EndArray()
	if err != nil {
		t.Fatal(err)
	}
	v := enc.value.Interface()
	err = enc.VisitString("abc")
	if err != nil {
		t.Fatal(err)
	}
	_ = v
}
