package variant

import "testing"

func TestEncoder(t *testing.T) {
	enc := &encoder{}
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
	err = enc.BeginObject()
	if err != nil {
		t.Fatal(err)
	}
	err = enc.ObjectField("foo")
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
	err = enc.EndObject()
	if err != nil {
		t.Fatal(err)
	}
	err = enc.EndArray()
	if err != nil {
		t.Fatal(err)
	}
	v := enc.value.Shredded.Interface()
	err = enc.VisitString("abc")
	if err != nil {
		t.Fatal(err)
	}
	_ = v
}
