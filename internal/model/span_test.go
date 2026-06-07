package model

import (
	"testing"
	"time"
)

func TestTraceIDString(t *testing.T) {
	id := TraceID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}
	want := "0102030405060708090a0b0c0d0e0f10"
	if got := id.String(); got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestTraceIDIsZero(t *testing.T) {
	if !(TraceID{}).IsZero() {
		t.Error("zero TraceID should be zero")
	}
	if (TraceID{1}).IsZero() {
		t.Error("non-zero TraceID should not be zero")
	}
}

func TestSpanIDIsZero(t *testing.T) {
	if !(SpanID{}).IsZero() {
		t.Error("zero SpanID should be zero")
	}
	if (SpanID{1}).IsZero() {
		t.Error("non-zero SpanID should not be zero")
	}
}

func TestResourceAttrValue(t *testing.T) {
	r := Resource{Attrs: []Attribute{
		StringAttr("service.name", "payments"),
		StringAttr("host.name", "prod-1"),
	}}
	if got := r.ServiceName(); got != "payments" {
		t.Errorf("ServiceName() = %q, want %q", got, "payments")
	}
	if got := r.AttrValue("host.name"); got != "prod-1" {
		t.Errorf("AttrValue(host.name) = %q, want %q", got, "prod-1")
	}
	if got := r.AttrValue("missing"); got != "" {
		t.Errorf("AttrValue(missing) = %q, want empty", got)
	}
}

func TestSpanHelpers(t *testing.T) {
	now := time.Now()
	s := Span{
		TraceID:   TraceID{1},
		SpanID:    SpanID{1},
		StartTime: now,
		EndTime:   now.Add(100 * time.Millisecond),
		Status:    StatusError,
		Attrs:     []Attribute{StringAttr("db.system", "postgres")},
	}

	if d := s.Duration(); d != 100*time.Millisecond {
		t.Errorf("Duration() = %v, want 100ms", d)
	}
	if !s.IsRoot() {
		t.Error("span with zero ParentSpanID should be root")
	}
	if !s.HasError() {
		t.Error("StatusError span should HasError()")
	}
	if got := s.AttrValue("db.system"); got != "postgres" {
		t.Errorf("AttrValue = %q, want %q", got, "postgres")
	}
	if got := s.AttrValue("missing"); got != "" {
		t.Errorf("AttrValue(missing) = %q, want empty", got)
	}
}

func TestSpanIsRoot(t *testing.T) {
	s := Span{ParentSpanID: SpanID{1}}
	if s.IsRoot() {
		t.Error("span with parent should not be root")
	}
}

func TestSpanKindString(t *testing.T) {
	cases := []struct {
		k    SpanKind
		want string
	}{
		{KindUnspecified, "unspecified"},
		{KindInternal, "internal"},
		{KindServer, "server"},
		{KindClient, "client"},
		{KindProducer, "producer"},
		{KindConsumer, "consumer"},
	}
	for _, tc := range cases {
		if got := tc.k.String(); got != tc.want {
			t.Errorf("SpanKind(%d).String() = %q, want %q", tc.k, got, tc.want)
		}
	}
}

func TestSpanStatusString(t *testing.T) {
	cases := []struct {
		s    SpanStatus
		want string
	}{
		{StatusUnset, "unset"},
		{StatusOK, "ok"},
		{StatusError, "error"},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("SpanStatus(%d).String() = %q, want %q", tc.s, got, tc.want)
		}
	}
}

func TestSpanSizeBytes(t *testing.T) {
	s := Span{
		Name:     "test",
		Attrs:    []Attribute{StringAttr("k", "v")},
		Resource: Resource{Attrs: []Attribute{StringAttr("service.name", "svc")}},
	}
	if s.SizeBytes() <= 0 {
		t.Error("SizeBytes should be positive")
	}
}

func TestAttributeValue_TypedValues(t *testing.T) {
	tests := []struct {
		name  string
		value AttributeValue
		kind  AttributeValueKind
		text  string
		raw   any
	}{
		{"string", StringValue("hello"), AttrString, "hello", "hello"},
		{"bool", BoolValue(true), AttrBool, "true", true},
		{"int", IntValue(42), AttrInt, "42", int64(42)},
		{"double", DoubleValue(3.14), AttrDouble, "3.14", 3.14},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.value.Kind() != tt.kind {
				t.Fatalf("kind = %v, want %v", tt.value.Kind(), tt.kind)
			}
			if tt.value.String() != tt.text {
				t.Fatalf("String() = %q, want %q", tt.value.String(), tt.text)
			}
			if tt.value.Interface() != tt.raw {
				t.Fatalf("Interface() = %#v, want %#v", tt.value.Interface(), tt.raw)
			}
		})
	}
}
