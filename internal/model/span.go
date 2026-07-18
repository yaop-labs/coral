package model

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

type TraceID [16]byte
type SpanID [8]byte

func (id TraceID) String() string { return hex.EncodeToString(id[:]) }
func (id SpanID) String() string  { return hex.EncodeToString(id[:]) }
func (id TraceID) IsZero() bool   { return id == TraceID{} }
func (id SpanID) IsZero() bool    { return id == SpanID{} }

type Attribute struct {
	Key   string
	Value AttributeValue
}

type AttributeValueKind uint8

const (
	AttrEmpty AttributeValueKind = iota
	AttrString
	AttrBool
	AttrInt
	AttrDouble
	AttrBytes
	AttrArray
	AttrMap
)

type AttributeValue struct {
	kind        AttributeValueKind
	stringValue string
	boolValue   bool
	intValue    int64
	doubleValue float64
	bytesValue  []byte
	arrayValue  []AttributeValue
	mapValue    []Attribute
}

func StringValue(v string) AttributeValue { return AttributeValue{kind: AttrString, stringValue: v} }
func BoolValue(v bool) AttributeValue     { return AttributeValue{kind: AttrBool, boolValue: v} }
func IntValue(v int64) AttributeValue     { return AttributeValue{kind: AttrInt, intValue: v} }
func DoubleValue(v float64) AttributeValue {
	return AttributeValue{kind: AttrDouble, doubleValue: v}
}
func BytesValue(v []byte) AttributeValue {
	cp := append([]byte(nil), v...)
	return AttributeValue{kind: AttrBytes, bytesValue: cp}
}
func ArrayValue(v []AttributeValue) AttributeValue {
	cp := append([]AttributeValue(nil), v...)
	return AttributeValue{kind: AttrArray, arrayValue: cp}
}
func MapValue(v []Attribute) AttributeValue {
	cp := append([]Attribute(nil), v...)
	return AttributeValue{kind: AttrMap, mapValue: cp}
}

func StringAttr(key, value string) Attribute {
	return Attribute{Key: key, Value: StringValue(value)}
}

func (v AttributeValue) Kind() AttributeValueKind { return v.kind }

func (v AttributeValue) String() string {
	switch v.kind {
	case AttrString:
		return v.stringValue
	case AttrBool:
		return strconv.FormatBool(v.boolValue)
	case AttrInt:
		return strconv.FormatInt(v.intValue, 10)
	case AttrDouble:
		return strconv.FormatFloat(v.doubleValue, 'f', -1, 64)
	case AttrBytes:
		return hex.EncodeToString(v.bytesValue)
	case AttrArray:
		return fmt.Sprint(v.Interface())
	case AttrMap:
		return fmt.Sprint(v.Interface())
	default:
		return ""
	}
}

func (v AttributeValue) Interface() any {
	switch v.kind {
	case AttrString:
		return v.stringValue
	case AttrBool:
		return v.boolValue
	case AttrInt:
		return v.intValue
	case AttrDouble:
		return v.doubleValue
	case AttrBytes:
		return append([]byte(nil), v.bytesValue...)
	case AttrArray:
		out := make([]any, 0, len(v.arrayValue))
		for _, item := range v.arrayValue {
			out = append(out, item.Interface())
		}
		return out
	case AttrMap:
		out := make(map[string]any, len(v.mapValue))
		for _, item := range v.mapValue {
			out[item.Key] = item.Value.Interface()
		}
		return out
	default:
		return nil
	}
}

func (v AttributeValue) SizeBytes() int {
	switch v.kind {
	case AttrString:
		return len(v.stringValue)
	case AttrBool:
		return 1
	case AttrInt, AttrDouble:
		return 8
	case AttrBytes:
		return len(v.bytesValue)
	case AttrArray:
		n := 0
		for _, item := range v.arrayValue {
			n += item.SizeBytes()
		}
		return n
	case AttrMap:
		n := 0
		for _, item := range v.mapValue {
			n += len(item.Key) + item.Value.SizeBytes()
		}
		return n
	default:
		return 0
	}
}

func (v AttributeValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.Interface())
}

type Resource struct {
	Attrs []Attribute
}

func (r Resource) ServiceName() string { return r.AttrValue("service.name") }

func (r Resource) AttrValue(key string) string {
	for _, a := range r.Attrs {
		if a.Key == key {
			return a.Value.String()
		}
	}
	return ""
}

type SpanKind uint8

const (
	KindUnspecified SpanKind = iota
	KindInternal
	KindServer
	KindClient
	KindProducer
	KindConsumer
)

func (k SpanKind) String() string {
	switch k {
	case KindInternal:
		return "internal"
	case KindServer:
		return "server"
	case KindClient:
		return "client"
	case KindProducer:
		return "producer"
	case KindConsumer:
		return "consumer"
	default:
		return "unspecified"
	}
}

type SpanStatus uint8

const (
	StatusUnset SpanStatus = iota
	StatusOK
	StatusError
)

func (s SpanStatus) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusError:
		return "error"
	default:
		return "unset"
	}
}

type Span struct {
	TraceID      TraceID
	SpanID       SpanID
	ParentSpanID SpanID
	Resource     Resource
	Name         string
	Kind         SpanKind
	StartTime    time.Time
	EndTime      time.Time
	Status       SpanStatus
	StatusMsg    string
	Attrs        []Attribute
}

func (s Span) Duration() time.Duration { return s.EndTime.Sub(s.StartTime) }
func (s Span) IsRoot() bool            { return s.ParentSpanID.IsZero() }
func (s Span) HasError() bool          { return s.Status == StatusError }

func (s Span) AttrValue(key string) string {
	for _, a := range s.Attrs {
		if a.Key == key {
			return a.Value.String()
		}
	}
	return ""
}

// SizeBytes returns a rough byte estimate of the span for memory accounting.
func (s Span) SizeBytes() int {
	n := 64 // fixed fields
	n += len(s.Name) + len(s.StatusMsg)
	for _, a := range s.Resource.Attrs {
		n += len(a.Key) + a.Value.SizeBytes()
	}
	for _, a := range s.Attrs {
		n += len(a.Key) + a.Value.SizeBytes()
	}
	return n
}

type Batch struct {
	Spans []Span
}

// SizeBytes returns the bounded in-memory estimate used for queue admission.
func (b Batch) SizeBytes() int {
	n := 0
	for _, s := range b.Spans {
		n += s.SizeBytes()
	}
	return n
}

// Len reports the number of spans, satisfying pipeline.Signal.
func (b Batch) Len() int { return len(b.Spans) }
