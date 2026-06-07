package jaeger

import (
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/hnlbs/collector/internal/model"
)

// buildBatch encodes a minimal Jaeger Batch in Thrift binary format.
// Structure: Batch struct { process: Process, spans: list<Span> }
func buildBatch(serviceName string, spans []testSpan) []byte {
	var b []byte

	// Field 1: process (struct)
	b = append(b, thriftTypeStruct) // field type
	b = appendI16(b, batchFieldProcess)
	b = appendProcess(b, serviceName)

	// Field 2: spans (list)
	b = append(b, thriftTypeList) // field type
	b = appendI16(b, batchFieldSpans)
	b = appendSpanList(b, spans)

	b = append(b, thriftTypeStop) // end of Batch struct
	return b
}

func appendProcess(b []byte, serviceName string) []byte {
	// Field 1: service_name (string)
	b = append(b, thriftTypeString)
	b = appendI16(b, processFieldServiceName)
	b = appendString(b, serviceName)
	b = append(b, thriftTypeStop)
	return b
}

type testSpan struct {
	traceHigh   int64
	traceLow    int64
	spanID      int64
	parentID    int64
	opName      string
	startTimeUS int64
	durationUS  int64
	tags        []testTag
}

type testTag struct {
	key   string
	vtype int32
	val   string
}

func appendSpanList(b []byte, spans []testSpan) []byte {
	b = append(b, thriftTypeStruct)
	b = appendI32(b, int32(len(spans)))
	for _, s := range spans {
		b = appendSpan(b, s)
	}
	return b
}

func appendSpan(b []byte, s testSpan) []byte {
	b = append(b, thriftTypeI64)
	b = appendI16(b, fieldTraceIDLow)
	b = appendI64(b, s.traceLow)

	b = append(b, thriftTypeI64)
	b = appendI16(b, fieldTraceIDHigh)
	b = appendI64(b, s.traceHigh)

	b = append(b, thriftTypeI64)
	b = appendI16(b, fieldSpanID)
	b = appendI64(b, s.spanID)

	if s.parentID != 0 {
		b = append(b, thriftTypeI64)
		b = appendI16(b, fieldParentSpanID)
		b = appendI64(b, s.parentID)
	}

	b = append(b, thriftTypeString)
	b = appendI16(b, fieldOpName)
	b = appendString(b, s.opName)

	b = append(b, thriftTypeI32)
	b = appendI16(b, fieldFlags)
	b = appendI32(b, 1) // sampled flag

	b = append(b, thriftTypeI64)
	b = appendI16(b, fieldStartTime)
	b = appendI64(b, s.startTimeUS)

	b = append(b, thriftTypeI64)
	b = appendI16(b, fieldDuration)
	b = appendI64(b, s.durationUS)

	if len(s.tags) > 0 {
		b = append(b, thriftTypeList)
		b = appendI16(b, fieldTags)
		b = append(b, thriftTypeStruct)
		b = appendI32(b, int32(len(s.tags)))
		for _, tag := range s.tags {
			b = appendTag(b, tag)
		}
	}

	b = append(b, thriftTypeStop) // end span
	return b
}

func appendTag(b []byte, t testTag) []byte {
	b = append(b, thriftTypeString)
	b = appendI16(b, tagFieldKey)
	b = appendString(b, t.key)

	b = append(b, thriftTypeI32)
	b = appendI16(b, tagFieldVType)
	b = appendI32(b, t.vtype)

	if t.vtype == tagVTypeString {
		b = append(b, thriftTypeString)
		b = appendI16(b, tagFieldVStr)
		b = appendString(b, t.val)
	}

	b = append(b, thriftTypeStop)
	return b
}

func appendI16(b []byte, v int16) []byte {
	return append(b, byte(v>>8), byte(v))
}

func appendI32(b []byte, v int32) []byte {
	return append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendI64(b []byte, v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return append(b, buf...)
}

func appendString(b []byte, s string) []byte {
	b = appendI32(b, int32(len(s)))
	return append(b, s...)
}

func TestDecodeBatch_Basic(t *testing.T) {
	startUS := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	data := buildBatch("payments", []testSpan{
		{
			traceHigh:   0x0102030405060708,
			traceLow:    0x090a0b0c0d0e0f10,
			spanID:      0x1122334455667788,
			opName:      "http.request",
			startTimeUS: startUS,
			durationUS:  50000,
		},
	})

	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	s := spans[0]
	if s.Resource.ServiceName() != "payments" {
		t.Errorf("service = %q", s.Resource.ServiceName())
	}
	if s.Name != "http.request" {
		t.Errorf("name = %q", s.Name)
	}
	if s.Duration() != 50*time.Millisecond {
		t.Errorf("duration = %v, want 50ms", s.Duration())
	}
	if !s.IsRoot() {
		t.Error("expected root span")
	}

	// Verify TraceID encoding.
	want := model.TraceID{
		0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	}
	if s.TraceID != want {
		t.Errorf("TraceID = %v, want %v", s.TraceID, want)
	}
}

func TestDecodeBatch_WithParent(t *testing.T) {
	data := buildBatch("svc", []testSpan{
		{traceLow: 1, spanID: 2, parentID: 1, opName: "child"},
	})
	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if spans[0].IsRoot() {
		t.Error("expected non-root span")
	}
}

func TestDecodeBatch_ErrorTag(t *testing.T) {
	data := buildBatch("svc", []testSpan{
		{
			traceLow: 1, spanID: 1, opName: "op",
			tags: []testTag{{key: "error", vtype: tagVTypeString, val: "true"}},
		},
	})
	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if !spans[0].HasError() {
		t.Error("expected error span")
	}
}

func TestDecodeBatch_MultipleSpans(t *testing.T) {
	data := buildBatch("svc", []testSpan{
		{traceLow: 1, spanID: 1, opName: "a"},
		{traceLow: 1, spanID: 2, opName: "b"},
		{traceLow: 1, spanID: 3, opName: "c"},
	})
	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 3 {
		t.Fatalf("expected 3 spans, got %d", len(spans))
	}
}

func TestDecodeBatch_EmptySpans(t *testing.T) {
	data := buildBatch("svc", nil)
	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 0 {
		t.Errorf("expected 0 spans, got %d", len(spans))
	}
}

func TestDecodeBatch_Truncated(t *testing.T) {
	data := buildBatch("svc", []testSpan{{traceLow: 1, spanID: 1, opName: "x"}})
	// Truncate the data.
	_, err := DecodeBatch(data[:len(data)/2])
	if err == nil {
		t.Error("expected error for truncated data")
	}
}

func TestReader_SkipField_Unknown(t *testing.T) {
	r := &reader{b: []byte{0x01}} // 1 byte for bool
	if err := r.skipField(thriftTypeBool); err != nil {
		t.Errorf("skipField(bool): %v", err)
	}
}

// appendTagBool appends a Thrift tag with a bool value.
func appendTagBool(b []byte, key string, val bool) []byte {
	b = append(b, thriftTypeString)
	b = appendI16(b, tagFieldKey)
	b = appendString(b, key)

	b = append(b, thriftTypeI32)
	b = appendI16(b, tagFieldVType)
	b = appendI32(b, tagVTypeBool)

	b = append(b, thriftTypeBool)
	b = appendI16(b, tagFieldVBool)
	if val {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}

	b = append(b, thriftTypeStop)
	return b
}

// appendTagLong appends a Thrift tag with an int64 value.
func appendTagLong(b []byte, key string, val int64) []byte {
	b = append(b, thriftTypeString)
	b = appendI16(b, tagFieldKey)
	b = appendString(b, key)

	b = append(b, thriftTypeI32)
	b = appendI16(b, tagFieldVType)
	b = appendI32(b, tagVTypeLong)

	b = append(b, thriftTypeI64)
	b = appendI16(b, tagFieldVLong)
	b = appendI64(b, val)

	b = append(b, thriftTypeStop)
	return b
}

// appendTagDouble appends a Thrift tag with a float64 value.
func appendTagDouble(b []byte, key string, val float64) []byte {
	b = append(b, thriftTypeString)
	b = appendI16(b, tagFieldKey)
	b = appendString(b, key)

	b = append(b, thriftTypeI32)
	b = appendI16(b, tagFieldVType)
	b = appendI32(b, tagVTypeDouble)

	b = append(b, thriftTypeDouble)
	b = appendI16(b, tagFieldVDouble)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], math.Float64bits(val))
	b = append(b, buf[:]...)

	b = append(b, thriftTypeStop)
	return b
}

// buildBatchRaw builds a batch where the span byte encoding is provided directly.
func buildBatchRaw(serviceName string, spanBytes []byte, nSpans int) []byte {
	var b []byte

	// Field 1: process (struct)
	b = append(b, thriftTypeStruct)
	b = appendI16(b, batchFieldProcess)
	b = appendProcess(b, serviceName)

	// Field 2: spans (list)
	b = append(b, thriftTypeList)
	b = appendI16(b, batchFieldSpans)
	b = append(b, thriftTypeStruct)
	b = appendI32(b, int32(nSpans))
	b = append(b, spanBytes...)

	b = append(b, thriftTypeStop)
	return b
}

// buildSpanWithCustomTags builds a single span using raw tag bytes.
func buildSpanWithCustomTags(tagBytes []byte, nTags int) []byte {
	s := testSpan{traceLow: 1, spanID: 1, opName: "op"}
	var b []byte

	b = append(b, thriftTypeI64)
	b = appendI16(b, fieldTraceIDLow)
	b = appendI64(b, s.traceLow)

	b = append(b, thriftTypeI64)
	b = appendI16(b, fieldTraceIDHigh)
	b = appendI64(b, s.traceHigh)

	b = append(b, thriftTypeI64)
	b = appendI16(b, fieldSpanID)
	b = appendI64(b, s.spanID)

	b = append(b, thriftTypeString)
	b = appendI16(b, fieldOpName)
	b = appendString(b, s.opName)

	b = append(b, thriftTypeI32)
	b = appendI16(b, fieldFlags)
	b = appendI32(b, 1) // sampled

	if nTags > 0 {
		b = append(b, thriftTypeList)
		b = appendI16(b, fieldTags)
		b = append(b, thriftTypeStruct)
		b = appendI32(b, int32(nTags))
		b = append(b, tagBytes...)
	}

	b = append(b, thriftTypeStop)
	return b
}

func TestDecodeBatch_AllKinds(t *testing.T) {
	cases := []struct {
		kindTag string
		want    model.SpanKind
	}{
		{"client", model.KindClient},
		{"server", model.KindServer},
		{"producer", model.KindProducer},
		{"consumer", model.KindConsumer},
		{"internal", model.KindInternal},
		{"", model.KindUnspecified},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.kindTag, func(t *testing.T) {
			var tags []testTag
			if tc.kindTag != "" {
				tags = []testTag{{key: "span.kind", vtype: tagVTypeString, val: tc.kindTag}}
			}
			data := buildBatch("svc", []testSpan{
				{traceLow: 1, spanID: 1, opName: "op", tags: tags},
			})
			spans, err := DecodeBatch(data)
			if err != nil {
				t.Fatalf("DecodeBatch: %v", err)
			}
			if len(spans) != 1 {
				t.Fatalf("expected 1 span, got %d", len(spans))
			}
			if got := spans[0].Kind; got != tc.want {
				t.Errorf("Kind = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDecodeBatch_DebugTag(t *testing.T) {
	// Build a span with a bool tag key="debug", value=true.
	var tagBytes []byte
	tagBytes = appendTagBool(tagBytes, "debug", true)
	spanBytes := buildSpanWithCustomTags(tagBytes, 1)
	data := buildBatchRaw("svc", spanBytes, 1)

	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if got := spans[0].AttrValue("debug"); got != "true" {
		t.Errorf("debug attr = %q, want %q", got, "true")
	}
}

func TestDecodeBatch_LargeAttributes(t *testing.T) {
	// Build 20 string tags of varying lengths.
	const nTags = 20
	wantAttrs := make(map[string]string, nTags)
	var tags []testTag
	for i := 0; i < nTags; i++ {
		key := fmt.Sprintf("attr.key.%02d", i)
		val := fmt.Sprintf("value-%0*d", i+1, i) // varying lengths
		tags = append(tags, testTag{key: key, vtype: tagVTypeString, val: val})
		wantAttrs[key] = val
	}
	data := buildBatch("svc", []testSpan{
		{traceLow: 1, spanID: 1, opName: "op", tags: tags},
	})
	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	for k, want := range wantAttrs {
		if got := spans[0].AttrValue(k); got != want {
			t.Errorf("attr[%q] = %q, want %q", k, got, want)
		}
	}
}

func TestDecodeBatch_DurationTag(t *testing.T) {
	// Tag key="http.status_code", vType=long (int64), value=200.
	var tagBytes []byte
	tagBytes = appendTagLong(tagBytes, "http.status_code", 200)
	spanBytes := buildSpanWithCustomTags(tagBytes, 1)
	data := buildBatchRaw("svc", spanBytes, 1)

	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if got := spans[0].AttrValue("http.status_code"); got != "200" {
		t.Errorf("http.status_code = %q, want %q", got, "200")
	}
}

func TestDecodeBatch_DoubleTag(t *testing.T) {
	// Tag key="latency", vType=double, value=1.5.
	var tagBytes []byte
	tagBytes = appendTagDouble(tagBytes, "latency", 1.5)
	spanBytes := buildSpanWithCustomTags(tagBytes, 1)
	data := buildBatchRaw("svc", spanBytes, 1)

	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	got := spans[0].AttrValue("latency")
	if got == "" {
		t.Error("latency attr is empty")
	}
	if got != "1.5" {
		t.Errorf("latency = %q, want %q", got, "1.5")
	}
}

func TestDecodeBatch_ZeroedIDs(t *testing.T) {
	data := buildBatch("svc", []testSpan{
		{traceHigh: 0, traceLow: 0, spanID: 0, opName: "zeroed"},
	})
	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if !spans[0].TraceID.IsZero() {
		t.Errorf("TraceID should be zero, got %v", spans[0].TraceID)
	}
	if !spans[0].SpanID.IsZero() {
		t.Errorf("SpanID should be zero, got %v", spans[0].SpanID)
	}
}

func TestDecodeBatch_ServiceNameFromProcess(t *testing.T) {
	data := buildBatch("my-service", []testSpan{
		{traceLow: 1, spanID: 1, opName: "op"},
	})
	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if got := spans[0].Resource.ServiceName(); got != "my-service" {
		t.Errorf("ServiceName = %q, want %q", got, "my-service")
	}
}

func TestDecodeBatch_MessageHeader(t *testing.T) {
	// Build a batch with a Thrift message header prefix.
	// Header: [version+type: 4 bytes][method name: string][seqid: 4 bytes]
	payload := buildBatch("svc", []testSpan{
		{traceLow: 1, spanID: 1, opName: "op"},
	})

	var header []byte
	// Version 0x8001_0001 = binary protocol v1, call type.
	header = append(header, 0x80, 0x01, 0x00, 0x01)
	header = appendString(header, "emitBatch")
	header = appendI32(header, 1) // seqid

	data := append(header, payload...)

	spans, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch with message header: %v", err)
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Name != "op" {
		t.Errorf("Name = %q, want %q", spans[0].Name, "op")
	}
}
