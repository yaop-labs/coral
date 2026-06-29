// Package jaeger implements Jaeger Thrift compact binary protocol receivers.
// It decodes the Thrift wire format directly.
package jaeger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/yaop-labs/coral/internal/model"
)

// Thrift type codes.
const (
	thriftTypeStop   = 0
	thriftTypeBool   = 2
	thriftTypeByte   = 3
	thriftTypeDouble = 4
	thriftTypeI16    = 6
	thriftTypeI32    = 8
	thriftTypeI64    = 10
	thriftTypeString = 11
	thriftTypeStruct = 12
	thriftTypeMap    = 13
	thriftTypeSet    = 14
	thriftTypeList   = 15
)

// Span field IDs.
const (
	fieldTraceIDLow   = 1
	fieldTraceIDHigh  = 2
	fieldSpanID       = 3
	fieldParentSpanID = 4
	fieldOpName       = 5
	fieldReferences   = 6
	fieldFlags        = 7
	fieldStartTime    = 8
	fieldDuration     = 9
	fieldTags         = 10
	fieldLogs         = 11 // ignored
)

// Tag field IDs.
const (
	tagFieldKey     = 1
	tagFieldVType   = 2
	tagFieldVStr    = 3
	tagFieldVDouble = 4
	tagFieldVBool   = 5
	tagFieldVLong   = 6
	tagFieldVBinary = 7
)

// Process field IDs.
const (
	processFieldServiceName = 1
	processFieldTags        = 2
)

// Batch field IDs.
const (
	batchFieldProcess = 1
	batchFieldSpans   = 2
)

// SpanRef types.
const (
	refTypeChildOf     = 0
	refTypeFollowsFrom = 1
)

// SpanRef field IDs.
const (
	refFieldTraceIDLow  = 1
	refFieldTraceIDHigh = 2
	refFieldSpanID      = 3
	refFieldRefType     = 4
)

// Tag value types.
const (
	tagVTypeString = 0
	tagVTypeBool   = 1
	tagVTypeLong   = 2
	tagVTypeDouble = 3
	tagVTypeBinary = 4
)

// reader is a simple cursor over a byte slice.
type reader struct {
	b []byte
	p int
}

func (r *reader) remaining() int { return len(r.b) - r.p }

func (r *reader) readByte() (byte, error) {
	if r.p >= len(r.b) {
		return 0, errors.New("thrift: unexpected EOF reading byte")
	}
	v := r.b[r.p]
	r.p++
	return v, nil
}

func (r *reader) readI16() (int16, error) {
	if r.p+2 > len(r.b) {
		return 0, errors.New("thrift: unexpected EOF reading i16")
	}
	v := int16(binary.BigEndian.Uint16(r.b[r.p:]))
	r.p += 2
	return v, nil
}

func (r *reader) readI32() (int32, error) {
	if r.p+4 > len(r.b) {
		return 0, errors.New("thrift: unexpected EOF reading i32")
	}
	v := int32(binary.BigEndian.Uint32(r.b[r.p:]))
	r.p += 4
	return v, nil
}

func (r *reader) readI64() (int64, error) {
	if r.p+8 > len(r.b) {
		return 0, errors.New("thrift: unexpected EOF reading i64")
	}
	v := int64(binary.BigEndian.Uint64(r.b[r.p:]))
	r.p += 8
	return v, nil
}

func (r *reader) readDouble() (float64, error) {
	bits, err := r.readI64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(uint64(bits)), nil
}

func (r *reader) readString() (string, error) {
	n, err := r.readI32()
	if err != nil {
		return "", err
	}
	if n < 0 || int(n) > r.remaining() {
		return "", fmt.Errorf("thrift: string length %d out of bounds", n)
	}
	s := string(r.b[r.p : r.p+int(n)])
	r.p += int(n)
	return s, nil
}

func (r *reader) readBytes() ([]byte, error) {
	n, err := r.readI32()
	if err != nil {
		return nil, err
	}
	if n < 0 || int(n) > r.remaining() {
		return nil, fmt.Errorf("thrift: bytes length %d out of bounds", n)
	}
	b := make([]byte, n)
	copy(b, r.b[r.p:r.p+int(n)])
	r.p += int(n)
	return b, nil
}

// skipField skips a Thrift field of the given type.
func (r *reader) skipField(typ byte) error {
	switch typ {
	case thriftTypeBool, thriftTypeByte:
		_, err := r.readByte()
		return err
	case thriftTypeI16:
		_, err := r.readI16()
		return err
	case thriftTypeI32:
		_, err := r.readI32()
		return err
	case thriftTypeI64, thriftTypeDouble:
		_, err := r.readI64()
		return err
	case thriftTypeString:
		_, err := r.readString()
		return err
	case thriftTypeStruct:
		return r.skipStruct()
	case thriftTypeMap:
		return r.skipMap()
	case thriftTypeList, thriftTypeSet:
		return r.skipCollection()
	default:
		return fmt.Errorf("thrift: unknown type %d", typ)
	}
}

func (r *reader) skipStruct() error {
	for {
		typ, err := r.readByte()
		if err != nil {
			return err
		}
		if typ == thriftTypeStop {
			return nil
		}
		if _, err := r.readI16(); err != nil {
			return err
		}
		if err := r.skipField(typ); err != nil {
			return err
		}
	}
}

func (r *reader) skipMap() error {
	keyType, err := r.readByte()
	if err != nil {
		return err
	}
	valType, err := r.readByte()
	if err != nil {
		return err
	}
	n, err := r.readI32()
	if err != nil {
		return err
	}
	for i := int32(0); i < n; i++ {
		if err := r.skipField(keyType); err != nil {
			return err
		}
		if err := r.skipField(valType); err != nil {
			return err
		}
	}
	return nil
}

func (r *reader) skipCollection() error {
	elemType, err := r.readByte()
	if err != nil {
		return err
	}
	n, err := r.readI32()
	if err != nil {
		return err
	}
	for i := int32(0); i < n; i++ {
		if err := r.skipField(elemType); err != nil {
			return err
		}
	}
	return nil
}

// DecodeBatch decodes a Jaeger Thrift Batch from binary-encoded bytes.
// The outer envelope (Thrift message header) is stripped by the transport;
// this function expects raw struct bytes starting from the Batch struct.
func DecodeBatch(data []byte) ([]model.Span, error) {
	r := &reader{b: data}

	// The UDP agent wraps the Batch in a Thrift message header.
	// Message header: version+type (4 bytes), name string, seqid (4 bytes).
	// We peek at the first byte to detect the header.
	if len(data) >= 4 {
		ver := binary.BigEndian.Uint32(data[:4])
		if ver>>16 == 0x8001 { // Thrift binary protocol message header magic
			// skip version+type (4 bytes)
			r.p = 4
			// skip method name string
			if _, err := r.readString(); err != nil {
				return nil, fmt.Errorf("thrift: read method name: %w", err)
			}
			// skip seqid (4 bytes)
			if _, err := r.readI32(); err != nil {
				return nil, fmt.Errorf("thrift: read seqid: %w", err)
			}
		}
	}

	return decodeBatchStruct(r)
}

// decodeBatchStruct parses a Batch Thrift struct.
func decodeBatchStruct(r *reader) ([]model.Span, error) {
	var serviceName string
	var processTags []model.Attribute
	var spans []model.Span

	for {
		typ, err := r.readByte()
		if err != nil {
			return nil, err
		}
		if typ == thriftTypeStop {
			break
		}
		fid, err := r.readI16()
		if err != nil {
			return nil, err
		}

		switch fid {
		case batchFieldProcess:
			if typ != thriftTypeStruct {
				r.skipField(typ)
				continue
			}
			serviceName, processTags, err = decodeProcess(r)
			if err != nil {
				return nil, err
			}
		case batchFieldSpans:
			if typ != thriftTypeList {
				r.skipField(typ)
				continue
			}
			spans, err = decodeSpanList(r)
			if err != nil {
				return nil, err
			}
		default:
			if err := r.skipField(typ); err != nil {
				return nil, err
			}
		}
	}

	// Attach process info to each span's Resource.
	res := model.Resource{}
	if serviceName != "" {
		res.Attrs = append(res.Attrs, model.StringAttr("service.name", serviceName))
	}
	res.Attrs = append(res.Attrs, processTags...)

	for i := range spans {
		spans[i].Resource = res
	}
	return spans, nil
}

func decodeProcess(r *reader) (string, []model.Attribute, error) {
	var serviceName string
	var tags []model.Attribute
	for {
		typ, err := r.readByte()
		if err != nil {
			return "", nil, err
		}
		if typ == thriftTypeStop {
			break
		}
		fid, err := r.readI16()
		if err != nil {
			return "", nil, err
		}
		switch fid {
		case processFieldServiceName:
			serviceName, err = r.readString()
		case processFieldTags:
			tags, err = decodeTagList(r)
		default:
			err = r.skipField(typ)
		}
		if err != nil {
			return "", nil, err
		}
	}
	return serviceName, tags, nil
}

func decodeSpanList(r *reader) ([]model.Span, error) {
	elemType, err := r.readByte()
	if err != nil {
		return nil, err
	}
	n, err := r.readI32()
	if err != nil {
		return nil, err
	}
	if elemType != thriftTypeStruct {
		for i := int32(0); i < n; i++ {
			r.skipField(elemType)
		}
		return nil, nil
	}
	out := make([]model.Span, 0, n)
	for i := int32(0); i < n; i++ {
		s, err := decodeSpan(r)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

func decodeSpan(r *reader) (model.Span, error) {
	var s model.Span
	var traceIDLow, traceIDHigh int64
	var startTimeUS, durationUS int64

	for {
		typ, err := r.readByte()
		if err != nil {
			return s, err
		}
		if typ == thriftTypeStop {
			break
		}
		fid, err := r.readI16()
		if err != nil {
			return s, err
		}

		switch fid {
		case fieldTraceIDLow:
			traceIDLow, err = r.readI64()
		case fieldTraceIDHigh:
			traceIDHigh, err = r.readI64()
		case fieldSpanID:
			var v int64
			v, err = r.readI64()
			binary.BigEndian.PutUint64(s.SpanID[:], uint64(v))
		case fieldParentSpanID:
			var v int64
			v, err = r.readI64()
			if v != 0 {
				binary.BigEndian.PutUint64(s.ParentSpanID[:], uint64(v))
			}
		case fieldOpName:
			s.Name, err = r.readString()
		case fieldFlags:
			var v int32
			v, err = r.readI32()
			if v&1 == 0 { // bit 0 = sampled; 0 = debug
				s.Attrs = append(s.Attrs, model.StringAttr("debug", "true"))
			}
		case fieldStartTime:
			startTimeUS, err = r.readI64()
		case fieldDuration:
			durationUS, err = r.readI64()
		case fieldTags:
			var tags []model.Attribute
			tags, err = decodeTagList(r)
			s.Attrs = append(s.Attrs, tags...)
		case fieldReferences:
			err = r.skipField(typ) // we extract parent from parentSpanID field
		default:
			err = r.skipField(typ)
		}
		if err != nil {
			return s, err
		}
	}

	binary.BigEndian.PutUint64(s.TraceID[:8], uint64(traceIDHigh))
	binary.BigEndian.PutUint64(s.TraceID[8:], uint64(traceIDLow))

	if startTimeUS != 0 {
		s.StartTime = time.UnixMicro(startTimeUS)
		s.EndTime = time.UnixMicro(startTimeUS + durationUS)
	}

	// Infer error status and span kind from tags.
	for _, a := range s.Attrs {
		if a.Key == "error" && (a.Value.String() == "true" || a.Value.String() == "1") {
			s.Status = model.StatusError
		}
		if a.Key == "span.kind" {
			s.Kind = spanKindFromString(a.Value.String())
		}
	}

	return s, nil
}

func spanKindFromString(k string) model.SpanKind {
	switch k {
	case "client":
		return model.KindClient
	case "server":
		return model.KindServer
	case "producer":
		return model.KindProducer
	case "consumer":
		return model.KindConsumer
	case "internal":
		return model.KindInternal
	default:
		return model.KindUnspecified
	}
}

func decodeTagList(r *reader) ([]model.Attribute, error) {
	elemType, err := r.readByte()
	if err != nil {
		return nil, err
	}
	n, err := r.readI32()
	if err != nil {
		return nil, err
	}
	if elemType != thriftTypeStruct {
		for i := int32(0); i < n; i++ {
			r.skipField(elemType)
		}
		return nil, nil
	}
	out := make([]model.Attribute, 0, n)
	for i := int32(0); i < n; i++ {
		a, err := decodeTag(r)
		if err != nil {
			return nil, err
		}
		if a.Key != "" {
			out = append(out, a)
		}
	}
	return out, nil
}

func decodeTag(r *reader) (model.Attribute, error) {
	var key string
	var vtype int32
	var strVal string
	var longVal int64
	var dblVal float64
	var boolVal bool

	for {
		typ, err := r.readByte()
		if err != nil {
			return model.Attribute{}, err
		}
		if typ == thriftTypeStop {
			break
		}
		fid, err := r.readI16()
		if err != nil {
			return model.Attribute{}, err
		}

		switch fid {
		case tagFieldKey:
			key, err = r.readString()
		case tagFieldVType:
			vtype, err = r.readI32()
		case tagFieldVStr:
			strVal, err = r.readString()
		case tagFieldVLong:
			longVal, err = r.readI64()
		case tagFieldVDouble:
			dblVal, err = r.readDouble()
		case tagFieldVBool:
			var b byte
			b, err = r.readByte()
			boolVal = b != 0
		case tagFieldVBinary:
			_, err = r.readBytes() // discard
		default:
			err = r.skipField(typ)
		}
		if err != nil {
			return model.Attribute{}, err
		}
	}

	var value model.AttributeValue
	switch vtype {
	case tagVTypeString:
		value = model.StringValue(strVal)
	case tagVTypeBool:
		value = model.BoolValue(boolVal)
	case tagVTypeLong:
		value = model.IntValue(longVal)
	case tagVTypeDouble:
		value = model.DoubleValue(dblVal)
	case tagVTypeBinary:
		value = model.StringValue("<binary>")
	default:
		value = model.StringValue("")
	}

	return model.Attribute{Key: key, Value: value}, nil
}
