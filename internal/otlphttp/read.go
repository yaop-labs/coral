// Package otlphttp holds the shared request-handling logic for coral's
// OTLP/HTTP receivers: transparent gzip decoding, Content-Type negotiation
// (protobuf or JSON), and the contract §3/§4 error responses (413/415).
package otlphttp

import (
	"compress/gzip"
	"errors"
	"io"
	"mime"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Encoding is the payload serialization negotiated from Content-Type.
type Encoding int

const (
	EncodingProtobuf Encoding = iota
	EncodingJSON
)

// ReadBody validates an OTLP/HTTP POST, transparently decompresses a gzip body
// (contract §3), and reports the payload encoding. On any problem it writes the
// contract-appropriate status — 405 (method), 415 (Content-Type/Encoding),
// 413 (too large), 400 (corrupt gzip) — and returns ok=false.
func ReadBody(w http.ResponseWriter, r *http.Request, maxBytes int64) (body []byte, enc Encoding, ok bool) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return nil, 0, false
	}

	enc, encOK := parseContentType(r.Header.Get("Content-Type"))
	if !encOK {
		http.Error(w, "unsupported content-type", http.StatusUnsupportedMediaType)
		return nil, 0, false
	}

	var reader io.Reader = http.MaxBytesReader(w, r.Body, maxBytes)
	switch ce := r.Header.Get("Content-Encoding"); ce {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(reader)
		if err != nil {
			http.Error(w, "bad gzip: "+err.Error(), http.StatusBadRequest)
			return nil, 0, false
		}
		defer gz.Close()
		// Read one byte past the limit so an oversized decompressed payload is
		// reported as 413 instead of being silently truncated and later rejected
		// as malformed protobuf/JSON.
		reader = io.LimitReader(gz, maxBytes+1)
	default:
		http.Error(w, "unsupported content-encoding", http.StatusUnsupportedMediaType)
		return nil, 0, false
	}

	body, err := io.ReadAll(reader)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
			return nil, 0, false
		}
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, 0, false
	}
	if int64(len(body)) > maxBytes {
		http.Error(w, "request too large", http.StatusRequestEntityTooLarge)
		return nil, 0, false
	}
	return body, enc, true
}

// Unmarshal decodes body into m using the negotiated encoding.
func Unmarshal(enc Encoding, body []byte, m proto.Message) error {
	if enc == EncodingJSON {
		return protojson.Unmarshal(body, m)
	}
	return proto.Unmarshal(body, m)
}

// parseContentType maps an OTLP Content-Type to an Encoding. An empty type
// defaults to protobuf per the OTLP spec; unknown types are rejected.
func parseContentType(ct string) (Encoding, bool) {
	if ct == "" {
		return EncodingProtobuf, true
	}
	mediaType, _, err := mime.ParseMediaType(ct)
	if err != nil {
		return 0, false
	}
	switch mediaType {
	case "application/x-protobuf", "application/protobuf":
		return EncodingProtobuf, true
	case "application/json":
		return EncodingJSON, true
	default:
		return 0, false
	}
}
