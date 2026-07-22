// Package delivery defines immutable metadata that follows admitted telemetry
// through asynchronous queues, stateful processors, and exporter fan-out.
package delivery

// RecordContribution identifies how many telemetry items in a batch belong to
// one durable journal record. Units let a record survive processor batching and
// tail-sampling splits without being acknowledged after only its first child.
type RecordContribution struct {
	RecordID string
	Attempt  uint64
	Units    int
}

// Metadata is the bounded delivery identity attached to one pipeline batch.
// Tenant is immutable routing data; Records describe durable ownership.
type Metadata struct {
	Tenant  string
	Records []RecordContribution
}

func (m Metadata) Empty() bool { return len(m.Records) == 0 }

// Carrier is implemented by signal batches that participate in durable
// completion. DeliveryMetadata must return an independent snapshot safe for
// concurrent exporter lanes.
type Carrier interface {
	DeliveryMetadata() Metadata
}
