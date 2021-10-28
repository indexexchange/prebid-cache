package backends

// These strings are prefixed onto data put in the backend, to designate its type.
const (
	XML_PREFIX  = "xml"
	JSON_PREFIX = "json"
)

type PutOptions struct {
	Source         string `json:"source,omitempty"`
	WriteTimeoutMs int    `json:"write_timeout_ms,omitempty"`
	WriteRetries   int    `json:"write_retries,omitempty"`
}
