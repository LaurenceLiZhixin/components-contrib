package configuration

// ConfigurationItem represents a configuration item with name, content and other information.
type Item struct {
	Key      string            `json:"key"`
	Value    string            `json:"value,omitempty"`
	Version  string            `json:"version,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// GetRequest is the object describing a get configuration request
type GetRequest struct {
	Keys     []string          `json:"keys"`
	Metadata map[string]string `json:"metadata"`
}

// SubscribeRequest is the object describing a subscribe configuration request
type SubscribeRequest struct {
	Keys     []string          `json:"keys"`
	Metadata map[string]string `json:"metadata"`
}

// UpdateEvent is the object describing a configuration update event
type UpdateEvent struct {
	Items []*Item `json:"items"`
}
