package models

type StreamMetadata struct {
	Controllers []string `json:"controllers"`
	Family      *string  `json:"family,omitempty"`
}
