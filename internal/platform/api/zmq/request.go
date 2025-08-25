package zmq

type ApiRequest struct {
	Action string `json:"action,omitempty"`
	Key    string `json:"key,omitempty"`
	Value  string `json:"value,omitempty"`
}

type ApiResponse struct {
	Entry   EntryResponse `json:"entry"`
	Success bool          `json:"success,omitempty"`
}

type EntryResponse struct {
	Key       string `json:"key,omitempty"`
	Value     string `json:"value,omitempty"`
	Tombstone bool   `json:"tombstone,omitempty"`
}
