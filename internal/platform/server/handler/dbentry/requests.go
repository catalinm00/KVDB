package dbentry

type SaveEntryRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
