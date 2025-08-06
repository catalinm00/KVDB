package domain

type DbEntry struct {
	key       string `json:"key,omitempty"`
	value     string `json:"value,omitempty"`
	tombstone bool   `json:"tombstone,omitempty"`
}

func NewDbEntry(key, value string, tombstone bool) DbEntry {
	return DbEntry{
		key:       key,
		value:     value,
		tombstone: tombstone,
	}
}

func (entry *DbEntry) Copy() DbEntry {
	return DbEntry{
		key:       entry.key,
		value:     entry.value,
		tombstone: entry.tombstone,
	}
}

func (entry *DbEntry) Key() string {
	return entry.key
}

func (entry *DbEntry) Value() string {
	return entry.value
}

func (entry *DbEntry) Tombstone() bool {
	return entry.tombstone
}

func (entry *DbEntry) Delete() {
	entry.tombstone = true
}

type DbEntryRepository interface {
	Save(entry DbEntry) DbEntry
	Delete(key string) (*DbEntry, bool)
	Get(key string) (DbEntry, bool)
}
