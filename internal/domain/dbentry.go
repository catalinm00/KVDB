package domain

import "context"

type DbEntry struct {
	key   string
	value string
}

func New(key, value string) DbEntry {
	return DbEntry{
		key:   key,
		value: value,
	}
}

func (entry DbEntry) Key() string {
	return entry.key
}

func (entry DbEntry) Value() string {
	return entry.value
}

type DbEntryRepository interface {
	Save(ctx context.Context, entry DbEntry)
	Delete(ctx context.Context, entry DbEntry)
	Get(ctx context.Context, key string)
}
