package lsm_tree

import (
	. "KVDB/internal/domain"
	"log"
	"sync"
)

type Memtable struct {
	mu       sync.RWMutex
	skiplist *SkipList
	wal      *WAL
	logger   log.Logger
}

func NewMemtable(wal *WAL) *Memtable {
	return &Memtable{
		skiplist: NewSkipList(5, 5),
		wal:      wal,
	}
}

func (mt *Memtable) Set(entry DbEntry) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	mt.skiplist.Set(entry)
	if err := mt.wal.Write(entry); err != nil {
		mt.logger.Panicf("write wal failed: %v", err)
	}
	//mt.logger.Printf("Memtable set [key: %v] [value: %v] [tombstone: %v]", entry.Key(), string(entry.Value()), entry.Tombstone())
}

func (mt *Memtable) Get(key string) (DbEntry, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.Get(key)
}
