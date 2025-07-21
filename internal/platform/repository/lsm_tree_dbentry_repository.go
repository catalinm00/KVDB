package repository

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/repository/lsm_tree"
)

type LSMTreeRepository struct {
	mt *lsm_tree.Memtable
}

func NewLSMTreeRepository(mt *lsm_tree.Memtable) *LSMTreeRepository {
	return &LSMTreeRepository{
		mt: mt,
	}
}

func (r *LSMTreeRepository) Save(e domain.DbEntry) domain.DbEntry {
	r.mt.Set(e)
	return e
}

func (r *LSMTreeRepository) Get(key string) (domain.DbEntry, bool) {
	return r.mt.Get(key)
}

func (r *LSMTreeRepository) Delete(key string) (*domain.DbEntry, bool) {
	entry, found := r.mt.Get(key)
	if !found {
		return nil, false
	}
	entry.Delete()
	r.Save(entry)
	return &entry, true
}
