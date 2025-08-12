package service

import (
	"KVDB/internal/domain"
)

type GetEntryService struct {
	repository domain.DbEntryRepository
}

func NewGetEntryService(repository domain.DbEntryRepository) *GetEntryService {
	return &GetEntryService{
		repository: repository,
	}
}

type GetEntryQuery struct {
	Key string
}

type GetEntryResult struct {
	Entry domain.DbEntry
	Found bool
}

func (s *GetEntryService) Execute(query GetEntryQuery) GetEntryResult {
	entry, found := s.repository.Get(query.Key)
	if !found {
		return GetEntryResult{Found: false}
	}
	if entry.Tombstone() {
		return GetEntryResult{Found: false}
	}
	return GetEntryResult{
		Entry: entry,
		Found: true,
	}
}
