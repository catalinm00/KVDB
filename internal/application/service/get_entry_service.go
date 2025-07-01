package service

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/repository"
)

type GetEntryService struct {
	repository *repository.LSMTreeRepository
}

func NewGetEntryService(repository *repository.LSMTreeRepository) *GetEntryService {
	return &GetEntryService{
		repository: repository,
	}
}

type GetEntryQuery struct {
	Key string
}

type GetEntryResult struct {
	Entry domain.DbEntry
}

func (s *GetEntryService) Execute(query GetEntryQuery) GetEntryResult {
	entry, found := s.repository.Get(query.Key)
	if !found {
		return GetEntryResult{}
	}
	return GetEntryResult{
		Entry: entry,
	}
}
