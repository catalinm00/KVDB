package service

import (
	"KVDB/internal/domain"
	"KVDB/internal/platform/repository"
)

type SaveEntryService struct {
	repository domain.DbEntryRepository
}

func NewSaveEntryService(repository *repository.LSMTreeRepository) *SaveEntryService {
	return &SaveEntryService{
		repository: repository,
	}
}

type SaveEntryCommand struct {
	Key   string
	Value string
}

type SaveEntryResult struct {
	Entry domain.DbEntry
}

func (s *SaveEntryService) Execute(command SaveEntryCommand) SaveEntryResult {
	entry := s.repository.Save(domain.NewDbEntry(command.Key, command.Value, false))
	return SaveEntryResult{Entry: entry}
}
