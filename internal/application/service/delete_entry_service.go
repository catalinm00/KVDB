package service

import (
	"KVDB/internal/domain"
	"errors"
	"fmt"
)

type DeleteEntryService struct {
	repository domain.DbEntryRepository
}

func NewDeleteEntryService(repository domain.DbEntryRepository) *DeleteEntryService {
	return &DeleteEntryService{
		repository: repository,
	}
}

type DeleteEntryCommand struct {
	Key string
}

type DeleteEntryResult struct {
	Entry domain.DbEntry
	err   error
}

func (s *DeleteEntryService) Execute(command DeleteEntryCommand) DeleteEntryResult {
	entry, found := s.repository.Get(command.Key)
	if !found {
		return DeleteEntryResult{
			err: errors.New(fmt.Sprintf("Entry with Key: %s not found in database", command.Key)),
		}
	}
	entry.Delete()

	s.repository.Save(entry)
	return DeleteEntryResult{
		Entry: entry,
	}
}
