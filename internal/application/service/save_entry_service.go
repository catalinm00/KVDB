package service

import (
	"KVDB/internal/domain"
)

type SaveEntryService struct {
	transactionManager domain.TransactionExecutionStrategy
}

func NewSaveEntryService(
	transactionManager domain.TransactionExecutionStrategy) *SaveEntryService {
	return &SaveEntryService{
		transactionManager: transactionManager,
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
	entry := domain.NewDbEntry(command.Key, command.Value, false)
	resCh := s.transactionManager.Execute(domain.TransactionFromWriteEntry(entry))
	res := <-resCh
	if !res.Success {
		return SaveEntryResult{}
	}
	return SaveEntryResult{Entry: entry}
}
