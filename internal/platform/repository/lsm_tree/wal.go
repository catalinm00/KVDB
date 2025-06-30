package lsm_tree

import (
	. "KVDB/internal/domain"
	"KVDB/internal/platform/utils"
	"fmt"
	"os"
	"path"
	"sync"
	"time"
)

type WAL struct {
	mu sync.Mutex
	//logger  log.Logger
	fd      *os.File
	dir     string
	path    string
	version string
}

func NewWal(dir string) (*WAL, error) {
	createdAt := time.Now()
	version := fmt.Sprintf("%s-%d", createdAt.Format("20060102150405"), createdAt.Nanosecond())
	name := path.Join(dir, fmt.Sprintf("wal-%s.log", version))

	file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}
	return &WAL{
		fd:      file,
		dir:     dir,
		path:    name,
		version: version,
	}, nil
}

func FromFile(fileName string) (*WAL, error) {

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil, err
	}
	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}
	createdAt := time.Now()
	version := fmt.Sprintf("%s-%d", createdAt.Format("20060102150405"), createdAt.Nanosecond())
	return &WAL{
		fd:      fd,
		dir:     path.Dir(fileName),
		path:    fileName,
		version: version,
	}, nil
}

func (w *WAL) Write(entries ...DbEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, entry := range entries {
		err := utils.AppendDbEntry(w.fd, entry)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) Read() ([]DbEntry, error) {
	res, err := utils.ReadAllEntries(w.fd)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.close()
}

func (w *WAL) close() error {
	// w.fd will be nil if close is already called
	if w.fd != nil {
		if err := w.fd.Close(); err != nil {
			return err
		}
		w.fd = nil
	}
	return nil
}

func (w *WAL) Version() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.version
}
