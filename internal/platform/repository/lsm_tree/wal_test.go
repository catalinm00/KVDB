package lsm_tree

import (
	. "KVDB/internal/domain"
	"os"
	"testing"
)

// helper para crear un WAL temporal
func createTempWal(t *testing.T) *WAL {
	tmpDir, err := os.MkdirTemp("", "waltest")
	if err != nil {
		t.Fatalf("error creando dir temporal: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	wal, err := NewWal(tmpDir)
	if err != nil {
		t.Fatalf("error creando WAL: %v", err)
	}
	t.Cleanup(func() {
		wal.fd.Close()
	})
	return wal
}

func TestNewWal(t *testing.T) {
	wal := createTempWal(t)
	if wal == nil {
		t.Fatal("WAL es nil")
	}
	if _, err := os.Stat(wal.path); os.IsNotExist(err) {
		t.Errorf("el archivo WAL no fue creado: %v", wal.path)
	}
	if wal.fd == nil {
		t.Error("el descriptor de archivo es nil")
	}
}

func TestWAL_Write(t *testing.T) {
	wal := createTempWal(t)
	entries := []DbEntry{
		NewDbEntry("k1", "v1", false),
		NewDbEntry("k2", "v2", false),
		NewDbEntry("k1", "", true),
	}

	err := wal.Write(entries...)
	if err != nil {
		t.Fatalf("error escribiendo en WAL: %v", err)
	}
}

func TestWAL_Read(t *testing.T) {
	wal := createTempWal(t)

	entries := []DbEntry{
		NewDbEntry("alpha", "1", false),
		NewDbEntry("beta", "2", false),
		NewDbEntry("alpha", "", true),
	}

	if err := wal.Write(entries...); err != nil {
		t.Fatalf("fallo al escribir en WAL: %v", err)
	}

	// simular reinicio
	wal.fd.Close()
	fd, err := os.Open(wal.path)
	if err != nil {
		t.Fatalf("error reabriendo archivo WAL: %v", err)
	}
	wal.fd = fd
	defer wal.fd.Close()

	readEntries, err := wal.Read()
	if err != nil {
		t.Fatalf("fallo al leer WAL: %v", err)
	}

	if len(readEntries) != len(entries) {
		t.Fatalf("esperado %d entradas, obtenido %d", len(entries), len(readEntries))
	}

	for i := range entries {
		if readEntries[i] != entries[i] {
			t.Errorf("entrada %d: esperada %+v, obtenida %+v", i, entries[i], readEntries[i])
		}
	}
}
