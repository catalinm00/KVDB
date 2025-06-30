package utils

import (
	. "KVDB/internal/domain"
	"bytes"
	"io"
	"os"
	"testing"
)

func TestAppendDbEntryAndReadOneEntry(t *testing.T) {
	var buf bytes.Buffer

	entry := NewDbEntry("clave", "valor", true)

	// Escribir al buffer
	err := AppendDbEntry(&buf, entry)
	if err != nil {
		t.Fatalf("AppendDbEntry falló: %v", err)
	}

	// Leer desde el buffer
	readEntry, err := ReadOneEntry(&buf)
	if err != nil {
		t.Fatalf("ReadOneEntry falló: %v", err)
	}

	if readEntry != entry {
		t.Errorf("entrada leída no coincide:\nesperado: %+v\nobtenido: %+v", entry, readEntry)
	}
}

func TestReadAllEntries(t *testing.T) {
	// Crear un archivo temporal
	tmpFile, err := os.CreateTemp("", "waltest")
	if err != nil {
		t.Fatalf("no se pudo crear archivo temporal: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Entradas esperadas
	expected := []DbEntry{
		NewDbEntry("uno", "1", false),
		NewDbEntry("dos", "2", true),
		NewDbEntry("tres", "3", false),
	}

	// Escribir todas al archivo
	for _, e := range expected {
		if err := AppendDbEntry(tmpFile, e); err != nil {
			t.Fatalf("fallo al escribir entrada: %v", err)
		}
	}

	// Resetear el puntero al inicio
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("fallo al hacer seek: %v", err)
	}

	// Leer todas las entradas
	entries, err := ReadAllEntries(tmpFile)
	if err != nil {
		t.Fatalf("ReadAllEntries falló: %v", err)
	}

	if len(entries) != len(expected) {
		t.Fatalf("esperadas %d entradas, obtenidas %d", len(expected), len(entries))
	}

	for i := range entries {
		if entries[i] != expected[i] {
			t.Errorf("entrada %d no coincide: esperada %+v, obtenida %+v", i, expected[i], entries[i])
		}
	}
}

func TestReadOneEntry_EOF(t *testing.T) {
	empty := bytes.NewReader(nil)

	_, err := ReadOneEntry(empty)
	if err == nil {
		t.Fatal("esperado error EOF, pero no se recibió error")
	}
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		t.Errorf("esperado EOF, obtenido: %v", err)
	}
}

func TestAppendDbEntry_TombstoneEncoding(t *testing.T) {
	entry := NewDbEntry("key", "value", true)
	var buf bytes.Buffer

	if err := AppendDbEntry(&buf, entry); err != nil {
		t.Fatalf("error en append: %v", err)
	}

	// Leer todo y examinar el último byte (tombstone)
	data := buf.Bytes()
	if len(data) < 1 {
		t.Fatal("no hay suficientes datos para validar tombstone")
	}
	tombstoneByte := data[len(data)-1]

	if tombstoneByte != 1 {
		t.Errorf("se esperaba tombstone = 1, obtenido %d", tombstoneByte)
	}
}
