package utils

import (
	. "KVDB/internal/domain"
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"
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

	// Leer desde el buffer usando scanner
	scanner := bufio.NewScanner(&buf)
	readEntry, err := ReadOneEntry(scanner)
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
	scanner := bufio.NewScanner(empty)

	_, err := ReadOneEntry(scanner)
	if err == nil {
		t.Fatal("esperado error EOF, pero no se recibió error")
	}
	if err != io.EOF {
		t.Errorf("esperado EOF, obtenido: %v", err)
	}
}

func TestAppendDbEntryTombstoneEncoding(t *testing.T) {
	entry := NewDbEntry("key", "value", true)
	var buf bytes.Buffer

	if err := AppendDbEntry(&buf, entry); err != nil {
		t.Fatalf("error en append: %v", err)
	}

	// Leer la línea completa y verificar que termine con ",1\n"
	data := buf.String()
	if !strings.HasSuffix(data, ",1\n") {
		t.Errorf("se esperaba que terminara con ',1\\n', obtenido: %q", data)
	}
}

func TestAppendDbEntryWithCommasInData(t *testing.T) {
	// Probar con datos que contengan comas
	entry := NewDbEntry("key,with,commas", "value,with,commas", false)
	var buf bytes.Buffer

	if err := AppendDbEntry(&buf, entry); err != nil {
		t.Fatalf("error en append: %v", err)
	}

	// Leer la entrada de vuelta
	scanner := bufio.NewScanner(&buf)
	readEntry, err := ReadOneEntry(scanner)
	if err != nil {
		t.Fatalf("ReadOneEntry falló: %v", err)
	}

	if readEntry != entry {
		t.Errorf("entrada leída no coincide:\nesperado: %+v\nobtenido: %+v", entry, readEntry)
	}
}

func TestMultipleEntriesInBuffer(t *testing.T) {
	var buf bytes.Buffer

	entries := []DbEntry{
		NewDbEntry("key1", "value1", false),
		NewDbEntry("key2", "value2", true),
		NewDbEntry("key3", "value3", false),
	}

	// Escribir todas las entradas
	for _, e := range entries {
		if err := AppendDbEntry(&buf, e); err != nil {
			t.Fatalf("fallo al escribir entrada: %v", err)
		}
	}

	// Leer todas las entradas de vuelta
	readEntries, err := ReadAllEntries(&buf)
	if err != nil {
		t.Fatalf("ReadAllEntries falló: %v", err)
	}

	if len(readEntries) != len(entries) {
		t.Fatalf("esperadas %d entradas, obtenidas %d", len(entries), len(readEntries))
	}

	for i := range entries {
		if readEntries[i] != entries[i] {
			t.Errorf("entrada %d no coincide: esperada %+v, obtenida %+v", i, entries[i], readEntries[i])
		}
	}
}
