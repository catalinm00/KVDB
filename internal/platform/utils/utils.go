package utils

import (
	. "KVDB/internal/domain"
	"encoding/binary"
	"errors"
	"io"
)

func AppendDbEntry(f io.Writer, entry DbEntry) error {
	// Convertimos strings a []byte
	keyBytes := []byte(entry.Key())
	valueBytes := []byte(entry.Value())

	// Escribir longitud y contenido de la key
	keyLen := uint32(len(keyBytes))
	if err := binary.Write(f, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if _, err := f.Write(keyBytes); err != nil {
		return err
	}

	// Escribir longitud y contenido del value
	valueLen := uint32(len(valueBytes))
	if err := binary.Write(f, binary.LittleEndian, valueLen); err != nil {
		return err
	}
	if _, err := f.Write(valueBytes); err != nil {
		return err
	}

	// Escribir el tombstone como un byte (0 = false, 1 = true)
	var tombstoneByte byte = 0
	if entry.Tombstone() {
		tombstoneByte = 1
	}
	if err := binary.Write(f, binary.LittleEndian, tombstoneByte); err != nil {
		return err
	}

	return nil
}

// ReadOneEntry lee una sola entrada desde r
func ReadOneEntry(r io.Reader) (DbEntry, error) {
	var entry DbEntry

	// Leer longitud key
	var keyLen uint32
	if err := binary.Read(r, binary.LittleEndian, &keyLen); err != nil {
		return entry, err
	}

	keyBytes := make([]byte, keyLen)
	if _, err := io.ReadFull(r, keyBytes); err != nil {
		return entry, err
	}

	// Leer longitud value
	var valueLen uint32
	if err := binary.Read(r, binary.LittleEndian, &valueLen); err != nil {
		return entry, err
	}

	valueBytes := make([]byte, valueLen)
	if _, err := io.ReadFull(r, valueBytes); err != nil {
		return entry, err
	}

	// Leer tombstone (1 byte)
	var tombstoneByte byte
	if err := binary.Read(r, binary.LittleEndian, &tombstoneByte); err != nil {
		return entry, err
	}

	entry = NewDbEntry(string(keyBytes), string(valueBytes), tombstoneByte != 0)

	return entry, nil
}

// ReadAllEntries lee todas las entradas de un archivo WAL
func ReadAllEntries(f io.Reader) ([]DbEntry, error) {
	var entries []DbEntry
	for {
		entry, err := ReadOneEntry(f)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // fin de archivo
			}
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
