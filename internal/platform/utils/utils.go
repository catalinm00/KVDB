package utils

import (
	. "KVDB/internal/domain"
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// AppendDbEntry escribe una entrada completa en una sola línea
func AppendDbEntry(f io.Writer, entry DbEntry) error {
	keyBytes := []byte(entry.Key())
	valueBytes := []byte(entry.Value())

	keyLen := uint32(len(keyBytes))
	valueLen := uint32(len(valueBytes))

	tombstone := byte(0)
	if entry.Tombstone() {
		tombstone = 1
	}

	// Crear buffer para escribir todos los datos binarios
	var line strings.Builder

	// Escribir longitud de key (4 bytes en little endian)
	keyLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyLenBytes, keyLen)
	line.WriteString(fmt.Sprintf("%d,", keyLen))

	// Escribir key
	line.WriteString(string(keyBytes))
	line.WriteString(",")

	// Escribir longitud de value (4 bytes en little endian)
	line.WriteString(fmt.Sprintf("%d,", valueLen))

	// Escribir value
	line.WriteString(string(valueBytes))
	line.WriteString(",")

	// Escribir tombstone
	line.WriteString(fmt.Sprintf("%d", tombstone))

	// Escribir nueva línea
	line.WriteString("\n")

	// Escribir toda la línea de una vez
	_, err := f.Write([]byte(line.String()))
	return err
}

// ReadOneEntry lee una sola entrada desde una línea
func ReadOneEntry(r *bufio.Scanner) (DbEntry, error) {
	var entry DbEntry

	if !r.Scan() {
		if err := r.Err(); err != nil {
			return entry, err
		}
		return entry, io.EOF
	}

	line := r.Text()
	if line == "" {
		return entry, io.EOF
	}

	// Parsear la línea: keyLen,key,valueLen,value,tombstone
	parts := strings.Split(line, ",")
	if len(parts) < 3 {
		return entry, errors.New("formato de línea inválido")
	}

	// Leer longitud de key
	keyLen, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return entry, fmt.Errorf("error parseando longitud de key: %v", err)
	}

	// Encontrar la key y value basándose en las longitudes
	// Necesitamos reconstruir la línea porque la key/value pueden contener comas
	remainingLine := strings.Join(parts[1:], ",")

	if len(remainingLine) < int(keyLen) {
		return entry, errors.New("longitud de key inválida")
	}

	key := remainingLine[:keyLen]
	remainingLine = remainingLine[keyLen:]

	if !strings.HasPrefix(remainingLine, ",") {
		return entry, errors.New("formato inválido después de key")
	}
	remainingLine = remainingLine[1:] // quitar la coma

	// Leer longitud de value
	commaIndex := strings.Index(remainingLine, ",")
	if commaIndex == -1 {
		return entry, errors.New("no se encontró separador después de longitud de value")
	}

	valueLenStr := remainingLine[:commaIndex]
	valueLen, err := strconv.ParseUint(valueLenStr, 10, 32)
	if err != nil {
		return entry, fmt.Errorf("error parseando longitud de value: %v", err)
	}

	remainingLine = remainingLine[commaIndex+1:]

	if len(remainingLine) < int(valueLen) {
		return entry, errors.New("longitud de value inválida")
	}

	value := remainingLine[:valueLen]
	remainingLine = remainingLine[valueLen:]

	if !strings.HasPrefix(remainingLine, ",") {
		return entry, errors.New("formato inválido después de value")
	}
	remainingLine = remainingLine[1:] // quitar la coma

	// Leer tombstone
	tombstoneStr := remainingLine
	tombstoneVal, err := strconv.ParseUint(tombstoneStr, 10, 8)
	if err != nil {
		return entry, fmt.Errorf("error parseando tombstone: %v", err)
	}

	entry = NewDbEntry(key, value, tombstoneVal != 0)
	return entry, nil
}

// ReadAllEntries lee todas las entradas de un archivo WAL
func ReadAllEntries(f io.Reader) ([]DbEntry, error) {
	var entries []DbEntry
	scanner := bufio.NewScanner(f)

	for {
		entry, err := ReadOneEntry(scanner)
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
