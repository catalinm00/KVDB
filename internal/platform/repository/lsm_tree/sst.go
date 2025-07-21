package lsm_tree

import "KVDB/internal/domain"

type BlockMetadata struct {
	Offset uint64
	Size   uint64
}
type Header struct {
	Version   uint32
	Timestamp uint64
	NumBlocks uint32
}

type DataBlock struct {
	Entries []domain.DbEntry
}

type IndexBlock struct {
	Entries  []IndexEntry
	Metadata BlockMetadata
}

type IndexEntry struct {
	FirstKey string
	LastKey  string
	Metadata BlockMetadata
}

type Footer struct {
	IndexMetadata  BlockMetadata
	HeaderMetadata BlockMetadata
	MagicNumber    uint64
}

type SortedStringsTable struct {
	Header *Header
	Data   *[]DataBlock
	Index  *IndexBlock
	Footer *Footer
}
