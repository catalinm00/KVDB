package lsm_tree

import (
	"KVDB/internal/domain"
	"math/rand"
	"time"
	"unsafe"
)

type SkipList struct {
	maxLevel int
	p        float64
	level    int
	rand     *rand.Rand
	size     int
	head     *Element
}

type Element struct {
	domain.DbEntry
	next []*Element
}

func NewSkipList(maxLevel int, p float64) *SkipList {
	return &SkipList{
		maxLevel: maxLevel,
		p:        p,
		level:    1,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		size:     0,
		head: &Element{
			DbEntry: domain.NewDbEntry("HEAD", "", false),
			next:    make([]*Element, maxLevel),
		},
	}
}

func (s *SkipList) Reset() *SkipList {
	return NewSkipList(s.maxLevel, s.p)
}

func (s *SkipList) Size() int {
	return s.size
}

func (e *Element) Set(entry domain.DbEntry) *Element {
	return &Element{
		entry,
		e.next,
	}
}

func (s *SkipList) Set(entry domain.DbEntry) {
	curr := s.head
	update := make([]*Element, s.maxLevel)

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].Key() < entry.Key() {
			curr = curr.next[i]
		}
		update[i] = curr
	}
	if curr.next[0] != nil && curr.next[0].Key() == entry.Key() {
		s.size += len(entry.Value()) - len(curr.next[0].Value())

		// update value and tombstone
		curr.next[0] = curr.next[0].Set(entry)
		return
	}
	// add entry
	level := s.randomLevel()

	if level > s.level {
		for i := s.level; i < level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	e := &Element{
		entry,
		make([]*Element, level),
	}

	for i := range level {
		e.next[i] = update[i].next[i]
		update[i].next[i] = e
	}
	s.size += len(entry.Key()) + len(entry.Value()) + int(unsafe.Sizeof(entry.Tombstone())) + len(e.next)*int(unsafe.Sizeof((*Element)(nil)))
}

func (s *SkipList) Get(key string) (domain.DbEntry, bool) {
	curr := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].Key() < key {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]

	if curr != nil && curr.Key() == key {
		return domain.NewDbEntry(curr.Key(), curr.Value(), curr.Tombstone()), true
	}
	return domain.DbEntry{}, false
}

func (s *SkipList) All() []domain.DbEntry {
	var all []domain.DbEntry

	for curr := s.head.next[0]; curr != nil; curr = curr.next[0] {
		all = append(all, domain.NewDbEntry(curr.Key(), curr.Value(), curr.Tombstone()))
	}

	return all
}

func (s *SkipList) randomLevel() int {
	level := 1
	for s.rand.Float64() < s.p && level < s.maxLevel {
		level++
	}
	return level
}
