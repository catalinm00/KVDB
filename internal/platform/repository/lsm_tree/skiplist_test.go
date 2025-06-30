package lsm_tree

import (
	"KVDB/internal/domain"
	_ "github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSkipList_SetAndGet(t *testing.T) {
	sl := NewSkipList(5, 0.5)

	entry := domain.NewDbEntry("key1", "value1", false)
	sl.Set(entry)

	got, ok := sl.Get("key1")
	assert.Equal(t, true, ok, "Expected to find key1")
	assert.Equal(t, "value1", got.Value(), "Expected to find key1 with value1")

	// Test update
	entryUpdated := domain.NewDbEntry("key1", "value2", false)
	sl.Set(entryUpdated)
	got, ok = sl.Get("key1")
	assert.Equal(t, "value2", got.Value(), "Expected to find key1 with value2")
}

func TestSkipList_GetNotFound(t *testing.T) {
	sl := NewSkipList(5, 0.5)
	_, ok := sl.Get("missing")
	if ok {
		t.Errorf("Expected to not find missing key")
	}
}

func TestSkipList_All(t *testing.T) {
	sl := NewSkipList(5, 0.5)
	sl.Set(domain.NewDbEntry("a", "1", false))
	sl.Set(domain.NewDbEntry("b", "2", false))
	sl.Set(domain.NewDbEntry("c", "3", false))

	all := sl.All()
	if len(all) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(all))
	}

	keys := map[string]bool{}
	for _, e := range all {
		keys[e.Key()] = true
	}

	for _, k := range []string{"a", "b", "c"} {
		if !keys[k] {
			t.Errorf("Expected key %s in All()", k)
		}
	}
}

func TestSkipList_Size(t *testing.T) {
	sl := NewSkipList(5, 0.5)

	initialSize := sl.Size()
	if initialSize != 0 {
		t.Errorf("Expected initial size 0, got %d", initialSize)
	}

	sl.Set(domain.NewDbEntry("a", "1", false))
	if sl.Size() <= 0 {
		t.Errorf("Expected size to increase, got %d", sl.Size())
	}

	sl.Set(domain.NewDbEntry("a", "12345", false))
	if sl.Size() <= 0 {
		t.Errorf("Expected size to increase with update, got %d", sl.Size())
	}
}

func TestSkipList_Reset(t *testing.T) {
	sl := NewSkipList(5, 0.5)
	sl.Set(domain.NewDbEntry("a", "1", false))
	sl.Set(domain.NewDbEntry("b", "2", false))

	newSl := sl.Reset()
	if newSl.Size() != 0 {
		t.Errorf("Expected reset skiplist to be empty, got size %d", newSl.Size())
	}

	if len(newSl.All()) != 0 {
		t.Errorf("Expected no elements in reset skiplist")
	}
}
