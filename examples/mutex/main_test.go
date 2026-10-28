package main

import (
	"context"
	"errors"
	"testing"
)

func TestInventoryFenceEnforcement(t *testing.T) {
	s := inventoryStore{rows: make(map[string]inventoryRow)}
	ctx := context.Background()
	if err := s.save(ctx, "shirt", 10, "9007199254740992"); err != nil {
		t.Fatal(err)
	}
	if err := s.save(ctx, "shirt", 8, "9007199254740993"); err != nil {
		t.Fatal(err)
	}
	if err := s.save(ctx, "shirt", 10, "9007199254740992"); !errors.Is(err, errStaleFence) {
		t.Fatal("stale write accepted")
	}
	if s.rows["shirt"].stock != 8 || s.rows["shirt"].fence != 9007199254740993 {
		t.Fatal("stale write changed protected row")
	}
	// Tokens are resource-local; another product must not inherit this high-water mark.
	if err := s.save(ctx, "hat", 4, "9"); err != nil {
		t.Fatal(err)
	}
	if err := s.save(ctx, "hat", 3, "10"); err != nil {
		t.Fatal("lexicographic token comparison")
	}
	if err := s.save(ctx, "hat", 2, "10"); err != nil {
		t.Fatal("same lease cannot write twice")
	}
	for _, token := range []string{"0", "bad", "18446744073709551616"} {
		if err := s.save(ctx, "hat", 99, token); err == nil {
			t.Fatal("invalid token accepted")
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.save(cancelled, "hat", 99, "11"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if s.rows["hat"].stock != 2 || s.rows["hat"].fence != 10 {
		t.Fatal("failed write changed data/fence")
	}
}
