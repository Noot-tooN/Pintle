//go:build darwin || linux

package integration

import (
	"fmt"
	"testing"
)

// Call/return are logical event numbers, not wall-clock timestamps. An unknown
// result may have committed or had no effect. This checker is deliberately
// bounded to small, single-resource P0 histories, not a general lease checker.
type historyOp struct {
	Call, Return uint64
	ID, Owner    string
	Kind         string // grant or vacate
	Result       historyResult
}
type historyResult struct {
	Kind            string // granted, contended, conflict, exhausted, unknown, vacated
	Fence, Revision uint64
	Replayed        bool
}
type historyReceipt struct {
	Owner           string
	Fence, Revision uint64
}
type historyState struct {
	Owned           bool
	Fence, Revision uint64
	Receipts        map[string]historyReceipt
}

func (s historyState) apply(op historyOp) (historyState, historyResult) {
	n := s
	n.Receipts = make(map[string]historyReceipt, len(s.Receipts)+1)
	for k, v := range s.Receipts {
		n.Receipts[k] = v
	}
	if op.Kind == "vacate" {
		n.Owned = false
		n.Revision++
		return n, historyResult{Kind: "vacated"}
	}
	if prior, ok := s.Receipts[op.ID]; ok {
		if prior.Owner != op.Owner {
			return n, historyResult{Kind: "conflict"}
		}
		return n, historyResult{Kind: "granted", Fence: prior.Fence, Revision: prior.Revision, Replayed: true}
	}
	if s.Owned {
		return n, historyResult{Kind: "contended"}
	}
	if s.Fence == ^uint64(0) || s.Revision == ^uint64(0) {
		return n, historyResult{Kind: "exhausted"}
	}
	n.Owned = true
	n.Fence++
	n.Revision++
	n.Receipts[op.ID] = historyReceipt{Owner: op.Owner, Fence: n.Fence, Revision: n.Revision}
	return n, historyResult{Kind: "granted", Fence: n.Fence, Revision: n.Revision}
}
func linearizable(ops []historyOp) bool {
	if len(ops) > 12 {
		panic("bounded history checker supports at most 12 operations")
	}
	for _, op := range ops {
		if op.Call >= op.Return {
			return false
		}
	}
	var visit func(historyState, uint64) bool
	visit = func(state historyState, done uint64) bool {
		if done == (uint64(1)<<len(ops))-1 {
			return true
		}
		for i, op := range ops {
			bit := uint64(1) << i
			if done&bit != 0 {
				continue
			}
			allowed := true
			for j, other := range ops {
				if done&(uint64(1)<<j) == 0 && other.Return < op.Call {
					allowed = false
					break
				}
			}
			if !allowed {
				continue
			}
			next, result := state.apply(op)
			if op.Result.Kind == "unknown" {
				if visit(state, done|bit) || visit(next, done|bit) {
					return true
				}
			} else if result == op.Result && visit(next, done|bit) {
				return true
			}
		}
		return false
	}
	return visit(historyState{Receipts: map[string]historyReceipt{}}, 0)
}
func TestHistoryCheckerRejectsInvalidHistories(t *testing.T) {
	fresh := historyResult{Kind: "granted", Fence: 1, Revision: 1}
	replay := fresh
	replay.Replayed = true
	a := historyOp{Call: 1, Return: 4, ID: "a", Owner: "alice", Result: fresh}
	b := historyOp{Call: 2, Return: 3, ID: "b", Owner: "bob", Result: historyResult{Kind: "contended"}}
	tests := []struct {
		name  string
		ops   []historyOp
		valid bool
	}{
		{"overlapping_return_order", []historyOp{a, b}, true},
		{"two_fresh_grants", []historyOp{a, {Call: 2, Return: 3, ID: "b", Owner: "bob", Result: fresh}}, false},
		{"replay_without_original", []historyOp{{Call: 1, Return: 2, ID: "a", Owner: "alice", Result: replay}}, false},
		{"invented_token", []historyOp{{Call: 1, Return: 2, ID: "a", Owner: "alice", Result: historyResult{Kind: "granted", Fence: 2, Revision: 1}}}, false},
		{"wrong_revision", []historyOp{{Call: 1, Return: 2, ID: "a", Owner: "alice", Result: historyResult{Kind: "granted", Fence: 1, Revision: 2}}}, false},
		{"real_time_order_matters", []historyOp{{Call: 1, Return: 2, ID: "b", Owner: "bob", Result: historyResult{Kind: "contended"}}, {Call: 3, Return: 4, ID: "a", Owner: "alice", Result: fresh}}, false},
		{"unknown_committed", []historyOp{{Call: 1, Return: 2, ID: "a", Owner: "alice", Result: historyResult{Kind: "unknown"}}, {Call: 3, Return: 4, ID: "a", Owner: "alice", Result: replay}}, true},
		{"unknown_aborted", []historyOp{{Call: 1, Return: 2, ID: "a", Owner: "alice", Result: historyResult{Kind: "unknown"}}, {Call: 3, Return: 4, ID: "a", Owner: "alice", Result: fresh}}, true},
		{"conflicting_principal", []historyOp{a, {Call: 5, Return: 6, ID: "a", Owner: "bob", Result: replay}}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := linearizable(tc.ops); got != tc.valid {
				t.Fatal(fmt.Sprintf("got %v expected %v: %+v", got, tc.valid, tc.ops))
			}
		})
	}
}
