package repeater

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type fakeSink struct {
	active       map[string]uint64
	resolution   map[string]uint64
	failResolve  bool
	resolveCalls []uint64
	openCalls    []uint64
}

func newFakeSink() *fakeSink {
	return &fakeSink{active: map[string]uint64{}, resolution: map[string]uint64{}}
}

func (s *fakeSink) OpenRepeaterIncident(repeaterID string, generation uint64) error {
	s.openCalls = append(s.openCalls, generation)
	if generation >= s.active[repeaterID] {
		s.active[repeaterID] = generation
	}
	return nil
}

func (s *fakeSink) ResolveRepeaterIncident(repeaterID string, generation uint64) error {
	s.resolveCalls = append(s.resolveCalls, generation)
	if s.failResolve {
		return errors.New("persist resolution event")
	}
	if active := s.active[repeaterID]; active != 0 && generation >= active {
		delete(s.active, repeaterID)
		s.resolution[repeaterID] = generation
	}
	return nil
}

func (s *fakeSink) Active(repeaterID string) (uint64, bool) {
	generation, ok := s.active[repeaterID]
	return generation, ok
}

func (s *fakeSink) LastResolution(repeaterID string) uint64 {
	return s.resolution[repeaterID]
}

func tripAt(t *testing.T, projector *HealthProjector, repeaterID string, generation, sequence uint64) {
	t.Helper()
	err := projector.ApplySample(context.Background(), telemetry.Sample{
		ID: "s", AssetID: repeaterID, Kind: telemetry.KindTemperature,
		Value: 90, Sequence: sequence, Generation: generation,
		ObservedAt: time.Date(2026, 10, 5, 1, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("trip sample: %v", err)
	}
}

func TestResetClearsProjectionAfterResolving(t *testing.T) {
	sink := newFakeSink()
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector := NewHealthProjector(sink, 85)
	projector.Register(asset)
	tripAt(t, projector, "R07", 3, 1)

	if err := projector.Reset("R07", 3); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if got := asset.Snapshot().Health; got != HealthNormal {
		t.Fatalf("health = %q, want %q", got, HealthNormal)
	}
	if _, active := sink.Active("R07"); active {
		t.Fatal("incident still active after reset")
	}
	if got := sink.LastResolution("R07"); got != 3 {
		t.Fatalf("last resolution = %d, want 3", got)
	}
}

func TestResetProjectionStaysLatchedWhenEventPersistFails(t *testing.T) {
	sink := newFakeSink()
	sink.failResolve = true
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector := NewHealthProjector(sink, 85)
	projector.Register(asset)
	tripAt(t, projector, "R07", 3, 1)

	err = projector.Reset("R07", 3)
	if err == nil {
		t.Fatal("expected resolution persistence error")
	}
	if got := asset.Snapshot().Health; got != HealthTripped {
		t.Fatalf("health = %q, want %q: projection must not recover before the event commits", got, HealthTripped)
	}
	if active, ok := sink.Active("R07"); !ok || active != 3 {
		t.Fatalf("active incident = (%d, %v), want (3, true)", active, ok)
	}
}

func TestResetRejectsStaleGenerationBeforeAnyCommit(t *testing.T) {
	sink := newFakeSink()
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector := NewHealthProjector(sink, 85)
	projector.Register(asset)
	tripAt(t, projector, "R07", 5, 1)

	err = projector.Reset("R07", 4)
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("reset error = %v, want ErrStaleGeneration", err)
	}
	if len(sink.resolveCalls) != 0 {
		t.Fatalf("resolve attempted for stale generation: %v", sink.resolveCalls)
	}
	if got := asset.Snapshot().Health; got != HealthTripped {
		t.Fatalf("health = %q, want %q", got, HealthTripped)
	}
	if active, ok := sink.Active("R07"); !ok || active != 5 {
		t.Fatalf("active incident = (%d, %v), want (5, true)", active, ok)
	}
}

func TestResetReopensIncidentWhenProjectionCannotClear(t *testing.T) {
	sink := newFakeSink()
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector := NewHealthProjector(sink, 85)
	projector.Register(asset)
	tripAt(t, projector, "R07", 5, 1)
	// A hotter sample from a newer generation advances the latched projection
	// without emitting a new open event (no untripped->tripped transition), so
	// projection latch (7) diverges from the active event generation (5).
	if err := projector.ApplySample(context.Background(), telemetry.Sample{
		ID: "s2", AssetID: "R07", Kind: telemetry.KindTemperature,
		Value: 91, Sequence: 2, Generation: 7,
		ObservedAt: time.Date(2026, 10, 5, 2, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("newer hot sample: %v", err)
	}
	if got := asset.Snapshot().TripGeneration; got != 7 {
		t.Fatalf("trip generation = %d, want 7", got)
	}
	if active, ok := sink.Active("R07"); !ok || active != 5 {
		t.Fatalf("active incident = (%d, %v), want (5, true)", active, ok)
	}

	if err := projector.Reset("R07", 6); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if got := asset.Snapshot().Health; got != HealthTripped {
		t.Fatalf("health = %q, want %q", got, HealthTripped)
	}
	if active, ok := sink.Active("R07"); !ok || active != 7 {
		t.Fatalf("active incident = (%d, %v), want (7, true) via compensation reopen", active, ok)
	}
}
