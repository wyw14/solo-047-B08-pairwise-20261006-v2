package repeater

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/telemetry"
)

type fakeIncidentSink struct {
	active       map[string]uint64
	resolution   map[string]uint64
	failResolve  bool
	resolveCalls int
	onResolve    func(repeaterID string, generation uint64)
}

func newFakeIncidentSink() *fakeIncidentSink {
	return &fakeIncidentSink{active: map[string]uint64{}, resolution: map[string]uint64{}}
}

func (f *fakeIncidentSink) OpenRepeaterIncident(repeaterID string, generation uint64) error {
	if generation >= f.active[repeaterID] {
		f.active[repeaterID] = generation
	}
	return nil
}

func (f *fakeIncidentSink) ResolveRepeaterIncident(repeaterID string, generation uint64) error {
	f.resolveCalls++
	if f.onResolve != nil {
		f.onResolve(repeaterID, generation)
	}
	if f.failResolve {
		return errors.New("event store unavailable")
	}
	if active := f.active[repeaterID]; active != 0 && generation >= active {
		delete(f.active, repeaterID)
		f.resolution[repeaterID] = generation
	}
	return nil
}

func (f *fakeIncidentSink) Active(repeaterID string) (uint64, bool) {
	generation, exists := f.active[repeaterID]
	return generation, exists
}

func (f *fakeIncidentSink) LastResolution(repeaterID string) uint64 {
	return f.resolution[repeaterID]
}

func tripProjector(t *testing.T, projector *HealthProjector, asset *Repeater, sequence, generation uint64) {
	t.Helper()
	at := time.Date(2026, 10, 6, 1, 0, 0, 0, time.UTC).Add(time.Duration(sequence) * time.Second)
	sample := telemetry.Sample{
		ID: "s1", AssetID: asset.Snapshot().ID, Kind: telemetry.KindTemperature,
		Value: 90, Sequence: sequence, Generation: generation, ObservedAt: at,
	}
	if err := projector.ApplySample(context.Background(), sample); err != nil {
		t.Fatalf("apply tripping sample: %v", err)
	}
}

func TestResetResolvesEventBeforeCommittingProjection(t *testing.T) {
	sink := newFakeIncidentSink()
	projector := NewHealthProjector(sink, 85)
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector.Register(asset)
	tripProjector(t, projector, asset, 1, 3)

	var healthAtResolve HealthState
	sink.onResolve = func(string, uint64) {
		// While the resolution event is being persisted, the projection must
		// still show the device as tripped.
		healthAtResolve = asset.Snapshot().Health
	}
	if err := projector.Reset("R07", 3); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if healthAtResolve != HealthTripped {
		t.Fatalf("projection changed before the event was committed: %s", healthAtResolve)
	}
	if got := asset.Snapshot().Health; got != HealthNormal {
		t.Fatalf("projection after committed reset = %s, want %s", got, HealthNormal)
	}
	if _, active := sink.Active("R07"); active {
		t.Fatal("incident still active after reset")
	}
}

func TestResetKeepsProjectionTrippedWhenEventPersistFails(t *testing.T) {
	sink := newFakeIncidentSink()
	projector := NewHealthProjector(sink, 85)
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector.Register(asset)
	tripProjector(t, projector, asset, 1, 3)

	sink.failResolve = true
	err = projector.Reset("R07", 3)
	if err == nil {
		t.Fatal("expected persistence failure to surface")
	}
	if got := asset.Snapshot().Health; got != HealthTripped {
		t.Fatalf("projection = %s after failed event persist, want %s (device must not look recovered)", got, HealthTripped)
	}
	active, ok := sink.Active("R07")
	if !ok || active != 3 {
		t.Fatalf("active incident = (%d, %v), want (3, true)", active, ok)
	}
}

func TestResetRejectsGenerationOlderThanActiveIncident(t *testing.T) {
	sink := newFakeIncidentSink()
	projector := NewHealthProjector(sink, 85)
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector.Register(asset)
	tripProjector(t, projector, asset, 1, 5)

	err = projector.Reset("R07", 4)
	if !errors.Is(err, ErrStaleRepeaterReset) {
		t.Fatalf("stale reset error = %v, want ErrStaleRepeaterReset", err)
	}
	if got := asset.Snapshot().Health; got != HealthTripped {
		t.Fatalf("projection = %s after stale reset, want %s", got, HealthTripped)
	}
	if active, ok := sink.Active("R07"); !ok || active != 5 {
		t.Fatalf("active incident = (%d, %v), want (5, true)", active, ok)
	}
	if sink.resolveCalls != 0 {
		t.Fatalf("stale reset must not attempt to persist a resolution, calls=%d", sink.resolveCalls)
	}
}

func TestResetRejectsGenerationOlderThanLastResolution(t *testing.T) {
	sink := newFakeIncidentSink()
	projector := NewHealthProjector(sink, 85)
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector.Register(asset)
	tripProjector(t, projector, asset, 1, 3)
	if err := projector.Reset("R07", 3); err != nil {
		t.Fatalf("reset: %v", err)
	}

	err = projector.Reset("R07", 2)
	if !errors.Is(err, ErrStaleRepeaterReset) {
		t.Fatalf("duplicate stale reset error = %v, want ErrStaleRepeaterReset", err)
	}
}

func TestResetAllowsNewGenerationAfterRetrip(t *testing.T) {
	sink := newFakeIncidentSink()
	projector := NewHealthProjector(sink, 85)
	asset, err := New("R07", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	projector.Register(asset)
	tripProjector(t, projector, asset, 1, 3)
	if err := projector.Reset("R07", 3); err != nil {
		t.Fatalf("first reset: %v", err)
	}
	tripProjector(t, projector, asset, 2, 5)
	if err := projector.Reset("R07", 5); err != nil {
		t.Fatalf("reset for the new generation: %v", err)
	}
	if got := asset.Snapshot().Health; got != HealthNormal {
		t.Fatalf("projection = %s, want %s", got, HealthNormal)
	}
	if _, active := sink.Active("R07"); active {
		t.Fatal("new incident still active after matching reset")
	}
}

func TestResetUnknownRepeaterIsNoop(t *testing.T) {
	projector := NewHealthProjector(newFakeIncidentSink(), 85)
	if err := projector.Reset("missing", 1); err != nil {
		t.Fatalf("reset unknown repeater: %v", err)
	}
}
