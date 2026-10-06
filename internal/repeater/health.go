package repeater

import (
	"context"
	"errors"
	"sync"

	"github.com/wyw14/cry-103/internal/telemetry"
)

// ErrStaleRepeaterReset is returned when a reset carries a generation older
// than the active incident or the generation of the most recent resolution.
var ErrStaleRepeaterReset = errors.New("repeater reset generation is older than the active or last-resolved incident")

type IncidentSink interface {
	OpenRepeaterIncident(repeaterID string, generation uint64) error
	ResolveRepeaterIncident(repeaterID string, generation uint64) error
	Active(repeaterID string) (uint64, bool)
	LastResolution(repeaterID string) uint64
}

type HealthProjector struct {
	mu        sync.RWMutex
	repeaters map[string]*Repeater
	incidents IncidentSink
	tripAtC   float64
}

func NewHealthProjector(incidents IncidentSink, tripAtC float64) *HealthProjector {
	return &HealthProjector{repeaters: make(map[string]*Repeater), incidents: incidents, tripAtC: tripAtC}
}

func (p *HealthProjector) Register(repeater *Repeater) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.repeaters[repeater.Snapshot().ID] = repeater
}

func (p *HealthProjector) ApplySample(_ context.Context, sample telemetry.Sample) error {
	if sample.Kind != telemetry.KindTemperature {
		return nil
	}
	p.mu.RLock()
	repeater := p.repeaters[sample.AssetID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	before := repeater.Snapshot()
	if sample.Sequence <= before.LastSequence || sample.Generation < before.TripGeneration {
		return nil
	}
	after := repeater.update(func(current *Snapshot) {
		current.LastSequence = sample.Sequence
		current.TemperatureC = sample.Value
		current.UpdatedAt = sample.ObservedAt.UTC()
		if sample.Value >= p.tripAtC {
			current.Health = HealthTripped
			current.TripGeneration = sample.Generation
		} else if current.Health != HealthTripped {
			current.Health = HealthNormal
		}
	})
	if before.Health != HealthTripped && after.Health == HealthTripped && p.incidents != nil {
		return p.incidents.OpenRepeaterIncident(after.ID, after.TripGeneration)
	}
	return nil
}

func (p *HealthProjector) Reset(repeaterID string, generation uint64) error {
	p.mu.RLock()
	repeater := p.repeaters[repeaterID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	if p.incidents != nil {
		active, isActive := p.incidents.Active(repeaterID)
		lastResolution := p.incidents.LastResolution(repeaterID)
		if (isActive && generation < active) || generation < lastResolution {
			// An older generation must not clear an incident that a newer
			// generation opened or that a newer reset already resolved.
			return ErrStaleRepeaterReset
		}
		// Commit the resolution event before touching the projection. When the
		// event cannot be persisted, the device keeps its tripped projection
		// and the incident stays active, so the two states never diverge; no
		// rollback is needed because nothing was changed yet.
		if err := p.incidents.ResolveRepeaterIncident(repeaterID, generation); err != nil {
			return err
		}
		// Re-check after the commit: a concurrent trip for a newer generation
		// must not be masked by this (now stale) reset.
		if active, isActive := p.incidents.Active(repeaterID); isActive && generation < active {
			return ErrStaleRepeaterReset
		}
	}
	// Event durably resolved: only now advance the projection. The generation
	// guard inside the update is the last line of defence against a newer trip
	// recorded concurrently.
	repeater.update(func(current *Snapshot) {
		if generation >= current.TripGeneration {
			current.Health = HealthNormal
			current.TripGeneration = generation
		}
	})
	return nil
}
