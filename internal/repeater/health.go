package repeater

import (
	"context"
	"errors"
	"sync"

	"github.com/wyw14/cry-103/internal/telemetry"
)

// ErrStaleGeneration is returned when a reset carries a generation older than
// the generation of the currently active incident.
var ErrStaleGeneration = errors.New("repeater reset rejected: stale generation")

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

// Reset clears a latched trip for the given generation. The resolution event
// is committed before the health projection is updated, so a failed event
// persistence leaves the repeater visibly tripped instead of showing a
// recovery that never happened. Resets older than the active incident
// generation are rejected before either side is touched.
func (p *HealthProjector) Reset(repeaterID string, generation uint64) error {
	p.mu.RLock()
	repeater := p.repeaters[repeaterID]
	p.mu.RUnlock()
	if repeater == nil {
		return nil
	}
	if p.incidents != nil {
		if active, ok := p.incidents.Active(repeaterID); ok && generation < active {
			return ErrStaleGeneration
		}
		// Commit the event first. On failure the projection stays latched,
		// matching the incident that remains active.
		if err := p.incidents.ResolveRepeaterIncident(repeaterID, generation); err != nil {
			return err
		}
	}
	before := repeater.Snapshot()
	after := repeater.update(func(current *Snapshot) {
		if generation >= current.TripGeneration {
			current.Health = HealthNormal
			current.TripGeneration = generation
		}
	})
	// Compensation: the event is resolved but the projection could not clear
	// itself (its latched trip is newer than the reset generation). Reopen the
	// incident at the projection's trip generation so event and projection
	// never disagree about whether the repeater is faulted.
	if p.incidents != nil && before.Health == HealthTripped && after.Health == HealthTripped {
		if err := p.incidents.OpenRepeaterIncident(repeaterID, after.TripGeneration); err != nil {
			return err
		}
	}
	return nil
}
