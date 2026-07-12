package idemlease_test

import (
	"testing"
	"time"

	"github.com/repenguin22/idemlease"
)

func TestRecordExpired(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		rec  idemlease.Record
		want bool
	}{
		{
			name: "reserved before lease expiry",
			rec:  idemlease.Record{State: idemlease.StateReserved, LeaseExpiresAt: now.Add(time.Second)},
			want: false,
		},
		{
			name: "reserved at lease expiry (boundary counts as expired)",
			rec:  idemlease.Record{State: idemlease.StateReserved, LeaseExpiresAt: now},
			want: true,
		},
		{
			name: "reserved past lease expiry",
			rec:  idemlease.Record{State: idemlease.StateReserved, LeaseExpiresAt: now.Add(-time.Second)},
			want: true,
		},
		{
			name: "completed before record expiry",
			rec:  idemlease.Record{State: idemlease.StateCompleted, RecordExpiresAt: now.Add(time.Second)},
			want: false,
		},
		{
			name: "completed past record expiry",
			rec:  idemlease.Record{State: idemlease.StateCompleted, RecordExpiresAt: now.Add(-time.Second)},
			want: true,
		},
		{
			name: "completed ignores stale lease expiry",
			rec: idemlease.Record{
				State:           idemlease.StateCompleted,
				LeaseExpiresAt:  now.Add(-time.Hour),
				RecordExpiresAt: now.Add(time.Hour),
			},
			want: false,
		},
		{
			name: "unknown state is always expired",
			rec:  idemlease.Record{LeaseExpiresAt: now.Add(time.Hour), RecordExpiresAt: now.Add(time.Hour)},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rec.Expired(now); got != tt.want {
				t.Errorf("Expired = %v, want %v", got, tt.want)
			}
		})
	}
}
