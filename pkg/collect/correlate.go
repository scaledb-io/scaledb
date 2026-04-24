package collect

import "time"

// correlatedWait is a raw wait event tagged with its parent's QUID.
type correlatedWait struct {
	ParentDigest string
	EventName    string
	TimerWait    uint64
	TimerStart   uint64
}

// correlateWaits associates raw wait events with their parent statement's
// QUID (digest). Waits whose parent statement was already evicted from
// the ring buffer get an empty ParentDigest.
func correlateWaits(samples []QuerySample, waits []rawWaitEvent) []correlatedWait {
	parentDigest := make(map[uint64]string, len(samples))
	for i := range samples {
		parentDigest[samples[i].EventID] = samples[i].Digest
	}

	result := make([]correlatedWait, 0, len(waits))
	for i := range waits {
		w := &waits[i]
		result = append(result, correlatedWait{
			ParentDigest: parentDigest[w.NestingEventID],
			EventName:    w.EventName,
			TimerWait:    w.TimerWait,
			TimerStart:   w.TimerStart,
		})
	}
	return result
}

// bucketTime floors a time to the nearest 5-second boundary.
func bucketTime(t time.Time) time.Time {
	sec := t.Unix()
	return time.Unix(sec-sec%5, 0).UTC()
}

// aggregateWaits groups correlated waits by (parent_digest, event_name, 5s bucket)
// and returns summaries ready for writing.
func aggregateWaits(
	instanceID, clusterID string,
	waits []correlatedWait,
	now time.Time,
) []WaitEventSummary {
	accum := make(map[waitKey]*waitAccum)

	bucket := bucketTime(now).UTC().Format("2006-01-02 15:04:05")
	for i := range waits {
		w := &waits[i]
		key := waitKey{
			ParentDigest: w.ParentDigest,
			EventName:    w.EventName,
			Bucket:       bucket,
		}
		a, ok := accum[key]
		if !ok {
			a = &waitAccum{}
			accum[key] = a
		}
		a.Count++
		a.TotalWait += w.TimerWait
	}

	result := make([]WaitEventSummary, 0, len(accum))
	for key, a := range accum {
		result = append(result, WaitEventSummary{
			InstanceID:   instanceID,
			ClusterID:    clusterID,
			ParentDigest: key.ParentDigest,
			EventName:    key.EventName,
			Count:        a.Count,
			TotalWait:    a.TotalWait,
			Timestamp:    key.Bucket,
		})
	}
	return result
}
