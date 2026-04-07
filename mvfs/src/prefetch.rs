use std::collections::HashSet;

// --- Prediction Source ---

/// Identifies which predictor produced a prediction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredictionSource {
    /// Stride detector (sequential / constant-delta patterns).
    Stride,
    /// Markov chain (one-step bigram lookup).
    Markov,
    /// Markov chain (multi-hop chaining past the first prediction).
    MarkovChain,
    /// Cold-start frequency fallback from ring buffer.
    Frequency,
}

/// Snapshot of predictor metrics, returned by `PrefetchPredictor::metrics()`.
#[derive(Debug, Clone, Default)]
pub struct PrefetchMetrics {
    pub predictions_made: u32,
    pub predictions_hit: u32,
    pub stride_predictions: u32,
    pub stride_hits: u32,
    pub markov_predictions: u32,
    pub markov_hits: u32,
    pub markov_chain_predictions: u32,
    pub markov_chain_hits: u32,
    pub frequency_predictions: u32,
    pub frequency_hits: u32,
    /// Total page reads recorded (calls to `record()`).
    pub page_reads: u32,
    /// Total cache misses that triggered prediction (calls to `multi_predict()` with max > 0).
    pub cache_misses: u32,
}

impl PrefetchMetrics {
    pub fn hit_rate(&self) -> f32 {
        if self.predictions_made == 0 {
            0.0
        } else {
            self.predictions_hit as f32 / self.predictions_made as f32
        }
    }
}

// --- Constants ---

/// Number of buckets in the Markov hash table. Must be a power of 2.
const MARKOV_BUCKETS: usize = 64;
/// Maximum linear probe distance in the Markov table.
const MAX_PROBE: usize = 4;
/// Ring buffer size for recent deltas.
const HISTORY_SIZE: usize = 16;
/// Number of recent predictions tracked for hit-rate metrics.
const PREDICTION_TRACK_SIZE: usize = 16;
/// Minimum probability threshold for Markov predictions.
const MARKOV_MIN_PROBABILITY: f32 = 0.15;
/// Minimum confidence for stride detector to emit predictions.
const STRIDE_MIN_CONFIDENCE: f32 = 0.5;
/// How often (in record() calls) to decay Markov counts.
const DECAY_INTERVAL: u32 = 256;

// --- Markov Table ---

#[derive(Clone, Copy)]
struct MarkovEntry {
    prev_delta: i64,
    next_delta: i64,
    count: u16,
}

impl Default for MarkovEntry {
    fn default() -> Self {
        Self {
            prev_delta: 0,
            next_delta: 0,
            count: 0,
        }
    }
}

struct MarkovTable {
    entries: [MarkovEntry; MARKOV_BUCKETS],
}

impl Default for MarkovTable {
    fn default() -> Self {
        Self {
            entries: [MarkovEntry::default(); MARKOV_BUCKETS],
        }
    }
}

impl MarkovTable {
    fn hash(prev_delta: i64, next_delta: i64) -> usize {
        // Simple hash combining both deltas
        let h = (prev_delta.wrapping_mul(0x9E3779B97F4A7C15_u64 as i64))
            ^ (next_delta.wrapping_mul(0x517CC1B727220A95_u64 as i64));
        (h as u64 as usize) & (MARKOV_BUCKETS - 1)
    }

    /// Record a (prev_delta -> next_delta) transition.
    fn record(&mut self, prev_delta: i64, next_delta: i64) {
        let start = Self::hash(prev_delta, next_delta);
        let mut min_count_idx = start;
        let mut min_count = u16::MAX;

        for i in 0..MAX_PROBE {
            let idx = (start + i) & (MARKOV_BUCKETS - 1);
            let entry = &mut self.entries[idx];

            if entry.count == 0 {
                // Empty slot — insert here
                entry.prev_delta = prev_delta;
                entry.next_delta = next_delta;
                entry.count = 1;
                return;
            }

            if entry.prev_delta == prev_delta && entry.next_delta == next_delta {
                // Exact match — increment (saturating)
                entry.count = entry.count.saturating_add(1);
                return;
            }

            if entry.count < min_count {
                min_count = entry.count;
                min_count_idx = idx;
            }
        }

        // Probe window full — evict lowest-count entry (LFU within window)
        let entry = &mut self.entries[min_count_idx];
        entry.prev_delta = prev_delta;
        entry.next_delta = next_delta;
        entry.count = 1;
    }

    /// Look up all next_deltas for a given prev_delta, returning (next_delta, count) pairs.
    fn lookup(&self, prev_delta: i64) -> Vec<(i64, u16)> {
        let mut results = Vec::new();
        // We need to scan all buckets since entries with the same prev_delta can hash
        // to different locations due to different next_delta values.
        for entry in &self.entries {
            if entry.count > 0 && entry.prev_delta == prev_delta {
                results.push((entry.next_delta, entry.count));
            }
        }
        results
    }

    /// Halve all counts (exponential decay).
    fn decay(&mut self) {
        for entry in &mut self.entries {
            entry.count >>= 1;
        }
    }
}

// --- Stride Detector ---

#[derive(Clone)]
struct StrideDetector {
    last_delta: i64,
    streak: u8,
    confidence: f32,
}

impl Default for StrideDetector {
    fn default() -> Self {
        Self {
            last_delta: 0,
            streak: 0,
            confidence: 0.0,
        }
    }
}

impl StrideDetector {
    fn record(&mut self, delta: i64) {
        if delta == self.last_delta && delta != 0 {
            self.streak = self.streak.saturating_add(1);
            self.confidence = (self.streak as f32 / 3.0).min(1.0);
        } else {
            self.confidence *= 0.3;
            self.streak = 1;
            self.last_delta = delta;
        }
    }

    fn predict(&self, current_page: u32, max_count: usize) -> Vec<(u32, f32)> {
        if self.confidence < STRIDE_MIN_CONFIDENCE || self.last_delta == 0 {
            return Vec::new();
        }

        let mut results = Vec::with_capacity(max_count);
        let mut page = current_page as i64;
        for step in 0..max_count {
            page += self.last_delta;
            if let Ok(p) = u32::try_from(page) {
                let conf = self.confidence * 0.9_f32.powi(step as i32);
                results.push((p, conf));
            } else {
                break;
            }
        }
        results
    }

    fn reset(&mut self) {
        self.streak = 0;
        self.confidence = 0.0;
        self.last_delta = 0;
    }
}

// --- Recent History ---

struct RecentHistory {
    deltas: [i64; HISTORY_SIZE],
    pos: usize,
    len: usize,
    prev_page: u32,
    has_prev_page: bool,
    last_delta_valid: bool,
}

impl Default for RecentHistory {
    fn default() -> Self {
        Self {
            deltas: [0; HISTORY_SIZE],
            pos: 0,
            len: 0,
            prev_page: 0,
            has_prev_page: false,
            last_delta_valid: false,
        }
    }
}

impl RecentHistory {
    fn push(&mut self, delta: i64) {
        self.deltas[self.pos % HISTORY_SIZE] = delta;
        self.pos += 1;
        if self.len < HISTORY_SIZE {
            self.len += 1;
        }
        self.last_delta_valid = true;
    }

    /// Get the most recent delta, if any. Returns None if no deltas recorded
    /// or if the last delta was invalidated (e.g., by a write boundary).
    fn last_delta(&self) -> Option<i64> {
        if self.len == 0 || !self.last_delta_valid {
            None
        } else {
            Some(self.deltas[(self.pos - 1) % HISTORY_SIZE])
        }
    }

    /// Invalidate the last delta so it won't be used as a Markov conditioning
    /// key. The history contents are preserved for frequency-based prediction.
    fn invalidate_last_delta(&mut self) {
        self.last_delta_valid = false;
    }

    /// Cold-start fallback: frequency count over the ring buffer.
    fn frequency_predict(&self, current_page: u32) -> Vec<(u32, f32)> {
        if self.len == 0 {
            return Vec::new();
        }

        let mut counts: Vec<(i64, u32)> = Vec::new();
        for i in 0..self.len {
            let d = self.deltas[(self.pos - 1 - i) % HISTORY_SIZE];
            if d == 0 {
                continue;
            }
            if let Some(entry) = counts.iter_mut().find(|e| e.0 == d) {
                entry.1 += 1;
            } else {
                counts.push((d, 1));
            }
        }

        let total = self.len as f32;
        counts.sort_by(|a, b| b.1.cmp(&a.1));
        counts
            .iter()
            .filter_map(|&(delta, count)| {
                let prob = count as f32 / total;
                if prob >= 0.2 {
                    u32::try_from(current_page as i64 + delta)
                        .ok()
                        .map(|p| (p, prob))
                } else {
                    None
                }
            })
            .collect()
    }

    fn reset(&mut self) {
        self.pos = 0;
        self.len = 0;
        self.prev_page = 0;
        self.has_prev_page = false;
        self.last_delta_valid = false;
    }
}

// --- Predictor Metrics ---

/// Per-prediction entry tracking page and its source.
#[derive(Clone, Copy)]
struct PredictionEntry {
    page: u32,
    source: PredictionSource,
}

struct PredictorMetrics {
    predictions_made: u32,
    predictions_hit: u32,
    stride_predictions: u32,
    stride_hits: u32,
    markov_predictions: u32,
    markov_hits: u32,
    markov_chain_predictions: u32,
    markov_chain_hits: u32,
    frequency_predictions: u32,
    frequency_hits: u32,
    page_reads: u32,
    cache_misses: u32,
    recent_predictions: [PredictionEntry; PREDICTION_TRACK_SIZE],
    recent_predictions_len: usize,
}

impl Default for PredictorMetrics {
    fn default() -> Self {
        Self {
            predictions_made: 0,
            predictions_hit: 0,
            stride_predictions: 0,
            stride_hits: 0,
            markov_predictions: 0,
            markov_hits: 0,
            markov_chain_predictions: 0,
            markov_chain_hits: 0,
            frequency_predictions: 0,
            frequency_hits: 0,
            page_reads: 0,
            cache_misses: 0,
            recent_predictions: [PredictionEntry {
                page: 0,
                source: PredictionSource::Stride,
            }; PREDICTION_TRACK_SIZE],
            recent_predictions_len: 0,
        }
    }
}

impl PredictorMetrics {
    fn check_hit(&mut self, page: u32) {
        for i in 0..self.recent_predictions_len {
            if self.recent_predictions[i].page == page {
                self.predictions_hit += 1;
                match self.recent_predictions[i].source {
                    PredictionSource::Stride => self.stride_hits += 1,
                    PredictionSource::Markov => self.markov_hits += 1,
                    PredictionSource::MarkovChain => self.markov_chain_hits += 1,
                    PredictionSource::Frequency => self.frequency_hits += 1,
                }
                return;
            }
        }
    }

    fn record_predictions(&mut self, entries: &[(u32, PredictionSource)]) {
        self.predictions_made += entries.len() as u32;
        for &(_, source) in entries {
            match source {
                PredictionSource::Stride => self.stride_predictions += 1,
                PredictionSource::Markov => self.markov_predictions += 1,
                PredictionSource::MarkovChain => self.markov_chain_predictions += 1,
                PredictionSource::Frequency => self.frequency_predictions += 1,
            }
        }
        let n = entries.len().min(PREDICTION_TRACK_SIZE);
        for i in 0..n {
            self.recent_predictions[i] = PredictionEntry {
                page: entries[i].0,
                source: entries[i].1,
            };
        }
        self.recent_predictions_len = n;
    }

    fn snapshot(&self) -> PrefetchMetrics {
        PrefetchMetrics {
            predictions_made: self.predictions_made,
            predictions_hit: self.predictions_hit,
            stride_predictions: self.stride_predictions,
            stride_hits: self.stride_hits,
            markov_predictions: self.markov_predictions,
            markov_hits: self.markov_hits,
            markov_chain_predictions: self.markov_chain_predictions,
            markov_chain_hits: self.markov_chain_hits,
            frequency_predictions: self.frequency_predictions,
            frequency_hits: self.frequency_hits,
            page_reads: self.page_reads,
            cache_misses: self.cache_misses,
        }
    }
}

// --- PrefetchPredictor (top-level) ---

pub struct PrefetchPredictor {
    markov: MarkovTable,
    stride: StrideDetector,
    history: RecentHistory,
    metrics: PredictorMetrics,
    record_count: u32,
}

impl PrefetchPredictor {
    pub fn new() -> Self {
        Self {
            markov: MarkovTable::default(),
            stride: StrideDetector::default(),
            history: RecentHistory::default(),
            metrics: PredictorMetrics::default(),
            record_count: 0,
        }
    }

    /// Return a snapshot of the current predictor metrics.
    pub fn metrics(&self) -> PrefetchMetrics {
        self.metrics.snapshot()
    }

    /// Record a page access. Called on every page read.
    pub fn record(&mut self, current_page: u32) {
        self.metrics.page_reads += 1;

        // Check if this page was predicted (metrics)
        self.metrics.check_hit(current_page);

        if !self.history.has_prev_page {
            // First access after init/reset/skip — no meaningful delta yet.
            // Just record the page so the next access can compute a real delta.
            self.history.prev_page = current_page;
            self.history.has_prev_page = true;
            return;
        }

        let delta = current_page as i64 - self.history.prev_page as i64;

        // Update Markov table with bigram (prev_delta -> current_delta)
        if let Some(prev_delta) = self.history.last_delta() {
            self.markov.record(prev_delta, delta);
        }

        // Update stride detector
        self.stride.record(delta);

        // Push into history ring buffer
        self.history.push(delta);
        self.history.prev_page = current_page;

        // Periodic decay
        self.record_count = self.record_count.wrapping_add(1);
        if self.record_count % DECAY_INTERVAL == 0 && self.record_count > 0 {
            self.markov.decay();
        }
    }

    /// Mark the previous page reference as invalid without clearing learned state.
    /// Called on writes so the predictor treats the write as an opaque boundary:
    /// no delta is computed across it, stride state is broken, and the next
    /// Markov bigram won't bridge pre-write and post-write deltas.
    pub fn skip_next_delta(&mut self) {
        self.history.has_prev_page = false;
        self.stride.reset();
        // Clear the ring buffer's notion of "last delta" so the next record()
        // after the boundary won't feed a stale prev_delta into the Markov table.
        self.history.invalidate_last_delta();
    }

    /// Predict next pages from the current page. Returns (page, confidence, source) triples
    /// sorted by confidence descending, limited to `max_pages` entries.
    fn predict(&self, current_page: u32, max_pages: usize) -> Vec<(u32, f32, PredictionSource)> {
        if max_pages == 0 {
            return Vec::new();
        }

        // Cold-start: fall back to frequency counting
        if self.history.len < 4 {
            let mut preds: Vec<(u32, f32, PredictionSource)> = self
                .history
                .frequency_predict(current_page)
                .into_iter()
                .map(|(page, conf)| (page, conf, PredictionSource::Frequency))
                .collect();
            preds.truncate(max_pages);
            return preds;
        }

        let mut predictions: Vec<(u32, f32, PredictionSource)> = Vec::new();

        // 1. Stride predictions (capped to max_pages)
        let stride_preds = self.stride.predict(current_page, max_pages);
        for (page, conf) in &stride_preds {
            predictions.push((*page, *conf, PredictionSource::Stride));
        }

        // 2. Markov predictions
        if let Some(current_delta) = self.history.last_delta() {
            let transitions = self.markov.lookup(current_delta);
            let total_count: u32 = transitions.iter().map(|t| t.1 as u32).sum();
            if total_count > 0 {
                for (next_delta, count) in &transitions {
                    let prob = *count as f32 / total_count as f32;
                    if prob >= MARKOV_MIN_PROBABILITY {
                        if let Ok(page) = u32::try_from(current_page as i64 + next_delta) {
                            // Check if already in predictions (from stride), take max confidence
                            if let Some(existing) =
                                predictions.iter_mut().find(|p| p.0 == page)
                            {
                                existing.1 = existing.1.max(prob);
                                // If both stride and markov predict the same page,
                                // keep the stride source (it was first).
                            } else {
                                predictions.push((page, prob, PredictionSource::Markov));
                            }
                        }
                    }
                }
            }
        }

        // Sort by confidence descending
        predictions.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Remove current page if present
        predictions.retain(|p| p.0 != current_page);

        // Enforce hard cap
        predictions.truncate(max_pages);

        predictions
    }

    /// Multi-step prediction chaining. Returns a set of predicted page IDs,
    /// containing at most `max_pages` entries.
    pub fn multi_predict(&mut self, current_page: u32, max_pages: usize) -> HashSet<u32> {
        if max_pages > 0 {
            self.metrics.cache_misses += 1;
        }

        let mut result: HashSet<u32> = HashSet::with_capacity(max_pages);
        let mut tagged: Vec<(u32, PredictionSource)> = Vec::with_capacity(max_pages);

        // Get initial one-step predictions (already capped)
        let initial = self.predict(current_page, max_pages);
        for &(page, _, source) in &initial {
            if result.insert(page) {
                tagged.push((page, source));
            }
        }

        // Chain further predictions via Markov, starting from the best
        // one-step prediction so we advance past the first hop.
        if let Some(&(first_page, _, _)) = initial.first() {
            let mut last_page = first_page;
            // The delta that led to first_page
            let mut last_delta = first_page as i64 - current_page as i64;

            while result.len() < max_pages {
                let transitions = self.markov.lookup(last_delta);
                if transitions.is_empty() {
                    break;
                }

                // Pick the highest-count transition
                let best = transitions.iter().max_by_key(|t| t.1);
                let Some((next_delta, _)) = best else {
                    break;
                };

                if let Ok(next_page) = u32::try_from(last_page as i64 + next_delta) {
                    if next_page == current_page || !result.insert(next_page) {
                        break;
                    }
                    tagged.push((next_page, PredictionSource::MarkovChain));
                    last_page = next_page;
                    last_delta = *next_delta;
                } else {
                    break;
                }
            }
        }

        // Record predictions for metrics
        self.metrics.record_predictions(&tagged);

        result
    }

    /// Reset temporal state (called on lock release / transaction boundary).
    /// Preserves the Markov table (structural patterns persist across transactions).
    pub fn reset(&mut self) {
        self.history.reset();
        self.stride.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequential_scan() {
        let mut pred = PrefetchPredictor::new();
        // Simulate sequential page reads: 1, 2, 3, 4, 5, 6, 7, 8
        for i in 1..=8 {
            pred.record(i);
        }
        let predictions = pred.multi_predict(8, 10);
        // Should predict pages 9, 10, 11... (sequential continuation)
        assert!(predictions.contains(&9), "should predict page 9");
        assert!(predictions.contains(&10), "should predict page 10");
    }

    #[test]
    fn test_stride_pattern() {
        let mut pred = PrefetchPredictor::new();
        // Simulate stride-3 reads: 1, 4, 7, 10, 13, 16
        for &p in &[1u32, 4, 7, 10, 13, 16] {
            pred.record(p);
        }
        let predictions = pred.multi_predict(16, 10);
        // Should predict pages 19, 22, 25... (stride-3 continuation)
        assert!(predictions.contains(&19), "should predict page 19");
        assert!(predictions.contains(&22), "should predict page 22");
    }

    #[test]
    fn test_repeated_sequence() {
        let mut pred = PrefetchPredictor::new();
        // Simulate repeating pattern: [10, 20, 30] x 4
        for _ in 0..4 {
            pred.record(10);
            pred.record(20);
            pred.record(30);
        }
        // After seeing 30, should predict 10 (delta -20 follows delta +10).
        // Use a larger budget so Markov predictions aren't crowded out by stride.
        let predictions = pred.multi_predict(30, 15);
        assert!(predictions.contains(&10), "should predict page 10 after 30 in repeating pattern");
    }

    #[test]
    fn test_cold_start() {
        let mut pred = PrefetchPredictor::new();
        // Only 2 accesses — cold start
        pred.record(5);
        pred.record(6);
        let predictions = pred.multi_predict(6, 5);
        // Should still produce something (frequency fallback with delta +1)
        assert!(
            predictions.contains(&7),
            "cold-start should predict next sequential page"
        );
    }

    #[test]
    fn test_cold_start_very_few() {
        let mut pred = PrefetchPredictor::new();
        pred.record(10);
        // Only 1 access — extremely cold
        let predictions = pred.multi_predict(10, 5);
        // May be empty or minimal — just ensure no panic
        let _ = predictions;
    }

    #[test]
    fn test_reset() {
        let mut pred = PrefetchPredictor::new();
        for i in 1..=10 {
            pred.record(i);
        }
        pred.reset();
        // After reset, history is cleared but Markov table preserved
        assert_eq!(pred.history.len, 0);
        assert_eq!(pred.history.prev_page, 0);
        assert_eq!(pred.stride.streak, 0);
        assert!(pred.stride.confidence < STRIDE_MIN_CONFIDENCE);

        // Markov table should still have entries
        let has_entries = pred.markov.entries.iter().any(|e| e.count > 0);
        assert!(has_entries, "Markov table should persist across reset");
    }

    #[test]
    fn test_decay() {
        let mut pred = PrefetchPredictor::new();
        // Record enough to build up counts, then trigger decay
        for i in 1..=20 {
            pred.record(i);
        }

        // Capture a count before decay
        let count_before: u16 = pred
            .markov
            .entries
            .iter()
            .filter(|e| e.count > 0)
            .map(|e| e.count)
            .max()
            .unwrap_or(0);

        pred.markov.decay();

        let count_after: u16 = pred
            .markov
            .entries
            .iter()
            .filter(|e| e.count > 0)
            .map(|e| e.count)
            .max()
            .unwrap_or(0);

        assert!(
            count_after <= count_before / 2 + 1,
            "decay should approximately halve counts"
        );
    }

    #[test]
    fn test_markov_eviction() {
        let mut table = MarkovTable::default();
        // Fill the table with many distinct entries to trigger eviction
        for i in 0..200i64 {
            table.record(i, i + 1000);
        }
        // Should not panic and table should still be functional
        let results = table.lookup(50);
        // May or may not find it (could have been evicted), but shouldn't panic
        let _ = results;
    }

    #[test]
    fn test_markov_lookup() {
        let mut table = MarkovTable::default();
        table.record(1, 2);
        table.record(1, 2);
        table.record(1, 3);

        let results = table.lookup(1);
        assert!(!results.is_empty(), "should find transitions from delta 1");

        // Delta 2 should have higher count than delta 3
        let count_2 = results.iter().find(|r| r.0 == 2).map(|r| r.1).unwrap_or(0);
        let count_3 = results.iter().find(|r| r.0 == 3).map(|r| r.1).unwrap_or(0);
        assert!(count_2 > count_3, "delta 2 should have higher count");
    }

    #[test]
    fn test_stride_detector() {
        let mut sd = StrideDetector::default();
        sd.record(1);
        sd.record(1);
        sd.record(1);
        sd.record(1);

        assert!(sd.confidence >= STRIDE_MIN_CONFIDENCE);
        let preds = sd.predict(10, 5);
        assert!(!preds.is_empty());
        assert_eq!(preds[0].0, 11); // 10 + stride of 1
        assert_eq!(preds[1].0, 12);
    }

    #[test]
    fn test_stride_broken() {
        let mut sd = StrideDetector::default();
        sd.record(1);
        sd.record(1);
        sd.record(1);
        sd.record(1);
        assert!(sd.confidence >= STRIDE_MIN_CONFIDENCE);

        // Break the stride
        sd.record(5);
        assert!(sd.confidence < STRIDE_MIN_CONFIDENCE);
    }

    #[test]
    fn test_metrics_hit_tracking() {
        let mut pred = PrefetchPredictor::new();
        // Build up pattern
        for i in 1..=8 {
            pred.record(i);
        }
        let _ = pred.multi_predict(8, 10);

        // Now read page 9 (which should have been predicted)
        pred.record(9);

        assert!(
            pred.metrics.predictions_hit > 0,
            "should have registered a prediction hit"
        );
    }

    #[test]
    fn test_mixed_pattern() {
        let mut pred = PrefetchPredictor::new();
        // Mostly sequential with occasional jumps
        for &p in &[1u32, 2, 3, 4, 5, 100, 6, 7, 8, 9, 10, 200, 11, 12, 13, 14, 15] {
            pred.record(p);
        }
        let predictions = pred.multi_predict(15, 10);
        // Sequential pattern should still be detectable
        assert!(
            predictions.contains(&16),
            "sequential pattern should survive occasional jumps"
        );
    }

    #[test]
    fn test_negative_delta_handling() {
        let mut pred = PrefetchPredictor::new();
        // Backward scan
        for &p in &[100u32, 99, 98, 97, 96, 95] {
            pred.record(p);
        }
        let predictions = pred.multi_predict(95, 5);
        assert!(
            predictions.contains(&94),
            "should predict backward continuation"
        );
    }

    #[test]
    fn test_zero_delta_ignored_by_stride() {
        let mut sd = StrideDetector::default();
        // Repeated access to same page
        sd.record(0);
        sd.record(0);
        sd.record(0);
        // Zero delta should not build confidence
        assert!(sd.confidence < STRIDE_MIN_CONFIDENCE);
    }

    #[test]
    fn test_max_pages_enforced() {
        let mut pred = PrefetchPredictor::new();
        // Build a warm sequential pattern
        for i in 1..=20 {
            pred.record(i);
        }
        // Request at most 3 predictions
        let predictions = pred.multi_predict(20, 3);
        assert!(
            predictions.len() <= 3,
            "multi_predict must respect max_pages cap, got {}",
            predictions.len()
        );
    }

    #[test]
    fn test_zero_max_pages_returns_empty() {
        let mut pred = PrefetchPredictor::new();
        for i in 1..=10 {
            pred.record(i);
        }
        let predictions = pred.multi_predict(10, 0);
        assert!(
            predictions.is_empty(),
            "max_pages=0 should return no predictions"
        );
    }

    #[test]
    fn test_no_synthetic_delta_after_reset() {
        let mut pred = PrefetchPredictor::new();
        // Build up some Markov state
        for i in 1..=10 {
            pred.record(i);
        }
        let entries_before: u32 = pred
            .markov
            .entries
            .iter()
            .filter(|e| e.count > 0)
            .map(|e| e.count as u32)
            .sum();

        pred.reset();

        // First access after reset at an arbitrary page — should NOT record a
        // synthetic delta from page 0 to page 500.
        pred.record(500);
        // Second access records a real delta
        pred.record(501);

        let entries_after: u32 = pred
            .markov
            .entries
            .iter()
            .filter(|e| e.count > 0)
            .map(|e| e.count as u32)
            .sum();

        // Only one new transition should have been recorded (delta +1 from 500->501),
        // not two (no synthetic delta from 0->500).
        // entries_after should be entries_before + 1 (for the +1 delta),
        // possibly more if (+1 -> +1) already existed and got incremented.
        // The key check: there should be no entry with a delta of +500.
        let has_500_delta = pred.markov.entries.iter().any(|e| {
            e.count > 0 && (e.prev_delta == 500 || e.next_delta == 500)
        });
        assert!(
            !has_500_delta,
            "reset should not produce synthetic delta of +500 from page 0"
        );
        // Sanity: total count should not have jumped by more than 2
        assert!(
            entries_after <= entries_before + 2,
            "only real transitions should be recorded after reset"
        );
    }

    #[test]
    fn test_skip_next_delta_creates_clean_boundary() {
        let mut pred = PrefetchPredictor::new();
        // Build sequential pattern
        for i in 1..=10 {
            pred.record(i);
        }
        let history_len_before = pred.history.len;

        // Simulate a write — preserves history buffer but breaks stride and
        // invalidates last delta so no bogus Markov bigram bridges the write.
        pred.skip_next_delta();

        assert_eq!(pred.history.len, history_len_before, "history buffer should be preserved");
        assert_eq!(pred.stride.confidence, 0.0, "stride should be reset across write boundary");
        assert!(!pred.history.has_prev_page, "prev_page should be invalidated");
        assert!(!pred.history.last_delta_valid, "last_delta should be invalidated");

        // Record Markov state before post-write reads
        let markov_sum_before: u32 = pred
            .markov
            .entries
            .iter()
            .filter(|e| e.count > 0)
            .map(|e| e.count as u32)
            .sum();

        // First read after write: just sets prev_page (no delta computed)
        pred.record(50);
        // Second read after write: computes delta +1 from 50->51, but does NOT
        // record a Markov bigram because last_delta was invalidated by the skip
        pred.record(51);

        let markov_sum_after: u32 = pred
            .markov
            .entries
            .iter()
            .filter(|e| e.count > 0)
            .map(|e| e.count as u32)
            .sum();

        // The second read records delta +1 into the ring buffer, but there's no
        // valid prev_delta to form a bigram. So no new Markov entry should appear.
        assert_eq!(
            markov_sum_before, markov_sum_after,
            "no Markov bigram should bridge across write boundary"
        );

        // Third read: now both prev_page and last_delta are valid, so Markov
        // training resumes normally
        pred.record(52);
        let markov_sum_final: u32 = pred
            .markov
            .entries
            .iter()
            .filter(|e| e.count > 0)
            .map(|e| e.count as u32)
            .sum();
        assert!(
            markov_sum_final > markov_sum_after,
            "Markov training should resume after clean boundary"
        );

        // Predictions should work using the history buffer (delta +1 dominant)
        let predictions = pred.multi_predict(52, 5);
        assert!(
            predictions.contains(&53),
            "predictions should work after write boundary"
        );
    }

    #[test]
    fn test_chaining_advances_past_first_hop() {
        let mut pred = PrefetchPredictor::new();
        // Build a repeating 3-step cycle: deltas +10, +10, +10, ...
        // so Markov learns (+10 -> +10) strongly
        for i in 0..10 {
            pred.record(100 + i * 10);
        }
        // With max_pages=5, chaining should produce multiple hops:
        // 190 -> 200 -> 210 -> 220 -> 230
        let predictions = pred.multi_predict(190, 5);
        assert!(
            predictions.contains(&200),
            "should predict first hop (200)"
        );
        // Chaining should extend beyond the first hop
        assert!(
            predictions.contains(&210),
            "chaining should reach second hop (210)"
        );
        assert!(
            predictions.contains(&220),
            "chaining should reach third hop (220)"
        );
    }
}
