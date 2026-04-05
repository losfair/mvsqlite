# Page Prefetch Predictor

mvsqlite includes a page prefetch system that predicts which database pages will
be needed next and fetches them in a single batched HTTP request alongside the
current page. This amortizes network round-trip latency across multiple pages.

## Enabling Prefetch

Prefetch is controlled by the `MVSQLITE_PREFETCH_DEPTH` environment variable,
which sets the **maximum number of extra pages** to fetch alongside each cache
miss. It defaults to `0` (disabled).

```bash
export MVSQLITE_PREFETCH_DEPTH=10
```

When a page read misses the local cache, the predictor generates up to
`PREFETCH_DEPTH` page predictions. If the predictor produces fewer predictions
than the configured depth, the remaining slots are filled with sequential pages
(page N+1, N+2, ...). All predicted pages that are not already cached are
fetched together with the requested page in one `read_many` HTTP call.

## Architecture

The predictor (`PrefetchPredictor` in `mvfs/src/prefetch.rs`) is a per-connection
structure composed of three cooperating components:

```
PrefetchPredictor
  |-- MarkovTable       Bigram Markov chain on page deltas
  |-- StrideDetector    Fast-path for sequential/strided scans
  |-- RecentHistory     Ring buffer of recent deltas (cold-start fallback)
  '-- PredictorMetrics  Hit-rate tracking
```

Total memory: ~1.5 KB per connection, all fixed-size arrays (no heap growth).

### Bigram Markov Chain

The Markov table captures transition probabilities between consecutive page
deltas. A "delta" is the difference between two successive page accesses
(e.g., reading pages 5, 8, 11 produces deltas +3, +3). The Markov table
records *pairs* of deltas: "when the previous delta was X, what delta followed?"

This is more expressive than simple frequency counting because it captures
temporal ordering. For example, a B-tree index scan that alternates between
leaf pages (delta +1) and interior pages (delta -50) generates a distinct
bigram pattern (+1 -> -50, -50 -> +1) that the Markov chain can learn.

Implementation details:
- 64-bucket open-addressed hash table with linear probing (max probe distance: 4)
- Collision eviction uses LFU (least-frequently-used) within the probe window
- Counts are `u16`, saturating at 65535
- Predictions require a minimum probability of 15% (`count / total_matching`)
- Every 256 `record()` calls, all counts are halved (exponential decay), so the
  model adapts to changing access patterns within a transaction

### Stride Detector

A lightweight fast-path that detects consecutive identical deltas (sequential
scans, strided traversals). When 2+ consecutive accesses have the same delta,
the stride detector builds confidence and emits predictions by extrapolating
the stride forward.

- Confidence formula: `min(1.0, streak / 3.0)` -- reaches full confidence at
  streak length 3
- Confidence decays sharply on stride break: `confidence *= 0.3`
- Predictions are emitted only when confidence >= 0.5
- Each successive stride prediction is discounted by 0.9x per step

This handles the most common SQLite access pattern (sequential table scans)
with near-zero overhead.

### Recent History Ring Buffer

A 16-entry circular buffer of recent page deltas. Serves two purposes:
1. Provides the "previous delta" needed to query the Markov table
2. Cold-start fallback: when fewer than 4 deltas have been recorded, the
   predictor falls back to frequency counting over this buffer (similar to
   the old predictor behavior)

## Prediction Flow

On each page read (`record()` is called):
1. Check if the current page was previously predicted (for metrics)
2. Compute delta = current_page - previous_page
3. Update the Markov table with the bigram (previous_delta -> current_delta)
4. Update the stride detector
5. Push delta into the ring buffer

On a cache miss (`multi_predict()` is called):
1. If fewer than 4 samples recorded, use frequency fallback
2. Otherwise, collect stride predictions (if confidence >= 0.5) and Markov
   predictions (if probability >= 15%), merge with max-confidence dedup
3. Chain Markov lookups up to `depth` steps to predict further ahead
4. Return the union of all predicted pages

The caller (in `mvfs/src/vfs.rs`) then:
- Fills remaining `PREFETCH_DEPTH` slots with sequential pages (fallback)
- Filters out pages already in the local cache
- Issues a single batched `read_many` request for the target page + all predictions

## Transaction Boundaries

On lock release (transaction end), `reset()` is called:
- The ring buffer and stride detector are cleared (temporal context is per-transaction)
- The Markov table is **preserved** -- it captures structural patterns (B-tree
  shape, index layout) that persist across transactions. The decay mechanism
  handles staleness of old patterns.

## Tunable Constants

These are compile-time constants in `mvfs/src/prefetch.rs`:

| Constant | Value | Description |
|---|---|---|
| `MARKOV_BUCKETS` | 64 | Hash table size for delta bigrams |
| `MAX_PROBE` | 4 | Max linear probe distance |
| `HISTORY_SIZE` | 16 | Ring buffer size for recent deltas |
| `MARKOV_MIN_PROBABILITY` | 0.15 | Minimum probability to emit a Markov prediction |
| `STRIDE_MIN_CONFIDENCE` | 0.5 | Minimum stride confidence to emit predictions |
| `DECAY_INTERVAL` | 256 | How often (in page reads) to halve Markov counts |

## Runtime Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `MVSQLITE_PREFETCH_DEPTH` | 0 | Max extra pages to prefetch per cache miss (0 = disabled) |
| `MVSQLITE_PAGE_CACHE_SIZE` | 5000 | Max pages in the per-connection LRU cache |

## Example Access Patterns

**Sequential table scan** (pages 1, 2, 3, 4, 5):
- Stride detector activates after page 3 (streak=2, confidence=0.67)
- Predicts 6, 7, 8, ... with high confidence

**Strided index scan** (pages 1, 4, 7, 10, 13):
- Stride detector learns delta=+3
- Predicts 16, 19, 22, ...

**Repeating pattern** (pages 10, 20, 30, 10, 20, 30):
- Markov chain learns: delta +10 -> delta +10, delta +10 -> delta -20, delta -20 -> delta +10
- After page 30, predicts page 10 (delta -20 follows delta +10)

**Backward scan** (pages 100, 99, 98, 97):
- Stride detector learns delta=-1
- Predicts 96, 95, 94, ...
