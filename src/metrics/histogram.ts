/**
 * Log-bucketed latency histogram.
 *
 * Storing every sample to compute p99 is O(requests); sorting on read is worse.
 * This keeps 3 significant-ish digits over ~5 orders of magnitude in a fixed
 * array of counters, so recording is O(1) with no allocation and the whole
 * thing is a few kB regardless of traffic. Same idea as HdrHistogram, minus
 * the parts a demo does not need.
 */
export class Histogram {
  private readonly buckets: Float64Array;
  private readonly base: number;
  public count = 0;
  public sum = 0;
  public max = 0;
  public min = Number.POSITIVE_INFINITY;

  constructor(
    private readonly bucketCount = 256,
    /** Ratio between adjacent bucket bounds. 1.06 => ~6% worst-case error. */
    growth = 1.06,
  ) {
    this.buckets = new Float64Array(bucketCount);
    this.base = Math.log(growth);
  }

  record(valueMs: number): void {
    if (!Number.isFinite(valueMs) || valueMs < 0) return;
    this.count++;
    this.sum += valueMs;
    if (valueMs > this.max) this.max = valueMs;
    if (valueMs < this.min) this.min = valueMs;
    const idx = this.indexFor(valueMs);
    this.buckets[idx] = (this.buckets[idx] ?? 0) + 1;
  }

  private indexFor(v: number): number {
    if (v <= 0.001) return 0;
    // 0.001 ms is bucket 0; each bucket is `growth` times wider than the last.
    const i = Math.floor(Math.log(v / 0.001) / this.base) + 1;
    return Math.min(this.bucketCount - 1, Math.max(0, i));
  }

  private valueAt(idx: number): number {
    if (idx === 0) return 0.001;
    return 0.001 * Math.exp(this.base * idx);
  }

  percentile(p: number): number {
    if (this.count === 0) return 0;
    const target = (p / 100) * this.count;
    let seen = 0;
    for (let i = 0; i < this.bucketCount; i++) {
      seen += this.buckets[i] ?? 0;
      if (seen >= target) return round(this.valueAt(i));
    }
    return round(this.max);
  }

  snapshot() {
    return {
      count: this.count,
      meanMs: this.count ? round(this.sum / this.count) : 0,
      minMs: this.count ? round(this.min) : 0,
      p50Ms: this.percentile(50),
      p95Ms: this.percentile(95),
      p99Ms: this.percentile(99),
      maxMs: round(this.max),
    };
  }

  reset(): void {
    this.buckets.fill(0);
    this.count = 0;
    this.sum = 0;
    this.max = 0;
    this.min = Number.POSITIVE_INFINITY;
  }
}

const round = (n: number) => Math.round(n * 1000) / 1000;
