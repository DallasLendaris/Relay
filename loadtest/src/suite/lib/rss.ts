/**
 * Peak-RSS sampling, cross-platform (no /proc, no `ps`).
 *
 *   const rss = new RssSampler();          // starts sampling this process once per second
 *   rss.addSource(() => handle.rssNow);    // child processes: a function returning their latest RSS in MB
 *   ...
 *   const peakMb = rss.stop();             // peak of (this process + all sources) in MB
 *
 * Children report their own RSS over IPC (see ./child.ts: `reportRss`, `ChildHandle.rssNow`), so the
 * same code works on Windows, Linux and macOS. Sources that return 0 or undefined count as 0.
 */

export function currentRssMb(): number {
  return process.memoryUsage().rss / 1024 / 1024;
}

export class RssSampler {
  private readonly sources = new Set<() => number | undefined>();
  private timer: NodeJS.Timeout | null;
  private peak = 0;

  constructor(intervalMs = 1000) {
    this.sample();
    this.timer = setInterval(() => this.sample(), intervalMs);
    this.timer.unref();
  }

  /** Adds a process (or anything) whose current RSS in MB `read` returns. Returns a remove function. */
  addSource(read: () => number | undefined): () => void {
    this.sources.add(read);
    return () => this.sources.delete(read);
  }

  /** Takes one sample now: self plus every source, summed. */
  sample(): number {
    let total = currentRssMb();
    for (const read of this.sources) total += read() ?? 0;
    if (total > this.peak) this.peak = total;
    return total;
  }

  /** Peak total so far, MB. */
  get peakMb(): number {
    return this.peak;
  }

  /** Takes a final sample, stops the timer, returns the peak MB (rounded). */
  stop(): number {
    this.sample();
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    return Math.round(this.peak);
  }
}
