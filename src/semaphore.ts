/**
 * QueueCraft — concurrency control
 *
 * A counting semaphore that bounds how many tasks may run at the same time.
 * Backpressure is handled by queuing callers: `acquire()` resolves immediately
 * while slots are free, and otherwise waits until a slot is released.
 */
export class Semaphore {
  /** Maximum number of permits that may be held simultaneously. */
  private readonly maxConcurrency: number;

  /** Number of permits currently held (i.e. tasks running right now). */
  private active = 0;

  /** FIFO queue of callers waiting for a permit. */
  private readonly waiters: Array<() => void> = [];

  /**
   * @param maxConcurrency - Upper bound on concurrent tasks. Must be a
   *                         positive integer (see `WorkerOptions.concurrency`).
   */
  constructor(maxConcurrency: number) {
    if (!Number.isInteger(maxConcurrency) || maxConcurrency < 1) {
      throw new RangeError(
        `maxConcurrency must be a positive integer, received: ${maxConcurrency}`,
      );
    }
    this.maxConcurrency = maxConcurrency;
  }

  /** Number of tasks currently holding a permit. */
  get activeCount(): number {
    return this.active;
  }

  /** Number of callers queued and waiting for a permit. */
  get pendingCount(): number {
    return this.waiters.length;
  }

  /**
   * Acquire a permit. Resolves immediately if a slot is free, otherwise
   * resolves once another holder calls `release()`.
   *
   * Every successful `acquire()` must be paired with exactly one `release()`.
   * Prefer `run()` where possible so releases are guaranteed.
   */
  acquire(): Promise<void> {
    if (this.active < this.maxConcurrency) {
      this.active++;
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  /**
   * Release a permit. If callers are waiting, the freed slot is handed
   * directly to the next one in line (the active count is unchanged);
   * otherwise the active count is decremented.
   */
  release(): void {
    const next = this.waiters.shift();
    if (next) {
      next();
    } else if (this.active > 0) {
      this.active--;
    }
  }

  /**
   * Run a task under a permit, releasing automatically even if it throws.
   * This is the safe, preferred way to use the semaphore.
   *
   * @typeParam T - Resolved value of the task.
   */
  async run<T>(task: () => Promise<T>): Promise<T> {
    await this.acquire();
    try {
      return await task();
    } finally {
      this.release();
    }
  }
}