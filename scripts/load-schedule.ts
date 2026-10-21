interface ArrivalSchedule {
  rate: number;
  durationMs: number;
  elapsedMs(): number;
  dispatch(index: number): void;
  canDispatch(): boolean;
  shouldStop(): boolean;
  onTick(elapsedMs: number): void;
  sleep?: () => Promise<void>;
}

// Account for every planned arrival, including arrivals due before the final delayed timer tick.
export async function scheduleArrivals(options: ArrivalSchedule): Promise<number> {
  const total = Math.floor(options.rate * options.durationMs / 1000);
  const sleep = options.sleep ?? (() => new Promise<void>(resolve => setTimeout(resolve, 5)));
  let index = 0;
  let unsent = 0;
  while (index < total && !options.shouldStop()) {
    const elapsed = options.elapsedMs();
    const due = Math.min(total, Math.floor(elapsed * options.rate / 1000) + 1);
    while (index < due) {
      if (!options.canDispatch()) { unsent += due - index; index = due; break; }
      options.dispatch(index++);
    }
    options.onTick(elapsed);
    if (options.shouldStop() || elapsed >= options.durationMs) break;
    await sleep();
  }
  return unsent + total - index;
}
