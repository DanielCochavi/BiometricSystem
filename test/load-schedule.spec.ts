import { scheduleArrivals } from '../scripts/load-schedule';

describe('Load generator arrival accounting', () => {
  function fixture(overrides: Record<string, unknown> = {}) {
    let elapsed = 0;
    const sent: number[] = [];
    return { sent, options: { rate: 1000, durationMs: 30,
      elapsedMs: () => elapsed, dispatch: (index: number) => { sent.push(index); },
      canDispatch: () => true, shouldStop: () => false, onTick: () => undefined,
      sleep: async () => { elapsed += 7; }, ...overrides } };
  }

  it('sends the final scheduled arrival even when the timer crosses the stage boundary', async () => {
    const { sent, options } = fixture();
    expect(await scheduleArrivals(options)).toBe(0);
    expect(sent).toEqual(Array.from({ length: 30 }, (_, i) => i));
  });

  it('counts requests blocked by the outstanding limit instead of hiding or replaying them', async () => {
    let pending = 0;
    const { sent, options } = fixture({ canDispatch: () => pending < 3 });
    options.dispatch = index => { pending++; sent.push(index); };
    expect(await scheduleArrivals(options)).toBe(27);
    expect(sent).toEqual([0, 1, 2]);
  });

  it('counts all remaining planned requests when a safety check stops the stage', async () => {
    let stop = false;
    const { sent, options } = fixture({ shouldStop: () => stop, onTick: () => { stop = true; } });
    expect(await scheduleArrivals(options)).toBe(29);
    expect(sent).toEqual([0]);
  });

  it('paces arrivals rather than dispatching the whole stage immediately', async () => {
    let elapsed = 0;
    const times: number[] = [];
    const { options } = fixture({ rate: 100, durationMs: 40, elapsedMs: () => elapsed,
      dispatch: () => { times.push(elapsed); }, sleep: async () => { elapsed += 5; } });
    expect(await scheduleArrivals(options)).toBe(0);
    expect(times).toEqual([0, 10, 20, 30]);
  });
});
