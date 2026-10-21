import { open, rename } from 'node:fs/promises';
import { dirname } from 'node:path';
import { ReceivingState } from './receiving-state';

// Save the displayed records, deduplication IDs, and replay progress together.
export async function saveReceiver(path: string, state: ReceivingState, cursor: string): Promise<void> {
  const file = await open(`${path}.tmp`, 'w', 0o600);
  try { await file.writeFile(JSON.stringify(state.snapshot(cursor))); await file.sync(); }
  finally { await file.close(); }
  await rename(`${path}.tmp`, path);
  const folder = await open(dirname(path), 'r');
  try { await folder.sync(); } finally { await folder.close(); }
  state.advance(cursor);
}
