import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { StalledRecoveryBudget } from './stalledRecoveryBudget';

test('recovery budget persists and expires as a rolling hour', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'stall-budget-'));
  try {
    const one = new StalledRecoveryBudget(dir);
    const now = 1_000_000_000;
    assert.equal(one.tryReserve('agent-1', now), 'reserved');
    assert.equal(one.tryReserve('agent-1', now + 1), 'reserved');
    assert.equal(new StalledRecoveryBudget(dir).tryReserve('agent-1', now + 2), 'exhausted');
    assert.equal(new StalledRecoveryBudget(dir).tryReserve('agent-1', now + 3_600_000), 'reserved');
    writeFileSync(path.join(dir, 'stalled-recovery-budget', 'agent-1.json'), 'broken');
    assert.equal(one.tryReserve('agent-1', now + 4_000_000), 'unavailable');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
