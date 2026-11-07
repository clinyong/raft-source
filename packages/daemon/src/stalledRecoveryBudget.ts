import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import path from 'node:path';

/** A persisted, fail-closed rolling limit for automatic stall recovery. */
export class StalledRecoveryBudget {
  constructor(private readonly dataDir: string) {}

  tryReserve(agentId: string, now: number): 'reserved' | 'exhausted' | 'unavailable' {
    if (!/^[a-zA-Z0-9_-]+$/.test(agentId)) return 'unavailable';
    const dir = path.join(this.dataDir, 'stalled-recovery-budget');
    const file = path.join(dir, `${agentId}.json`);
    let attempts: number[] = [];
    try {
      const data: unknown = JSON.parse(readFileSync(file, 'utf8'));
      if (!Array.isArray(data) || !data.every((v) => typeof v === 'number' && Number.isFinite(v) && v <= now)) return 'unavailable';
      attempts = data.filter((v) => v > now - 60 * 60_000);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') return 'unavailable';
    }
    if (attempts.length >= 2) return 'exhausted';
    try {
      mkdirSync(dir, { recursive: true, mode: 0o700 });
      const tmp = `${file}.${randomUUID()}.tmp`;
      writeFileSync(tmp, JSON.stringify([...attempts, now]), { mode: 0o600, flag: 'wx' });
      renameSync(tmp, file);
      return 'reserved';
    } catch {
      return 'unavailable';
    }
  }
}
