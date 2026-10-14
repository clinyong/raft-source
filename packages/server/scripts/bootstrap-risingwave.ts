/**
 * Production-shaped RisingWave CDC/MV bootstrap for a self-hosted instance.
 *
 * The repository ships `bootstrap-risingwave-local.ts`, which is deliberately unusable
 * outside raftdev: it demands dev-only env, loopback hosts, the `/slock` + `/dev`
 * database names and a managed dev container name. This script performs the same
 * sequence against a real host, a real database name and real credentials.
 *
 * Three modes, selected by `RAFT_RW_BOOTSTRAP_MODE`:
 *
 *   verify    (default) read every input, probe both sides, WRITE NOTHING.
 *   init      first-time creation. Refuses when anything already exists on either side.
 *   recovery  explicit rebuild of the RisingWave side (drops our own source and slot).
 *             Requires `RAFT_RW_BOOTSTRAP_MODE=recovery` AND
 *             `RAFT_RW_BOOTSTRAP_CONFIRM=rebuild`.
 *
 * A normal restart must not run this script at all; because the default mode is read-only,
 * an accidental invocation reports instead of rebuilding. Order of operations, in every
 * mode that writes: validate every parameter and compose every statement BEFORE connecting,
 * then probe both sides read-only, then write. A bad parameter therefore writes nothing.
 *
 * The old `RAFT_RW_BOOTSTRAP=1` guard is gone: it only said "yes, I meant this", not which
 * of the three flows was wanted, and it did not stop a re-run from rebuilding a live chain.
 * Setting it without a mode now fails with an explanation and writes nothing.
 *
 * NOT YET VERIFIED against a real rebuild: the init and recovery paths have been exercised
 * only in an isolated environment; see the deploy checklist's VERIFY items.
 */
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  RISINGWAVE_ARTIFACT_CDC_TABLES,
  RISINGWAVE_BOOTSTRAP_ARTIFACTS,
  RISINGWAVE_CDC_TABLES,
  RISINGWAVE_PRODUCTION_SOURCE,
  RISINGWAVE_PUBLICATION_TABLES,
  RISINGWAVE_REQUIRED_RELATIONS,
  RISINGWAVE_LOCAL_SOURCE,
  buildRisingWaveBootstrapStatements,
  createdRelationName,
} from "../../../scripts/dev/raftdev-risingwave-bootstrap";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..");
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

type Mode = "verify" | "init" | "recovery";

interface Plan {
  mode: Mode;
  postgresUrl: URL;
  risingwaveUrl: URL;
  sourceName: string;
  slotName: string;
  publicationName: string;
  cdcHost: string;
  cdcPort: number;
  cdcUsername: string;
  cdcPassword: string;
  cdcDatabase: string;
  cdcSchema: string;
  statements: string[];
}

interface PostgresProbe {
  walLevel: string;
  publicationExists: boolean;
  publicationTables: string[];
  slotExists: boolean;
  slotActive: boolean;
  /** Set when the slot name is already taken by ANOTHER database: slot names are cluster-wide. */
  slotForeignDatabase: string | null;
}

interface RisingWaveProbe {
  sourceExists: boolean;
  presentRelations: string[];
  missingRelations: string[];
}

class Refusal extends Error {
  constructor(message: string, readonly action: string) {
    super(message);
  }
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function identifier(value: string, label: string): string {
  if (!IDENTIFIER.test(value)) {
    throw new Error(`${label} must match ${IDENTIFIER} (lowercase letters, digits, underscore); got '${value}'`);
  }
  return value;
}

function sqlIdentifier(value: string): string {
  return identifier(value, "SQL identifier");
}

function sqlLiteral(value: string): string {
  if (value.includes("\0")) throw new Error("values cannot contain NUL bytes");
  return `'${value.replaceAll("'", "''")}'`;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Every input is read and checked here, before a connection is opened anywhere. */
function readPlan(): Plan {
  if (process.env.RAFT_RW_BOOTSTRAP === "1" && !process.env.RAFT_RW_BOOTSTRAP_MODE) {
    throw new Error(
      "RAFT_RW_BOOTSTRAP=1 was the old all-or-nothing guard and has been replaced by " +
        "RAFT_RW_BOOTSTRAP_MODE=verify|init|recovery. Nothing was written. " +
        "Run with RAFT_RW_BOOTSTRAP_MODE=verify for a read-only report first.",
    );
  }

  const rawMode = process.env.RAFT_RW_BOOTSTRAP_MODE?.trim() || "verify";
  if (rawMode !== "verify" && rawMode !== "init" && rawMode !== "recovery") {
    throw new Error(`RAFT_RW_BOOTSTRAP_MODE must be verify, init or recovery; got '${rawMode}'`);
  }
  const mode: Mode = rawMode;

  if (mode === "recovery" && process.env.RAFT_RW_BOOTSTRAP_CONFIRM !== "rebuild") {
    throw new Error(
      "recovery rebuilds our RisingWave source and replication slot; set " +
        "RAFT_RW_BOOTSTRAP_CONFIRM=rebuild together with RAFT_RW_BOOTSTRAP_MODE=recovery. Nothing was written.",
    );
  }

  const postgresUrl = new URL(requiredEnv("DATABASE_URL"));
  const risingwaveUrl = new URL(requiredEnv("RISINGWAVE_DATABASE_URL"));

  const sourceName = identifier(
    process.env.RISINGWAVE_CDC_SOURCE_NAME?.trim() || RISINGWAVE_PRODUCTION_SOURCE,
    "RISINGWAVE_CDC_SOURCE_NAME",
  );
  const slotName = identifier(process.env.RISINGWAVE_CDC_SLOT?.trim() || "slock_rw_slot", "RISINGWAVE_CDC_SLOT");
  const publicationName = identifier(
    process.env.RISINGWAVE_CDC_PUBLICATION?.trim() || "slock_rw_publication",
    "RISINGWAVE_CDC_PUBLICATION",
  );

  const cdcHost = requiredEnv("RISINGWAVE_CDC_HOST");
  const cdcPort = Number(process.env.RISINGWAVE_CDC_PORT ?? "5432");
  if (!Number.isInteger(cdcPort) || cdcPort < 1 || cdcPort > 65535) {
    throw new Error("RISINGWAVE_CDC_PORT must be an integer between 1 and 65535");
  }
  const cdcDatabase = identifier(
    process.env.RISINGWAVE_CDC_DATABASE?.trim() || postgresUrl.pathname.replace(/^\//, ""),
    "RISINGWAVE_CDC_DATABASE",
  );
  const pgDatabase = postgresUrl.pathname.replace(/^\//, "");
  if (!pgDatabase) {
    throw new Error("DATABASE_URL must name a database");
  }
  if (cdcDatabase !== pgDatabase) {
    // This script proves things about the database in DATABASE_URL; letting the CDC source
    // consume a different one would check one database while publishing another.
    throw new Error(
      `RISINGWAVE_CDC_DATABASE ('${cdcDatabase}') must equal the database in DATABASE_URL ('${pgDatabase}')`,
    );
  }
  const cdcSchema = process.env.RISINGWAVE_CDC_SCHEMA?.trim() || "public";
  if (cdcSchema !== "public") {
    // The committed SQL and the publication tables are all `public`; another schema would
    // create a source nobody reads and hide the mismatch.
    throw new Error(`RISINGWAVE_CDC_SCHEMA must be 'public' (got '${cdcSchema}'); publish from another schema deliberately, not by accident`);
  }

  // Compose every statement now: a missing artifact or an inconsistent graph must fail here,
  // not halfway through a live rebuild.
  const artifacts = Object.fromEntries(
    RISINGWAVE_BOOTSTRAP_ARTIFACTS.map((artifact) => [artifact.file, readFileSync(join(ROOT, artifact.file), "utf8")]),
  );
  const statements = buildRisingWaveBootstrapStatements(artifacts).map((statement) =>
    // The composition rewrites the production source name to the dev one; undo it so the
    // committed SQL runs as written against the source this run creates.
    statement.replaceAll(new RegExp(`\\b${RISINGWAVE_LOCAL_SOURCE}\\b`, "g"), sourceName),
  );

  return {
    mode,
    postgresUrl,
    risingwaveUrl,
    sourceName,
    slotName,
    publicationName,
    cdcHost,
    cdcPort,
    cdcUsername: requiredEnv("RISINGWAVE_CDC_USERNAME"),
    cdcPassword: requiredEnv("RISINGWAVE_CDC_PASSWORD"),
    cdcDatabase,
    cdcSchema,
    statements,
  };
}

async function probePostgres(postgres: pg.Client, plan: Plan): Promise<PostgresProbe> {
  const wal = await postgres.query<{ wal_level: string }>("SHOW wal_level");
  const publication = await postgres.query<{ pubname: string }>(
    "SELECT pubname FROM pg_publication WHERE pubname = $1",
    [plan.publicationName],
  );
  const tables = await postgres.query<{ tablename: string }>(
    "SELECT tablename FROM pg_publication_tables WHERE pubname = $1 ORDER BY tablename",
    [plan.publicationName],
  );
  const slot = await postgres.query<{ database: string | null; active: boolean; ours: boolean }>(
    // pg_replication_slots is CLUSTER-wide in two ways that matter here:
    //   1. a slot on another database of the same server is visible from this connection, so the
    //      row must be scoped to current_database() before it is treated as ours;
    //   2. slot NAMES are unique per server, not per database - a name owned by another database
    //      cannot be created here, and rebuilds must never try to drop it. That case is reported
    //      as slotForeignDatabase instead of being filtered away.
    "SELECT database, active, (database = current_database()) AS ours FROM pg_replication_slots WHERE slot_name = $1",
    [plan.slotName],
  );
  const slotRow = slot.rows[0];
  const slotIsOurs = slotRow?.ours === true;
  return {
    walLevel: wal.rows[0]?.wal_level ?? "unknown",
    publicationExists: publication.rowCount > 0,
    publicationTables: tables.rows.map((row) => row.tablename).sort(),
    slotExists: slotIsOurs,
    slotActive: slotIsOurs ? slotRow.active === true : false,
    slotForeignDatabase: slotRow && !slotIsOurs ? (slotRow.database ?? "unknown") : null,
  };
}

async function probeRisingWave(risingwave: pg.Client, plan: Plan): Promise<RisingWaveProbe> {
  const sources = await risingwave.query<{ name: string }>(
    "SELECT name FROM rw_catalog.rw_sources WHERE name = $1",
    [plan.sourceName],
  );
  const relations = await risingwave.query<{ name: string }>(
    `SELECT name FROM rw_catalog.rw_relations WHERE name IN (${RISINGWAVE_REQUIRED_RELATIONS.map(sqlLiteral).join(", ")})`,
  );
  const present = new Set(relations.rows.map((row) => row.name));
  return {
    sourceExists: sources.rowCount > 0,
    presentRelations: [...present].sort(),
    missingRelations: RISINGWAVE_REQUIRED_RELATIONS.filter((name) => !present.has(name)),
  };
}

function publicationMismatch(plan: Plan, probe: PostgresProbe): string[] {
  const expected = [...RISINGWAVE_PUBLICATION_TABLES].sort();
  if (!probe.publicationExists) return [`publication ${plan.publicationName} does not exist`];
  const actual = probe.publicationTables;
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    const missing = expected.filter((name) => !actual.includes(name));
    const extra = actual.filter((name) => !expected.includes(name));
    return [
      `publication ${plan.publicationName} table set differs: missing ${missing.length} (${missing.slice(0, 5).join(", ") || "-"}), extra ${extra.length} (${extra.slice(0, 5).join(", ") || "-"})`,
    ];
  }
  return [];
}

async function replacePublication(postgres: pg.Client, plan: Plan): Promise<void> {
  await postgres.query(`DROP PUBLICATION IF EXISTS ${sqlIdentifier(plan.publicationName)}`);
  const tables = RISINGWAVE_PUBLICATION_TABLES.map((name) => `public.${sqlIdentifier(name)}`).join(", ");
  await postgres.query(`CREATE PUBLICATION ${sqlIdentifier(plan.publicationName)} FOR TABLE ${tables}`);
}

async function createCdcSource(risingwave: pg.Client, plan: Plan): Promise<void> {
  const sourceSql = [
    `CREATE SOURCE ${sqlIdentifier(plan.sourceName)} WITH (`,
    "  connector = 'postgres-cdc',",
    `  hostname = ${sqlLiteral(plan.cdcHost)},`,
    `  port = ${sqlLiteral(String(plan.cdcPort))},`,
    `  username = ${sqlLiteral(plan.cdcUsername)},`,
    `  password = ${sqlLiteral(plan.cdcPassword)},`,
    `  database.name = ${sqlLiteral(plan.cdcDatabase)},`,
    `  schema.name = ${sqlLiteral(plan.cdcSchema)},`,
    `  slot.name = ${sqlLiteral(plan.slotName)},`,
    `  publication.name = ${sqlLiteral(plan.publicationName)},`,
    "  publication.create.enable = 'false'",
    ")",
  ].join("\n");
  await risingwave.query(sourceSql);
}

async function waitForExactSourceCounts(
  postgres: pg.Client,
  risingwave: pg.Client,
  tables: readonly { upstream: string; name: string }[],
  timeoutMs = 180_000,
): Promise<Record<string, number>> {
  const deadline = Date.now() + timeoutMs;
  let last = "";
  while (Date.now() <= deadline) {
    const counts: Record<string, number> = {};
    const mismatches: string[] = [];
    for (const table of tables) {
      const pgResult = await postgres.query(`SELECT count(*)::text AS count FROM public.${sqlIdentifier(table.upstream)}`);
      const rwResult = await risingwave.query(`SELECT count(*)::text AS count FROM ${sqlIdentifier(table.name)}`);
      const pgCount = Number(pgResult.rows[0]?.count ?? -1);
      const rwCount = Number(rwResult.rows[0]?.count ?? -1);
      counts[table.name] = rwCount;
      if (pgCount !== rwCount) mismatches.push(`${table.name}:${rwCount}/${pgCount}`);
    }
    if (mismatches.length === 0) return counts;
    last = mismatches.join(", ");
    await sleep(500);
  }
  throw new Error(`CDC snapshot count parity timed out (${last || "no readable counts"})`);
}

async function assertRequiredRelations(risingwave: pg.Client): Promise<void> {
  const names = RISINGWAVE_REQUIRED_RELATIONS.map(sqlLiteral).join(", ");
  const result = await risingwave.query<{ name: string }>(
    `SELECT name FROM rw_catalog.rw_relations WHERE name IN (${names})`,
  );
  const present = new Set(result.rows.map((row) => row.name));
  const missing = RISINGWAVE_REQUIRED_RELATIONS.filter((name) => !present.has(name));
  if (missing.length > 0) throw new Error(`RisingWave serving graph is missing: ${missing.join(", ")}`);
}

async function buildGraph(postgres: pg.Client, risingwave: pg.Client, plan: Plan): Promise<{
  statementCount: number;
  sourceCounts: Record<string, number>;
}> {
  await createCdcSource(risingwave, plan);

  const pendingCdcRelations = new Set(RISINGWAVE_CDC_TABLES.map((table) => table.name));
  let sourceCounts: Record<string, number> | null = null;
  for (let index = 0; index < plan.statements.length; index++) {
    const statement = plan.statements[index];
    const started = Date.now();
    try {
      await risingwave.query(statement);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      throw new Error(
        `RisingWave statement ${index + 1}/${plan.statements.length} failed: ${detail}. ` +
          `The chain is partially built; re-run with RAFT_RW_BOOTSTRAP_MODE=recovery and RAFT_RW_BOOTSTRAP_CONFIRM=rebuild to rebuild it.`,
        { cause: error },
      );
    }
    const created = createdRelationName(statement);
    if (created) pendingCdcRelations.delete(created);
    if (pendingCdcRelations.size === 0 && sourceCounts === null) {
      sourceCounts = await waitForExactSourceCounts(postgres, risingwave, RISINGWAVE_CDC_TABLES);
    }
    console.error(`[rw-bootstrap] ${index + 1}/${plan.statements.length} ok (${Date.now() - started}ms)`);
  }
  if (sourceCounts === null) {
    throw new Error(`bootstrap composition did not create CDC relations: ${[...pendingCdcRelations].join(", ")}`);
  }
  await waitForExactSourceCounts(postgres, risingwave, RISINGWAVE_ARTIFACT_CDC_TABLES);
  await assertRequiredRelations(risingwave);
  return { statementCount: plan.statements.length, sourceCounts };
}

async function main(): Promise<void> {
  const plan = readPlan();
  console.error(
    `[rw-bootstrap] mode=${plan.mode} postgres=${plan.postgresUrl.host}${plan.postgresUrl.pathname} ` +
      `risingwave=${plan.risingwaveUrl.host}${plan.risingwaveUrl.pathname} ` +
      `cdc=${plan.cdcHost}:${plan.cdcPort}/${plan.cdcDatabase} source=${plan.sourceName} ` +
      `slot=${plan.slotName} publication=${plan.publicationName} statements=${plan.statements.length}`,
  );

  const postgres = new pg.Client({ connectionString: plan.postgresUrl.toString(), connectionTimeoutMillis: 15_000 });
  const risingwave = new pg.Client({ connectionString: plan.risingwaveUrl.toString(), connectionTimeoutMillis: 15_000 });
  await postgres.connect();
  await risingwave.connect();
  try {
    const pgProbe = await probePostgres(postgres, plan);
    const rwProbe = await probeRisingWave(risingwave, plan);
    const mismatches = publicationMismatch(plan, pgProbe);

    if (plan.mode === "verify") {
      const problems = [
        ...(pgProbe.walLevel === "logical" ? [] : [`Postgres wal_level is ${pgProbe.walLevel}, not logical`]),
        ...(pgProbe.slotForeignDatabase
          ? [
              `slot name ${plan.slotName} is already used by database ${pgProbe.slotForeignDatabase} on this PostgreSQL server (slot names are cluster-wide)`,
            ]
          : []),
        ...mismatches,
        ...(pgProbe.slotExists ? [] : [`replication slot ${plan.slotName} does not exist`]),
        ...(rwProbe.sourceExists ? [] : [`RisingWave source ${plan.sourceName} does not exist`]),
        ...(rwProbe.missingRelations.length === 0
          ? []
          : [`RisingWave serving graph is missing ${rwProbe.missingRelations.length} relation(s)`]),
      ];
      console.log(
        JSON.stringify(
          {
            mode: "verify",
            verdict: problems.length === 0 ? "healthy" : "needs-attention",
            problems,
            postgres: pgProbe,
            risingwave: { sourceExists: rwProbe.sourceExists, relationCount: rwProbe.presentRelations.length, missing: rwProbe.missingRelations },
          },
          null,
          2,
        ),
      );
      if (problems.length > 0) {
        process.exitCode = 1;
      }
      return;
    }

    if (
      plan.mode === "init" &&
      (pgProbe.publicationExists ||
        pgProbe.slotExists ||
        pgProbe.slotForeignDatabase ||
        rwProbe.sourceExists ||
        rwProbe.presentRelations.length > 0)
    ) {
      throw new Refusal(
        "init refuses to touch a chain that already exists",
        [
          "run RAFT_RW_BOOTSTRAP_MODE=verify for a read-only report;",
          pgProbe.slotForeignDatabase
            ? `the slot name ${plan.slotName} already belongs to database ${pgProbe.slotForeignDatabase} on this server; choose another RAFT_RW_BOOTSTRAP slot name (slot names are cluster-wide);`
            : "",
          pgProbe.slotActive
            ? `the replication slot ${plan.slotName} is ACTIVE - something is consuming it; stop that consumer before any rebuild;`
            : "",
          rwProbe.presentRelations.length > 0
            ? `${rwProbe.presentRelations.length} of our relations already exist in RisingWave database ${plan.risingwaveUrl.pathname.replace(/^\//, "")}, so this is not an empty chain;`
            : "",
          rwProbe.sourceExists && rwProbe.missingRelations.length > 0
            ? `if the RisingWave side was lost but Postgres still holds publication ${plan.publicationName} and slot ${plan.slotName}, rebuild with RAFT_RW_BOOTSTRAP_MODE=recovery RAFT_RW_BOOTSTRAP_CONFIRM=rebuild`
            : "",
        ]
          .filter(Boolean)
          .join(" "),
      );
    }

    if (plan.mode !== "verify" && pgProbe.walLevel !== "logical") {
      throw new Refusal(
        `Postgres wal_level is ${pgProbe.walLevel}, not logical — a CDC source cannot work against it`,
        "fix PostgreSQL (wal_level=logical, then restart it) before any rebuild; nothing was written.",
      );
    }

    if (plan.mode === "recovery") {
      if (pgProbe.slotForeignDatabase) {
        throw new Refusal(
          `the slot name ${plan.slotName} belongs to database ${pgProbe.slotForeignDatabase}, not to ${plan.cdcDatabase}`,
          "slot names are cluster-wide; pick another RAFT_RW_BOOTSTRAP slot name rather than dropping a slot this database does not own. Nothing was written.",
        );
      }
      // Drop our own source first: it is the usual reason the slot is active, so deciding before
      // dropping it would block recovery exactly when it is needed. Only after that does an active
      // slot mean somebody else is reading it.
      if (rwProbe.sourceExists) {
        console.error(`[rw-bootstrap] recovery: dropping RisingWave source ${plan.sourceName} CASCADE`);
        await risingwave.query(`DROP SOURCE ${sqlIdentifier(plan.sourceName)} CASCADE`);
      }
      if (pgProbe.slotExists) {
        let stillActive = false;
        const deadline = Date.now() + 30_000;
        while (Date.now() < deadline) {
          const after = await probePostgres(postgres, plan);
          stillActive = after.slotActive;
          if (!stillActive) break;
          await sleep(1_000);
        }
        if (stillActive) {
          throw new Refusal(
            `the replication slot ${plan.slotName} is still active after our RisingWave source was dropped`,
            "something else is reading that slot - stop it (or the container) first, then re-run recovery; nothing else was written.",
          );
        }
        console.error(`[rw-bootstrap] recovery: dropping replication slot ${plan.slotName} (database ${plan.cdcDatabase})`);
        await postgres.query("SELECT pg_drop_replication_slot($1)", [plan.slotName]);
      }
    }

    if (plan.mode === "init") {
      // init only ever runs on an empty chain; the probe above proved there is no publication,
      // no slot and no source to preserve.
      await replacePublication(postgres, plan);
    } else if (mismatches.length > 0) {
      console.error(`[rw-bootstrap] recovery: rebuilding publication ${plan.publicationName}: ${mismatches.join("; ")}`);
      await replacePublication(postgres, plan);
    } else {
      console.error(`[rw-bootstrap] ${plan.mode}: publication ${plan.publicationName} already matches, reused`);
    }

    const built = await buildGraph(postgres, risingwave, plan);
    console.log(
      JSON.stringify(
        {
          ok: true,
          mode: plan.mode,
          source: plan.sourceName,
          publication: plan.publicationName,
          slot: plan.slotName,
          statementCount: built.statementCount,
          publicationTableCount: RISINGWAVE_PUBLICATION_TABLES.length,
          cdcTableCount: RISINGWAVE_CDC_TABLES.length + RISINGWAVE_ARTIFACT_CDC_TABLES.length,
          relationCount: RISINGWAVE_REQUIRED_RELATIONS.length,
          sourceCounts: built.sourceCounts,
        },
        null,
        2,
      ),
    );
  } finally {
    await Promise.allSettled([postgres.end(), risingwave.end()]);
  }
}

main().catch((error) => {
  const secretValues = [
    process.env.DATABASE_URL,
    process.env.RISINGWAVE_DATABASE_URL,
    process.env.RISINGWAVE_CDC_PASSWORD,
  ].filter((value): value is string => Boolean(value));
  const redact = (text: string) => secretValues.reduce((acc, secret) => acc.replaceAll(secret, "<redacted>"), text);

  if (error instanceof Refusal) {
    console.error(`[rw-bootstrap] refused: ${redact(error.message)}`);
    console.error(`[rw-bootstrap] next action: ${redact(error.action)}`);
  } else {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`RisingWave production bootstrap failed: ${redact(message)}`);
  }
  process.exit(1);
});
