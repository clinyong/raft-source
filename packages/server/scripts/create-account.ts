/**
 * Create ONE human account by hand — and/or add an existing account to a server.
 *
 * Why this exists (leo-lab, 2026-10-15): the operator wants to give a friend access while public
 * registration stays closed. Building invitation-based registration was scoped, then dropped in favour of
 * a one-off manual action, so this is a tool for an OPERATOR, not a product surface: it has no HTTP route,
 * it must run on the server, and it refuses to guess anything.
 *
 * It reuses the application's own code paths instead of re-implementing them in SQL:
 *   - `userService.createUser`  -> the same argon2 password hashing, handle rules and uniqueness checks the
 *                                  register route performs (called with `deferProfileSetup: true`, exactly as
 *                                  that route does, so the new person picks their own handle/display name at
 *                                  first login);
 *   - `serverService.addMember` -> the same join path the invite and community flows use, including the
 *                                  server's self-serve agreement (if the server has an active one, its id must
 *                                  be supplied explicitly — this tool never bypasses an agreement).
 * A hand-rolled INSERT would drift from those rules silently, and the drift would show up as a login that
 * almost works.
 *
 * Modes (one must be chosen explicitly; an accidental invocation writes nothing):
 *   --create            create the account (requires --email and a password source)
 *   --add-existing      add an already-registered account to a server (requires --email and --server)
 *   --dry-run           do everything except the writes, and print what would happen
 *
 * Usage:
 *   node --import @oxc-node/core/register scripts/create-account.ts --create \
 *     --email friend@example.com [--name handle] [--verified] (--password-stdin | --generate-password) \
 *     [--server <slug|uuid> --role member|guest] [--dry-run] [--print-password | --password-out <file>]
 *
 *   node --import @oxc-node/core/register scripts/create-account.ts --add-existing \
 *     --email friend@example.com --server <slug|uuid> [--role member|guest] [--dry-run]
 *
 * Notes for whoever runs it:
 *   - the password NEVER comes from the command line (argv is visible in `ps`): pipe it with
 *     `--password-stdin` or let the tool generate one. A GENERATED password is never written to a
 *     non-interactive stdout by accident: when stdout is not a terminal you must ask for it explicitly with
 *     `--print-password`, or have it written to a 0600 file with `--password-out <file>` — otherwise the file
 *     you were logging to becomes the password's home (task #22 review point);
 *   - `--verified` marks the mailbox verified. There is no mail transport on this instance, so a
 *     verification link cannot be delivered; the operator asserts the address instead. Without it the
 *     account exists but stays unverified, and several routes require verification;
 *   - the first login is where the new person sets their handle; they should change the initial password
 *     right after (there is a change-password flow in Settings).
 */
import { randomBytes } from "node:crypto";
import { createInterface } from "node:readline";

import { eq } from "drizzle-orm";

import { closeDatabase, getDb, initDatabase } from "../src/db/index";
import { users } from "../src/db/schema";
import { normalizeEmail } from "../src/services/emailNormalization";
import * as serverService from "../src/services/serverService";
import * as serverAgreementService from "../src/services/serverAgreementService";
import * as userService from "../src/services/userService";
import { CURRENT_LEGAL_ACCEPTANCE } from "@botiverse/raft-shared";

type Mode = "create" | "add-existing";

interface ParsedArgs {
  mode: Mode;
  email: string;
  name: string | null;
  server: string | null;
  role: "member" | "guest";
  passwordStdin: boolean;
  generatePassword: boolean;
  printPassword: boolean;
  passwordOut: string | null;
  verified: boolean;
  dryRun: boolean;
  agreementId: string | null;
}

const USAGE = `usage:
  create-account.ts --create --email <address> [--name <handle>] [--verified] (--password-stdin | --generate-password) [--server <slug|uuid>] [--role member|guest] [--dry-run]
  create-account.ts --add-existing --email <address> --server <slug|uuid> [--role member|guest] [--agreement-id <id>] [--dry-run]`;

function fail(message: string, code = 1): never {
  console.error(`[create-account] REFUSED: ${message}`);
  process.exit(code);
}

function parseArgs(argv: string[]): ParsedArgs {
  const args: Record<string, string | boolean> = {};
  const takesValue = new Set(["--email", "--name", "--server", "--role", "--agreement-id", "--password-out"]);
  for (let i = 0; i < argv.length; i += 1) {
    const flag = argv[i];
    if (!flag.startsWith("--")) fail(`unexpected argument "${flag}"\n\n${USAGE}`, 2);
    if (takesValue.has(flag)) {
      const value = argv[i + 1];
      if (value === undefined || value.startsWith("--")) fail(`${flag} needs a value\n\n${USAGE}`, 2);
      args[flag] = value;
      i += 1;
    } else {
      args[flag] = true;
    }
  }
  const unknown = Object.keys(args).filter(
    (k) => !["--create", "--add-existing", "--dry-run", "--email", "--name", "--server", "--role", "--password-stdin", "--generate-password", "--verified", "--agreement-id", "--print-password", "--password-out"].includes(k),
  );
  if (unknown.length > 0) fail(`unknown flag(s): ${unknown.join(", ")}\n\n${USAGE}`, 2);

  const wantsCreate = args["--create"] === true;
  const wantsAdd = args["--add-existing"] === true;
  if (wantsCreate === wantsAdd) fail(`choose exactly one of --create / --add-existing\n\n${USAGE}`, 2);
  const mode: Mode = wantsCreate ? "create" : "add-existing";

  const email = typeof args["--email"] === "string" ? args["--email"].trim() : "";
  if (!email) fail(`--email is required\n\n${USAGE}`, 2);
  if (!email.includes("@")) fail(`--email does not look like an address: ${JSON.stringify(email)}`, 2);

  const role = typeof args["--role"] === "string" ? args["--role"] : "member";
  if (role !== "member" && role !== "guest") fail(`--role must be member or guest (got "${role}")`, 2);

  if (mode === "add-existing" && typeof args["--server"] !== "string") {
    fail(`--add-existing needs --server (which server to join)`, 2);
  }
  if (mode === "add-existing" && (args["--password-stdin"] || args["--generate-password"] || args["--verified"])) {
    fail(`--add-existing does not create an account: --password-stdin/--generate-password/--verified make no sense with it`, 2);
  }

  const passwordStdin = args["--password-stdin"] === true;
  const generatePassword = args["--generate-password"] === true;
  if (mode === "create" && passwordStdin === generatePassword) {
    fail(`choose exactly one way to set the initial password: --password-stdin (for a private channel) or --generate-password\n\n${USAGE}`, 2);
  }
  // A generated password must not quietly become one more line in a log file (review point, task #22): only
  // surfaces it when the operator asked for it, or when a human is actually watching the terminal.
  const printPassword = args["--print-password"] === true;
  const passwordOut = typeof args["--password-out"] === "string" ? args["--password-out"] : null;
  if (generatePassword && !printPassword && !passwordOut && !process.stdout.isTTY) {
    fail(
      "refusing to print a generated password to a non-interactive stdout: it would land in whatever you are redirecting to. " +
        "Add --print-password if that is really what you want, or --password-out <file> (written 0600). " +
        "Alternatively supply the password yourself with --password-stdin.",
    );
  }
  if (passwordOut && mode !== "create") fail("--password-out only applies with --create", 2);

  return {
    mode,
    email,
    name: typeof args["--name"] === "string" ? args["--name"].trim() : null,
    server: typeof args["--server"] === "string" ? args["--server"] : null,
    role,
    passwordStdin,
    generatePassword,
    printPassword: args["--print-password"] === true,
    passwordOut: typeof args["--password-out"] === "string" ? args["--password-out"] : null,
    verified: args["--verified"] === true,
    dryRun: args["--dry-run"] === true,
    agreementId: typeof args["--agreement-id"] === "string" ? args["--agreement-id"] : null,
  };
}

async function readPasswordFromStdin(): Promise<string> {
  if (process.stdin.isTTY) {
    fail("--password-stdin was given but stdin is a terminal; pipe the password in (echo -n '…' | …)");
  }
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8").replace(/\r?\n$/, "");
}

function generatePassword(): string {
  // 24 url-safe characters: enough that a first login is not the weak link, short enough to read out loud
  // once. It is printed a single time and is meant to be handed over privately and then changed.
  return randomBytes(18).toString("base64url");
}

function buildLegalAcceptance() {
  // The register route requires the current terms/privacy versions; this tool records the same values, with
  // the operator asserting the friend accepts them (the friend has no way to click through before the account
  // exists). Same helper, same versions — not a hand-written copy.
  return {
    acceptTerms: true,
    termsVersion: CURRENT_LEGAL_ACCEPTANCE.termsVersion,
    privacyVersion: CURRENT_LEGAL_ACCEPTANCE.privacyVersion,
  };
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));
  // A script has to open the connection the server opens at startup; without this every `getDb()` call throws
  // "Database not initialized" (found the first time this was run against the isolated database).
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) fail("DATABASE_URL is not set — run this against the instance's database on purpose");
  await initDatabase(databaseUrl);
  const db = getDb();
  const normalizedEmail = normalizeEmail(args.email);

  // Every lookup first, every write after: a bad parameter must leave the database untouched.
  const [existing] = await db.select({ id: users.id, emailVerified: users.emailVerified }).from(users).where(eq(users.email, normalizedEmail));

  let targetServer: { id: string; name: string; slug: string } | null = null;
  if (args.server) {
    // `getServer` takes a uuid, so only try it when the argument IS one: passing a non-uuid made Postgres raise a
    // cast error and the operator saw a raw query failure instead of "no server matches" (found by running the
    // refusal paths, not by reading them).
    const looksLikeUuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(args.server);
    const found = (await serverService.getServerBySlug(args.server)) ?? (looksLikeUuid ? await serverService.getServer(args.server) : null);
    if (!found) fail(`no server matches --server ${JSON.stringify(args.server)} (tried slug${looksLikeUuid ? ", then id" : "; pass a uuid if you meant an id"})`);
    targetServer = { id: found.id, name: found.name, slug: found.slug };
  }

  let agreement: { id: string } | null = null;
  if (targetServer && args.mode === "add-existing") {
    const active = await db.transaction((tx) => serverAgreementService.getActiveAgreement(targetServer!.id, tx));
    if (active) {
      if (!args.agreementId) {
        fail(
          `server "${targetServer.name}" has an active agreement (id ${active.id}); pass --agreement-id ${active.id} to record that the joiner accepts it (this tool never bypasses an agreement)`,
        );
      }
      if (args.agreementId !== active.id) fail(`--agreement-id ${args.agreementId} is not the active agreement (${active.id})`);
      agreement = { id: active.id };
    }
  }

  if (args.mode === "create" && existing) {
    fail(
      `an account for ${normalizedEmail} already exists (id ${existing.id}); to only add it to a server, use --add-existing`,
    );
  }
  if (args.mode === "add-existing" && !existing) {
    fail(`no account for ${normalizedEmail}; create it first with --create`);
  }

  const password = args.mode === "create"
    ? (args.passwordStdin ? await readPasswordFromStdin() : generatePassword())
    : null;
  if (args.mode === "create" && password !== null && password.length < 8) {
    fail("the password is shorter than 8 characters, which the register route itself refuses");
  }

  console.log(`[create-account] mode=${args.mode} email=${normalizedEmail}${args.server ? ` server=${targetServer!.slug} role=${args.role}` : ""}${args.dryRun ? " (dry run: nothing will be written)" : ""}`);
  if (existing) console.log(`[create-account] account already present: id=${existing.id} verified=${existing.emailVerified}`);
  if (agreement) console.log(`[create-account] the server has an active agreement; recording id=${agreement.id}`);

  let userId = existing?.id ?? null;

  if (args.mode === "create") {
    if (args.dryRun) {
      console.log(`[create-account] WOULD create the account (name=${args.name ?? "<deferred to first login>"}, verified=${args.verified})`);
    } else {
      const created = await userService.createUser(
        normalizedEmail,
        password!,
        args.name,
        buildLegalAcceptance(),
        { ipAddress: null, userAgent: "create-account.ts (operator)", locale: null },
        "signup",
        { deferProfileSetup: true },
      );
      userId = created.id;
      console.log(`[create-account] created account id=${userId}`);
      if (args.verified) {
        // No mail transport is configured on this instance, so no verification link can be delivered. Setting
        // the flag here is the operator vouching for the address; stated out loud rather than implied.
        await db.update(users).set({ emailVerified: true }).where(eq(users.id, userId));
        console.log(`[create-account] marked ${normalizedEmail} as verified (operator vouches; no mail channel exists)`);
      }
    }
  }

  if (targetServer) {
    // Test seam: prove the failure reporting for the one window that matters — the account exists and the join
    // did not happen (@Dassai 2026-10-15 asked for exactly that evidence). It needs TWO explicit opt-ins, the
    // same shape as the cutover gate's seams (`RAFT_CUTOVER_TEST_SEAMS`), so no single stray variable in a real
    // run can inject a failure into an operator's account creation.
    const seamsOn = process.env.RAFT_CREATE_ACCOUNT_TEST_SEAMS === "1";
    if (seamsOn && process.env.RAFT_CREATE_ACCOUNT_TEST_FAIL_BEFORE_JOIN === "1") {
      throw new Error("test seam: failing after the account was created and before the server join");
    }
    const alreadyMember = userId ? await serverService.isMember(targetServer.id, userId) : false;
    if (args.dryRun) {
      console.log(`[create-account] WOULD add ${alreadyMember ? "(already a member — nothing to do)" : "as " + args.role} to server "${targetServer.name}" (${targetServer.slug})`);
    } else if (alreadyMember) {
      console.log(`[create-account] already a member of "${targetServer.name}"; nothing to do`);
    } else {
      await db.transaction(async (tx) => {
        const agreementInput = agreement ? { agreementId: agreement.id } : undefined;
        await serverAgreementService.requireSelfServeAgreement(tx, targetServer!.id, agreementInput);
        await serverService.addMember(targetServer!.id, userId!, args.role, {
          executor: tx,
          agreementAudit: agreement
            ? {
                actorUserId: userId!,
                // "admin-add" is the source the audit trail already reserves for exactly this: a privileged
                // operator adding somebody, rather than the person joining through invite/join/request-access.
                source: "admin-add" as const,
                agreementId: agreement.id,
                ipAddress: null,
                userAgent: "create-account.ts (operator)",
              }
            : undefined,
        });
      });
      console.log(`[create-account] added id=${userId} to "${targetServer.name}" as ${args.role}`);
    }
  }

  if (args.mode === "create" && !args.dryRun) {
    if (args.passwordOut) {
      // 0600 and no copy in the run's output: whoever needs the password reads that file, and it never
      // travels through a shell history or a log collector.
      const { writeFileSync, chmodSync } = await import("node:fs");
      writeFileSync(args.passwordOut, `${password}\n`, { mode: 0o600 });
      chmodSync(args.passwordOut, 0o600);
      console.log(`[create-account] initial password written to ${args.passwordOut} (mode 0600) — hand it over, then delete the file`);
    } else if (args.printPassword || process.stdout.isTTY) {
      console.log("[create-account] ---------------------------------------------------------------");
      console.log(`[create-account] hand over privately (NOT in a chat channel, NOT into a shared log):`);
      console.log(`[create-account]   address : ${normalizedEmail}`);
      console.log(`[create-account]   password: ${password}`);
      console.log("[create-account] the first login is where they set their handle; they should change this password in Settings afterwards.");
      console.log("[create-account] ---------------------------------------------------------------");
    } else {
      fail("the account was created, but no way to surface the password was chosen: re-run with --print-password or --password-out");
    }
  }
  process.exit(0);
}

main().catch(async (error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[create-account] FAILED: ${message}`);
  // Say exactly what state the database is in, and how to finish by hand: "it failed" is not enough when the
  // failure can happen after the account row exists (@Dassai 2026-10-15 — the account must be reportable and
  // completable with --add-existing, without turning this into a bigger tool).
  const email = process.argv.find((a) => a.includes("@")) ?? "";
  if (email) {
    try {
      const db = getDb();
      const normalized = normalizeEmail(email);
      const [row] = await db.select({ id: users.id, emailVerified: users.emailVerified }).from(users).where(eq(users.email, normalized));
      if (row) {
        console.error(`[create-account] the account EXISTS: id=${row.id} email=${normalized} verified=${row.emailVerified}`);
        console.error('[create-account] to finish the server join by hand, re-run with: --add-existing --email ' + normalized + ' --server <slug|uuid> [--role member|guest] [--agreement-id <id>]');
      } else {
        console.error(`[create-account] no account row exists for ${normalized}; nothing was created`);
      }
    } catch {
      console.error('[create-account] could not read the account state back; check the database by hand');
    }
    await closeDatabase().catch(() => undefined);
  }
  process.exit(1);
});
