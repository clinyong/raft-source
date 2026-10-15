/**
 * Validate the configuration this process would run with — without starting the service.
 *
 * Why (leo-lab, 2026-10-15, @Dassai): the login rate limit refuses to start the server when its value is
 * unusable, which is the right failure for the SERVER. It would be the wrong failure for a DEPLOY: the update
 * would stop the old instance and only then discover the typo. So the deploy step runs THIS script against the
 * candidate configuration FIRST, in the candidate image, and abandons the update if it fails — the old instance
 * keeps serving. It must use the same validation the server uses, or the two drift apart, so it imports the
 * same function rather than repeating the rule.
 *
 * Exit code 0 = the configuration is usable; 1 = refuse (the message says why); 2 = the validator itself broke.
 */
import { loginRateLimitConfig } from "../src/app";

function main(): number {
  const checks: Array<{ name: string; run: () => void }> = [
    {
      name: "RAFT_AUTH_LOGIN_RATE_LIMIT_MAX / _WINDOW_MS",
      run: () => {
        const limit = loginRateLimitConfig(process.env);
        if (limit) {
          console.log(
            `[validate-config] login rate limit: ${limit.max} requests per ${limit.windowMs} ms per client`,
          );
        } else {
          console.log("[validate-config] login rate limit: not configured (upstream behaviour)");
        }
      },
    },
  ];

  let failed = 0;
  for (const check of checks) {
    try {
      check.run();
    } catch (error) {
      failed += 1;
      console.error(`[validate-config] REFUSED: ${check.name}: ${(error as Error).message}`);
    }
  }
  if (failed > 0) {
    console.error("[validate-config] the candidate configuration is not usable — the running instance must be left alone");
    return 1;
  }
  console.log("[validate-config] OK");
  return 0;
}

process.exit(main());
