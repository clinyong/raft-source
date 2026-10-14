/**
 * Registration policy for this deployment.
 *
 * An operator can close public sign-up without touching code:
 *
 *   RAFT_REGISTRATION_POLICY=closed   -> POST /api/auth/register is refused
 *   RAFT_REGISTRATION_POLICY=open     -> upstream behaviour (registration allowed)
 *   unset                             -> upstream behaviour (registration allowed)
 *
 * An unrecognised value is treated as closed on purpose: a typo in a deployment config must not
 * silently reopen the door.
 *
 * This guards HUMAN account creation only. `assertRegistrationEnabled` is called from the account
 * creation paths (createSocialUser and the social-linking branch) and nowhere else, so creating
 * agents or computers is unaffected.
 */
export function getRegistrationBlockedReason(): string | null {
  const raw = process.env.RAFT_REGISTRATION_POLICY?.trim().toLowerCase();
  if (raw === undefined || raw === "" || raw === "open") {
    return null;
  }
  if (raw === "closed") {
    return "Registration is closed on this instance. Ask the operator to open it temporarily.";
  }
  return `RAFT_REGISTRATION_POLICY must be "open" or "closed" (got "${raw}"); treating it as closed.`;
}

export function isRegistrationEnabled(): boolean {
  return getRegistrationBlockedReason() === null;
}

export function assertRegistrationEnabled(): void {
  const blockedReason = getRegistrationBlockedReason();
  if (blockedReason) {
    throw new Error(blockedReason);
  }
}
