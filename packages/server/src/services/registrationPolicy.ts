/**
 * Registration policy for this deployment.
 *
 * An operator can close public sign-up without touching code:
 *
 *   RAFT_REGISTRATION_POLICY=closed        -> POST /api/auth/register is refused, always (unchanged meaning)
 *   RAFT_REGISTRATION_POLICY=invite-only    -> refused unless the request redeems a VALID invite
 *   RAFT_REGISTRATION_POLICY=open           -> upstream behaviour (registration allowed)
 *   unset                                   -> upstream behaviour (registration allowed)
 *
 * An unrecognised value is treated as closed on purpose: a typo in a deployment config must not
 * silently reopen the door.
 *
 * `invite-only` is deliberately a SEPARATE mode rather than a relaxation of `closed` (@Dassai
 * 2026-10-15): `closed` has to keep meaning "no new accounts, period", or an operator who chose it
 * would silently gain a second, narrower door. The narrower door also is not this module's business
 * to open on its own — the caller must have RESOLVED the invite (still pending, not expired, email
 * matching) and redeem it in the same transaction as account creation, which is why this module only
 * takes a boolean about that check and never a token.
 *
 * This guards HUMAN account creation only. `assertRegistrationEnabled` is called from the social
 * account-creation paths (createSocialUser and the social-linking branch) and nowhere else; the
 * email/password route checks the policy itself, and creating agents or computers is unaffected.
 */
export type RegistrationPolicyMode = "open" | "closed" | "invite-only";

export function registrationPolicyMode(env: NodeJS.ProcessEnv = process.env): RegistrationPolicyMode {
  const raw = env.RAFT_REGISTRATION_POLICY?.trim().toLowerCase();
  if (raw === undefined || raw === "") return "open";
  if (raw === "open" || raw === "closed" || raw === "invite-only") return raw;
  return "closed";
}

export function getRegistrationBlockedReason(
  options: { hasValidInvite?: boolean } = {},
): string | null {
  switch (registrationPolicyMode()) {
    case "open":
      return null;
    case "invite-only":
      return options.hasValidInvite
        ? null
        : "Registration on this instance is by invitation only. Ask the operator for an invite link.";
    case "closed":
      return "Registration is closed on this instance. Ask the operator to open it temporarily.";
    default: {
      // Unreachable while registrationPolicyMode maps unknown values to "closed"; kept so a future
      // mode cannot fall through to "allowed" by accident.
      const raw = process.env.RAFT_REGISTRATION_POLICY?.trim().toLowerCase();
      return `RAFT_REGISTRATION_POLICY must be "open", "closed" or "invite-only" (got "${raw}"); treating it as closed.`;
    }
  }
}

/** Public sign-up, from the outside: true only when anyone may register without an invite. */
export function isRegistrationEnabled(): boolean {
  return registrationPolicyMode() === "open";
}

export function assertRegistrationEnabled(): void {
  const blockedReason = getRegistrationBlockedReason();
  if (blockedReason) {
    throw new Error(blockedReason);
  }
}
