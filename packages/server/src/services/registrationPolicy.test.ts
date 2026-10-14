import assert from "node:assert/strict";

import {
  getRegistrationBlockedReason,
  isRegistrationEnabled,
} from "./registrationPolicy";

// The deployment switch must be opt-in: upstream behaviour is the default, and only an explicit,
// recognised value may change it. "Default behaviour unchanged" is the property that keeps this patch
// cheap to carry across upstream releases, so it is asserted rather than assumed — including the
// fail-closed case, where a typo in a deployment config must not silently reopen public sign-up.

const KEY = "RAFT_REGISTRATION_POLICY";
const original = process.env[KEY];

afterEach(() => {
  if (original === undefined) {
    delete process.env[KEY];
  } else {
    process.env[KEY] = original;
  }
});

describe("registration policy", () => {
  it("keeps upstream behaviour when the variable is unset", () => {
    delete process.env[KEY];
    assert.equal(getRegistrationBlockedReason(), null);
    assert.equal(isRegistrationEnabled(), true);
  });

  it("keeps upstream behaviour for an explicitly open policy", () => {
    for (const value of ["open", "OPEN", "  open  "]) {
      process.env[KEY] = value;
      assert.equal(getRegistrationBlockedReason(), null, `value ${JSON.stringify(value)}`);
      assert.equal(isRegistrationEnabled(), true, `value ${JSON.stringify(value)}`);
    }
  });

  it("treats an empty value as unset", () => {
    process.env[KEY] = "   ";
    assert.equal(getRegistrationBlockedReason(), null);
  });

  it("blocks registration when closed", () => {
    process.env[KEY] = "closed";
    assert.match(getRegistrationBlockedReason() ?? "", /closed/i);
    assert.equal(isRegistrationEnabled(), false);
  });

  it("fails closed on an unrecognised value instead of reopening", () => {
    process.env[KEY] = "opne";
    const reason = getRegistrationBlockedReason() ?? "";
    assert.match(reason, /closed/i);
    assert.equal(isRegistrationEnabled(), false);
  });
});
