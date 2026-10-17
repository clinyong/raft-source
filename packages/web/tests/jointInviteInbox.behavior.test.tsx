import assert from "node:assert/strict";
import "./helpers/domSetup";
import type { ReactNode } from "react";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import JointInviteInbox from "../src/components/channel/JointInviteInbox";
import api from "../src/api/client";
import { IntlProviderWrapper } from "../src/i18n/IntlProviderWrapper";
import { LocaleProvider } from "../src/i18n/LocaleProvider";
import { useChannelStore } from "../src/store/channelStore";
import { useServerStore } from "../src/store/serverStore";

/**
 * Behavior of the sidebar's pending-Joint-Channel-invite notice.
 *
 * This exists because invites are email-only and an instance without a mail transport (ours) drops them
 * into the server log: the invited side never saw them (2026-10-17). The endpoint it consumes
 * (`GET /channels/joint-invites`) is scoped server-side to "my pending invites for the selected server",
 * open to owners/admins; these cases pin the client half: don't ask when the viewer cannot act, show what
 * arrives, accept through the existing endpoint, and behave when the accept is refused.
 */
const originalGet = api.get;
const originalPost = api.post;
const originalEnsureChannel = useChannelStore.getState().ensureChannel;

afterEach(() => {
  cleanup();
  api.get = originalGet;
  api.post = originalPost;
  useChannelStore.setState({ ensureChannel: originalEnsureChannel } as never);
  useServerStore.setState({ current: null, servers: [], members: [] } as never);
});

function providers(children: ReactNode) {
  return (
    <LocaleProvider>
      <IntlProviderWrapper>{children}</IntlProviderWrapper>
    </LocaleProvider>
  );
}

function LocationProbe() {
  const location = useLocation();
  return <div data-testid="location">{location.pathname}</div>;
}

function renderInbox() {
  return render(
    providers(
      <MemoryRouter initialEntries={["/s/leo-lab"]}>
        <JointInviteInbox />
        <LocationProbe />
      </MemoryRouter>,
    ),
  );
}

function setRole(role: "owner" | "admin" | "member") {
  useServerStore.setState({
    current: { id: "leo-lab-id", name: "leo-lab", slug: "leo-lab", role },
    servers: [],
    members: [],
  } as never);
}

function invite(id: string, server: string, channel: string) {
  return { id, fromServerName: server, fromServerSlug: server, channelName: channel, expiresAt: null };
}

test("a member is never asked: the endpoint is not called and nothing renders", async () => {
  setRole("member");
  let called = 0;
  api.get = (async () => {
    called += 1;
    return { data: { invites: [invite("inv-1", "hotfree", "watermark-app")] } };
  }) as typeof api.get;

  renderInbox();

  await waitFor(() => assert.equal(screen.queryByTestId("joint-invite-inbox"), null));
  assert.equal(called, 0, "a member must not poll an endpoint that only returns their own pending invites");
});

test("a pending invite is shown for an admin and accepted through the existing endpoint", async () => {
  setRole("admin");
  const gets: string[] = [];
  api.get = (async (url: string) => {
    gets.push(url);
    return { data: { invites: [invite("inv-1", "hotfree", "watermark-app")] } };
  }) as typeof api.get;
  let posted: string | null = null;
  api.post = (async (url: string) => {
    posted = url;
    return { data: { id: "chan-1" } };
  }) as typeof api.post;
  const ensured: string[] = [];
  useChannelStore.setState({
    ensureChannel: async (channelId: string) => {
      ensured.push(channelId);
      return null;
    },
  } as never);

  renderInbox();

  assert.deepEqual(gets, ["/channels/joint-invites"]);
  const banner = await screen.findByTestId("joint-invite-inbox");
  assert.ok(banner.textContent?.includes("hotfree"), "the inviting server is named");
  assert.ok(banner.textContent?.includes("watermark-app"), "the channel is named");

  fireEvent.click(screen.getByTestId("joint-invite-accept"));

  await waitFor(() => assert.equal(posted, "/channels/joint-invites/inv-1/accept"));
  await waitFor(() => assert.equal(screen.queryByTestId("joint-invite-inbox"), null));
  assert.deepEqual(ensured, ["chan-1"], "the accepted channel is loaded into the store");
  await waitFor(() => assert.equal(screen.getByTestId("location").textContent, "/s/leo-lab/channel/chan-1"));
});

test("a refused accept keeps the invite and reconciles the list", async () => {
  setRole("admin");
  let gets = 0;
  api.get = (async () => {
    gets += 1;
    return { data: { invites: [invite("inv-1", "hotfree", "watermark-app")] } };
  }) as typeof api.get;
  api.post = (async () => {
    throw { response: { status: 409, data: { error: "Joint channel invite expired" } } };
  }) as typeof api.post;

  renderInbox();
  assert.ok(await screen.findByTestId("joint-invite-inbox"));

  fireEvent.click(screen.getByTestId("joint-invite-accept"));

  await waitFor(() => assert.equal(gets, 2, "the list is re-read after a refusal"));
  assert.ok(screen.getByTestId("joint-invite-inbox"), "the invite is not dropped on a refusal");
  assert.equal(screen.getByTestId("joint-invite-accept").hasAttribute("disabled"), false);
});

test("two invites offer two accepts, and the second one posts its own id", async () => {
  setRole("owner");
  api.get = (async () => ({
    data: {
      invites: [
        invite("inv-1", "hotfree", "watermark-app"),
        invite("inv-2", "clara-lab", "storage"),
      ],
    },
  })) as typeof api.get;
  const posted: string[] = [];
  api.post = (async (url: string) => {
    posted.push(url);
    return { data: {} };
  }) as typeof api.post;

  renderInbox();
  const buttons = await screen.findAllByTestId("joint-invite-accept");
  assert.equal(buttons.length, 2);

  fireEvent.click(buttons[1]);

  await waitFor(() => assert.deepEqual(posted, ["/channels/joint-invites/inv-2/accept"]));
  await waitFor(() => assert.equal(screen.queryAllByTestId("joint-invite-accept").length, 1));
});
