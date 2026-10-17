import { useCallback, useEffect, useState } from "react";
import { Button } from "raft-ui";
import { useIntl } from "react-intl";
import api from "../../api/client";
import { useAppNavigate } from "../../hooks/useAppNavigate";
import { useServerPermissions } from "../../hooks/useServerPermissions";
import { useChannelStore } from "../../store/channelStore";
import { useServerStore } from "../../store/serverStore";
import { jointInviteAcceptErrorMessage } from "../../utils/jointChannelLimit";
import ChannelBanner from "../ui/Banner";
import { showToast } from "../toastBridge";

/**
 * Pending Joint Channel invites, on the sidebar of the server that was invited.
 *
 * Why this exists (@leocc, 2026-10-17): invites are delivered by EMAIL ONLY, and an instance without a
 * mail transport (ours) writes them into the server log instead — so the invited side never learned an
 * invite existed, and the only way in was a link copied out of that log. The API for the recipient side
 * (`GET /api/channels/joint-invites`, already scoped to "my pending invites for the selected server",
 * admins/owners only) has existed all along; nothing in the web called it.
 *
 * Deliberately narrow: it renders nothing when there is nothing to accept, it does not resend or dismiss
 * (an invite disappears by being accepted or by expiring), and it does not touch the accept rules — the
 * server still requires the invited user themself plus owner/admin on the target server.
 */
interface PendingJointInvite {
  id: string;
  channelName: string;
  fromServerName: string;
  fromServerSlug?: string | null;
  expiresAt?: string | null;
}

export default function JointInviteInbox() {
  const { formatMessage } = useIntl();
  const nav = useAppNavigate();
  const serverId = useServerStore((state) => state.current?.id);
  const { capabilities } = useServerPermissions();
  // The invite is addressed to a person, but only owners/admins of the invited server may accept one
  // (channelService.acceptJointChannelInvite), and the endpoint returns nothing for anyone else — so do
  // not even ask when this viewer could not act on the answer.
  const canAccept = capabilities.federateChannels;
  const [invites, setInvites] = useState<PendingJointInvite[]>([]);
  const [acceptingId, setAcceptingId] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!serverId || !canAccept) {
      setInvites([]);
      return;
    }
    try {
      const { data } = await api.get("/channels/joint-invites");
      const rows: unknown = data?.invites;
      setInvites(Array.isArray(rows) ? (rows as PendingJointInvite[]) : []);
    } catch {
      // A sidebar must not break because this poll failed; the next server switch retries.
      setInvites([]);
    }
  }, [serverId, canAccept]);

  useEffect(() => {
    void load();
  }, [load]);

  const accept = useCallback(async (invite: PendingJointInvite) => {
    setAcceptingId(invite.id);
    try {
      const { data } = await api.post(`/channels/joint-invites/${encodeURIComponent(invite.id)}/accept`);
      setInvites((prev) => prev.filter((row) => row.id !== invite.id));
      const channelId = typeof data?.id === "string" ? data.id : null;
      if (channelId) {
        await useChannelStore.getState().ensureChannel(channelId);
        nav.toChannel(channelId);
      }
    } catch (err) {
      // 409 means expired / already taken / already a member: reconcile instead of insisting.
      showToast({ title: jointInviteAcceptErrorMessage(err, formatMessage), type: "error" });
      await load();
    } finally {
      setAcceptingId(null);
    }
  }, [formatMessage, load, nav]);

  if (invites.length === 0) return null;

  return (
    <div className="mb-2 space-y-2" data-testid="joint-invite-inbox">
      {/* One banner per invite, and the accept button goes in the banner's ACTION slot: a control-size
          Button inside the description is the shape scripts/check-banner-inline-action.mjs exists to
          stop (a sm Button squatting in a sentence, inflating the banner and wrapping on narrow
          viewports). The title is rendered once, on the first invitation. */}
      {invites.map((invite, index) => (
        <ChannelBanner
          key={invite.id}
          intent="info"
          density="sm"
          title={index === 0 ? formatMessage({ id: "channel.joint.pendingInvitesTitle" }) : undefined}
          actions={
            <Button
              size="sm"
              type="button"
              disabled={acceptingId === invite.id}
              onClick={() => void accept(invite)}
              data-testid="joint-invite-accept"
            >
              {formatMessage({ id: "channel.joint.accept" })}
            </Button>
          }
        >
          {formatMessage(
            { id: "channel.joint.pendingInvitesBody" },
            { server: invite.fromServerName, channel: invite.channelName },
          )}
        </ChannelBanner>
      ))}
    </div>
  );
}
