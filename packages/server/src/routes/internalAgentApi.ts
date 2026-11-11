import { raftbenchContextQuerySchema, raftbenchReportSchema } from "@botiverse/raft-shared";
import { readContext as readRaftbenchContext, submitReport as submitRaftbenchReport, RaftbenchError } from "../raftbench/service";
import { getAppInstallationSetup } from "../services/appSetupService";
import { issueAgentAudienceJwt, AgentAudienceJwtError } from "../services/agentAudienceJwtService";
import { respondToTaskWriteError } from "../lib/taskWriteErrorResponse";
import { sharedSseStreamRegistry } from "../services/sseStreamRegistry";
import { isReadMutationFenceRefusal } from "../services/readMutationSequencer";
import { serializeErrorForLog } from "../tracing/safeErrorLog";
import { refuseIfRemindersMovedToApp } from "./reminderAppRollout";
import { evaluateFeatureFlags, REMINDER_APP_FEATURE_FLAG_KEY, VAULT_APP_FEATURE_FLAG_KEY } from "../services/featureFlagService";
// /internal/agent-api/* — RFC v0.8 sk_agent_* runner data-plane surface.
// New managed-runner daemon builds use this surface for agent data-plane work
// after successful runner credential mint. If mint fails, startup hard-fails;
// the daemon must not silently fall back to legacy `/internal/agent/:id/*`.
// Operators deploy server first and roll back the daemon binary if this
// surface must be disabled.
//
// Auth is wired upstream via `requireAgentCredentialAuth`. By the time
// these handlers run:
//   - req.principalKind        === "agent_credential"
//   - req.actingAgentId        === <bound agent id>
//   - req.serverId             === <agent's server id>
//   - req.agentCredentialId    === <credential row id>
//   - req.agentCredentialScopes === <credential scope list>
//
// Cross-principal rejection (RFC §5.6) is enforced upstream — an
// sk_machine_* / sk_computer_* / JWT caller hitting this surface gets
// 401 invalid_principal before reaching here.
//
// IMPORTANT — no `:id` path param. All routes act on the credential's
// bound agent identity. This is the core "agent-self" contract: a single
// credential cannot impersonate another agent on the same server.
//
// v0.8 handlers mirror the relevant legacy `/internal/agent/:id/*`
// behavior, minus the `loadOwnedMachineAgent` ownership check (the
// credential is already bound to the agent at auth time). Shared logic must
// live in principal-agnostic helpers that receive an already-authorized actor
// value; do not move machine-vs-runner auth branching into shared core.

import { randomUUID } from "node:crypto";
import {
  Router,
  type NextFunction,
  type Request,
  type RequestHandler,
  type Response,
  type Router as RouterType,
} from "express";
import multer from "multer";
import { and, desc, eq, gt, inArray, isNotNull, isNull, not, or, sql } from "drizzle-orm";
import {
  AGENT_API_ATTACHMENT_DOWNLOAD_UNAVAILABLE_RESPONSE,
  AGENT_API_ATTACHMENT_DOWNLOAD_URL_TTL_SECONDS,
  AGENT_API_USER_CHANNELS_DEFAULT_LIMIT,
  AGENT_API_USER_NOT_FOUND_CODE,
  AGENT_API_ATTACHMENT_DOWNLOAD_URL_UNAVAILABLE_RESPONSE,
  ATTENTION_HINT_COPY_VERSION,
  ATTENTION_HINT_SCHEMA,
  DAEMON_CAPABILITY_MODEL_SEEN_BOUNDARY,
  EXTERNAL_AGENT_ACTIVITY_INGEST_SCHEMA,
  RAFT_AGENT_STATUS_DETAIL_LIMIT,
  RAFT_AGENT_STATUS_VALUES,
  buildApmFreshnessDecisionProducerFactId,
  agentApiContract,
  getAgentApiResponseKind,
  type AgentApiAttachmentCommentsResponse,
  type AgentApiFeedbackLocatorListQuery,
  type AgentApiContractRoute,
  type AgentApiHeldFreshnessResponse,
  type AgentApiMessageEnvelope,
  type AgentApiRequestBodyByRoute,
  type AgentApiRequestParamsByRoute,
  type AgentApiRequestQueryByRoute,
  type AgentScope,
  type AgentApiResponseByRoute,
  type AgentApiRouteKey,
  buildRaftCliGuideMarkdown,
  formatInboxMessageTarget,
  isExternalAgentActivityLegacyStatus,
  isExternalAgentRuntime,
  isRaftAgentStatus,
  isReminderStatus,
  isRaftOAuthScope,
  projectApmHeldFreshnessActivity,
  projectApmHeldFreshnessEnvelope,
  asChannelId,
  asMachineId,
  canonicalizeOAuthClientCategory,
  getServerCapabilities,
  type AgentApiTaskClaimConflict,
  type AgentMessage,
  type ApmFreshnessSideEffectAction,
  type ExternalAgentActivityEvent,
  type ExternalAgentActivityIngestRequest,
  isTaskStatus,
  renderThirdPartyInertText,
  isSlackBridgedExternalProvider,
  type ActionCardAction,
  type ReminderStatus,
  type ServerId,
  type TaskStatus,
  failpoints,
  AGENT_API_MESSAGE_SEARCH_DEFAULT_LIMIT,
  AGENT_API_MESSAGE_SEARCH_MAX_LIMIT,
  AGENT_API_INBOX_DEFAULT_LIMIT,
  AGENT_API_INBOX_MAX_LIMIT,
  type AgentApiInboxListQuery,
  AGENT_API_ASSISTANT_INBOX_DEFAULT_LIMIT,
  AGENT_API_ASSISTANT_INBOX_MAX_LIMIT,
  AGENT_API_ASSISTANT_MESSAGES_DEFAULT_LIMIT,
  AGENT_API_ASSISTANT_MESSAGES_MAX_LIMIT,
  type AgentApiAssistantOwnerInboxQuery,
  type AgentApiAssistantOwnerMessagesQuery,
  THIRD_PARTY_EVENT_DELIVERED_REPORT_MAX_IDS,
  isThirdPartyEventId,
  buildIntegrationInviteUrl,
  channelCardRev,
  peerCardHasContent,
  type AgentApiChannelCard,
  type AgentApiPeerCard,
  type Tracer,
} from "@botiverse/raft-shared";
import * as agentService from "../services/agentService";
import * as assistantService from "../services/assistantService";
import { resolveReadableAttachmentAuthorityContext } from "../services/attachmentAuthorityService";
import * as actionCardsService from "../services/actionCardsService";
import * as idempotentAgentApiService from "../services/agentApiIdempotencyService";
import * as attachmentCommentService from "../services/attachmentCommentService";
import { renderAgentCommentScopedContent } from "../services/attachmentCommentAnchorLabel";
import * as channelService from "../services/channelService";
import { readPositionSettleMs, scheduleDeferredReadAdvance, settledReadThroughSeq } from "../services/readPositionSettle";

/** Rows the deferred read-advance check scans in (fromSeq, toSeq]; a history page is at most 100. */
const DEFERRED_READ_ADVANCE_SCAN_LIMIT = 500;
import { CHANNEL_NOT_FOUND_BODY, threadAnchorNotFoundBody } from "./channelAccessDenial";
import * as messageService from "../services/messageService";
import {
  loadCanonicalTaskFactsByMessageId,
  refreshQueuedAgentTaskProjections,
} from "../services/messageTaskProjection";
import * as mentionDeliveryOccurrenceService from "../services/mentionDeliveryOccurrenceService";
import { projectMentionDeliveryForSender } from "../services/senderMentionDeliveryProjection";
import * as searchService from "../services/searchService";
import { resolveSearchSenderFilter } from "../services/searchSenderFilterService";
import * as serverService from "../services/serverService";
import * as machineService from "../services/machineService";
import * as agentScopesService from "../services/agentScopesService";
import * as oauthService from "../services/oauthService";
import * as integrationAppQueryService from "../services/integrationAppQueryService";
import * as reminderCrud from "../apps/reminder/crud";
import * as reminderService from "../apps/reminder/service";
import {
  ackBuiltInAppSource,
  publishTaskResourceExpiryFollowup,
  taskResourceExpiryFollowups,
} from "../registry.manifest";
import { publishReminderEvent } from "../apps/reminder/realtime";
import {
  getRapAppConfig,
  patchRapAppConfig,
  RapAppConfigError,
} from "../services/rapAppConfigService";
import { pushBuiltInAppConfigForOwner } from "../services/appConfigTransportComposition";
import * as taskService from "../services/taskService";
import {
  getTaskRealtimeSurfaceTargets,
  resolveTaskChannelSurface,
  resolveTaskChannelSurfaces,
  isHistoricalJointTaskReadOnly,
  type TaskChannelSurface,
  type TaskSurfaceChannel,
} from "../services/taskChannelSurface";
import { emitTaskCreated, emitTaskDeleted, emitTaskMessageNew } from "../services/taskRealtimeEvents";
import {
  describeTaskMutation,
  emitTaskMutationToSurfaces,
} from "../services/taskMutationBroadcast";
import { projectRichMessageSocketPayload } from "../services/messageRealtimeEvents";
import { mutateMessageReaction } from "../services/messageReactionService";
import { ChannelConversionInProgressError } from "../services/channelConversionFenceService";
import { getHistoryCutoff, getServerPlan, isChannelReadOnlyByBillingFeature, isChannelReadOnlyByQuota } from "../services/planService";
import { getAttachmentFileSizeLimitBytes } from "../services/attachmentUploadPolicy";
import { uploadAttachmentBuffers } from "../services/attachmentUploadWriterService";
import type { AttachmentUploadSessionService } from "./attachmentUploadSessions";
import { resolveScheduleInput } from "../services/reminderScheduleInput";
import { computeNextFire, parseRecurrenceString, type Recurrence } from "../services/recurrence";
import * as agentPermalinkRenderService from "../services/agentPermalinkRenderService";
import {
  acknowledgeAgentInboxEventsCursor,
  canAgentAccessQueuedMessageTarget,
  normalizeAgentApiWireValue,
  partitionDeliverableInboxMessages,
  projectAgentInboxEvents,
  recordAgentInboxEventsPendingAck,
} from "../services/agentInboxEvents";
import {
  AgentInboxPushError,
  deleteAgentInboxPushRegistration,
  getAgentInboxPushStatus,
  putAgentInboxPushRegistration,
} from "../services/agentInboxPushService";
import * as attestedSendService from "../services/attestedSendService";
import { emitScopeReadUpdated } from "../services/readReceiptService";
import { emitThreadFollowersUpdated } from "../services/threadFollowerRealtimeService";
import { paginateHistoryProbe } from "./historyCursor";
import {
  buildServerInfoAgentSummaries,
  buildServerInfoHumanSummaries,
  findServerInfoAgentSummaryByName,
  findServerInfoHumanSummaryByName,
  buildAgentProfileView,
  filterAgentVisibleHumansForHiddenDirectory,
  messageResolveErrorPayload,
  resolveAgentVisibleMessagePayload,
  resolveProfileViewForAgent,
} from "./internal";
import { getDb } from "../db/index";
import { managedMcpAgentErrorMessage } from "./managedMcpAgentErrors";
import { MCP_CALL_INTERNAL_ERROR_TEXT } from "@botiverse/raft-shared";
import {
  agents,
  attachments,
  channelAgents,
  channels,
  jointChannels,
  jointChannelServers,
  managedMcpServers,
  raftbenchConfigs,
  messageMentions,
  messages,
} from "../db/schema";
import { messageIdShortPrefixConditions, UUID_RE } from "../lib/messageId";
import { guardUuidPathParams } from "../lib/uuidPathParams";
import { forbiddenMessageForTarget, notFoundMessageForTarget, resolveWritableAgentTarget } from "./agentWritableTarget";
import { isDurableAgentInboxMessage, type AgentOrchestrator } from "../services/agentOrchestrator";
import type { Server as SocketServer } from "socket.io";
import type { AgentCapability } from "../services/agentCredentialService";
import { getCdnStorage, getStorage, isStorageTimeoutError } from "../services/storageService";
import { streamStorageResponse } from "../services/storageResponseStream";
import {
  buildAttachmentTooLargeResponse,
  buildAttachmentContentDisposition,
  buildAttachmentContentLengthHeader,
  buildAttachmentResponseContentType,
  canGenerateImagePreview,
  generateSvgRasterPreview,
  generateThumbnail,
  getThumbnailUrl,
  isEmptyUploadedFile,
  isOversizedUploadedFile,
  isSvgAttachmentMimeType,
  normalizeAttachmentFilename,
  normalizeUploadedMimeType,
  resolveAttachmentMimeType,
  resolveRequestAttachmentFileSizeLimitBytes,
  runSingleAttachmentUpload,
} from "./attachments";
import { addTraceEvent, errorClassOf, getCurrentTraceSpan, recordTraceEvent, tracePhase, withTraceChildSpan, withTraceRoot } from "../tracing/semanticTrace";
import { agentRequestContextFromHeaders, recordAgentContextReturned } from "../tracing/agentContextReturnTrace";
import { agentIdHashAttrs, serverIdHashAttrs } from "../tracing/traceIdentity";
import { traceSendRouteFailure, traceSendRouteCatch } from "../tracing/sendRouteFailure";
import { traceQuerySpan } from "../tracing/queryTrace";
import { handleAgentKnowledgeGet, handleAgentKnowledgeSearch } from "./agentKnowledge";
import { AttachmentLinkError } from "../services/attachmentLinkingService";
import { bindRequestAbortSignal } from "./requestAbortSignal";
import { broadcastAgentUpdated } from "./agentUpdatedBroadcast";
import { createChannelForAgent } from "./agentChannelCreate";
import { addChannelMemberForAgent, removeChannelMemberForAgent } from "./agentChannelMembers";
import { setChannelArchivedForAgent } from "./agentChannelLifecycle";
import { updateChannelForAgent } from "./agentChannelUpdate";
import { assertAgentCanManageServerProfile, updateServerProfileForAgent } from "./agentServerManage";
import { respondToDmTargetResolutionError, sendJsonServerError, transientSendConflictBody } from "./errorResponse";
import { DmTargetResolutionError } from "../services/dmTargetResolutionError";
import { transientTransactionSqlState } from "../db/transientTransactionRetry";
import {
  createAvatarUpload,
  MAX_PROFILE_AVATAR_BYTES,
  PROFILE_AVATAR_BAD_FORMAT_MESSAGE,
  PROFILE_AVATAR_TOO_LARGE_MESSAGE,
  runSingleAvatarUpload,
  storeAgentAvatar,
  storeServerAvatar,
} from "../services/avatarService";
import {
  FileUploadQuotaExceededError,
  buildFileUploadQuotaExceededResponse,
  getFileUploadQuotaSummary,
} from "../services/fileUploadQuotaService";
import {
  buildPendingMentionActionPayload,
  executeMentionActionId,
  listPendingMentionActionsForSender,
  type MentionActionExecutionOptions,
  type MentionActionKind,
  type MentionActionResult,
} from "../services/mentionActionService";
import { actorHasServerCapabilityInServer, getActorServerRoleInServer, getAgentServerRolesInServer } from "../lib/actorPermissions";
import { getAppUrl } from "../config/appUrl";
import {
  getAgentServerLabs,
  patchAgentServerLabsAccess,
  putAgentServerLabEnrollment,
} from "./serverLabs";
import {
  executeManagedMcpCall,
  getManagedMcpRuntimeSnapshot,
  ManagedMcpServiceError,
} from "../services/managedMcpService";
import { ManagedMcpCredentialError } from "../services/managedMcpCredentialService";
import { recordAgentApiSeen } from "../services/externalAgentPresence";
import { isAgentCredentialActive } from "../services/agentCredentialService";
import { subscribeAgentCredentialRevocation } from "../services/agentCredentialRevocationBus";
import { ManagedMcpGatewayError } from "../services/managedMcpGateway";
import { ManagedMcpOAuthError } from "../services/managedMcpOAuthService";
import { getHostedRuntimeSummary } from "../services/agentRuntimeProvisionService";
import { setRequestTraceErrorCode } from "../middleware/requestObservability";
import { taskStatusRejectionCode } from "./agentApiErrorTrace";
import {
  FeedbackLocatorIngestError,
  ingestFeedbackLocator,
  queryFeedbackLocators,
} from "../services/productFeedbackLocatorService";

export const internalAgentApiRouter: RouterType = Router();

// Uniform 404 for non-UUID path params (task #12). `labKey` is excluded
// (string key, not a UUID). `messageId`/`attachmentId` are deliberately NOT
// guarded here: those routes cloak unknown AND malformed ids behind the same
// contract bodies (ATTACHMENT_UNAVAILABLE / {status:"NOT_JOINABLE"}) so a
// malformed id is indistinguishable from a foreign one — guarding would leak
// the difference (internalAgentApi.auth/senderMentionDeliveries tests).
guardUuidPathParams(internalAgentApiRouter, {
  channelId: "Channel",
});

async function syncReminderToComputer(
  req: Request,
  row: reminderService.ReminderRow,
  mode: "upsert" | "cancel",
): Promise<void> {
  const orchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
  if (!orchestrator) return;
  try {
    if (mode === "cancel") {
      await orchestrator.pushReminderCancel(row.ownerAgentId, row.id, row.version);
    } else {
      await orchestrator.pushReminderUpsert(row.ownerAgentId, row);
    }
  } catch (error) {
    console.warn(`[reminder] Computer sync failed for ${row.id}@${row.version}:`, serializeErrorForLog(error));
  }
}

const MAX_REACTION_LENGTH = 16;
const ATTESTED_SEND_HELD_CONTEXT_LIMIT = 3;
const profileAvatarUpload = createAvatarUpload();
const serverAvatarUpload = createAvatarUpload();
const integrationLogoUpload = createAvatarUpload();
const MAX_AGENT_PROFILE_DESCRIPTION_LENGTH = 3000;
const MAX_AGENT_PROFILE_DISPLAY_NAME_LENGTH = 80;
const REMINDER_MAX_TITLE_LEN = 500;

function getFullUuidParam(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  return UUID_RE.test(raw) ? raw : null;
}

function sendAttachmentNotFound(res: Response): void {
  res.status(404).json({ error: "Attachment not found" });
}

function sendAttachmentDownloadUnavailable(res: Response): void {
  res.status(404).json(AGENT_API_ATTACHMENT_DOWNLOAD_UNAVAILABLE_RESPONSE);
}

function thirdPartyEventIdFromMessage(message: AgentMessage): string | null {
  const eventId = message.third_party_event?.id;
  if (typeof eventId === "string" && eventId.length > 0) return eventId;
  if (message.sender_type !== "third_party_app") return null;
  const messageId = (message as { message_id?: unknown }).message_id;
  return typeof messageId === "string" && messageId.length > 0 ? messageId : null;
}

async function rebuildPendingThirdPartyAgentEvents(
  agentOrchestrator: AgentOrchestrator,
  agentId: string,
): Promise<void> {
  const pendingEventIds = agentOrchestrator.peekPendingMessages(agentId)
    .map(thirdPartyEventIdFromMessage)
    .filter((id): id is string => id !== null);
  const rebuilt = await oauthService.rebuildPendingThirdPartyAgentEventMessages({
    agentId,
    excludeEventIds: pendingEventIds,
  });
  const serviceOnly = await agentService.isRaftBenchServiceAgent(agentId);
  for (const message of rebuilt) {
    if (serviceOnly && message.third_party_event?.client_id !== "system.raftbench") continue;
    agentOrchestrator.deliverToLocalInbox(agentId, message, { notifyPendingReceive: false });
  }
}

function validateAgentApiRequestPart<T, K extends AgentApiRouteKey>(
  routeKey: K,
  part: "params" | "query" | "body",
  value: unknown,
  res: Response,
): T | null {
  const route = agentApiContract[routeKey];
  let schema;
  if (part === "params") {
    schema = "params" in route.request ? route.request.params : undefined;
  } else if (part === "query") {
    schema = "query" in route.request ? route.request.query : undefined;
  } else {
    schema = "body" in route.request ? route.request.body : undefined;
  }
  if (!schema) return null;
  const parsed = schema.safeParse(value ?? {});
  if (!parsed.success) {
    res.status(400).json({
      error: `Invalid agent-api ${route.key} ${part}`,
      code: "agent_api_contract_invalid",
      issues: parsed.error.issues.map((issue) => ({
        path: issue.path.join("."),
        message: issue.message,
      })),
    });
    return null;
  }
  return parsed.data as T;
}

function validateAgentApiParams<K extends AgentApiRouteKey>(
  routeKey: K,
  req: Request,
  res: Response,
): AgentApiRequestParamsByRoute[K] | null {
  return validateAgentApiRequestPart<AgentApiRequestParamsByRoute[K], K>(routeKey, "params", req.params, res);
}

function validateAgentApiQuery<K extends AgentApiRouteKey>(
  routeKey: K,
  req: Request,
  res: Response,
): AgentApiRequestQueryByRoute[K] | null {
  return validateAgentApiRequestPart<AgentApiRequestQueryByRoute[K], K>(routeKey, "query", req.query, res);
}

function validateAgentApiBody<K extends AgentApiRouteKey>(
  routeKey: K,
  req: Request,
  res: Response,
): AgentApiRequestBodyByRoute[K] | null {
  return validateAgentApiRequestPart<AgentApiRequestBodyByRoute[K], K>(routeKey, "body", req.body, res);
}

function validateAgentApiParamsMiddleware<K extends AgentApiRouteKey>(routeKey: K) {
  return (req: Request, res: Response, next: NextFunction) => {
    const parsed = validateAgentApiParams(routeKey, req, res);
    if (!parsed) return;
    req.params = parsed as Request["params"];
    next();
  };
}

function validateAgentApiBodyMiddleware<K extends AgentApiRouteKey>(routeKey: K) {
  return (req: Request, res: Response, next: NextFunction) => {
    const parsed = validateAgentApiBody(routeKey, req, res);
    if (!parsed) return;
    req.body = parsed;
    next();
  };
}

function validateAgentApiQueryMiddleware<K extends AgentApiRouteKey>(routeKey: K) {
  return (req: Request, res: Response, next: NextFunction) => {
    const parsed = validateAgentApiQuery(routeKey, req, res);
    if (!parsed) return;
    Object.defineProperty(req, "query", {
      value: parsed as Request["query"],
      configurable: true,
      enumerable: true,
      writable: true,
    });
    next();
  };
}

function sendAgentApiResponse<K extends AgentApiRouteKey>(
  routeKey: K,
  res: Response,
  body: AgentApiResponseByRoute[K],
): void {
  const route: AgentApiContractRoute = agentApiContract[routeKey];
  if (getAgentApiResponseKind(route.response) === "binary") {
    console.error(`internal.agent-api.${route.key} binary route attempted JSON response helper`);
    res.status(500).json({ error: "Agent API response contract violation" });
    return;
  }
  if (!("body" in route.response)) {
    console.error(`internal.agent-api.${route.key} JSON response route is missing a body schema`);
    res.status(500).json({ error: "Agent API response contract violation" });
    return;
  }
  const parsed = route.response.body.safeParse(normalizeAgentApiWireValue(body));
  if (!parsed.success) {
    console.error(`internal.agent-api.${route.key} response contract violation:`, parsed.error.flatten());
    res.status(500).json({ error: "Agent API response contract violation" });
    return;
  }
  res.json(parsed.data);
}

/**
 * Idempotency scope for a keyed write, or null when the request carries no
 * usable key (behaviour then is unchanged: every request writes).
 */
function agentApiIdempotencyScope(
  agentId: string,
  route: idempotentAgentApiService.AgentApiIdempotentRoute,
  rawKey: unknown,
  normalizedRequest: unknown,
): idempotentAgentApiService.AgentApiIdempotencyScope | null {
  const idempotencyKey = idempotentAgentApiService.normalizeAgentApiIdempotencyKey(rawKey);
  if (!idempotencyKey) return null;
  return {
    agentId,
    route,
    idempotencyKey,
    requestFingerprint: idempotentAgentApiService.fingerprintAgentApiRequest(route, normalizedRequest),
  };
}

/**
 * Answer a keyed write from its ledger: the first response (status and body)
 * when the key is bound to this same request, without any side effect.
 * Returns false when the key is unused. A key bound to a different request
 * throws AgentApiIdempotencyConflictError (see respondToAgentApiIdempotencyConflict).
 */
async function replayAgentApiIdempotentWrite(
  scope: idempotentAgentApiService.AgentApiIdempotencyScope,
  res: Response,
): Promise<boolean> {
  const stored = await idempotentAgentApiService.findAgentApiIdempotentResponse(scope);
  if (!stored) return false;
  res.status(stored.status);
  sendAgentApiResponse(scope.route, res, stored.body as AgentApiResponseByRoute[typeof scope.route]);
  return true;
}

/** Same 409 shape and code as message send's reused-key refusal. */
function respondToAgentApiIdempotencyConflict(err: unknown, res: Response): boolean {
  if (!(err instanceof idempotentAgentApiService.AgentApiIdempotencyConflictError)) return false;
  res.status(err.status).json({
    error: err.message,
    code: err.code,
    mismatch: err.mismatch,
    suggestedNextAction: err.suggestedNextAction,
  });
  return true;
}

function validateAgentApiResponseMiddleware<K extends AgentApiRouteKey>(routeKey: K) {
  const route = agentApiContract[routeKey];
  if (getAgentApiResponseKind(route.response) === "binary") {
    return (_req: Request, _res: Response, next: NextFunction) => next();
  }
  if (!("body" in route.response)) {
    throw new Error(`Agent API ${route.key} JSON response route is missing a body schema`);
  }
  const responseBodySchema = route.response.body;
  return (_req: Request, res: Response, next: NextFunction) => {
    const originalJson = res.json.bind(res);
    res.json = ((body: unknown) => {
      if (res.statusCode >= 200 && res.statusCode < 300) {
        const parsed = responseBodySchema.safeParse(normalizeAgentApiWireValue(body));
        if (!parsed.success) {
          console.error(`internal.agent-api.${route.key} bridged response contract violation:`, parsed.error.flatten());
          res.status(500);
          return originalJson({ error: "Agent API response contract violation" });
        }
        return originalJson(parsed.data);
      }
      return originalJson(body);
    }) as Response["json"];
    next();
  };
}

async function canAgentAccessLinkedJointAttachment(
  attachment: typeof attachments.$inferSelect,
  serverId: string,
  agentId: string,
): Promise<boolean> {
  if (!attachment.messageId) return false;

  const db = getDb();
  const [message] = await db
    .select({ channelId: messages.channelId })
    .from(messages)
    .where(eq(messages.id, attachment.messageId))
    .limit(1);
  if (!message) return false;

  const projections = await channelService.getActiveJointChannelProjectionsByLocalChannel(message.channelId);
  const localProjection = projections.find((projection) => projection.serverId === serverId);
  if (!localProjection) return false;

  return channelService.canAgentAccessChannel(localProjection.localChannelId, agentId);
}

async function canAgentAccessAttachment(
  attachment: typeof attachments.$inferSelect,
  serverId: string,
  agentId: string,
): Promise<boolean> {
  const channel = await channelService.getChannel(attachment.channelId);
  if (channel?.serverId === serverId && await channelService.canAgentAccessChannel(attachment.channelId, agentId)) {
    return true;
  }
  return canAgentAccessLinkedJointAttachment(attachment, serverId, agentId);
}

async function resolveAgentReadableAttachment(
  projection: typeof attachments.$inferSelect,
  serverId: string,
  agentId: string,
): Promise<typeof attachments.$inferSelect | null> {
  if (!projection.objectId) {
    return await canAgentAccessAttachment(projection, serverId, agentId) ? projection : null;
  }
  const context = await resolveReadableAttachmentAuthorityContext({
    projectionId: projection.id,
    requestServerId: serverId as ServerId,
    principal: { type: "agent", id: agentId },
  });
  if (!context) return null;
  return {
    ...context.projection,
    uploaderId: context.object.uploaderId,
    uploaderType: context.object.uploaderType,
    mimeType: context.object.mimeType,
    sizeBytes: context.object.sizeBytes,
    storageKey: context.object.storageKey,
    thumbnailKey: context.object.thumbnailKey,
    contentHash: context.object.contentHash,
    width: context.object.width,
    height: context.object.height,
  };
}

async function resolveReminderMsgIdForAgentApi(
  serverId: string,
  agentId: string,
  msgId: string,
): Promise<
  | { ok: true; messageId: string }
  | { ok: false; status: 400 | 404; error: string }
> {
  const db = getDb();
  if (/^[0-9a-f]{8}$/i.test(msgId)) {
    const rows = await db
      .select({ id: messages.id })
      .from(messages)
      .innerJoin(channels, eq(channels.id, messages.channelId))
      .leftJoin(jointChannels, and(
        eq(jointChannels.canonicalChannelId, messages.channelId),
        eq(jointChannels.status, "active"),
      ))
      .leftJoin(jointChannelServers, and(
        eq(jointChannelServers.jointChannelId, jointChannels.id),
        eq(jointChannelServers.serverId, serverId),
        eq(jointChannelServers.status, "active"),
      ))
      .where(and(
        sql`(${channels.serverId} = ${serverId} OR ${jointChannelServers.serverId} = ${serverId})`,
        ...messageIdShortPrefixConditions(msgId),
      ));

    const visibleRows = [];
    for (const row of rows) {
      const payload = await resolveAgentVisibleMessagePayload(row.id, serverId, agentId);
      if (payload) visibleRows.push(row);
    }

    if (visibleRows.length === 1) return { ok: true, messageId: visibleRows[0].id };
    if (visibleRows.length === 0) return { ok: false, status: 404, error: "message not found" };
    return { ok: false, status: 400, error: "msgId short id is ambiguous" };
  }

  if (!UUID_RE.test(msgId)) {
    return { ok: false, status: 400, error: "msgId must be a full UUID or 8-char short id" };
  }

  const [row] = await db
    .select({ id: messages.id })
    .from(messages)
    .innerJoin(channels, eq(channels.id, messages.channelId))
    .leftJoin(jointChannels, and(
      eq(jointChannels.canonicalChannelId, messages.channelId),
      eq(jointChannels.status, "active"),
    ))
    .leftJoin(jointChannelServers, and(
      eq(jointChannelServers.jointChannelId, jointChannels.id),
      eq(jointChannelServers.serverId, serverId),
      eq(jointChannelServers.status, "active"),
    ))
    .where(and(
      sql`(${channels.serverId} = ${serverId} OR ${jointChannelServers.serverId} = ${serverId})`,
      eq(messages.id, msgId),
    ))
    .limit(1);

  if (!row) return { ok: false, status: 404, error: "message not found" };
  const payload = await resolveAgentVisibleMessagePayload(row.id, serverId, agentId);
  if (!payload) return { ok: false, status: 404, error: "message not found" };
  return { ok: true, messageId: row.id };
}

function parseReminderStatusFilter(
  req: Request,
  res: Response,
): ReminderStatus[] | null {
  const statusRaw = typeof req.query.status === "string" ? req.query.status : "";
  let statusFilter: ReminderStatus[] = ["scheduled", "fired"];
  if (statusRaw) {
    const parts = statusRaw.split(",").map((s) => s.trim()).filter(Boolean);
    const validated: ReminderStatus[] = [];
    for (const p of parts) {
      if (!isReminderStatus(p)) {
        res.status(400).json({ error: "Invalid status value" });
        return null;
      }
      validated.push(p);
    }
    statusFilter = validated;
  } else if (req.query.all === "true") {
    statusFilter = ["scheduled", "fired", "canceled"];
  }
  return statusFilter;
}

function readReminderTimezone(
  tz: unknown,
  res: Response,
): string | null {
  if (tz === undefined) return "UTC";
  if (typeof tz !== "string" || tz.length === 0) {
    res.status(400).json({ error: "tz must be a non-empty IANA timezone string" });
    return null;
  }
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: tz }).format(new Date());
  } catch {
    res.status(400).json({ error: `unknown timezone "${tz}"` });
    return null;
  }
  return tz;
}

function parseReminderRecurrence(
  repeat: unknown,
  tz: unknown,
  res: Response,
): Recurrence | null | undefined {
  if (repeat == null) return null;
  if (typeof repeat !== "string") {
    res.status(400).json({ error: "repeat must be a string (e.g. every:15m | daily@09:00 | weekly:mon,fri@09:00)" });
    return undefined;
  }
  const ruleTz = readReminderTimezone(tz, res);
  if (ruleTz === null) return undefined;
  const parsed = parseRecurrenceString(repeat, ruleTz);
  if (!parsed.ok) {
    res.status(400).json({ error: `repeat: ${parsed.error}` });
    return undefined;
  }
  if (
    (parsed.recurrence.rule.kind === "daily" || parsed.recurrence.rule.kind === "weekly") &&
    tz === undefined
  ) {
    res.status(400).json({
      error: "tz is required for daily@ or weekly: rules (IANA name, e.g. America/Los_Angeles)",
    });
    return undefined;
  }
  return parsed.recurrence;
}

async function loadOwnedReminder(
  reminderId: string,
  serverId: string,
  agentId: string,
): Promise<reminderService.ReminderRow | null> {
  const existing = await reminderCrud.getAppReminderById(reminderId);
  if (!existing || existing.serverId !== serverId || existing.ownerAgentId !== agentId) return null;
  return existing;
}

function requireAgentCapability(capability: AgentCapability) {
  return (req: Request, res: Response, next: NextFunction) => {
    const failure = getAgentCapabilityFailure(req, capability);
    if (failure) {
      res.status(failure.status).json(failure.body);
      return;
    }
    next();
  };
}

function getAgentCapabilityFailure(
  req: Request,
  capability: AgentCapability,
): { status: number; body: { error: string; code: string; requiredCapability: AgentCapability } } | null {
  const scopes = Array.isArray(req.agentCredentialScopes) ? req.agentCredentialScopes : [];
  if (!scopes.includes(capability)) {
    return {
      status: 403,
      body: {
        error: "Agent credential is not authorized for this capability",
        code: "capability_not_authorized",
        requiredCapability: capability,
      },
    };
  }
  const activeRaw = typeof req.headers["x-slock-agent-active-capabilities"] === "string"
    ? req.headers["x-slock-agent-active-capabilities"]
    : "";
  if (activeRaw.trim()) {
    const active = new Set(activeRaw.split(",").map((item) => item.trim()).filter(Boolean));
    if (!active.has(capability)) {
      return {
        status: 501,
        body: {
          error: "The current runner session does not support this capability",
          code: "unsupported_capability",
          requiredCapability: capability,
        },
      };
    }
  }
  return null;
}

const continueToLegacyBridge = (_req: Request, _res: Response, next: NextFunction) => next();

function agentApiRequestValidators<K extends AgentApiRouteKey>(routeKey: K): RequestHandler[] {
  const route = agentApiContract[routeKey];
  const validators: RequestHandler[] = [];
  if ("params" in route.request) {
    validators.push(validateAgentApiParamsMiddleware(routeKey));
  }
  if ("query" in route.request) {
    validators.push(validateAgentApiQueryMiddleware(routeKey));
  }
  if ("body" in route.request) {
    validators.push(validateAgentApiBodyMiddleware(routeKey));
  }
  return validators;
}

/**
 * Per-agent grant required by each Agent API operation, in addition to the
 * credential capability. Grants are the human-managed scope set
 * (`agentScopesService`, `PUT /internal/agent/:id/scopes`); the credential
 * capability only says what a token may carry. Both must hold, exactly as on
 * the legacy `/internal/agent/:id/*` routes, whose `requireAgentScope(...)`
 * gate is the source of each entry below (`routes/internal.ts`):
 *
 *   operation                     legacy route                              grant
 *   attachmentUpload              POST   /agent/:id/upload                  attachment:upload
 *   attachmentUploadSessionCreate (chunked form of POST /agent/:id/upload)  attachment:upload
 *   attachmentUploadSessionComplete (chunked form of POST /agent/:id/upload) attachment:upload
 *   messageSend / messageSendV2   POST   /agent/:id/send                    message:send
 *   messageReactionAdd            POST   /agent/:id/messages/:m/reactions   message:send
 *   messageReactionRemove         DELETE /agent/:id/messages/:m/reactions   message:send
 *   events                        GET    /agent/:id/receive                 message:read
 *   messageResolve                GET    /agent/:id/messages/:m/resolve     message:read
 *   historyRead                   GET    /agent/:id/history                 message:read
 *   messageSearch                 GET    /agent/:id/search                  message:read
 *   channelJoin                   POST   /agent/:id/channels/:c/join        channel:join
 *   channelLeave                  POST   /agent/:id/channels/:c/leave       channel:leave
 *   threadUnfollow                POST   /agent/:id/threads/unfollow        thread:unfollow
 *   channelMembers                GET    /agent/:id/channel-members         channel:read
 *   serverInfo                    GET    /agent/:id/server                  server:read
 *   serverUpdate                  PATCH  /agent/:id/server                  server:update
 *   channelCreate                 POST   /agent/:id/channels                channel:create
 *   channelUpdate                 PATCH  /agent/:id/channels/:c             channel:update
 *   channelMemberAdd              POST   /agent/:id/channels/:c/members     channel:add_member
 *   channelMemberRemove           DELETE /agent/:id/channels/:c/members     channel:remove_member
 *   knowledgeGet                  GET    /agent/:id/knowledge               knowledge:read
 *   knowledgeSearch               GET    /agent/:id/knowledge/search        knowledge:read
 *   taskList                      GET    /agent/:id/tasks                   task:read
 *   taskCreate                    POST   /agent/:id/tasks                   task:write
 *   taskClaim                     POST   /agent/:id/tasks/claim             task:write
 *   taskUnclaim                   POST   /agent/:id/tasks/unclaim           task:write
 *   taskUpdateStatus              POST   /agent/:id/tasks/update-status     task:write
 *   actionPrepare                 POST   /agent/:id/prepare-action          action:prepare
 *
 * userChannels (GET /users/:name/channels, `raft user info`) has no legacy
 * twin: it replaces serverInfo + channelMembers and requires channel:read here
 * and server:read inline.
 *
 * The server avatar route is not a contract operation (multipart upload); it
 * carries `requireAgentGrant(...)` inline below.
 * `internalAgentApi.grantCoverage.test.ts` derives the expected pairs from the
 * legacy router and fails when a counterpart here lacks the same grant.
 */
export const AGENT_API_ROUTE_GRANTS: Partial<Record<AgentApiRouteKey, AgentScope>> = {
  raftbenchContext: "message:read",
  attachmentUpload: "attachment:upload",
  attachmentUploadSessionCreate: "attachment:upload",
  attachmentUploadSessionComplete: "attachment:upload",
  messageSend: "message:send",
  messageSendV2: "message:send",
  messageReactionAdd: "message:send",
  messageReactionRemove: "message:send",
  events: "message:read",
  messageResolve: "message:read",
  historyRead: "message:read",
  messageSearch: "message:read",
  channelJoin: "channel:join",
  channelLeave: "channel:leave",
  threadUnfollow: "thread:unfollow",
  channelMembers: "channel:read",
  serverInfo: "server:read",
  // Plus server:read inline: it replaces serverInfo and channelMembers.
  userChannels: "channel:read",
  serverUpdate: "server:update",
  channelCreate: "channel:create",
  channelUpdate: "channel:update",
  channelMemberAdd: "channel:add_member",
  channelMemberRemove: "channel:remove_member",
  knowledgeGet: "knowledge:read",
  knowledgeSearch: "knowledge:read",
  taskList: "task:read",
  taskCreate: "task:write",
  taskClaim: "task:write",
  taskUnclaim: "task:write",
  taskUpdateStatus: "task:write",
  actionPrepare: "action:prepare",
};

/**
 * Deny with the legacy `requireAgentScope` 403 shape when the bound agent's
 * grant set lacks `scope`. Default-mode agents (no `agent_scopes` row) carry
 * every grantable scope, so this only bites once a human saved a custom set.
 * Tagged with `__agentScope` like the legacy middleware so route walkers can
 * read the required grant off the handler chain.
 */
function requireAgentGrant(scope: AgentScope): RequestHandler {
  const handler: RequestHandler = async (req, res, next) => {
    const agentId = req.actingAgentId;
    if (!agentId) {
      res.status(500).json({ error: "Agent credential state missing" });
      return;
    }
    let reason: "missing_scope" | "scope_lookup_failed";
    try {
      if (await agentScopesService.agentHasScope(agentId, scope)) {
        next();
        return;
      }
      reason = "missing_scope";
    } catch (err) {
      console.error("agent-api scope lookup failed", serializeErrorForLog(err));
      reason = "scope_lookup_failed";
    }
    req.scopeDenyReason = reason;
    req.scopeRequired = scope;
    if (req.raftbenchServiceAgent) {
      res.status(403).json({ error: "This service agent no longer has the required read permission.", code: "raftbench_read_permission_revoked", requiredScope: scope, suggestedNextAction: "Stop this run. An administrator has to restore its read grants; retrying will not help." });
      return;
    }
    res.status(403).json({ error: "missing required scope", requiredScope: scope, reason });
  };
  (handler as RequestHandler & { __agentScope?: AgentScope }).__agentScope = scope;
  return handler;
}

function registerAgentApiRoute<K extends AgentApiRouteKey>(
  routeKey: K,
  ...handlers: RequestHandler[]
): void {
  const route: AgentApiContractRoute = agentApiContract[routeKey];
  const grant = AGENT_API_ROUTE_GRANTS[routeKey];
  const routeHandlers: RequestHandler[] = [
    requireAgentCapability(route.capability),
    ...(grant ? [requireAgentGrant(grant)] : []),
    ...handlers,
  ];
  switch (route.method) {
    case "GET":
      internalAgentApiRouter.get(route.path, ...routeHandlers);
      return;
    case "POST":
      internalAgentApiRouter.post(route.path, ...routeHandlers);
      return;
    case "PUT":
      internalAgentApiRouter.put(route.path, ...routeHandlers);
      return;
    case "PATCH":
      internalAgentApiRouter.patch(route.path, ...routeHandlers);
      return;
    case "DELETE":
      internalAgentApiRouter.delete(route.path, ...routeHandlers);
      return;
  }
}

function registerAgentApiBridgeRoute<K extends AgentApiRouteKey>(routeKey: K): void {
  registerAgentApiRoute(
    routeKey,
    ...agentApiRequestValidators(routeKey),
    validateAgentApiResponseMiddleware(routeKey),
    continueToLegacyBridge,
  );
}

registerAgentApiRoute(
  "feedbackLocatorIngest",
  ...agentApiRequestValidators("feedbackLocatorIngest"),
  validateAgentApiResponseMiddleware("feedbackLocatorIngest"),
  async (req, res) => {
    try {
      const receipt = await ingestFeedbackLocator({
        serverId: req.serverId!,
        agentId: req.actingAgentId!,
        artifactKind: req.body.artifact_kind,
        eventKind: req.body.event_kind,
        payload: req.body.payload,
      });
      res.json(receipt);
    } catch (error) {
      if (error instanceof FeedbackLocatorIngestError) {
        const status = error.reasonCode === "report_identity_conflict"
          ? 409
          : error.reasonCode === "storage_failed"
            ? 503
            : 400;
        res.status(status).json({ status: "failed", reason_code: error.reasonCode });
        return;
      }
      console.error("internal.agent-api.feedback-locators failed", serializeErrorForLog(error));
      res.status(503).json({ status: "failed", reason_code: "storage_failed" });
    }
  },
);

registerAgentApiRoute(
  "feedbackLocatorList",
  ...agentApiRequestValidators("feedbackLocatorList"),
  validateAgentApiResponseMiddleware("feedbackLocatorList"),
  async (req, res) => {
    const query = req.query as AgentApiFeedbackLocatorListQuery;
    const rows = await queryFeedbackLocators({
      serverId: req.serverId!,
      reportId: query.report_id,
      runtime: query.runtime,
      nativeStatus: query.native_status,
      nativeLookupMethod: query.native_lookup_method,
      servedExactSha256: query.served_exact_sha256,
      routeBasis: query.route_basis,
      limit: query.limit ? Number(query.limit) : undefined,
    });
    res.json({
      locators: rows.map((row) => ({
        report_id: row.reportId,
        receipt_id: row.receiptId,
        captured_at: row.capturedAt.toISOString(),
        runtime: row.runtime,
        native_status: row.nativeStatus,
        native_lookup_method: row.nativeLookupMethod,
        native_locator_kind: row.nativeLocatorKind,
        has_served_exact: row.hasServedExact,
        served_exact_sha256: row.servedExactSha256,
        route_basis: row.routeBasis,
        route_target: row.routeTarget,
      })),
    });
  },
);

function parseReactionEmoji(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const emoji = value.trim();
  if (!emoji || emoji.length > MAX_REACTION_LENGTH || /\s/.test(emoji)) return null;
  return emoji;
}

function toAgentFacingActorType(type: "user" | "agent" | "external_projection"): "human" | "agent" | "third_party_app" {
  return type === "external_projection" ? "third_party_app" : type === "user" ? "human" : "agent";
}

type AgentApiMessageSource = Record<string, unknown> & {
  content?: string;
  createdAt?: Date | string;
  messageType?: string;
  senderHandle?: string | null;
  senderName?: string | null;
  senderType?: string;
  taskAssigneeType?: string | null;
  taskAssigneeName?: string | null;
  timestamp?: Date | string;
  taskCurrentProjection?: {
    title: string;
    description: string | null;
    revision: number;
    superseded: boolean;
    amendedAt: Date | string | null;
    amendedByType: "user" | "agent" | "system" | null;
    amendedByName: string | null;
    source: "tasks_current_projection";
  };
};

function toAgentFacingActorTypeFromRow(value: unknown): "human" | "agent" | "third_party_app" {
  return value === "external_projection" ? "third_party_app" : value === "user" ? "human" : "agent";
}

function toAgentFacingNullableActorTypeFromRow(value: unknown): "human" | "agent" | null {
  return value === "user" ? "human" : value === "agent" ? "agent" : null;
}

function toAgentApiTimestamp(value: unknown): string | undefined {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? undefined : value.toISOString();
  }
  return typeof value === "string" ? value : undefined;
}

function toAgentApiMessageEnvelope(
  message: AgentApiMessageSource,
  content: string,
  viewerAgentId: string,
): AgentApiMessageEnvelope {
  const { senderHandle, ...messageEnvelopeFields } = message;
  const createdAt = toAgentApiTimestamp(message.createdAt);
  const timestamp = toAgentApiTimestamp(message.timestamp);
  const senderType = message.messageType === "system"
    ? "system"
    : toAgentFacingActorTypeFromRow(message.senderType);
  const taskAssigneeType = toAgentFacingNullableActorTypeFromRow(message.taskAssigneeType);
  const external = message.senderType === "external_projection";
  if (external) {
    return messageService.projectAgentVisibleHttpMessageResponse(
      message as messageService.EnrichedMessageRow,
      content,
      viewerAgentId,
    ) as AgentApiMessageEnvelope;
  }
  const envelope: AgentApiMessageEnvelope = {
    ...messageEnvelopeFields,
    senderName: senderHandle ?? message.senderName ?? undefined,
    senderType,
    taskAssigneeType,
    taskAssigneeName: message.taskAssigneeName ?? null,
    content,
    createdAt,
    timestamp,
    ...(message.taskCurrentProjection && {
      taskCurrentProjection: {
        ...message.taskCurrentProjection,
        amendedAt: toAgentApiTimestamp(message.taskCurrentProjection.amendedAt) ?? null,
      },
    }),
  };
  return envelope;
}

function isHistoryAnchorShape(value: string): boolean {
  return /^\d+$/.test(value) || /^[0-9a-f]{8}$/i.test(value) || UUID_RE.test(value);
}

function historyAnchorErrorPayload(
  channelRef: string,
  anchor: string,
  reason: "not_found" | "ambiguous" | "invalid",
) {
  if (reason === "invalid") {
    return {
      status: 400,
      body: {
        error: `Message anchor must be a seq, full UUID, or 8-character short id in ${channelRef}: ${anchor}`,
        errorCode: "INVALID_ARG",
      },
    };
  }
  if (reason === "ambiguous") {
    return {
      status: 400,
      body: {
        error: `Message anchor is ambiguous in ${channelRef}: ${anchor}`,
        errorCode: "AMBIGUOUS_ID",
        suggestedNextAction: "Use the full message UUID instead of the 8-character short id.",
      },
    };
  }
  return {
    status: 404,
    body: {
      error: `Message not found in ${channelRef}: ${anchor}`,
      errorCode: "NOT_FOUND",
    },
  };
}

function parseIdAllowlist(value: string | undefined): Set<string> {
  return new Set((value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean));
}

function isSendFreshnessEnabled(serverId: string, agentId: string): boolean {
  const mode = (process.env.SLOCK_ATTESTED_SEND_MODE ?? "on").trim().toLowerCase();
  if (mode === "on" || mode === "force" || mode === "1" || mode === "true") return true;
  if (mode !== "allowlist") return false;

  const serverAllowlist = parseIdAllowlist(process.env.SLOCK_ATTESTED_SEND_SERVER_IDS);
  const agentAllowlist = parseIdAllowlist(process.env.SLOCK_ATTESTED_SEND_AGENT_IDS);
  return serverAllowlist.has("*") ||
    agentAllowlist.has("*") ||
    serverAllowlist.has(serverId) ||
    agentAllowlist.has(agentId);
}

function toAttestedSendTargetType(type: "channel" | "private" | "joint" | "dm" | "thread"): attestedSendService.AttestedSendTargetType {
  return type === "private" || type === "joint" ? "channel" : type;
}

function formatAttestedMessageCount(count: number): string {
  return `${count} newer message${count === 1 ? "" : "s"}`;
}

function inferAgentApiSendTargetKind(target: unknown): string {
  if (typeof target !== "string") return "missing";
  const trimmed = target.trim();
  if (!trimmed) return "missing";
  if (trimmed.startsWith("dm:") && trimmed.includes(":")) return "dm_thread";
  if (trimmed.startsWith("dm:")) return "dm";
  if (trimmed.startsWith("#") && trimmed.includes(":")) return "channel_thread";
  if (trimmed.startsWith("#")) return "channel";
  if (trimmed.startsWith("@")) return "dm_peer";
  if (trimmed.startsWith("channelId:")) return "channel_id";
  return "unknown";
}

const DRIVE_BY_JOINED_TO_POST_WINDOW_MS = 24 * 60 * 60 * 1000;

async function buildDriveByJoinedToPostAttention(
  actingAgentId: string,
  resolved: { channelId: string; type: "channel" | "private" | "joint" | "dm" | "thread" },
  target: string,
): Promise<{
  driveByJoinedToPost: {
    reason: string;
    muteCommand: string;
    stillArrives: string[];
  };
} | null> {
  const trimmedTarget = target.trim();
  if (resolved.type !== "channel" || !trimmedTarget.startsWith("#") || trimmedTarget.includes(":")) return null;

  const db = getDb();
  const [membership] = await db
    .select({ addedAt: channelAgents.addedAt })
    .from(channelAgents)
    .where(and(
      eq(channelAgents.channelId, resolved.channelId),
      eq(channelAgents.agentId, actingAgentId),
    ))
    .limit(1);
  if (!membership) return null;

  const joinedAtMs = membership.addedAt.getTime();
  if (!Number.isFinite(joinedAtMs) || Date.now() - joinedAtMs >= DRIVE_BY_JOINED_TO_POST_WINDOW_MS) return null;

  const [priorMessage] = await db
    .select({ id: messages.id })
    .from(messages)
    .where(and(
      eq(messages.channelId, resolved.channelId),
      eq(messages.senderType, "agent"),
      eq(messages.senderId, actingAgentId),
    ))
    .limit(1);
  if (priorMessage) return null;

  // No D(t) oracle is needed here: recent channel join plus first top-level
  // post is the drive-by signature. Channel mute preserves personal @mention
  // pierce and followed-thread delivery; thread quieting requires unfollow.
  return {
    driveByJoinedToPost: {
      reason: "first_agent_message_with_recent_channel_join",
      muteCommand: `raft channel mute "${trimmedTarget}"`,
      stillArrives: ["@mentions still reach you, and threads you started stay followed and keep delivering until you unfollow them."],
    },
  };
}

function buildAgentChannelMuteResponse(
  channel: { id: string; name: string },
  state: channelService.InboxTargetActivityMuteState,
) {
  const channelRef = `#${channel.name}`;
  if (state.activityMuted) {
    return {
      ...state,
      attention: {
        state: "muted",
        muteFromSeq: state.muteFromSeq,
        ordinaryActivity: `Ordinary activity for ${channelRef} is muted from sequence ${state.muteFromSeq}. Existing Activity facts are not removed.`,
        unmuteCommand: `raft channel unmute ${channelRef}`,
        unmuteApi: `POST /internal/agent-api/channels/${channel.id}/unmute`,
        stillArrives: [
          "Channel mute does not mute personal @mentions; they still notify this agent.",
          "DMs still notify this agent.",
          "A task in this channel notifies this agent only when it personally @mentions you — being a task does not pierce channel mute.",
        ],
        threadBoundary: "Channel mute suppresses ordinary Activity from this channel only. Threads you follow keep delivering independently until you unfollow them.",
        catchUp: `Messages remain in ${channelRef} history. Open or read the channel to catch up; unmute does not backfill Activity suppressed while muted.`,
      },
    };
  }
  return {
    ...state,
    attention: {
      state: "unmuted",
      muteFromSeq: null,
      ordinaryActivity: `Future ordinary activity for ${channelRef} can be promoted again. Activity suppressed during the muted period is not backfilled.`,
      muteCommand: `raft channel mute ${channelRef}`,
      muteApi: `POST /internal/agent-api/channels/${channel.id}/mute`,
    },
  };
}

function buildAgentChannelLeaveAttention(channel: { name: string }) {
  const channelRef = `#${channel.name}`;
  return {
    state: "left",
    ordinaryActivity: `Ordinary channel delivery for ${channelRef} has stopped.`,
    stillArrives: [
      `If ${channelRef} is public, followed threads still notify until you unfollow them.`,
      "Personal @mentions can still notify when current visibility allows.",
    ],
    threadBoundary: "Leaving a channel does not unfollow existing thread follows. Private channel/thread content still requires current parent access.",
    manageCommand: `raft thread unfollow --target "${channelRef}:<thread-short-id>"`,
    manageApi: "POST /internal/agent-api/threads/unfollow",
  };
}

async function resolveAgentActivityMuteTarget(req: Request, res: Response) {
  const actingAgentId = req.actingAgentId!;
  const serverId = req.serverId!;
  const channelId = typeof req.params.channelId === "string" ? req.params.channelId : "";
  const channel = await channelService.getChannel(channelId);
  if (!channel || channel.serverId !== serverId || channel.deletedAt) {
    res.status(404).json({ error: "Channel not found" });
    return null;
  }
  if (channel.type === "thread") {
    res.status(400).json({
      error: "Threads do not have a separate mute state. Ordinary thread delivery is controlled by follow/unfollow and is independent from the parent channel's mute state; unfollow this thread to stop it.",
    });
    return null;
  }
  if (channel.type === "dm") {
    res.status(400).json({ error: "DM activity cannot be muted with channel mute" });
    return null;
  }
  if (channel.archivedAt) {
    res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
    return null;
  }
  const isMember = channelService.isEnabledAllChannel(channel)
    || await channelService.isChannelAgent(channel.id, actingAgentId);
  if (!isMember) {
    res.status(403).json({ error: "Agent can only mute channels it belongs to" });
    return null;
  }
  return channel;
}

/** What a gated write's freshness check found: see {@link evaluateAgentFreshness}. */
type AgentFreshnessEvaluation = {
  latestFreshnessSeq: number;
  latestFreshnessMessageId: string | null;
  attestedBoundarySeq: number;
  attestedBoundaryMessageId: string | null;
  newMessageCount: number;
  attestedTargetType: attestedSendService.AttestedSendTargetType;
  boundarySource: string;
  formalMentionFacts: Awaited<ReturnType<typeof attestedSendService.getFormalMentionFacts>>;
  useAttentionFacts: boolean;
  seenExactSeqs: number[];
};

/**
 * The freshness check a gated write runs (a send, and a task claim, status
 * update or amend): the newest message in the conversation the agent did not
 * write itself, against the model-seen boundary its request attests. One
 * function, so every gated write holds by the same rule.
 */
async function evaluateAgentFreshness(input: {
  agentId: string;
  channelId: string;
  channelType: "channel" | "private" | "joint" | "dm" | "thread";
  seenUpToSeq: number | undefined;
  requestedSeenExactSeqs: number[] | undefined;
}): Promise<AgentFreshnessEvaluation> {
  const { agentId, channelId, channelType, seenUpToSeq, requestedSeenExactSeqs } = input;
  const selfSender = { senderType: "agent" as const, senderId: agentId };
  const useAttentionFacts = channelType !== "thread";
  const latestAnchor = useAttentionFacts
    ? await attestedSendService.getLatestAgentAttentionAnchor(agentId, channelId, {
      excludeSender: selfSender,
    })
    : await attestedSendService.getLatestMessageAnchor(channelId, {
      excludeSender: selfSender,
    });
  const hasClientSeenBoundary = typeof seenUpToSeq === "number" && Number.isFinite(seenUpToSeq);
  const clientSeenUpToSeq = hasClientSeenBoundary
    ? Math.max(0, Math.floor(seenUpToSeq))
    : 0;
  const threadParentAnchor = !hasClientSeenBoundary && channelType === "thread"
    ? await attestedSendService.getThreadParentAnchor(channelId)
    : null;
  // Freshness proof must come from the current model-seen boundary.
  // Durable legacy read-ish cursors cannot prove the model actually saw
  // the messages; delivery ack state is volatile inbox state. The only
  // fallback is a cold thread's parent message.
  const latestFreshnessSeq = Math.max(latestAnchor.seq, threadParentAnchor?.seq ?? 0);
  const latestFreshnessMessageId = latestAnchor.seq > 0 && latestAnchor.seq >= (threadParentAnchor?.seq ?? 0)
    ? latestAnchor.messageId
    : (threadParentAnchor?.messageId ?? latestAnchor.messageId);
  const fallbackBoundarySeq = threadParentAnchor?.seq ?? 0;
  const attestedBoundarySeq = Math.min(
    hasClientSeenBoundary ? clientSeenUpToSeq : fallbackBoundarySeq,
    latestFreshnessSeq,
  );
  const attestedBoundaryMessageId = attestedBoundarySeq <= 0
    ? null
    : attestedBoundarySeq === threadParentAnchor?.seq
      ? threadParentAnchor.messageId
      : await attestedSendService.getMessageIdForSeq(channelId, attestedBoundarySeq);
  const seenExactSeqs = [...new Set((requestedSeenExactSeqs ?? [])
    .map((seq) => Math.floor(Number(seq)))
    .filter((seq) => Number.isInteger(seq)
      && seq > attestedBoundarySeq
      && seq <= latestFreshnessSeq))]
    .slice(0, 2_500);
  const newMessageCount = latestFreshnessSeq > attestedBoundarySeq
    ? useAttentionFacts
      ? await attestedSendService.countAgentAttentionMessagesAfterSeq(agentId, channelId, attestedBoundarySeq, {
        excludeSender: selfSender,
        latestSeq: latestFreshnessSeq,
        excludeExactSeqs: seenExactSeqs,
      })
      : await attestedSendService.countMessagesAfterSeq(channelId, attestedBoundarySeq, {
        excludeSender: selfSender,
        latestSeq: latestFreshnessSeq,
        excludeExactSeqs: seenExactSeqs,
      })
    : 0;
  const attestedTargetType = toAttestedSendTargetType(channelType);
  const boundarySource = hasClientSeenBoundary
    ? "client_seen"
    : seenExactSeqs.length > 0
      ? "client_seen_exact"
    : threadParentAnchor
      ? "thread_parent"
      : "none";
  const formalMentionFacts = latestFreshnessSeq > attestedBoundarySeq
    ? await attestedSendService.getFormalMentionFacts(
        agentId,
        channelId,
        attestedBoundarySeq,
        latestFreshnessSeq,
        seenExactSeqs,
      )
    : { count: 0, firstMessageId: null, firstHandle: null };
  return {
    latestFreshnessSeq,
    latestFreshnessMessageId,
    attestedBoundarySeq,
    attestedBoundaryMessageId,
    newMessageCount,
    attestedTargetType,
    boundarySource,
    formalMentionFacts,
    useAttentionFacts,
    seenExactSeqs,
  };
}

async function listRecentFreshnessMessagesAfterSeq(
  agentId: string,
  agentServerId: string,
  channelId: string,
  afterSeq: number,
  limit: number,
  latestSeq: number,
  useAttentionFacts: boolean,
  options?: attestedSendService.FreshnessMessageRangeOptions,
): Promise<AgentApiMessageEnvelope[]> {
  const freshnessMessages = useAttentionFacts
    ? await attestedSendService.listRecentAgentAttentionMessagesAfterSeq(
      agentId,
      channelId,
      afterSeq,
      limit,
      { ...options, latestSeq },
    )
    : await attestedSendService.listRecentMessagesAfterSeq(
      channelId,
      afterSeq,
      limit,
      { ...options, latestSeq },
    );
  const recentMessages = await messageService.listMessagesByIds(
    freshnessMessages.map((message) => message.messageId),
    {
      forwardedBundleViewerAgentId: agentId,
      forwardedBundleViewerServerId: agentServerId,
      includeSenderTimezone: true,
    },
  );
  if (recentMessages.length === 0) return [];
  // Message rows carry no conversation identity; without it clients cannot
  // form a reply target and drop every held message. Resolve the agent-facing
  // identity of the send target once and label each held envelope with it.
  const identity = await messageService.resolveAgentConversationIdentity(agentServerId, agentId, channelId);
  return recentMessages.map((message) => ({
    ...toAgentApiMessageEnvelope(message, typeof message.content === "string" ? message.content : "", agentId),
    ...identity,
  }));
}

/**
 * The held response of a gated write (a send, a task claim, status update or
 * amend) whose freshness check found newer messages: the bounded window of
 * them, the read position and delivery acknowledgements when every counted
 * message was shown, the recorded hold (`gate_triggered`, with the board's
 * channel id so a task write's retry can find it), and the agent's activity.
 * One function, so every gated write holds with the same envelope.
 */
async function renderAgentFreshnessHold(req: Request, hold: {
  action: ApmFreshnessSideEffectAction;
  agentId: string;
  serverId: string;
  agentServerId: string;
  channelId: string;
  channelType: "channel" | "private" | "joint" | "dm" | "thread";
  target: string;
  withhold: boolean;
  agentOrchestrator: AgentOrchestrator;
  evaluation: AgentFreshnessEvaluation;
  /** Task writes: which write was held, so only its own rerun is covered by the hold. */
  writeKey?: string;
  /** Send only: the draft this send replaced, how often it was held, and whether it is `--send-draft`. */
  replacedExisting?: boolean;
  reholdCount?: number;
  isSendDraft?: boolean;
}): Promise<AgentApiHeldFreshnessResponse> {
  const { action, agentId, serverId, agentServerId, channelId, channelType, target, withhold, agentOrchestrator } = hold;
  const replacedExisting = hold.replacedExisting ?? false;
  const reholdCount = hold.reholdCount ?? 0;
  const isSendDraft = hold.isSendDraft ?? false;
  const {
    attestedBoundarySeq: boundarySeq,
    attestedBoundaryMessageId: boundaryMessageId,
    latestFreshnessSeq: latestSeq,
    latestFreshnessMessageId: latestMessageId,
    boundarySource,
    attestedTargetType,
    useAttentionFacts,
    seenExactSeqs,
  } = hold.evaluation;
  const mentionCount = hold.evaluation.formalMentionFacts.count;
  const heldMessages = withhold
    ? []
    : await listRecentFreshnessMessagesAfterSeq(
        agentId,
        serverId,
        channelId,
        boundarySeq,
        ATTESTED_SEND_HELD_CONTEXT_LIMIT,
        latestSeq,
        useAttentionFacts,
        {
          excludeSender: { senderType: "agent", senderId: agentId },
          excludeExactSeqs: seenExactSeqs,
        },
      );
  const heldSeqs = heldMessages
    .map((message) => Number((message as { seq?: unknown }).seq))
    .filter((seq) => Number.isInteger(seq) && seq > 0);
  recordAgentContextReturned({
    source: "hold",
    agentId,
    channelId,
    ...agentRequestContextFromHeaders(req.headers),
    seqs: heldSeqs,
  });
  const attentionHoldCount = latestSeq > boundarySeq
    ? useAttentionFacts
      ? await attestedSendService.countAgentAttentionMessagesAfterSeq(agentId, channelId, boundarySeq, {
        excludeSender: { senderType: "agent", senderId: agentId },
        latestSeq,
        excludeExactSeqs: seenExactSeqs,
      })
      : await attestedSendService.countMessagesAfterSeq(channelId, boundarySeq, {
        excludeSender: { senderType: "agent", senderId: agentId },
        latestSeq,
        excludeExactSeqs: seenExactSeqs,
      })
    : 0;
  const shownMessageCount = heldMessages.length;
  // Without a model-seen boundary, do not present the whole target
  // history as "new messages"; this is a bounded first-touch context.
  const hasModelSeenProof = boundarySource === "client_seen" || boundarySource === "client_seen_exact";
  const firstTouchContextCount = withhold
    ? Math.min(attentionHoldCount, ATTESTED_SEND_HELD_CONTEXT_LIMIT)
    : shownMessageCount;
  const holdCount = hasModelSeenProof ? attentionHoldCount : firstTouchContextCount;
  const omittedMessageCount = !withhold && hasModelSeenProof
    ? Math.max(0, attentionHoldCount - shownMessageCount)
    : 0;
  // Chat semantics (task #41 final review): the hold's obligation is the honest
  // in-the-moment sentence: true skipped count (omittedMessageCount,
  // attention semantics — never a seq span) plus the runnable --before
  // anchor (minShownSeq). Nothing is persisted or re-surfaced.
  const minShownSeq = heldSeqs.length > 0 ? Math.min(...heldSeqs) : null;
  // Thread digest anchor: where this conversation started. Only fetched
  // for threads, only when context is shown, and only when the parent
  // is not already inside the displayed window.
  const threadParentMessage = !withhold && channelType === "thread"
    ? await attestedSendService.getThreadParentMessage(channelId)
    : null;
  // The read position moves to latestSeq only when this hold showed every
  // message it counts. When some were omitted (the context cap), they
  // have not been handed to the agent; marking them read would drop them
  // from its inbox (an external agent never receives them otherwise), so
  // the position stays and they are delivered as usual.
  const showedEveryHeldMessage = shownMessageCount >= attentionHoldCount;
  if (!withhold && showedEveryHeldMessage) {
    // Task #93 line B: the agent's own read is a side effect of the send; a read-state fence refusal must not fail
    // the send. Any other error still surfaces.
    const readState = await channelService.markRead(
      { kind: "agent", id: agentId },
      channelId,
      latestSeq,
    ).catch((error: unknown) => {
      if (isReadMutationFenceRefusal(error)) return null;
      throw error;
    });
    if (readState) {
      await emitScopeReadUpdated({
        io: req.app.get("io") as SocketServer | undefined,
        serverId: agentServerId,
        scopeId: channelId,
        peerKind: "agent",
        peerId: agentId,
        maxReadSeq: readState.maxReadSeq,
        changed: readState.changed,
      });
    }
  }
  if (!withhold && latestSeq > boundarySeq) {
    // Held context is returned inline; do not also redeliver it as unread.
    // Only what was shown: omitted messages stay queued for delivery.
    if (showedEveryHeldMessage) {
      agentOrchestrator.acknowledgeDeliveredMessagesForChannelUpToSeq?.(
        agentId,
        channelId,
        latestSeq,
      ) ?? agentOrchestrator.acknowledgeDeliveredMessagesForChannel(agentId, channelId, heldSeqs);
    } else {
      agentOrchestrator.acknowledgeDeliveredMessagesForChannel(agentId, channelId, heldSeqs);
    }
  }
  await attestedSendService.recordGateTriggered({
    id: randomUUID(),
    agentId: agentId,
    serverId: agentServerId,
    targetType: attestedTargetType,
    targetRef: target,
  }, {
    lastSeenMessageId: boundaryMessageId,
    latestMessageId,
    hasFormalMentionSinceLastSeen: false,
    draftReplacedExisting: replacedExisting,
    newMessageCount: holdCount,
    boundarySource,
    boundarySeq,
    latestSeq,
    action,
    channelId,
    writeKey: hold.writeKey,
  });
  const freshnessDecision = {
    action,
    decision: "syncing_hold" as const,
    ...(withhold ? { freshnessContextMode: "withheld" as const } : {}),
    target,
    reason: hasModelSeenProof ? "server_stale_model_boundary" : "server_first_touch_context",
    pendingMaxSeq: latestSeq,
    modelSeenSeq: boundarySource === "client_seen" ? boundarySeq : 0,
    heldMessageCount: shownMessageCount,
    omittedMessageCount,
  };
  const producerFactId = buildApmFreshnessDecisionProducerFactId(agentId, freshnessDecision);
  const envelope = projectApmHeldFreshnessEnvelope<AgentApiMessageEnvelope>({
    producerFactId,
    action,
    decision: "syncing_hold",
    heldMessages,
    newMessageCount: holdCount,
    omittedMessageCount,
    seenUpToSeq: latestSeq,
    freshnessContextMode: withhold ? "withheld" : undefined,
  });
  if (withhold) {
    await recordAgentRaftAction(req, agentId, {
      title: "Reviewer-isolation freshness hold",
      text: `${formatAttestedMessageCount(holdCount)} withheld`,
    });
  } else {
    const activity = projectApmHeldFreshnessActivity({
      producerFactId,
      action,
      decision: "syncing_hold",
      target,
      messageCount: shownMessageCount,
    });
    const title = action === "send" && (reholdCount > 0 || isSendDraft) ? "Send draft held" : activity.entry.title;
    await recordAgentRaftAction(req, agentId, {
      title,
      text: activity.entry.text,
      producerFactId,
      activity: activity.statusEntry.activity,
      activityDetail: title,
    });
  }
  return {
    ...(!withhold ? { ok: true } : {}),
    ...envelope.body,
    ...(!withhold
      ? { mentionAnnotation: { formalMentionCount: mentionCount } }
      : {}),
    ...(!withhold && action === "send"
      ? { continueAnywaySuggested: reholdCount >= 3 }
      : {}),
    ...(!withhold
      ? { seenUpToMessageId: latestMessageId }
      : {}),
    ...(!withhold && minShownSeq !== null
      ? { firstShownSeq: minShownSeq }
      : {}),
    ...(!withhold
      && threadParentMessage
      && (minShownSeq === null || threadParentMessage.seq < minShownSeq)
      ? {
        threadParentMessage: {
          seq: threadParentMessage.seq,
          messageId: threadParentMessage.messageId,
          senderName: threadParentMessage.senderName,
          createdAt: threadParentMessage.createdAt?.toISOString() ?? null,
          content: threadParentMessage.content,
        },
      }
      : {}),
  };
}

type TaskWriteFreshnessAction = "task_claim" | "task_update" | "task_amend";

/**
 * Glimpse G2: the freshness gate on a task write (claim, status update,
 * amend), by the same check and hold as a send.
 *
 * - It runs only when the request attests what its model context has seen
 *   (`seenUpToSeq`, or a non-empty `seenExactSeqs`). A request without them,
 *   as every client before G2 sends, is never held.
 * - The conversation is the task's board channel (the daemon's hold target
 *   too); a task's thread is not checked.
 * - No draft and no `--anyway`: the write is not applied, and rerunning it
 *   after reading passes.
 * - At most one hold per catch-up: when this same write (`writeKey`: the
 *   action and its task) was held on this board within
 *   TASK_WRITE_REHOLD_WINDOW_MS and the rerun's boundary covers that hold,
 *   newer messages that arrived meanwhile do not hold it again, or a busy
 *   board would hold every retry. Another write on the board is checked as
 *   usual. The hold is a row, so a rerun on any replica finds it; it is looked
 *   up only when about to hold. Reviewer isolation shows no messages, so its
 *   rerun cannot catch up: there, a recent hold of the write lets it pass.
 *
 * Returns the held response, or null to apply the write.
 */
async function holdTaskWriteForFreshness(req: Request, input: {
  action: TaskWriteFreshnessAction;
  /** The write's identity on the board: the action and its task(s). */
  writeKey: string;
  agentId: string;
  serverId: string;
  ctx: AgentApiTaskChannelContext;
  /** The board as the client spelled it, echoed in the hold. */
  target: string;
  seenUpToSeq: number | undefined;
  seenExactSeqs: number[] | undefined;
  freshnessContextMode: string | undefined;
}): Promise<AgentApiHeldFreshnessResponse | null> {
  const { action, agentId, serverId, ctx } = input;
  const attested = typeof input.seenUpToSeq === "number" || (input.seenExactSeqs?.length ?? 0) > 0;
  if (!attested || !isSendFreshnessEnabled(ctx.agent.serverId, agentId)) return null;
  const withhold = input.freshnessContextMode === "withheld";
  const gate = await tracePhase(
    async () => {
      const evaluation = await evaluateAgentFreshness({
        agentId,
        channelId: ctx.channelId,
        channelType: ctx.channelType,
        seenUpToSeq: input.seenUpToSeq,
        requestedSeenExactSeqs: input.seenExactSeqs,
      });
      if (evaluation.newMessageCount === 0) return { evaluation, outcome: "passed" as const };
      const lastHold = await attestedSendService.getRecentGateTriggered(
        agentId,
        ctx.channelId,
        input.writeKey,
        attestedSendService.TASK_WRITE_REHOLD_WINDOW_MS,
      );
      const caughtUp = lastHold !== null && (withhold || evaluation.attestedBoundarySeq >= lastHold.latestSeq);
      return { evaluation, outcome: caughtUp ? "repeat_passed" as const : "held" as const };
    },
    (_durationMs, result) => ({
      name: "agent_api_task.freshness.evaluated",
      attrs: {
        action,
        outcome: result.outcome,
        boundary_source: result.evaluation.boundarySource,
        new_message_count: result.evaluation.newMessageCount,
        freshness_context_mode: withhold ? "withheld" : "inline",
      },
    }),
  );
  if (gate.outcome !== "held") return null;
  return renderAgentFreshnessHold(req, {
    action,
    agentId,
    serverId,
    agentServerId: ctx.agent.serverId,
    channelId: ctx.channelId,
    channelType: ctx.channelType,
    target: input.target,
    withhold,
    agentOrchestrator: req.app.get("agentOrchestrator") as AgentOrchestrator,
    evaluation: gate.evaluation,
    writeKey: input.writeKey,
  });
}

async function recordAgentRaftAction(
  req: Request,
  agentId: string,
  event: Parameters<AgentOrchestrator["recordAgentRaftAction"]>[1],
): Promise<void> {
  const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
  if (typeof agentOrchestrator?.recordAgentRaftAction !== "function") return;
  await agentOrchestrator.recordAgentRaftAction(agentId, event);
}

/**
 * Closed-set client label for send traces: `cli` (raft CLI, also via the
 * daemon credential proxy), `daemon`, `other`, or `absent` (no header: SDK and
 * hosted runtimes do not send one today).
 */
function agentApiClientKind(req: Request): "cli" | "daemon" | "other" | "absent" {
  const client = req.header("X-Raft-Client") ?? req.header("X-Slock-Client");
  if (!client) return "absent";
  if (client === "cli") return "cli";
  if (client.startsWith("daemon")) return "daemon";
  return "other";
}

function isRaftCliRequest(req: Request): boolean {
  // Dual-read is permanent: senders switched from X-Slock-Client to
  // X-Raft-Client in the slock→raft rename, but already-deployed CLI/daemon
  // builds keep sending the legacy header indefinitely.
  const client = req.header("X-Raft-Client") ?? req.header("X-Slock-Client");
  return client === "cli";
}

function recordRaftCliActivity(
  req: Request,
  agentId: string,
  event: Parameters<AgentOrchestrator["recordRaftCliAction"]>[1],
) {
  if (!isRaftCliRequest(req)) return;
  const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
  agentOrchestrator?.recordRaftCliAction?.(agentId, event);
}

async function serializeOAuthClientForAgentApi(
  client: Awaited<ReturnType<typeof oauthService.listOAuthClients>>[number],
  serverId: string,
) {
  const isThirdPartyGlobal = client.appType === "third_party_global";
  const agentManifest = oauthService.resolveAgentManifest(client);
  return {
    id: client.id,
    clientId: client.clientId,
    appType: client.appType,
    name: isThirdPartyGlobal
      ? renderThirdPartyInertText({ field: "app_name", value: client.name })
      : client.name,
    description: isThirdPartyGlobal && client.description
      ? renderThirdPartyInertText({ field: "description", value: client.description })
      : (client.description ?? null),
    // whenToUse is rendered verbatim into an agent system prompt; always run
    // it through the inert renderer, for every app type, so a bare @name can
    // never reach a daemon as a reference token.
    whenToUse: client.whenToUse
      ? renderThirdPartyInertText({ field: "when_to_use", value: client.whenToUse })
      : null,
    homepageUrl: client.homepageUrl ?? null,
    returnUrl: client.returnUrl ?? null,
    ...(client.setupUrl ? {
      setupUrl: client.setupUrl,
      setupDescription: client.setupDescription ? renderThirdPartyInertText({ field: "description", value: client.setupDescription }) : null,
      setup: await getAppInstallationSetup({ clientId: client.id, serverId }),
    } : {}),
    agentManifestUrl: agentManifest.url,
    agentManifestUrlSource: agentManifest.source ?? null,
    createdAt: client.createdAt.toISOString(),
    updatedAt: client.updatedAt.toISOString(),
    ...await oauthService.projectOfficialAppDiscoveryForAgent(client),
  };
}

async function serializeMarketplaceOAuthClientForAgentApi(
  client: Awaited<ReturnType<typeof oauthService.searchPublicMarketplaceOAuthClients>>[number],
) {
  const agentManifest = oauthService.resolveAgentManifest(client);
  return {
    id: client.id,
    clientId: client.clientId,
    name: renderThirdPartyInertText({ field: "app_name", value: client.name }),
    description: client.description
      ? renderThirdPartyInertText({ field: "description", value: client.description })
      : null,
    category: client.category,
    dataAccessSummary: client.dataAccessSummary
      ? renderThirdPartyInertText({ field: "data_access", value: client.dataAccessSummary })
      : null,
    homepageUrl: client.homepageUrl ?? null,
    ...(client.setupUrl ? { setupUrl: client.setupUrl, setupDescription: client.setupDescription ? renderThirdPartyInertText({ field: "description", value: client.setupDescription }) : null } : {}),
    agentManifestUrl: agentManifest.url,
    agentManifestUrlSource: agentManifest.source ?? null,
    allowedScopes: client.allowedScopes ?? [],
    logoUrl: client.logoUrl ?? null,
    installedOnServer: client.installedAt !== null,
    updatedAt: client.updatedAt.toISOString(),
    ...await oauthService.projectOfficialAppDiscoveryForAgent(client),
  };
}

function normalizeAgentLoginScopes(
  raw: unknown,
  client: Pick<oauthService.OAuthClientRecord, "allowedScopes">,
): string[] {
  if (raw === undefined || raw === null) {
    const scopes = oauthService.defaultAgentLoginScopes(client);
    if (scopes.length === 0) throw new Error("invalid_scope");
    return scopes;
  }
  if (!Array.isArray(raw)) throw new Error("scopes must be an array");

  const scopes = Array.from(new Set(raw.map((scope) => {
    if (typeof scope !== "string") throw new Error("scopes must contain strings");
    const trimmed = scope.trim();
    if (!trimmed) throw new Error("scope values must be non-empty");
    return trimmed;
  }))).sort();

  if (scopes.length === 0) throw new Error("at least one scope is required");
  return scopes;
}

function normalizeIntegrationAppPrepareScopes(raw: unknown): string[] {
  if (raw === undefined || raw === null) return [];
  if (!Array.isArray(raw)) throw new Error("scopes must be an array");
  return Array.from(new Set(raw.map((scope) => {
    if (typeof scope !== "string") throw new Error("scopes must contain strings");
    const trimmed = scope.trim();
    if (!trimmed) throw new Error("scope values must be non-empty");
    return trimmed;
  }))).sort();
}

function optionalIntegrationString(raw: unknown): string | undefined {
  return typeof raw === "string" ? raw.trim() || undefined : undefined;
}

function resolveAgentLoginClient(
  clients: Awaited<ReturnType<typeof oauthService.listOAuthClients>>,
  rawService: unknown,
):
  | { ok: true; client: Awaited<ReturnType<typeof oauthService.listOAuthClients>>[number] }
  | { ok: false; status: 400 | 404 | 409; error: string } {
  if (typeof rawService !== "string" || !rawService.trim()) {
    return { ok: false, status: 400, error: "service is required" };
  }

  const service = rawService.trim();
  const exact = clients.find((client) => client.id === service || client.clientId === service);
  if (exact) return { ok: true, client: exact };

  const normalized = service.toLowerCase();
  const nameMatches = clients.filter((client) => client.name.toLowerCase() === normalized);
  if (nameMatches.length === 1) return { ok: true, client: nameMatches[0] };
  if (nameMatches.length > 1) {
    return { ok: false, status: 409, error: "Service name is ambiguous; use the service id or client id" };
  }

  return { ok: false, status: 404, error: "Registered service not found" };
}

type AgentApiTaskChannelContext = {
  agent: NonNullable<Awaited<ReturnType<typeof agentService.getAgent>>>;
  channelId: string;
  storageChannelId: string;
  localChannel: TaskSurfaceChannel;
  channelType: "channel" | "private" | "joint" | "dm" | "thread";
  surface: TaskChannelSurface;
};

/** Null when the channel can't be used for tasks; the reason goes on the request span (o11y T1), the body stays "Agent or channel not found". */
async function resolveAgentApiTaskChannel(
  agentId: string,
  serverId: string,
  channelRef: string,
  res: Response,
): Promise<AgentApiTaskChannelContext | null> {
  const agent = await agentService.getAgent(agentId);
  if (!agent || agent.serverId !== serverId) {
    setRequestTraceErrorCode(res, "agent_not_found");
    return null;
  }
  const resolved = await channelService.resolveChannelByName(serverId, agentId, channelRef);
  if (!resolved) {
    setRequestTraceErrorCode(res, "channel_not_found");
    return null;
  }
  const surface = await resolveTaskChannelSurface(serverId, resolved.channelId);
  if (!surface) {
    setRequestTraceErrorCode(res, "task_channel_unavailable");
    return null;
  }
  return {
    agent,
    channelId: surface.localChannel.id,
    storageChannelId: surface.storageChannelId,
    localChannel: surface.localChannel,
    channelType: surface.localChannel.type,
    surface,
  };
}

function rejectAgentApiTaskWriteIfNeeded(canPost: boolean, res: Response): boolean {
  if (!canPost) {
    setRequestTraceErrorCode(res, "channel_membership_required");
    res.status(403).json({ error: "Agent must join this channel to modify tasks" });
    return true;
  }
  return false;
}

function rejectHistoricalJointTaskWriteIfNeeded(
  surface: TaskChannelSurface,
  createdAt: Date | string,
  res: Response,
): boolean {
  if (!isHistoricalJointTaskReadOnly(surface, createdAt)) return false;
  res.status(403).json({
    error: "Historical tasks are read-only from a participant workspace",
    code: "joint_task_read_only",
  });
  return true;
}

async function assertAgentApiTaskWritableChannel(
  ctx: AgentApiTaskChannelContext,
  agentId: string,
  res: Response,
  options: { threadError: string },
): Promise<boolean> {
  if (ctx.channelType === "thread") {
    res.status(409).json({ error: options.threadError });
    return false;
  }
  if (rejectAgentApiTaskWriteIfNeeded(await channelService.canAgentPostToChannel(ctx.channelId, agentId), res)) return false;
  if (await channelService.isChannelArchived(ctx.channelId)) {
    res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
    return false;
  }
  return true;
}

const EXTERNAL_ACTIVITY_EVENT_LIMIT = 200;
const FORBIDDEN_EXTERNAL_ACTIVITY_KEYS = new Set(["transcript_path", "transcriptPath"]);
const ALLOWED_EXTERNAL_ACTIVITY_KEYS = new Set([
  "schema",
  "eventId",
  "event_id",
  "sessionId",
  "session_id",
  "hookEventName",
  "hook_event_name",
  "toolName",
  "tool_name",
  "status",
  "detail",
  "occurredAt",
  "occurred_at",
  "durationMs",
  "duration_ms",
  "toolUseId",
  "tool_use_id",
  "exitCode",
  "exit_code",
  "ok",
  "errorClass",
  "error_class",
  "toolInput",
  "tool_input",
  "toolOutput",
  "tool_output",
  "toolInputTruncated",
  "tool_input_truncated",
  "toolOutputTruncated",
  "tool_output_truncated",
  "truncated",
  // A subagent's event (ids and enums; validated at ingest).
  "subagent",
]);

const EXTERNAL_ACTIVITY_IGNORED_FIELD_REPORT_LIMIT = 20;

type ParsedExternalAgentActivityIngest = ExternalAgentActivityIngestRequest & {
  /** Unknown event field names stripped before ingest (bounded, for logs). */
  ignoredFields: string[];
};

function parseExternalAgentActivityIngest(body: unknown): ParsedExternalAgentActivityIngest | { error: string; code: string } {
  if (!body || typeof body !== "object") {
    return { error: "Request body is required", code: "body_required" };
  }
  const candidate = body as Record<string, unknown>;
  if (candidate.schema !== EXTERNAL_AGENT_ACTIVITY_INGEST_SCHEMA) {
    return { error: "schema must be raft-agent-activity-ingest.v1", code: "schema_invalid" };
  }
  if (!Array.isArray(candidate.events)) {
    return { error: "events must be an array", code: "events_invalid" };
  }
  if (candidate.events.length > EXTERNAL_ACTIVITY_EVENT_LIMIT) {
    return { error: `events cannot exceed ${EXTERNAL_ACTIVITY_EVENT_LIMIT}`, code: "events_too_many" };
  }
  const events: ExternalAgentActivityEvent[] = [];
  const ignoredFields = new Set<string>();
  for (const event of candidate.events) {
    if (!event || typeof event !== "object" || Array.isArray(event)) {
      return { error: "events must contain objects", code: "event_invalid" };
    }
    // Unknown fields are stripped, not rejected: a 400 fails the whole batch and
    // stalls the sender's cursor, so a newer bridge adding a field would stop
    // all its activity. Only allowlisted fields reach ingest.
    const known: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(event)) {
      if (FORBIDDEN_EXTERNAL_ACTIVITY_KEYS.has(key)) {
        return { error: "transcript_path is not accepted on activity ingest", code: "transcript_path_forbidden" };
      }
      if (ALLOWED_EXTERNAL_ACTIVITY_KEYS.has(key)) {
        known[key] = value;
      } else if (ignoredFields.size < EXTERNAL_ACTIVITY_IGNORED_FIELD_REPORT_LIMIT) {
        ignoredFields.add(key.slice(0, 64));
      }
    }
    events.push(known as ExternalAgentActivityEvent);
    // raft-agent-status.v1 fields. Legacy hook-outcome `status` values are
    // still accepted (and ignored for status) so existing bridges keep working.
    const { status, detail } = event as { status?: unknown; detail?: unknown };
    if (status !== undefined && !isRaftAgentStatus(status) && !isExternalAgentActivityLegacyStatus(status)) {
      return { error: `status must be one of ${RAFT_AGENT_STATUS_VALUES.join(", ")}`, code: "status_invalid" };
    }
    if (detail !== undefined && typeof detail !== "string") {
      return { error: "detail must be a string", code: "detail_invalid" };
    }
    if (typeof detail === "string" && detail.length > RAFT_AGENT_STATUS_DETAIL_LIMIT) {
      return { error: `detail cannot exceed ${RAFT_AGENT_STATUS_DETAIL_LIMIT} characters`, code: "detail_too_long" };
    }
  }
  return {
    schema: EXTERNAL_AGENT_ACTIVITY_INGEST_SCHEMA,
    events,
    ignoredFields: [...ignoredFields],
    ...(typeof candidate.coreSessionId === "string" ? { coreSessionId: candidate.coreSessionId } : {}),
    ...(typeof candidate.adapterInstance === "string" ? { adapterInstance: candidate.adapterInstance } : {}),
    ...(typeof candidate.dropped === "number" ? { dropped: candidate.dropped } : {}),
  };
}

type AgentApiWakeReason = "message_pending" | "system_notice_pending" | "task_pending";

interface AgentApiWakeHint {
  event_id: string;
  seq: number | null;
  message_id: string | null;
  /**
   * Canonical send target of the pending message (`#name`, `#name:<8hex>`,
   * `dm:@peer[~kind]`, `dm:@peer:<8hex>`), the same string `/events`
   * `reply_target` and the CLI print and `/send` resolves. Null when the queued
   * message carries no conversation name to derive it from.
   */
  target: string | null;
  channel_id: string;
  channel_name: string;
  channel_type: AgentMessage["channel_type"];
  wake_reason: AgentApiWakeReason;
  attention_hint?: AgentMessage["attention_hint"];
  traceparent?: string;
}

function queuedMessageChannelId(message: AgentMessage): string {
  return (message as AgentMessage & { channelId?: string }).channel_id
    ?? (message as AgentMessage & { channelId?: string }).channelId
    ?? "";
}

function queuedMessageSeq(message: AgentMessage): number | null {
  return Number.isInteger(message.seq) && (message.seq ?? 0) > 0 ? message.seq! : null;
}

function queuedMessageReason(message: AgentMessage): AgentApiWakeReason {
  if (message.task_number != null) return "task_pending";
  if (message.sender_type === "system") return "system_notice_pending";
  return "message_pending";
}

function buildWakeHint(message: AgentMessage): AgentApiWakeHint | null {
  const channelId = queuedMessageChannelId(message);
  if (!channelId) return null;
  const seq = queuedMessageSeq(message);
  const messageId = message.message_id ?? null;
  const eventAnchor = messageId ?? (seq !== null ? `seq-${seq}` : `${channelId}-${message.timestamp}`);
  const hint: AgentApiWakeHint = {
    event_id: `wake-hint:${eventAnchor}`,
    seq,
    message_id: messageId,
    // Derived from the routing fields the queued message already carries
    // (channel/parent names, DM peer ref) -- no per-hint DB read.
    target: formatInboxMessageTarget(message),
    channel_id: channelId,
    channel_name: message.channel_name,
    channel_type: message.channel_type,
    wake_reason: queuedMessageReason(message),
  };
  if (message.attention_hint) hint.attention_hint = message.attention_hint;
  if (message.traceparent) hint.traceparent = message.traceparent;
  return hint;
}

function sendManagedMcpAgentApiError(error: unknown, res: Response, context: { serverName?: string | null; toolName?: string | null; mcpServerId?: string | null; isCall?: boolean } = {}): void {
  // The agent-facing words for the common failures (managedMcpAgentErrors),
  // also as suggestedNextAction, which raft-sdk shows as the next action (it
  // never shows a raw `error`); anything else keeps its original message.
  const body = (code: string, message: string) => {
    const agentText = managedMcpAgentErrorMessage(code, context);
    return agentText ? { error: agentText, code, suggestedNextAction: agentText } : { error: message, code };
  };
  if (error instanceof ManagedMcpServiceError) {
    const status = error.code.endsWith("not_found") ? 404 : 409;
    res.status(status).json(body(error.code, error.message));
    return;
  }
  if (error instanceof ManagedMcpCredentialError) {
    res.status(503).json(body(error.code, error.message));
    return;
  }
  if (error instanceof ManagedMcpGatewayError) {
    res.status(error.code === "managed_mcp_unreachable" ? 502 : 409).json(body(error.code, error.message));
    return;
  }
  if (error instanceof ManagedMcpOAuthError) {
    res.status(error.code === "managed_mcp_oauth_busy" ? 503 : 409).json(body(error.code, error.message));
    return;
  }
  // On a call, an unexpected failure can come after the remote tool ran: say so (same words as raft-sdk).
  if (context.isCall) {
    res.status(500).json({ error: MCP_CALL_INTERNAL_ERROR_TEXT, code: "managed_mcp_internal_error", suggestedNextAction: MCP_CALL_INTERNAL_ERROR_TEXT });
    return;
  }
  res.status(500).json({ error: "Managed MCP request failed", code: "managed_mcp_internal_error" });
}

registerAgentApiRoute("managedMcpTools", validateAgentApiResponseMiddleware("managedMcpTools"), async (req, res) => {
  try {
    res.json(await getManagedMcpRuntimeSnapshot(req.serverId!, req.actingAgentId!));
  } catch (error) {
    sendManagedMcpAgentApiError(error, res);
  }
});

registerAgentApiRoute(
  "managedMcpCall",
  ...agentApiRequestValidators("managedMcpCall"),
  validateAgentApiResponseMiddleware("managedMcpCall"),
  async (req, res) => {
  try {
    const request = req.body as AgentApiRequestBodyByRoute["managedMcpCall"];
    res.json(await withTraceChildSpan(
      "server.managed_mcp.gateway_call",
      {
        surface: "server",
        attrs: {
          event_kind: "managed_mcp_gateway_call",
          server_id: req.serverId!,
          agent_id: req.actingAgentId!,
          mcp_server_id: request.mcpServerId,
        },
      },
      () => executeManagedMcpCall(req.serverId!, req.actingAgentId!, request),
      {
        onSuccess: (result) => ({
          outcome: "success",
          result_block_count: result.content.length,
          remote_error: result.isError,
        }),
        onError: (error) => ({
          outcome: "error",
          error_code: typeof (error as { code?: unknown } | null)?.code === "string"
            ? (error as { code: string }).code
            : "managed_mcp_internal_error",
        }),
      },
    ));
  } catch (error) {
    const request = req.body as AgentApiRequestBodyByRoute["managedMcpCall"];
    const [named] = await getDb().select({ name: managedMcpServers.name }).from(managedMcpServers)
      .where(and(eq(managedMcpServers.id, request.mcpServerId), eq(managedMcpServers.serverId, req.serverId!)))
      .limit(1)
      .catch(() => []);
    sendManagedMcpAgentApiError(error, res, { serverName: named?.name ?? null, toolName: request.toolName, mcpServerId: request.mcpServerId, isCall: true });
  }
  },
);

/**
 * GET /internal/agent-api
 *
 * RFC v0.8 — whoami. Returns the bound agent identity, server,
 * credential id, and the MAX-set scopes (active-set wiring lands later).
 *
 * Response 200:
 *   { agentId, agentName, agentDisplayName, serverId, credentialId, scopes }
 */
registerAgentApiRoute("selfSessionReset", async (req, res) => {
  if (req.body && (typeof req.body !== "object" || Array.isArray(req.body) || Object.keys(req.body).length !== 0)) {
    res.status(400).json({ error: "This operation accepts no target or other arguments", code: "invalid_request" });
    return;
  }
  const agentId = req.actingAgentId!;
  const launchId = req.header("X-Agent-Launch-Id")?.trim();
  if (!launchId || launchId.length > 128) {
    res.status(400).json({ error: "A current managed launch is required", code: "launch_required" });
    return;
  }
  const orchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
  if (!orchestrator) {
    res.status(503).json({ error: "Agent orchestrator unavailable", code: "orchestrator_unavailable" });
    return;
  }
  try {
    const result = await orchestrator.requestSelfSessionReset(agentId, launchId);
    if (result !== "accepted") {
      const error = result === "unsupported" ? "Runtime does not support self session reset"
        : result === "idle" ? "The requesting turn has already ended" : "Launch is no longer current";
      res.status(409).json({ error, code: result });
      return;
    }
    res.status(202).json({ status: "accepted" });
  } catch {
    res.status(503).json({ error: "The daemon did not confirm; the request may still have been accepted. Check status before retrying.", code: "reset_acceptance_unknown" });
  }
});

internalAgentApiRouter.get("/", async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId;
    const serverId = req.serverId;
    const credentialId = req.agentCredentialId;
    const scopes = req.agentCredentialScopes;
    if (!actingAgentId || !serverId || !credentialId || !scopes) {
      res.status(500).json({ error: "Agent credential state missing" });
      return;
    }

    const agent = await agentService.getAgent(actingAgentId);
    if (!agent) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }
    const serverRole = await getActorServerRoleInServer(serverId, "agent", actingAgentId);

    res.json({
      agentId: actingAgentId,
      agentName: agent.name,
      agentDisplayName: agent.displayName ?? null,
      serverId,
      serverRole,
      serverCapabilities: getServerCapabilities(serverRole),
      credentialId,
      scopes,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load agent identity",
      logPrefix: "internal.agent-api.whoami error:",
      err,
    });
  }
});

/**
 * GET /internal/agent-api/server
 *
 * Agent-self equivalent of the legacy `/internal/agent/:id/server` route.
 * The acting agent is the credential-bound runner principal; there is no
 * URL `:id` to spoof.
 */
/**
 * The rollout flags a managed agent's Computer gets at start (AgentConfig
 * reminderApp / vaultApp), for agents that build their own prompt. An
 * evaluation failure reads as off, as it does at agent start.
 */
async function agentContextFeatures(serverId: string): Promise<{ reminderApp: boolean; vaultApp: boolean }> {
  try {
    const [reminderApp, vaultApp] = await evaluateFeatureFlags([
      { key: REMINDER_APP_FEATURE_FLAG_KEY, serverId },
      { key: VAULT_APP_FEATURE_FLAG_KEY, serverId },
    ]);
    return { reminderApp: reminderApp?.enabled === true, vaultApp: vaultApp?.enabled === true };
  } catch (error) {
    console.warn("[agent-context] app rollout flag evaluation failed; reporting both off", serializeErrorForLog(error));
    return { reminderApp: false, vaultApp: false };
  }
}

// Identity bootstrap. A managed agent learns who it is from the standing
// system prompt its daemon builds; an external agent has no daemon, so it asks
// here. The prompt is the self-hosted-runner guide rendered by the same shared
// builder the daemon uses for the `raft-cli-overview` manual, filled with this
// agent's identity.
registerAgentApiRoute("agentContext", async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }
    const server = await serverService.getServer(serverId);
    if (!server) {
      res.status(401).json({ error: "Server no longer exists" });
      return;
    }
    const external = isExternalAgentRuntime(agent.runtime);
    const serverRole = await getActorServerRoleInServer(serverId, "agent", actingAgentId);
    const prompt = external
      ? {
          audience: "self-hosted-runner" as const,
          text: buildRaftCliGuideMarkdown({
            handle: agent.name,
            displayName: agent.displayName || agent.name,
            description: agent.description,
            serverName: server.name,
            serverRole,
          }),
        }
      : null;
    sendAgentApiResponse("agentContext", res, {
      agent: {
        id: agent.id,
        name: agent.name,
        displayName: agent.displayName ?? null,
        description: agent.description ?? null,
        runtime: agent.runtime,
        external,
        assistantOf: agent.assistantOwnerUserId ? { userId: agent.assistantOwnerUserId } : null,
        serverRole,
      },
      server: { id: server.id, slug: server.slug, name: server.name },
      credential: { capabilities: [...(req.agentCredentialScopes ?? [])] },
      prompt,
      features: await agentContextFeatures(serverId),
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load agent context",
      logPrefix: "internal.agent-api.context error:",
      err,
    });
  }
});

registerAgentApiRoute("serverInfo", async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const channels = (await channelService.listChannelsForAgent(serverId, actingAgentId)).map((channel) => ({
      ...channel,
      id: asChannelId(channel.id),
    }));
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
    const allAgents = await buildServerInfoAgentSummaries(serverId, agentOrchestrator);

    const humans = await buildServerInfoHumanSummaries(serverId, actingAgentId);
    const machine = agent.machineId ? await machineService.getMachine(asMachineId(agent.machineId)) : null;
    const serverRole = await getActorServerRoleInServer(serverId, "agent", actingAgentId);
    const runtimeContext = {
      agentId: agent.id,
      runtime: agent.runtime,
      model: agent.model,
      reasoningEffort: agent.reasoningEffort ?? null,
      serverId: agent.serverId,
      machineId: agent.machineId,
      machineName: machine?.name ?? null,
      machineDescription: machine?.description ?? null,
      machineHostname: machine?.hostname ?? null,
      machineOs: machine?.os ?? null,
      daemonVersion: agent.machineId ? agentOrchestrator?.getMachineDaemonVersion(agent.machineId) ?? machine?.daemonVersion ?? null : null,
      workspacePath: null,
    };

    sendAgentApiResponse("serverInfo", res, { runtimeContext, serverRole, serverCapabilities: getServerCapabilities(serverRole), channels, agents: allAgents, humans });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to get server info",
      logPrefix: "internal.agent-api.server error:",
      err,
    });
  }
});

// `raft user info`: one user's serverInfo entry and their memberships among one
// window of the caller's serverInfo channels, each decided exactly as the
// caller's roster read (`channelMembers` on `#<name>`) would decide it, except
// that a joint channel's roster, which spans every server's projection, is
// matched by the user's identity rather than by name, and that the caller's
// built-in app conversations are inspected and list no one. Each membership
// row is the channel's own facts plus the user's membership (`joined`, and
// their `channelRole` where channel roles exist), none of the caller's. It
// replaces serverInfo plus one roster per channel, so it is gated by both:
// the contract's `channels` capability and `channel:read` grant (channelMembers),
// and serverInfo's `read` capability and `server:read` grant, below.
registerAgentApiRoute(
  "userChannels",
  requireAgentCapability("read"),
  requireAgentGrant("server:read"),
  ...agentApiRequestValidators("userChannels"),
  async (req, res) => {
    try {
      const actingAgentId = req.actingAgentId!;
      const serverId = req.serverId!;
      const agent = await agentService.getAgent(actingAgentId);
      if (!agent || agent.serverId !== serverId) {
        res.status(401).json({ error: "Agent no longer exists" });
        return;
      }
      const params = req.params as AgentApiRequestParamsByRoute["userChannels"];
      const query = req.query as AgentApiRequestQueryByRoute["userChannels"];
      const name = params.name.startsWith("@") ? params.name.slice(1) : params.name;
      const offset = Number(query.offset ?? 0);
      const limit = Number(query.limit ?? AGENT_API_USER_CHANNELS_DEFAULT_LIMIT);

      // serverInfo's lookup: exact name, agents before humans, the caller's directory.
      const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
      const agentMatch = await findServerInfoAgentSummaryByName(serverId, name, agentOrchestrator);
      const humanMatch = agentMatch ? null : await findServerInfoHumanSummaryByName(serverId, actingAgentId, name);
      const subjectId = agentMatch?.id ?? humanMatch?.id;
      if (!subjectId) {
        res.status(404).json({ error: "User not found or not visible", code: AGENT_API_USER_NOT_FOUND_CODE });
        return;
      }
      const kind = agentMatch ? "agent" as const : "human" as const;

      const channels = (await channelService.listChannelsForAgent(serverId, actingAgentId)).map((channel) => ({
        ...channel,
        id: asChannelId(channel.id),
      }));
      const window = channels.slice(offset, offset + limit);
      const rosters = await channelService.getChannelRosterNameMatchesForAgentWindow(serverId, actingAgentId, window, { kind, name, id: subjectId });
      // The roster route's human filter: the caller's hidden directory.
      const visibleHumans = new Set(await filterAgentVisibleHumansForHiddenDirectory(
        serverId,
        actingAgentId,
        rosters.flatMap((roster) => roster?.humans ?? []),
      ));
      // A membership row describes the channel and the subject's membership,
      // never the caller's (see userChannelMembershipRow).
      const uncheckedCount = rosters.filter((roster) => !roster).length;
      const memberships = window.flatMap((channel, index) => {
        const roster = rosters[index];
        if (!roster) return [];
        const listed = kind === "agent"
          ? roster.agents[0]
          : roster.humans.find((human) => visibleHumans.has(human));
        return listed ? [channelService.userChannelMembershipRow(channel, listed.channelRole)] : [];
      });

      const page = { total: channels.length, offset, limit };
      if (agentMatch) {
        sendAgentApiResponse("userChannels", res, { user: agentMatch.summary, kind: "agent", memberships, uncheckedCount, page });
      } else {
        sendAgentApiResponse("userChannels", res, { user: humanMatch!.summary, kind: "human", memberships, uncheckedCount, page });
      }
    } catch (err) {
      sendJsonServerError(req, res, {
        error: "Failed to get user channels",
        logPrefix: "internal.agent-api.user-channels error:",
        err,
      });
    }
  },
);

registerAgentApiRoute("serverUpdate", async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const result = await updateServerProfileForAgent({
      actor: { id: agent.id, name: agent.name, serverId: agent.serverId },
      serverId,
      body: req.body,
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to update server",
      logPrefix: "internal.agent-api.server.update error:",
      err,
    });
  }
});

// Server Labs uses the same transactional service and role policy as the
// human Settings API. The credential scope is an additional transport gate;
// it never substitutes for owner/admin membership authorization.
// Labs keep their own body checks (stable codes such as invalid_enabled and
// invalid_lab_key); the contract supplies the capability and the response shape.
registerAgentApiRoute("serverLabs", validateAgentApiResponseMiddleware("serverLabs"), getAgentServerLabs);
registerAgentApiRoute("serverLabsAccess", validateAgentApiResponseMiddleware("serverLabsAccess"), patchAgentServerLabsAccess);
registerAgentApiRoute("serverLabEnrollment", validateAgentApiResponseMiddleware("serverLabEnrollment"), putAgentServerLabEnrollment);

internalAgentApiRouter.post("/server/avatar", requireAgentCapability("server"), requireAgentGrant("server:update"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const authFailure = await assertAgentCanManageServerProfile({
      actor: { id: agent.id, name: agent.name, serverId: agent.serverId },
      serverId,
    });
    if (authFailure) {
      res.status(authFailure.status).json(authFailure.body);
      return;
    }

    const server = await serverService.getServer(serverId);
    if (!server) {
      res.status(404).json({ error: "Server not found" });
      return;
    }
    const uploaded = await runSingleAvatarUpload(serverAvatarUpload, req);
    if (!uploaded) {
      res.status(400).json({ error: "No avatar file provided" });
      return;
    }

    const avatarUrl = await storeServerAvatar(server.id, server.avatarUrl, uploaded.buffer);
    const updated = await serverService.updateServerProfile(server.id, { avatarUrl });
    if (!updated) {
      res.status(404).json({ error: "Server not found" });
      return;
    }
    res.json(updated);
  } catch (err) {
    if (err instanceof multer.MulterError && err.code === "LIMIT_FILE_SIZE") {
      res.status(400).json({
        error: PROFILE_AVATAR_TOO_LARGE_MESSAGE,
        errorCode: "PROFILE_AVATAR_TOO_LARGE",
        maxBytes: MAX_PROFILE_AVATAR_BYTES,
      });
      return;
    }
    if (err instanceof Error && err.message.includes(PROFILE_AVATAR_BAD_FORMAT_MESSAGE)) {
      res.status(400).json({
        error: PROFILE_AVATAR_BAD_FORMAT_MESSAGE,
        errorCode: "PROFILE_AVATAR_BAD_FORMAT",
      });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to upload avatar",
      logPrefix: "internal.agent-api.server.avatar error:",
      err,
    });
  }
});

registerAgentApiRoute("knowledgeGet", async (req, res) => {
  const actingAgentId = req.actingAgentId;
  const serverId = req.serverId;
  if (!actingAgentId || !serverId) {
    res.status(500).json({ error: "Agent credential state missing" });
    return;
  }
  await handleAgentKnowledgeGet(req, res, { agentId: actingAgentId, serverId });
});

registerAgentApiRoute("knowledgeSearch", async (req, res) => {
  const actingAgentId = req.actingAgentId;
  const serverId = req.serverId;
  if (!actingAgentId || !serverId) {
    res.status(500).json({ error: "Agent credential state missing" });
    return;
  }
  await handleAgentKnowledgeSearch(req, res, { agentId: actingAgentId, serverId });
});

/** Channel types that have a card (passive-AX `channel` resource): the listable ones, not DMs or threads. */
const CHANNEL_CARD_TYPES: ReadonlySet<string> = new Set(channelService.LISTABLE_CHANNEL_TYPES);

type ChannelCardRow = { id: string; name: string; type: string; description: string | null; archivedAt: Date | null };

function channelCardOf(row: ChannelCardRow): AgentApiChannelCard {
  return {
    id: row.id,
    name: row.name,
    type: row.type,
    description: row.description ?? null,
    archived: row.archivedAt != null,
  };
}

/**
 * The channel whose card goes with `channel`: the channel itself, a thread's
 * parent channel, or a joint thread's local parent projection (its projection
 * has no parent message). Null for DMs and threads under them. The one rule
 * for both history reads and sends, so a card recorded on read passes a send.
 */
async function resolveChannelCardRow(
  channel: (ChannelCardRow & { parentMessageId?: string | null }) | null | undefined,
  serverId: string,
): Promise<ChannelCardRow | null> {
  if (!channel) return null;
  if (CHANNEL_CARD_TYPES.has(channel.type)) return channel;
  if (channel.type !== "thread") return null;
  const parentChannelId = channel.parentMessageId
    ? (await messageService.getMessage(channel.parentMessageId))?.channelId ?? null
    : (await channelService.getJointThreadProjectionByLocalThread(channel.id, serverId))?.localParentChannelId ?? null;
  const parent = parentChannelId ? await channelService.getChannel(parentChannelId) : null;
  return parent && CHANNEL_CARD_TYPES.has(parent.type) ? parent : null;
}

/**
 * GET /internal/agent-api/history?channel=<ref>&limit=<n>&before=<id|seq>&after=<id|seq>&around=<id|seq>
 *
 * Slice-1 thin port of `GET /internal/agent/:id/history`. Returns
 * channel history visible to the bound agent. No plan-cutoff handling
 * yet — that's intentionally left to the legacy surface in v0 and will
 * be folded back here in slice-2.
 */
registerAgentApiRoute("historyRead", async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const historyQuery = validateAgentApiQuery("historyRead", req, res);
    if (!historyQuery) return;
    const channelRef = historyQuery.channel;
    const limit = Math.min(Number(historyQuery.limit) || 50, 100);
    // `consume=false`: return the page without marking it read, for reads
    // whose output may never reach the model.
    const consume = historyQuery.consume !== "false";

    if (!channelRef) {
      res.status(400).json({ error: "channel query param is required (e.g. #all, dm:@richard)" });
      return;
    }

    // `agent-event:<id8>` is not a channel: it is the address every rendered
    // third-party line already prints. `message check` hands the body over once
    // and consumes the cursor, so without this an agent that was woken by an
    // event can never get back to it (task #257).
    // Accepts the 8-hex short form the message line prints AND the full event
    // UUID, which the rendered body already carries as `event_id: <uuid>`. Both
    // are scoped to the acting agent inside the query.
    const eventRef = /^agent-event:([0-9a-fA-F]{8}|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/
      .exec(channelRef.trim())?.[1];
    if (eventRef) {
      const found = await oauthService.readThirdPartyAgentEventForAgent({
        agentId: actingAgentId,
        ref: eventRef,
      });
      if (found.kind === "ambiguous") {
        // Only reachable from owner-scoped rows, so naming the condition leaks
        // nothing. Returning the neutral 404 here would be safe but would leave
        // both of the caller's own live events permanently unreadable, which is
        // the guarantee this route exists to provide.
        res.status(409).json({
          error: "That 8-character event address matches more than one of your events. "
            + "Use the full event id, printed as `event_id:` in the event body.",
          errorCode: "AMBIGUOUS_ID",
          suggestedNextAction: "raft message read --target 'agent-event:<full-event-id>'",
        });
        return;
      }
      if (found.kind === "expired") {
        // Safe to distinguish: only ever reached for an event that IS yours, so it
        // tells you nothing you did not already have visibility of.
        res.status(410).json({
          error: "This third-party event has expired and its payload is no longer retained.",
          errorCode: "EXPIRED",
        });
        return;
      }
      if (found.kind === "absent") {
        // Deliberately the SAME neutral body used for an invisible channel, and a
        // CONSTANT: echoing the requested id here would make two ids distinguishable
        // even while errorCode stayed identical. The span (never the caller) says which.
        setRequestTraceErrorCode(res, "third_party_event_not_found");
        res.status(404).json(CHANNEL_NOT_FOUND_BODY);
        return;
      }
      res.json({
        messages: [found.message],
        has_more: false,
        has_older: false,
        has_newer: false,
      });
      return;
    }

    const resolved = await channelService.resolveChannelByName(serverId, actingAgentId, channelRef);
    if (!resolved) {
      // Report which entity is actually missing. Saying "Channel not found" for a
      // thread ref whose channel resolves fine sent one reader into 8 retries and
      // a "the read surface is flaky" verdict for a deterministic empty (#145).
      //
      // The short id is evaluated ONLY inside the already-visible parent: a
      // `#channel:shortid` target is a thread of that channel or nothing. Looking
      // it up server-wide would hand the caller a cross-channel enumeration
      // surface they never had, and anything we then said about the other channel
      // would be a permission side-channel. @Tenny's ruling on #145.
      const { baseRef, threadShortId } = channelService.parseChannelRef(channelRef);
      if (threadShortId) {
        const parent = await channelService.resolveChannelByName(serverId, actingAgentId, baseRef);
        if (parent) {
          const anchored = await messageService.messageShortIdExistsInChannel(
            parent.channelId,
            threadShortId,
          );
          if (anchored) {
            // The anchor is genuinely here and genuinely has no replies -- the only
            // case where we may say so, because we checked.
            setRequestTraceErrorCode(res, "thread_has_no_replies");
            res.status(404).json({
              error: `No thread on message ${threadShortId} in ${baseRef}: the message is here, but it has no replies yet.`,
              errorCode: "NOT_FOUND",
              suggestedNextAction: `Read the message itself with: raft message read --target '${baseRef}' --around ${threadShortId}`,
            });
            return;
          }
          // No anchor here. Covers both "no such message anywhere" and "it lives in
          // another channel" -- one body for both, so they cannot be told apart.
          setRequestTraceErrorCode(res, "thread_anchor_not_found");
          res.status(404).json(threadAnchorNotFoundBody(baseRef));
          return;
        }
      }
      // The parent channel itself is missing or invisible. Shared neutral body:
      // it must not distinguish "does not exist" from "you cannot see it".
      setRequestTraceErrorCode(res, threadShortId ? "thread_parent_channel_not_found" : "channel_not_found");
      res.status(404).json(CHANNEL_NOT_FOUND_BODY);
      return;
    }

    const channelId = resolved.channelId;
    const hasAccess = await channelService.canAgentAccessChannel(channelId, actingAgentId);
    if (!hasAccess) {
      setRequestTraceErrorCode(res, "history_not_accessible");
      res.status(403).json({ error: "You do not have access to this history" });
      return;
    }

    const channel = await channelService.getChannel(channelId);
    const jointThreadProjection = channel?.type === "thread"
      ? await channelService.getJointThreadProjectionByLocalThread(channelId, serverId)
      : null;
    const resolvedAccess = channel?.type === "joint"
      ? await channelService.resolveChannelAccess({ serverId, channelId })
      : null;
    const storageChannelId = jointThreadProjection?.canonicalThreadChannelId
      ?? (resolvedAccess?.kind === "joint" ? resolvedAccess.canonicalChannelId : channelId);
    const projectionChannelId = jointThreadProjection?.localThreadChannelId
      ?? (resolvedAccess?.kind === "joint" ? resolvedAccess.localChannelId : channelId);
    const beforeAnchor = historyQuery.before?.trim();
    const afterAnchor = historyQuery.after?.trim();
    const around = historyQuery.around?.trim();
    // `unread=true` (`raft message read --unread`): start right after the
    // agent's read position, the one `raft inbox check` counts unread from.
    const unread = historyQuery.unread === "true";
    if (unread && (beforeAnchor || afterAnchor || around)) {
      res.status(400).json({
        error: "--unread cannot be combined with --before, --after, or --around.",
        errorCode: "INVALID_ARG",
        suggestedNextAction: `raft message read --target '${channelRef}' --unread`,
      });
      return;
    }
    if (beforeAnchor && !isHistoryAnchorShape(beforeAnchor)) {
      const failure = historyAnchorErrorPayload(channelRef, beforeAnchor, "invalid");
      res.status(failure.status).json(failure.body);
      return;
    }
    if (afterAnchor && !isHistoryAnchorShape(afterAnchor)) {
      const failure = historyAnchorErrorPayload(channelRef, afterAnchor, "invalid");
      res.status(failure.status).json(failure.body);
      return;
    }
    const beforeResolution = beforeAnchor
      ? await messageService.resolveMessageSeqAnchor(storageChannelId, beforeAnchor, "pagination")
      : undefined;
    if (beforeResolution && !beforeResolution.ok) {
      const failure = historyAnchorErrorPayload(channelRef, beforeAnchor!, beforeResolution.reason);
      res.status(failure.status).json(failure.body);
      return;
    }
    const afterResolution = afterAnchor
      ? await messageService.resolveMessageSeqAnchor(storageChannelId, afterAnchor, "pagination")
      : undefined;
    if (afterResolution && !afterResolution.ok) {
      const failure = historyAnchorErrorPayload(channelRef, afterAnchor!, afterResolution.reason);
      res.status(failure.status).json(failure.body);
      return;
    }
    const beforeSeq = beforeResolution?.ok ? beforeResolution.seq : undefined;
    const lastReadSeq = await channelService.getAgentLegacyReadCursor(actingAgentId, channelId);
    const unreadAfterSeq = unread ? Math.max(0, Number(lastReadSeq) || 0) : undefined;
    const afterSeq = afterResolution?.ok ? afterResolution.seq : unreadAfterSeq;
    let rawMsgs;
    let hasOlder = false;
    let hasNewer = false;
    if (around) {
      if (!isHistoryAnchorShape(around)) {
        const failure = historyAnchorErrorPayload(channelRef, around, "invalid");
        res.status(failure.status).json(failure.body);
        return;
      }
      const aroundResolution = await messageService.resolveMessageSeqAnchor(storageChannelId, around, "around");
      if (!aroundResolution.ok) {
        const failure = historyAnchorErrorPayload(channelRef, around, aroundResolution.reason);
        res.status(failure.status).json(failure.body);
        return;
      }
      const beforeCount = Math.floor((limit - 1) / 2);
      const afterCount = limit - beforeCount - 1;
      const context = await messageService.getMessageContextBySeq(
        storageChannelId,
        aroundResolution.seq,
        beforeCount,
        afterCount,
        undefined,
        {
          forwardedBundleViewerAgentId: actingAgentId,
          forwardedBundleViewerServerId: serverId,
          includeSenderTimezone: true,
        },
      );
      if (!context || context.channelId !== storageChannelId) {
        const failure = historyAnchorErrorPayload(channelRef, around, "not_found");
        res.status(failure.status).json(failure.body);
        return;
      }
      rawMsgs = context.messages;
      hasOlder = context.hasOlder;
      hasNewer = context.hasNewer;
    } else {
      rawMsgs = await messageService.listMessages(storageChannelId, limit + 1, beforeSeq, afterSeq, undefined, {
        forwardedBundleViewerAgentId: actingAgentId,
        forwardedBundleViewerServerId: serverId,
        includeSenderTimezone: true,
      });
    }
    const msgs = jointThreadProjection || resolvedAccess?.kind === "joint"
      ? await messageService.projectJointMessagesToLocalChannel(rawMsgs, projectionChannelId, serverId)
      : rawMsgs;
    const page = around
      ? { messages: msgs, hasOlder, hasNewer }
      : paginateHistoryProbe(
          msgs,
          limit,
          afterSeq !== undefined ? "after" : beforeSeq !== undefined ? "before" : "latest",
        );
    const pageMsgs = page.messages;
    hasOlder = page.hasOlder;
    hasNewer = page.hasNewer;

    let modelSeenUpToSeq: number | null = null;
    let readThroughSeq: number | null = null;
    if (consume && !around && pageMsgs.length > 0) {
      const pageSeqs = pageMsgs
        .map((message) => Number(message.seq))
        .filter((seq) => Number.isInteger(seq) && seq > 0);
      const minSeq = pageSeqs.length > 0 ? Math.min(...pageSeqs) : 0;
      const maxSeq = pageSeqs.length > 0 ? Math.max(...pageSeqs) : 0;
      const priorReadSeq = Math.max(0, Number(lastReadSeq) || 0);
      // A read window may advance a high-water boundary only when it joins the
      // prior boundary without a hidden gap. Latest/before pages are safe when
      // they include the oldest row or overlap the prior cursor. An `after`
      // page is safe only when its exclusive anchor is at/below that cursor.
      // Otherwise the returned bodies remain useful exact-seq observations,
      // but must not consume earlier unseen messages outside the window.
      const canAdvanceModelSeen = afterSeq !== undefined
        ? afterSeq <= priorReadSeq
        : !hasOlder || minSeq <= priorReadSeq;
      if (maxSeq > 0 && canAdvanceModelSeen) {
        modelSeenUpToSeq = maxSeq;
        // Seq order is not commit order: a transaction holding seq 100 can
        // commit after one holding 101. Moving the read position to 101 the
        // moment 101 is visible would skip 100 forever (the next `--unread`
        // and `inbox check` both count from the read position). So the read
        // position only moves through the settled prefix of the page: rows
        // older than readPositionSettleMs(). Newer rows are still returned and
        // stay unread for now; a deferred check (below) moves the position
        // over them once the window has passed and no late lower seq appeared.
        readThroughSeq = settledReadThroughSeq(pageMsgs, Date.now() - readPositionSettleMs());
      }
      const markAgentRead = (seq: number) => channelService.markRead({ kind: "agent", id: actingAgentId }, channelId, seq).then((readState) =>
        emitScopeReadUpdated({
          io: req.app.get("io") as SocketServer | undefined,
          serverId,
          scopeId: channelId,
          peerKind: "agent",
          peerId: actingAgentId,
          maxReadSeq: readState.maxReadSeq,
          changed: readState.changed,
        })
      );
      if (readThroughSeq !== null && readThroughSeq > priorReadSeq) {
        // An unread read is awaited: the next `--unread` must start after this
        // page, not race the write and return the same messages again.
        const marking = markAgentRead(readThroughSeq).catch(() => {});
        if (unread) await marking;
      }
      if (maxSeq > 0 && canAdvanceModelSeen && maxSeq > Math.max(priorReadSeq, readThroughSeq ?? 0)) {
        scheduleDeferredReadAdvance({
          agentId: actingAgentId,
          channelId,
          fromSeq: Math.max(priorReadSeq, readThroughSeq ?? 0),
          returnedSeqs: pageSeqs,
          listSeqs: async (fromSeq, toSeq) => (
            await messageService.listMessages(storageChannelId, DEFERRED_READ_ADVANCE_SCAN_LIMIT, toSeq + 1, fromSeq)
          ).map((message) => Number(message.seq)).filter((seq) => Number.isInteger(seq) && seq > fromSeq && seq <= toSeq),
          advance: async (seq) => { await markAgentRead(seq); },
          record: (outcome, attrs) => recordTraceEvent("server.agent_api.history_read.deferred_advance", {
            outcome,
            agent_id: actingAgentId,
            channel_id: channelId,
            ...attrs,
          }),
        });
      }
    }

    // The agent's own messages are never unread for it: the inbox count and the
    // `/events` pull leave them out, and a send can leave the read position
    // below them (markAgentOwnSendRead). So `--unread` prints only others'
    // messages, while the read position above moved over the whole window, own
    // messages included, so the next `--unread` starts after them. A full page
    // followed only by own messages has no more unread.
    const ownSender = { senderType: "agent" as const, senderId: actingAgentId };
    const shownMsgs = unread
      ? pageMsgs.filter((message) => !(message.senderType === ownSender.senderType && message.senderId === ownSender.senderId))
      : pageMsgs;
    if (unread && hasNewer && pageMsgs.length > 0) {
      const pageLastSeq = Math.max(...pageMsgs.map((message) => Number(message.seq)));
      hasNewer = (await messageService.listMessages(storageChannelId, 1, undefined, pageLastSeq, undefined, { excludeSender: ownSender })).length > 0;
    }

    const renderedContents = await agentPermalinkRenderService.renderAgentReadablePermalinksInTexts(
      shownMsgs.map((message) => message.content),
      serverId,
    );
    // Resolve the evidence key from the same authority that resolved this
    // request. Message envelopes intentionally omit channel identity fields,
    // so reconstructing a target from a rendered row can only guess (and in
    // CLI 0.0.28 guessed `#undefined`, collapsing every target together).
    let resolvedHistoryTarget = await channelService.resolveAgentFacingChannelRef(
      serverId,
      actingAgentId,
      projectionChannelId,
    );
    let dmHistoryScope = channel?.type === "dm";
    const cardChannel = await resolveChannelCardRow(channel, serverId);
    if (channel?.type === "thread" && channel.parentMessageId) {
      const parent = await messageService.getMessage(channel.parentMessageId);
      const parentChannel = parent ? await channelService.getChannel(parent.channelId) : null;
      dmHistoryScope = parentChannel?.type === "dm";
      const parentTarget = parent
        ? await channelService.resolveAgentFacingChannelRef(serverId, actingAgentId, parent.channelId)
        : null;
      if (parentTarget) {
        resolvedHistoryTarget = `${parentTarget}:${channel.parentMessageId.slice(0, 8)}`;
      }
    }
    const peerCard = dmHistoryScope ? await resolvePeerCard(serverId, actingAgentId, channel) : null;
    recordAgentContextReturned({
      source: "history",
      agentId: actingAgentId,
      channelId: projectionChannelId,
      ...agentRequestContextFromHeaders(req.headers),
      seqs: shownMsgs.map((message) => message.seq),
    });
    sendAgentApiResponse("historyRead", res, {
      target: resolvedHistoryTarget ?? channelRef,
      ...(dmHistoryScope && channel && { consumption_scope: {
        agent_id: actingAgentId,
        channel_id: projectionChannelId,
        channel_type: channel.type as "dm" | "thread",
        target: resolvedHistoryTarget ?? channelRef,
      } }),
      messages: shownMsgs.map((message, index: number) =>
        toAgentApiMessageEnvelope(
          message,
          messageService.appendAgentFacingForwardedSnapshot(
            renderAgentCommentScopedContent(
              renderedContents[index] ?? "",
              message.commentRef,
            ),
            message.actionMetadata,
          ),
          actingAgentId,
        )
      ),
      has_more: hasOlder || hasNewer,
      has_older: hasOlder,
      has_newer: hasNewer,
      last_read_seq: lastReadSeq,
      model_seen_up_to_seq: modelSeenUpToSeq,
      ...(unreadAfterSeq !== undefined && {
        unread_after_seq: unreadAfterSeq,
        read_through_seq: Math.max(unreadAfterSeq, readThroughSeq ?? 0),
      }),
      ...(cardChannel && { channel_card: channelCardOf(cardChannel) }),
      ...(peerCard && { peer_card: peerCard }),
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to read history",
      logPrefix: "internal.agent-api.history error:",
      err,
    });
  }
});

/**
 * GET /internal/agent-api/mentions?limit=<n>&before_seq=<seq>
 *
 * RFC v0.8 + #proj-aiax v0 mention-CLI spec. Returns messages where
 * the bound agent was @-mentioned, newest first. v0 lock is frozen-baseline
 * + send-path-only (no backfill), filtered by `source = 'send_path'`.
 *
 * Slice-1 minimal: returns target-visible message_mentions rows. Pending
 * outsider rows remain sender-side actions until they were notifiable at send
 * time or an explicit notify/add action sets notified_at.
 */
registerAgentApiRoute("mentionsList", async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 100);
    const beforeSeq = req.query.before_seq ? Number(req.query.before_seq) : undefined;

    const db = getDb();
    const baseConditions = [
      eq(messageMentions.targetType, "agent"),
      eq(messageMentions.targetId, actingAgentId),
      eq(messageMentions.serverId, serverId),
      eq(messageMentions.source, "send_path"),
      or(eq(messageMentions.notifiableAtSend, true), isNotNull(messageMentions.notifiedAt)),
    ];

    const rows = await db
      .select({
        id: messageMentions.id,
        messageId: messageMentions.messageId,
        messageSeq: messageMentions.messageSeq,
        channelId: messageMentions.channelId,
        handleAtSendTime: messageMentions.handleAtSendTime,
        source: messageMentions.source,
        confidence: messageMentions.confidence,
        createdAt: messageMentions.createdAt,
      })
      .from(messageMentions)
      .where(and(...baseConditions))
      .orderBy(desc(messageMentions.messageSeq))
      .limit(limit + 1);

    // beforeSeq pagination applied in memory for v0 simplicity; index
    // `idx_message_mentions_inbox` already orders by message_seq DESC, so
    // a future optimization is a single drizzle `lt(messageSeq, beforeSeq)`.
    const filteredRows = beforeSeq
      ? rows.filter((r) => Number(r.messageSeq) < beforeSeq)
      : rows;
    const pageRows = filteredRows.slice(0, limit);

    res.json({
      mentions: pageRows.map((r) => ({
        id: r.id,
        messageId: r.messageId,
        messageSeq: Number(r.messageSeq),
        channelId: r.channelId,
        handleAtSendTime: r.handleAtSendTime,
        source: r.source,
        confidence: r.confidence,
        createdAt: r.createdAt instanceof Date ? r.createdAt.toISOString() : r.createdAt,
      })),
      has_more: filteredRows.length > limit,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load mentions",
      logPrefix: "internal.agent-api.mentions error:",
      err,
    });
  }
});

/**
 * Exact self-diagnostic for one mention occurrence. The credential-bound
 * actingAgentId is the receiver identity; callers cannot select another agent.
 */
internalAgentApiRouter.get("/mentions/:messageId/delivery", requireAgentCapability("mentions"), async (req, res) => {
  try {
    const messageId = typeof req.params.messageId === "string" ? req.params.messageId : "";
    if (!UUID_RE.test(messageId)) {
      res.status(404).json({ status: "NOT_JOINABLE" });
      return;
    }
    const result = await mentionDeliveryOccurrenceService.lookupMentionDeliveryOccurrence(
      messageId,
      req.actingAgentId!,
    );
    res.status(result.status === "NOT_JOINABLE" ? 404 : 200).json(result);
  } catch (err) {
    console.error("internal.agent-api.mention-delivery error:", serializeErrorForLog(err));
    // LOOKUP_FAILED, not INSTRUMENT_FAILED. @Hipp traced that one token was carrying three
    // meanings: (a) a terminal error CODE in MentionDeliveryTerminalErrorCode — "delivery ended in
    // instrument failure"; (b) a lookup VERDICT in MentionDeliveryLookupResult.status — "I read the
    // row and it contradicts its own receipts"; (c) THIS — "the query did not run". (b) and (c)
    // both travel in a field named `status`, so a consumer could not tell them apart by shape.
    // The split is not cosmetic: (c) means "I cannot see", (a)/(b) mean "I saw it and it is
    // broken" — opposite next actions for whoever reads it. Same rule as the exit-code partition
    // ruled earlier today: codes divide by REMEDIATION, not by cause.
    // Local change: the shared union is MentionDeliveryTerminalErrorCode (meaning (a)); this 500
    // body is an untyped inline literal, so no consumer contract moves with it.
    res.status(500).json({ status: "LOOKUP_FAILED" });
  }
});

/**
 * task #153 — GET /internal/agent-api/messages/:messageId/mention-deliveries
 *
 * The AUTHOR's view: for a message I sent, what happened to each target I
 * mentioned. This is the counterpart of `/mentions/:messageId/delivery`, which
 * is receiver-bound by design; without this the server knows a mention was
 * permanently lost and the only party who cannot find out is the one who sent it.
 *
 * Authority is authorship, and a non-author gets the SAME `404 NOT_JOINABLE`
 * shape a non-participant gets, so this cannot be used to probe whether someone
 * else's message exists.
 */
internalAgentApiRouter.get("/messages/:messageId/mention-deliveries", requireAgentCapability("mentions"), async (req, res) => {
  try {
    const messageId = typeof req.params.messageId === "string" ? req.params.messageId : "";
    if (!UUID_RE.test(messageId)) {
      res.status(404).json({ status: "NOT_JOINABLE" });
      return;
    }
    const [message] = await getDb()
      .select({ senderType: messages.senderType, senderId: messages.senderId, channelId: messages.channelId })
      .from(messages)
      .where(eq(messages.id, messageId))
      .limit(1);
    // Same body for "no such message" and "not yours": a distinguishable
    // response here would answer a question the caller is not entitled to ask.
    if (!message || message.senderType !== "agent" || message.senderId !== req.actingAgentId) {
      res.status(404).json({ status: "NOT_JOINABLE" });
      return;
    }
    const rows = await mentionDeliveryOccurrenceService.listMentionDeliveryOccurrencesForMessage(messageId);
    // @Stone finding 1: identify the target by the handle the AUTHOR actually
    // wrote, not by whatever the agent is called today. The sender is
    // diagnosing a token they typed; if the target was renamed after the send,
    // the current name is a different string from the one they are looking for.
    // occurrenceId IS messageMentions.id, so this is an exact join, and
    // handleAtSendTime is notNull — no fallback to the live name is needed.
    const handles = new Map<string, string>();
    if (rows.length > 0) {
      const authored = await getDb()
        .select({ id: messageMentions.id, handleAtSendTime: messageMentions.handleAtSendTime })
        .from(messageMentions)
        .where(inArray(messageMentions.id, rows.map((row) => row.occurrenceId)));
      for (const mention of authored) handles.set(mention.id, mention.handleAtSendTime);
    }
    const serviceTargets = await getDb().select({ agentId: agents.id }).from(agents)
      .innerJoin(raftbenchConfigs, eq(raftbenchConfigs.agentId, agents.id))
      .where(inArray(agents.id, rows.map(row => row.agentId)));
    const serviceIds = new Set(serviceTargets.map(row => row.agentId));
    const deliveries = rows.map((row) => serviceIds.has(row.agentId)
      ? { targetHandle: handles.get(row.occurrenceId) ?? "", outcome: "lost" as const, reasonCategory: "unclassified" as const }
      : projectMentionDeliveryForSender(handles.get(row.occurrenceId) ?? "", mentionDeliveryOccurrenceService.evaluateMentionDeliveryOccurrence(row)));
    const dmPeers = await getDb().select({ agentId: agents.id, name: agents.name }).from(channels)
      .innerJoin(channelAgents, eq(channelAgents.channelId, channels.id))
      .innerJoin(agents, eq(agents.id, channelAgents.agentId))
      .innerJoin(raftbenchConfigs, eq(raftbenchConfigs.agentId, agents.id))
      .where(and(eq(channels.id, message.channelId), eq(channels.type, "dm")));
    for (const peer of dmPeers) {
      if (!serviceIds.has(peer.agentId)) deliveries.push({ targetHandle: peer.name, outcome: "lost", reasonCategory: "unclassified" });
    }
    res.status(200).json({ messageId, deliveries });
  } catch (err) {
    console.error("internal.agent-api.sender-mention-deliveries error:", serializeErrorForLog(err));
    // LOOKUP_FAILED, not a verdict: "the query did not run" and "I read the rows
    // and they are broken" have opposite next actions. Same split the sibling
    // route already records.
    res.status(500).json({ status: "LOOKUP_FAILED" });
  }
});

/**
 * GET /internal/agent-api/mention-actions/pending
 *
 * Sender-side mention-AX action query. These are outsider mentions emitted by
 * this bound agent where send-time delivery was intentionally withheld.
 */
registerAgentApiRoute("mentionActionsPending", async (req, res) => {
  try {
    const query = validateAgentApiQuery("mentionActionsPending", req, res);
    if (!query) return;
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const limit = Math.min(Math.max(Number(query.limit) || 50, 1), 100);
    const rows = await listPendingMentionActionsForSender(serverId, "agent", actingAgentId, limit + 1);
    const pageRows = rows.slice(0, limit);
    sendAgentApiResponse("mentionActionsPending", res, {
      pendingMentionActions: pageRows.map(buildPendingMentionActionPayload),
      has_more: rows.length > limit,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load pending mention actions",
      logPrefix: "internal.agent-api.mention-actions.pending error:",
      err,
    });
  }
});

/**
 * POST /internal/agent-api/mention-actions/execute
 *
 * Executes sender-side mention resolution actions. Every id is revalidated at
 * action time and returns a typed per-id status instead of failing the batch.
 */
registerAgentApiRoute("mentionActionsExecute", ...agentApiRequestValidators("mentionActionsExecute"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const action = typeof req.body?.action === "string" ? req.body.action : "";
    if (action !== "notify" && action !== "add") {
      res.status(400).json({ error: "action must be notify or add" });
      return;
    }
    const actionKind: MentionActionKind = action;
    const rawIds: unknown[] = Array.isArray(req.body?.resolutionIds)
      ? req.body.resolutionIds
      : Array.isArray(req.body?.ids)
        ? req.body.ids
        : [];
    const resolutionIds = [...new Set(rawIds
      .filter((id): id is string => typeof id === "string")
      .map((id) => id.trim())
      .filter(Boolean))]
      .slice(0, 100);
    if (resolutionIds.length === 0) {
      res.status(400).json({ error: "resolutionIds must include at least one id" });
      return;
    }

    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
    const executionOptions: MentionActionExecutionOptions = agentOrchestrator
      ? {
          notifyAgent: ({ messageId, targetId }) =>
            messageService.deliverMessageToAgent(
              agentOrchestrator,
              messageId,
              targetId,
              { requireQueueReceipt: true, nonMemberMention: true },
            ),
        }
      : {};
    const results: MentionActionResult[] = [];
    for (const resolutionId of resolutionIds) {
      results.push(await executeMentionActionId(
        resolutionId,
        serverId,
        "agent",
        actingAgentId,
        actionKind,
        executionOptions,
      ));
    }
    if (agentOrchestrator && actionKind === "add") {
      await Promise.all(
        results.map(async (result) => {
          if (
            result.status !== "delivered"
            || result.reason === "already_delivered"
            || result.targetType !== "agent"
            || !result.targetId
            || !result.messageId
          ) {
            return;
          }
          try {
            await messageService.deliverMessageToAgent(
              agentOrchestrator,
              result.messageId,
              result.targetId,
              { requireQueueReceipt: true, reconcileNonMemberMention: true },
            );
          } catch (err) {
            console.error(
              `[internal.agent-api.mention-actions.execute] failed to deliver resolved mention ${result.resolutionId} to agent ${result.targetId}:`,
              err,
            );
          }
        }),
      );
    }
    sendAgentApiResponse("mentionActionsExecute", res, { ok: true, action, results });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to execute mention action",
      logPrefix: "internal.agent-api.mention-actions.execute error:",
      err,
    });
  }
});

/**
 * Shared reaction add/remove logic. Mirrors legacy /internal/agent/:id
 * reaction handlers without the loadOwnedMachineAgent check (the credential
 * is already bound to the agent at auth time).
 */
async function handleReaction(
  req: import("express").Request,
  res: import("express").Response,
  mode: "add" | "remove",
): Promise<void> {
  const actingAgentId = req.actingAgentId!;
  const serverId = req.serverId!;
  const emoji = parseReactionEmoji(req.body?.emoji);
  if (!emoji) {
    setRequestTraceErrorCode(res, "emoji_invalid");
    res.status(400).json({ error: "A valid emoji is required" });
    return;
  }

  const msgId = typeof req.params.msgId === "string" ? req.params.msgId : "";
  const resolvedMessageId = await messageService.resolveMessageIdVisibleToAgent(serverId, actingAgentId, msgId);
  if (!resolvedMessageId.ok) {
    setRequestTraceErrorCode(res, resolvedMessageId.status === 404 ? "message_not_found" : "message_id_invalid");
    res.status(resolvedMessageId.status).json({ error: resolvedMessageId.error });
    return;
  }
  const message = await messageService.getMessage(resolvedMessageId.messageId);
  if (!message) {
    setRequestTraceErrorCode(res, "message_not_found");
    res.status(404).json({ error: "Message not found" });
    return;
  }

  const channel = await channelService.getChannel(message.channelId);
  if (!channel || channel.serverId !== serverId) {
    setRequestTraceErrorCode(res, "channel_not_found");
    res.status(404).json({ error: "Channel not found" });
    return;
  }
  const canAccess = await channelService.canAgentAccessChannel(message.channelId, actingAgentId);
  if (!canAccess) {
    setRequestTraceErrorCode(res, "message_not_found");
    res.status(404).json({ error: "Message not found" });
    return;
  }
  if (message.messageType === "system") {
    setRequestTraceErrorCode(res, "system_message_not_reactable");
    res.status(400).json({ error: "System messages cannot receive reactions" });
    return;
  }
  const canPost = await channelService.canAgentPostToChannel(message.channelId, actingAgentId);
  if (!canPost) {
    setRequestTraceErrorCode(res, "channel_membership_required");
    res.status(403).json({ error: "Agent must join this channel to react to messages" });
    return;
  }
  try {
    await channelService.assertChannelNotArchived(message.channelId);
  } catch (err) {
    if (err instanceof channelService.ChannelArchivedError) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }
    throw err;
  }
  if (await isChannelReadOnlyByBillingFeature(message.channelId, serverId)) {
    setRequestTraceErrorCode(res, "channel_read_only");
    res.status(403).json({ error: "This joint channel is read-only because it has more than 2 free servers. It becomes writable again when a server upgrades or a free server leaves." });
    return;
  }
  if (await isChannelReadOnlyByQuota(message.channelId, serverId)) {
    setRequestTraceErrorCode(res, "channel_read_only");
    res.status(403).json({ error: "This channel is read-only on your current plan. Upgrade to continue." });
    return;
  }

  await mutateMessageReaction({
    channelId: message.channelId,
    messageId: message.id,
    emoji,
    actor: { kind: "agent", id: actingAgentId },
    operation: mode,
  });

  const context = await messageService.getMessageContext(message.id, 0, 0, undefined, {
    forwardedBundleViewerAgentId: actingAgentId,
    forwardedBundleViewerServerId: serverId,
    includeSenderTimezone: true,
  });
  const enriched = context?.messages[0];
  if (!enriched) {
    res.status(500).json({ error: "Failed to reload updated message" });
    return;
  }
  const io = req.app.get("io");
  // message-realtime-producer: route-agent-api.reaction.updated
  io.to(`channel:${message.channelId}`).emit(
    "message:updated",
    projectRichMessageSocketPayload(messageService.stripViewerScopedAttachmentCommentMetadata(enriched)),
  );
  res.json(toAgentApiMessageEnvelope(enriched, enriched.content, actingAgentId));
}

registerAgentApiRoute("messageReactionAdd", ...agentApiRequestValidators("messageReactionAdd"), validateAgentApiResponseMiddleware("messageReactionAdd"), async (req, res) => {
  try {
    await handleReaction(req, res, "add");
  } catch (err) {
    if (err instanceof ChannelConversionInProgressError) {
      res.status(err.status).json({ error: err.message, code: err.code, conversionEpoch: err.conversionEpoch });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to add reaction",
      logPrefix: "internal.agent-api.reactions.add error:",
      err,
    });
  }
});

registerAgentApiRoute("messageReactionRemove", ...agentApiRequestValidators("messageReactionRemove"), validateAgentApiResponseMiddleware("messageReactionRemove"), async (req, res) => {
  try {
    await handleReaction(req, res, "remove");
  } catch (err) {
    if (err instanceof ChannelConversionInProgressError) {
      res.status(err.status).json({ error: err.message, code: err.code, conversionEpoch: err.conversionEpoch });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to remove reaction",
      logPrefix: "internal.agent-api.reactions.remove error:",
      err,
    });
  }
});

registerAgentApiRoute("channelJoin", ...agentApiRequestValidators("channelJoin"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const channelId = typeof req.params.channelId === "string" ? req.params.channelId : "";
    const channel = await channelService.getChannel(channelId);
    if (!channel || channel.serverId !== serverId) {
      res.status(404).json({ error: "Channel not found" });
      return;
    }
    if (channel.type === "thread") {
      res.status(400).json({ error: "Thread membership is managed via follow/unfollow" });
      return;
    }
    if (channel.type === "dm") {
      res.status(403).json({ error: "Cannot join DM channels" });
      return;
    }
    if (channel.type === "private") {
      res.status(403).json({ error: "Private channels require an invitation" });
      return;
    }
    if (channel.type === "joint") {
      res.status(403).json({ error: "Joint channels require an invitation" });
      return;
    }
    if (channel.archivedAt) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }
    const wasAgentMember = await channelService.isChannelAgent(channel.id, actingAgentId);
    if (wasAgentMember) {
      sendAgentApiResponse("channelJoin", res, { ok: true });
      return;
    }
    if (!await actorHasServerCapabilityInServer(serverId, "agent", actingAgentId, "joinPublicChannels")) {
      res.status(403).json({ error: "Server role cannot join public channels" });
      return;
    }

    await channelService.addAgent(channel.id, actingAgentId);
    if (!wasAgentMember) {
      const io = req.app.get("io") as SocketServer | undefined;
      const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
      if (io && agentOrchestrator) {
        await messageService.broadcastSystemMessage(
          io,
          agentOrchestrator,
          channel.id,
          `@${agent.name} joined this channel.`,
          {
            inboxFactPolicy: {
              mode: "record",
              producer: "agent.join_channel",
              reason: "agent joining a channel is shared channel activity",
              causalActor: { type: "agent", id: actingAgentId },
            },
            // The joining agent should not see its own join as unread.
          },
        );
      }
    }
    req.app.get("io")?.to(`server:${serverId}`).emit("channel:members-updated", { channelId: channel.id });
    sendAgentApiResponse("channelJoin", res, { ok: true });
  } catch (err: any) {
    if (err instanceof Error && err.message === channelService.ASSISTANT_CHANNEL_FORBIDDEN_MESSAGE) {
      res.status(403).json({ error: err.message, code: channelService.ASSISTANT_CHANNEL_FORBIDDEN_CODE });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to join channel",
      code: "agent_api_channel_join_failed",
      logPrefix: "[AgentAPI] Failed to join channel",
      err,
    });
  }
});

registerAgentApiRoute("channelLeave", ...agentApiRequestValidators("channelLeave"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const channelId = typeof req.params.channelId === "string" ? req.params.channelId : "";
    const channel = await channelService.getChannel(channelId);
    if (!channel || channel.serverId !== serverId) {
      res.status(404).json({ error: "Channel not found" });
      return;
    }
    // Joint channels carry ordinary membership: a human can leave one, so an
    // agent can too. Leaving only removes this agent's row on this server's
    // projection; other servers' members are untouched.
    if (channel.type !== "channel" && channel.type !== "private" && channel.type !== "joint") {
      res.status(403).json({ error: "Agents can only leave regular or joint channels" });
      return;
    }
    const hasAccess = await channelService.canAgentAccessChannel(channel.id, actingAgentId);
    if (!hasAccess) {
      res.status(403).json({ error: "Agents can only leave channels they can see" });
      return;
    }
    if (channel.archivedAt) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }

    await channelService.removeAgent(channel.id, actingAgentId);
    if (channel.type !== "channel") {
      const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
      await agentOrchestrator.purgeAgentInboxForChannelTree(
        actingAgentId,
        channel.id,
        "channel_membership_removed",
      );
    }
    req.app.get("io")?.to(channel.type === "private" ? `channel:${channel.id}` : `server:${serverId}`).emit("channel:members-updated", { channelId: channel.id });
    sendAgentApiResponse("channelLeave", res, {
      ok: true,
      attention: buildAgentChannelLeaveAttention(channel),
    });
  } catch (err: any) {
    const msg = err?.message || "";
    if (msg.includes("Cannot remove")) {
      res.status(403).json({ error: msg });
      return;
    }
    res.status(500).json({ error: "Failed to leave channel" });
  }
});

registerAgentApiRoute("channelCreate", ...agentApiRequestValidators("channelCreate"), validateAgentApiResponseMiddleware("channelCreate"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const result = await createChannelForAgent({
      actor: { id: agent.id, name: agent.name, serverId: agent.serverId },
      serverId,
      body: req.body,
      io: req.app.get("io") as SocketServer | undefined,
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to create channel",
      logPrefix: "internal.agent-api.channels.create error:",
      err,
    });
  }
});

registerAgentApiRoute("channelUpdate", ...agentApiRequestValidators("channelUpdate"), validateAgentApiResponseMiddleware("channelUpdate"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const result = await updateChannelForAgent({
      actor: { id: agent.id, name: agent.name, serverId: agent.serverId },
      serverId,
      channelId: String(req.params.channelId),
      body: req.body,
      io: req.app.get("io") as SocketServer | undefined,
      agentOrchestrator: req.app.get("agentOrchestrator") as AgentOrchestrator | undefined,
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to update channel",
      logPrefix: "internal.agent-api.channels.update error:",
      err,
    });
  }
});

function agentChannelLifecycleHandler(archived: boolean): RequestHandler {
  const action = archived ? "archive" : "unarchive";
  return async (req, res) => {
    try {
      const actingAgentId = req.actingAgentId!;
      const serverId = req.serverId!;
      const agent = await agentService.getAgent(actingAgentId);
      if (!agent || agent.serverId !== serverId) {
        res.status(401).json({ error: "Agent no longer exists" });
        return;
      }

      const target = String((req.body as { target: string }).target);
      const resolved = await channelService.resolveChannelByName(serverId, actingAgentId, target);
      if (!resolved) {
        res.status(404).json({ error: "Channel not found" });
        return;
      }

      const result = await setChannelArchivedForAgent({
        actor: { id: agent.id, name: agent.name, serverId: agent.serverId },
        serverId,
        channelId: resolved.channelId,
        archived,
        io: req.app.get("io") as SocketServer | undefined,
        agentOrchestrator: req.app.get("agentOrchestrator") as AgentOrchestrator | undefined,
      });
      res.status(result.status).json(result.body);
    } catch (err) {
      sendJsonServerError(req, res, {
        error: `Failed to ${action} channel`,
        logPrefix: `internal.agent-api.channels.${action} error:`,
        err,
      });
    }
  };
}

registerAgentApiRoute(
  "channelArchive",
  ...agentApiRequestValidators("channelArchive"),
  validateAgentApiResponseMiddleware("channelArchive"),
  agentChannelLifecycleHandler(true),
);
registerAgentApiRoute(
  "channelUnarchive",
  ...agentApiRequestValidators("channelUnarchive"),
  validateAgentApiResponseMiddleware("channelUnarchive"),
  agentChannelLifecycleHandler(false),
);

registerAgentApiRoute("channelMemberAdd", ...agentApiRequestValidators("channelMemberAdd"), validateAgentApiResponseMiddleware("channelMemberAdd"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const result = await addChannelMemberForAgent({
      actor: { id: agent.id, name: agent.name, serverId: agent.serverId },
      serverId,
      channelId: String(req.params.channelId),
      body: req.body,
      io: req.app.get("io") as SocketServer | undefined,
      agentOrchestrator: req.app.get("agentOrchestrator") as AgentOrchestrator | undefined,
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to add member",
      logPrefix: "internal.agent-api.channels.add-member error:",
      err,
    });
  }
});

registerAgentApiRoute("channelMemberRemove", ...agentApiRequestValidators("channelMemberRemove"), validateAgentApiResponseMiddleware("channelMemberRemove"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const result = await removeChannelMemberForAgent({
      actor: { id: agent.id, name: agent.name, serverId: agent.serverId },
      serverId,
      channelId: String(req.params.channelId),
      body: req.body,
      io: req.app.get("io") as SocketServer | undefined,
      agentOrchestrator: req.app.get("agentOrchestrator") as AgentOrchestrator | undefined,
    });
    res.status(result.status).json(result.body);
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to remove member",
      logPrefix: "internal.agent-api.channels.remove-member error:",
      err,
    });
  }
});

registerAgentApiRoute("channelMute", ...agentApiRequestValidators("channelMute"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const channel = await resolveAgentActivityMuteTarget(req, res);
    if (!channel) return;

    const state = await channelService.setInboxTargetActivityMuteState({
      receiverType: "agent",
      receiverId: actingAgentId,
      serverId,
      sourceChannelId: channel.id,
      activityMuted: true,
    });
    addTraceEvent("activity_mute.agent_api.updated", {
      server_id: serverId,
      source_channel_id: channel.id,
      receiver_type: "agent",
      receiver_id: actingAgentId,
      activity_mute_state: "muted",
      activity_muted: true,
      mute_from_seq: state.muteFromSeq,
      negative_evidence_bucket: "does_not_prove_future_message_suppression",
    });
    const acceptedHint = (req.body as AgentApiRequestBodyByRoute["channelMute"] | undefined)?.attentionHintAccepted;
    if (acceptedHint && acceptedHint.scope === `#${channel.name}`) {
      addTraceEvent("attention_hint_accepted", {
        server_id: serverId,
        source_channel_id: channel.id,
        receiver_type: "agent",
        receiver_id: actingAgentId,
        schema: acceptedHint.schema || ATTENTION_HINT_SCHEMA,
        trigger: acceptedHint.trigger,
        scope: acceptedHint.scope,
        suggested_command: acceptedHint.suggested_command ?? `raft channel mute "${acceptedHint.scope}"`,
        copy_version: acceptedHint.copy_version || ATTENTION_HINT_COPY_VERSION,
        epoch_ms: acceptedHint.epoch_ms,
        action: "channel_mute",
      });
    }
    sendAgentApiResponse("channelMute", res, buildAgentChannelMuteResponse(channel, state));
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to mute channel activity",
      logPrefix: "internal.agent-api.channels.mute error:",
      err,
    });
  }
});

registerAgentApiRoute("channelUnmute", ...agentApiRequestValidators("channelUnmute"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const channel = await resolveAgentActivityMuteTarget(req, res);
    if (!channel) return;

    const state = await channelService.setInboxTargetActivityMuteState({
      receiverType: "agent",
      receiverId: actingAgentId,
      serverId,
      sourceChannelId: channel.id,
      activityMuted: false,
    });
    addTraceEvent("activity_mute.agent_api.updated", {
      server_id: serverId,
      source_channel_id: channel.id,
      receiver_type: "agent",
      receiver_id: actingAgentId,
      activity_mute_state: "unmuted",
      activity_muted: false,
      mute_from_seq: null,
      negative_evidence_bucket: "does_not_prove_future_message_suppression",
    });
    sendAgentApiResponse("channelUnmute", res, buildAgentChannelMuteResponse(channel, state));
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to unmute channel activity",
      logPrefix: "internal.agent-api.channels.unmute error:",
      err,
    });
  }
});

/**
 * GET /internal/agent-api/wake-hints?since=<seq|latest>&limit=<n>
 *
 * Step1 bridge D7 — content-free wake hint polling surface. This is a peek
 * over the server-side volatile delivery buffer: it returns enough metadata
 * for a bridge to wake an external runtime, but never returns message bodies,
 * never calls `/events`, and never acknowledges/drains domain delivery.
 */

/**
 * Peek the agent's deliverable pending messages and project them as
 * content-free wake hints (shared by the poll route and the SSE stream —
 * task #72). Never drains, never acks, never advances cursors; inaccessible
 * queued messages are discarded exactly as the poll route always did.
 */
/**
 * An external agent's persisted messages from its durable inbox (see
 * messageService.pullExternalAgentInbox). A failed or unavailable pull yields
 * none: they are still unread and come on a later call.
 */
async function pullExternalAgentInboxMessages(
  agentOrchestrator: AgentOrchestrator,
  agentId: string,
  opts: Parameters<typeof messageService.pullExternalAgentInbox>[2],
): Promise<Extract<messageService.ExternalAgentInboxPull, { status: "pulled" }> | null> {
  try {
    const pulled = await messageService.pullExternalAgentInbox(agentOrchestrator, agentId, opts);
    return pulled.status === "pulled" ? pulled : null;
  } catch (err) {
    console.error(`internal.agent-api external agent inbox pull failed (${opts.route}):`, serializeErrorForLog(err));
    return null;
  }
}

/**
 * The pending items to serve. For an external agent (`durable` given):
 * persisted messages come only from the durable inbox pull, and the buffer
 * contributes only items with no durable row. Otherwise the buffer as is.
 */
function pendingAgentItems(
  buffered: AgentMessage[],
  durable: AgentMessage[] | undefined,
): { items: AgentMessage[]; staleBuffered: AgentMessage[] } {
  if (!durable) return { items: buffered, staleBuffered: [] };
  const bufferOnly = buffered.filter((message) => !isDurableAgentInboxMessage(message));
  const staleBuffered = buffered.filter((message) => isDurableAgentInboxMessage(message));
  return { items: [...bufferOnly, ...durable], staleBuffered };
}

async function collectDeliverableWakeHints(
  agentOrchestrator: AgentOrchestrator,
  actingAgentId: string,
  serverId: string,
  sinceSeq: number | null,
  limit: number,
  durable?: AgentMessage[],
): Promise<{ wakeHints: AgentApiWakeHint[]; hasMore: boolean }> {
  const { items: queued } = pendingAgentItems(agentOrchestrator.peekPendingMessages(actingAgentId), durable);
  const deliverableQueued: AgentMessage[] = [];
  const undeliverableQueued: AgentMessage[] = [];
  for (const message of queued) {
    const channelId = queuedMessageChannelId(message);
    const canAccess = await canAgentAccessQueuedMessageTarget(channelId, actingAgentId, serverId, message);
    if (canAccess) {
      deliverableQueued.push(message);
    } else {
      undeliverableQueued.push(message);
    }
  }
  if (undeliverableQueued.length > 0) {
    agentOrchestrator.discardUndeliverableMessages(actingAgentId, undeliverableQueued);
  }
  const filtered = sinceSeq !== null
    ? deliverableQueued.filter((message) => {
        const seq = queuedMessageSeq(message);
        if (seq === null && thirdPartyEventIdFromMessage(message) !== null) return true;
        return seq !== null && seq > sinceSeq;
      })
    : deliverableQueued;
  const trimmed = filtered.slice(0, limit);
  const wakeHints = trimmed
    .map(buildWakeHint)
    .filter((hint): hint is AgentApiWakeHint => hint !== null);
  return { wakeHints, hasMore: filtered.length > trimmed.length };
}

internalAgentApiRouter.get("/wake-hints", requireAgentCapability("read"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const sinceRaw = typeof req.query.since === "string" ? req.query.since.trim() : "";
    const limit = Math.min(Math.max(Number(req.query.limit) || 50, 1), 200);

    let sinceSeq: number | null = null;
    if (sinceRaw && sinceRaw !== "latest") {
      const parsed = Number(sinceRaw);
      if (!Number.isFinite(parsed) || parsed < 0) {
        res.status(400).json({
          error: "since must be a non-negative integer (messageSeq) or 'latest'",
          code: "since_invalid",
        });
        return;
      }
      sinceSeq = parsed;
    }

    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
    // External agents: persisted messages are peeked from the durable inbox
    // (rate-limited per agent). No cursor moves here.
    const durable = isExternalAgentRuntime(agent.runtime)
      ? (await pullExternalAgentInboxMessages(agentOrchestrator, actingAgentId, { route: "wake_hints", slot: "wake_hints" }))?.messages ?? []
      : undefined;
    await rebuildPendingThirdPartyAgentEvents(agentOrchestrator, actingAgentId)
      .catch((err) => console.error("internal.agent-api third-party event rebuild failed:", serializeErrorForLog(err)));
    const { wakeHints, hasMore } = await collectDeliverableWakeHints(
      agentOrchestrator,
      actingAgentId,
      serverId,
      sinceSeq,
      limit,
      durable,
    );
    const newestHint = wakeHints[wakeHints.length - 1];

    res.json({
      wake_hints: wakeHints,
      last_hint_seq: newestHint?.seq ?? sinceSeq,
      has_more: hasMore,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load wake hints",
      logPrefix: "internal.agent-api.wake-hints error:",
      err,
    });
  }
});

// Activity ingest intentionally uses read capability: the same minimal
// runner credential that can observe inbox state may report its own
// plugin-observed activity, while approve/send/write paths stay separate.
internalAgentApiRouter.post("/activity", requireAgentCapability("read"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const parsed = parseExternalAgentActivityIngest(req.body);
    if ("error" in parsed) {
      res.status(400).json(parsed);
      return;
    }

    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
    if (typeof agentOrchestrator?.recordExternalAgentActivity !== "function") {
      res.status(503).json({ error: "Agent activity ingest is unavailable", code: "activity_ingest_unavailable" });
      return;
    }

    const { ignoredFields, ...ingest } = parsed;
    if (ignoredFields.length > 0) {
      console.warn(`[internal.agent-api.activity] agent ${actingAgentId}: ignored unknown event fields: ${ignoredFields.join(", ")}`);
    }
    const result = await agentOrchestrator.recordExternalAgentActivity(actingAgentId, ingest, serverId);
    res.json({ ok: true, ...result, ...(ignoredFields.length > 0 ? { ignoredFields } : {}) });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to ingest activity",
      logPrefix: "internal.agent-api.activity error:",
      err,
    });
  }
});

/**
 * POST /internal/agent-api/third-party-events/delivered — task #175.
 *
 * A managed daemon answers `/events` from its Local Inbox and never forwards
 * that request, so the `/events` ack below never runs for the third-party
 * events it served. After writing such a response to the runtime, the daemon
 * reports the served event ids here. Idempotent by id (only queued/delivering
 * rows move to delivered) and scoped to the acting agent's own events.
 */
internalAgentApiRouter.post("/third-party-events/delivered", requireAgentCapability("read"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const rawIds: unknown = req.body?.eventIds;
    if (
      !Array.isArray(rawIds)
      || rawIds.length === 0
      || rawIds.length > THIRD_PARTY_EVENT_DELIVERED_REPORT_MAX_IDS
      || !rawIds.every(isThirdPartyEventId)
    ) {
      res.status(400).json({
        error: `eventIds must be a non-empty array of at most ${THIRD_PARTY_EVENT_DELIVERED_REPORT_MAX_IDS} event ids`,
        code: "event_ids_invalid",
      });
      return;
    }
    const eventIds = [...new Set((rawIds as string[]).map((id) => id.toLowerCase()))];

    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const delivered = await oauthService.markThirdPartyAgentEventsDelivered(eventIds, { agentId: actingAgentId });
    // The server buffer may still hold these seq-less entries (the daemon's
    // delivery ack carries seq 0, which clears nothing); drop them so they are
    // not served again by a later forwarded `/events`.
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
    agentOrchestrator?.acknowledgeDeliveredMessages?.(actingAgentId, [], eventIds);
    res.json({ ok: true, delivered });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to record third-party event delivery",
      logPrefix: "internal.agent-api.third-party-events.delivered error:",
      err,
    });
  }
});

/**
 * GET /internal/agent-api/wake-hints/stream — D7 T1 push (task #72).
 *
 * SSE projection of the same content-free wake-hint peek as the poll route:
 * on connect it replays the currently deliverable pending hints (after
 * `Last-Event-ID`/`?since` filtering), then pushes live as the orchestrator
 * delivers to this external agent's inbox. Observationally identical to
 * polling: no bodies, no draining, no acks, no cursor movement — wake-hint
 * receipt must never advance any consume/read boundary (FH-EXT-001
 * orthogonality; wire contract #wg-external-agent:0b2a7438 msg=3f265263).
 */
internalAgentApiRouter.get("/wake-hints/stream", requireAgentCapability("read"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;

    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const sinceRaw = typeof req.query.since === "string" && req.query.since.trim() !== ""
      ? req.query.since.trim()
      : (typeof req.headers["last-event-id"] === "string" ? req.headers["last-event-id"] : "");
    let lastSentSeq: number | null = null;
    if (sinceRaw && sinceRaw !== "latest") {
      const parsed = Number(sinceRaw);
      if (!Number.isFinite(parsed) || parsed < 0) {
        res.status(400).json({
          error: "since / Last-Event-ID must be a non-negative integer (messageSeq) or 'latest'",
          code: "since_invalid",
        });
        return;
      }
      lastSentSeq = parsed;
    }

    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;

    // The stream lives for hours; its request span covers only the connection.
    // Each pull (the open one, every heartbeat, every wake) is its own root
    // trace that names the stream it came from: hanging them all on the
    // request span folded thousands of pulls into one trace (#proj-o11y task
    // #6). Wake and revocation callbacks also run in the context of whoever
    // fired them, so without their own root they would land in the sender's
    // or revoker's trace.
    const streamSpan = getCurrentTraceSpan();
    const streamContext = streamSpan?.context ?? null;
    const streamTracer = req.app.get("serverTracer") as Tracer | undefined;
    let tickOrdinal = 0;
    type StreamTickTrigger = "open" | "heartbeat" | "wake" | "wake_follow_up" | "revocation";
    const inStreamTickTrace = <T>(trigger: StreamTickTrigger, work: () => Promise<T>): Promise<T> =>
      withTraceRoot(streamTracer, "server.agent_api.wake_hints_stream.tick", {
        parent: null,
        surface: "server",
        kind: "internal",
        attrs: {
          trigger,
          tick_ordinal: ++tickOrdinal,
          ...(streamContext ? { stream_trace_id: streamContext.traceId, stream_span_id: streamContext.spanId } : {}),
          ...agentIdHashAttrs(actingAgentId),
          ...serverIdHashAttrs(serverId),
        },
      }, work);
    // A failed tick is already recorded on its own span; log it like the
    // flush does and keep the stream (and the process) running.
    const runStreamTick = (trigger: StreamTickTrigger, work: () => Promise<unknown>): void => {
      void inStreamTickTrace(trigger, work).catch((err) => {
        console.error("internal.agent-api.wake-hints-stream tick error:", serializeErrorForLog(err));
      });
    };

    // External agents: persisted messages are peeked from the durable inbox;
    // the latest pull is kept for this stream's flushes. No cursor moves here.
    const isExternal = isExternalAgentRuntime(agent.runtime);
    let durablePending: AgentMessage[] | undefined;
    const pullDurable = async (route: "wake_hints_stream_open" | "wake_hints_stream_flush", force: boolean) => {
      if (!isExternal) return;
      const pulled = await pullExternalAgentInboxMessages(agentOrchestrator, actingAgentId, { route, slot: "stream", force });
      if (pulled) durablePending = pulled.messages;
      durablePending ??= [];
    };
    await inStreamTickTrace("open", async () => {
      await pullDurable("wake_hints_stream_open", false);
      await rebuildPendingThirdPartyAgentEvents(agentOrchestrator, actingAgentId)
        .catch((err) => console.error("internal.agent-api third-party event rebuild failed:", serializeErrorForLog(err)));
    });

    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    });
    res.write(": connected\n\n");
    sharedSseStreamRegistry.register(res);

    let closed = false;
    let flushing = false;
    let flushQueued = false;

    const flushHints = async () => {
      if (closed) return;
      if (flushing) {
        flushQueued = true;
        return;
      }
      flushing = true;
      try {
        do {
          flushQueued = false;
          const { wakeHints } = await collectDeliverableWakeHints(
            agentOrchestrator,
            actingAgentId,
            serverId,
            lastSentSeq,
            200,
            durablePending,
          );
          for (const hint of wakeHints) {
            if (closed) return;
            res.write(`event: wake-hint\nid: ${hint.seq}\ndata: ${JSON.stringify(hint)}\n\n`);
            if (typeof hint.seq === "number" && (lastSentSeq === null || hint.seq > lastSentSeq)) {
              lastSentSeq = hint.seq;
            }
          }
        } while (flushQueued && !closed);
      } catch (err) {
        console.error("internal.agent-api.wake-hints-stream flush error:", serializeErrorForLog(err));
      } finally {
        flushing = false;
      }
    };

    // Every flush pulls the durable inbox first (external agents only). A wake
    // signal (`force`) means something was just delivered — here or on
    // another replica (option C, #wg-external-agent 2026-06-11) — so it skips
    // the per-agent pull interval; heartbeat ticks respect it.
    const auditAndFlush = async (force: boolean): Promise<void> => {
      await pullDurable("wake_hints_stream_flush", force);
      await rebuildPendingThirdPartyAgentEvents(agentOrchestrator, actingAgentId)
        .catch((err) => console.error("internal.agent-api.wake-hints-stream third-party event rebuild failed:", serializeErrorForLog(err)));
      await flushHints();
    };

    // After a wake signal the chain may not have the message yet (see
    // EXTERNAL_WAKE_FOLLOW_UP_PULL_DELAYS_MS): pull again a bounded number of
    // times. A new signal replaces the pending follow-ups rather than stacking.
    let followUpTimers: ReturnType<typeof setTimeout>[] = [];
    const clearFollowUps = () => {
      for (const timer of followUpTimers) clearTimeout(timer);
      followUpTimers = [];
    };
    const onDelivered = (agentId: string) => {
      if (agentId !== actingAgentId) return;
      runStreamTick("wake", () => auditAndFlush(true));
      if (!isExternal) return;
      clearFollowUps();
      followUpTimers = messageService.externalWakeFollowUpPullDelaysMs().map((delayMs) => {
        const timer = setTimeout(() => {
          if (!closed) runStreamTick("wake_follow_up", () => auditAndFlush(true));
        }, delayMs);
        timer.unref?.();
        return timer;
      });
    };
    agentOrchestrator.on("external-inbox-delivered", onDelivered);

    // Heartbeat doubles as a server-side reconcile tick (field incident
    // 2026-06-11, #wg-external-agent:00fcc8f7): the live push path depends on
    // an IN-PROCESS orchestrator event (also fed cross-replica via the Redis
    // wake signal), and the periodic durable inbox pull remains the
    // correctness floor when both event layers miss. flushHints dedupes via
    // lastSentSeq and stays non-draining / zero-cursor, so a quiet tick
    // writes nothing.
    const heartbeatMs = Math.min(Math.max(Number(process.env.SLOCK_WAKE_STREAM_HEARTBEAT_MS) || 25_000, 250), 60_000);
    //
    // The credential was authenticated once, at open. Every tick re-validates
    // it (not revoked, agent and server not deleted) before doing anything
    // on its behalf, and ends the stream when it no longer holds — so a
    // revoked credential or deleted agent loses the stream within one
    // heartbeat even if the revocation broadcast below is missed. A failed
    // check also ends the stream (fail closed); the client reconnects and
    // re-authenticates.
    const credentialId = req.agentCredentialId!;
    let unsubscribeRevocation: () => void = () => {};
    const cleanup = () => {
      closed = true;
      sharedSseStreamRegistry.unregister(res);
      clearInterval(heartbeat);
      clearFollowUps();
      agentOrchestrator.off("external-inbox-delivered", onDelivered);
      unsubscribeRevocation();
    };
    const credentialStillValid = async (): Promise<boolean> => {
      if (closed) return false;
      const active = await isAgentCredentialActive(credentialId).catch((err) => {
        console.error("internal.agent-api.wake-hints-stream credential check failed:", serializeErrorForLog(err));
        return false;
      });
      if (active || closed) return active && !closed;
      cleanup();
      // Connection-level: it ends the stream, so it stays on the stream's span.
      streamSpan?.addEvent("agent_api.wake_hints_stream.credential_invalidated", { agent_id: actingAgentId, credential_id: credentialId });
      res.write(`event: credential-revoked\ndata: {"code":"credential_revoked"}\n\n`);
      res.end();
      return false;
    };
    // Each valid tick also counts as "seen" for presence (the open itself
    // already did, via the auth middleware), so a quiet but connected agent
    // stays online. Throttled per credential inside the recorder.
    const heartbeat = setInterval(() => {
      if (closed) return;
      runStreamTick("heartbeat", async () => {
        if (!(await credentialStillValid())) return;
        res.write(": ka\n\n");
        recordAgentApiSeen(req);
        await auditAndFlush(false);
      });
    }, heartbeatMs);
    heartbeat.unref?.();

    // Prompt close on revoke / agent delete: the writer broadcasts the
    // agentId to every replica after commit (replicaRouter
    // `broadcastAgentCredentialRevocation`); re-check this stream's
    // credential now instead of waiting for the next tick.
    unsubscribeRevocation = subscribeAgentCredentialRevocation(actingAgentId, () => {
      runStreamTick("revocation", credentialStillValid);
    });

    req.on("close", cleanup);

    // Initial replay of currently pending hints.
    await flushHints();
  } catch (err) {
    console.error("internal.agent-api.wake-hints-stream error:", serializeErrorForLog(err));
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to open wake hint stream" });
    } else {
      res.end();
    }
  }
});

/**
 * POST /internal/agent-api/send
 *
 * Slice-2 send: resolves target, runs the same freshness/held-draft gate
 * as the legacy `/internal/agent/:id/send` surface, then broadcasts +
 * delivers via `messageService.broadcastAndDeliver`.
 *
 * Body: {
 *   target: <ref>,
 *   content: string,
 *   attachmentIds?: string[],
 *   idempotencyKey?: string,
 *   sendDraft?: boolean,
 *   continueAnyway?: boolean,
 *   draftReholdCount?: number,
 *   draftReplacedExisting?: boolean,
 *   seenUpToSeq?: number
 * }
 */
/**
 * GET /internal/agent-api/events?since=<seq|latest>&limit=<n>[&ack=cursor]
 *
 * RFC v0.8 — catch-up envelope. Returns messages delivered to the
 * bound agent since the given anchor, plus per-turn awareness fields:
 *   - last_seen_msgId      : delivery-batch tail id for client paging/debugging;
 *                            NOT a model-seen boundary for send freshness
 *   - last_seen_seq        : numeric cursor to pass back as `since`
 *   - reply_target         : canonical send target of the newest event
 *                            (e.g. `#general`, `#general:1a2b3c4d`, `dm:@alice`);
 *                            null when the batch is empty
 *   - pending_notice_ids   : transient system events queued for delivery
 *   - wake_reason          : placeholder until #164 lifecycle event RFC lands
 *   - ack_mode             : "cursor" | "immediate" — how this batch is acknowledged
 *
 * Slice-1 minimal contract: aggregates undelivered inbox entries via the
 * AgentOrchestrator. Designed to be the external-runtime equivalent of
 * the daemon's `agent:deliver` push channel.
 *
 * The `since` param accepts either a numeric `messageSeq` (canonical) or
 * the special string `"latest"` (return whatever is currently queued
 * without claiming an anchor). If a UUID/short id is passed, v0.8
 * returns 400; msg-id resolution is a later slice.
 *
 * Acknowledgement. By default (`ack_mode: "immediate"`, every published
 * client) the returned batch is acknowledged before the response is sent, so a
 * response lost in transit loses its messages. With `ack=cursor` (external
 * agents), durable inbox rows are acknowledged on the NEXT request instead: a
 * cursor-mode request with `since=<seq>` acknowledges the rows the previous
 * cursor-mode response handed over with seq <= since, then returns the next
 * batch unacknowledged. A client that did not get a response repeats its old
 * `since` and receives the same rows again. Items with no durable row
 * (third-party app events) are still acknowledged immediately.
 */
registerAgentApiRoute("events", async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const eventsQuery = validateAgentApiQuery("events", req, res);
    if (!eventsQuery) return;
    const sinceRaw = eventsQuery.since?.trim() ?? "";
    const limit = Math.min(Math.max(Number(eventsQuery.limit) || 50, 1), 200);
    const ackRaw = typeof eventsQuery.ack === "string" ? eventsQuery.ack.trim() : "";
    if (ackRaw && ackRaw !== "cursor" && ackRaw !== "immediate") {
      res.status(400).json({ error: "ack must be 'cursor' or 'immediate'", code: "ack_invalid" });
      return;
    }

    let sinceSeq: number | null = null;
    if (sinceRaw && sinceRaw !== "latest") {
      const parsed = Number(sinceRaw);
      if (!Number.isFinite(parsed) || parsed < 0) {
        res.status(400).json({
          error: "since must be a non-negative integer (messageSeq) or 'latest'",
          code: "since_invalid",
        });
        return;
      }
      sinceSeq = parsed;
    }

    const agent = await agentService.getAgent(actingAgentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }

    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
    const isExternal = isExternalAgentRuntime(agent.runtime);
    // Cursor acknowledgement applies to the durable inbox, which only external
    // agents are served from.
    const cursorAck = isExternal && ackRaw === "cursor";
    // The previous cursor-mode response's rows up to `since` were received:
    // acknowledge them before pulling, so the pull starts after them.
    const cursorAckedSeqs = new Set<number>();
    if (cursorAck && sinceSeq !== null) {
      const acked = await acknowledgeAgentInboxEventsCursor(agentOrchestrator, actingAgentId, sinceSeq);
      for (const seq of acked) cursorAckedSeqs.add(seq);
    }
    // External agents: persisted messages come only from the durable inbox
    // pull (oldest first per conversation); the buffer serves only items with
    // no durable row. The pull itself moves no cursor; the ack below does.
    const pulled = isExternal
      ? await pullExternalAgentInboxMessages(agentOrchestrator, actingAgentId, { route: "events", slot: "events" })
      : null;
    // A pull already in flight may predate the cursor ack above.
    const durable = isExternal
      ? (pulled?.messages ?? []).filter((m) => !cursorAckedSeqs.has(m.seq ?? -1))
      : undefined;
    const durableSeqs = new Set((durable ?? []).map((m) => m.seq));
    await rebuildPendingThirdPartyAgentEvents(agentOrchestrator, actingAgentId)
      .catch((err) => console.error("internal.agent-api third-party event rebuild failed:", serializeErrorForLog(err)));

    // Pull whatever the orchestrator has queued for this agent. Non-blocking
    // (block=false, timeoutMs=0) — v0.8 does NOT implement long-poll on
    // this surface; the external runtime is expected to poll on its own
    // cadence or layer a WS/SSE transport on top in a later slice.
    const abortController = new AbortController();
    const buffered = await agentOrchestrator.receiveMessages(
      actingAgentId,
      false,
      0,
      abortController.signal,
    );
    const { items: queued, staleBuffered } = pendingAgentItems(buffered, durable);
    // A persisted message left in an external agent's buffer (queued before
    // it was pulled-only) is dropped: the inbox pull is its only source.
    if (staleBuffered.length > 0) {
      agentOrchestrator.discardUndeliverableMessages(actingAgentId, staleBuffered);
    }
    const { deliverable: deliverableQueued, undeliverable: undeliverableQueued } =
      await partitionDeliverableInboxMessages(queued, actingAgentId, serverId);
    if (undeliverableQueued.length > 0) {
      agentOrchestrator.discardUndeliverableMessages(actingAgentId, undeliverableQueued);
    }
    // Pulled rows that cannot be handed over are acked with the batch, as live
    // delivery would have dropped them; left unread they would be pulled again
    // on every call.
    const droppedDurableSeqs = [
      ...(pulled?.droppedSeqs ?? []).filter((seq) => !cursorAckedSeqs.has(seq)),
      ...undeliverableQueued.filter((m) => durableSeqs.has(m.seq)).map((m) => m.seq!),
    ];

    // Apply `since` filter in memory. Slice-1 simplicity — once we have
    // a typed inbox cursor on the orchestrator we'll push this down. In cursor
    // mode `since` is an acknowledgement, not a filter, for durable rows: what
    // the pull returns is exactly what is unacknowledged, including older rows
    // of other conversations.
    const filtered = sinceSeq !== null
      ? deliverableQueued.filter((m) => {
          if (cursorAck && durableSeqs.has(m.seq)) return true;
          const seq = Number((m as { seq?: unknown }).seq);
          if ((!Number.isFinite(seq) || seq <= 0) && thirdPartyEventIdFromMessage(m) !== null) return true;
          return Number.isFinite(seq) && seq > sinceSeq!;
        })
      : deliverableQueued;
    const trimmed = filtered.slice(0, limit);
    // Refresh every persisted message id now, before rendering or
    // acknowledging. The projection throws on canonical/provenance failure so
    // this whole request exits with zero ack and the batch is kept for retry.
    const events = await projectAgentInboxEvents(trimmed, serverId);
    const ackSeqs = events
      .map((m) => Number((m as { seq?: unknown }).seq))
      .filter((seq): seq is number => Number.isInteger(seq) && seq > 0);
    // Only when the whole pull was returned: the ack takes the highest seq per
    // conversation, so a dropped row above a trimmed one would skip it.
    if (filtered.length === trimmed.length) ackSeqs.push(...droppedDurableSeqs);
    const ackMessageIds = events
      .filter((m) => !Number.isInteger(Number(m.seq)) || Number(m.seq) <= 0)
      .map((m) => typeof m.message_id === "string" ? m.message_id : "")
      .filter(Boolean);
    const ackedThirdPartyEventIds = events
      .map(thirdPartyEventIdFromMessage)
      .filter((id): id is string => id !== null);
    // Cursor mode: durable rows wait for the next request's `since`; the rest
    // (non-durable items) is acknowledged now, as before.
    const cursorPendingSeqs = cursorAck ? ackSeqs.filter((seq) => durableSeqs.has(seq) || droppedDurableSeqs.includes(seq)) : [];
    const immediateAckSeqs = cursorAck ? ackSeqs.filter((seq) => !cursorPendingSeqs.includes(seq)) : ackSeqs;
    if (cursorAck) {
      await recordAgentInboxEventsPendingAck(actingAgentId, cursorPendingSeqs);
    }
    if (immediateAckSeqs.length > 0 || ackMessageIds.length > 0) {
      // `/events` is the agent-api equivalent of legacy receive+receive-ack:
      // claim exactly the returned batch so `slock message check` does not
      // replay the same inbox forever when using an sk_agent_* credential.
      agentOrchestrator.acknowledgeDeliveredMessages(actingAgentId, immediateAckSeqs, ackMessageIds);
      // Handing messages over advances the agent's read position — the agent
      // counterpart of a human reading them — unless a daemon tracks what its
      // model actually saw (model-seen boundary) and reports that instead. An
      // external agent has no daemon: for it, receiving IS reading, by rule
      // (not by the absence of a machine). Freshness proof is separate; send
      // still requires `seenUpToSeq`.
      const readPositionFollowsModelSeen = !isExternal
        && typeof agentOrchestrator.hasMachineCapability === "function"
        && agentOrchestrator.hasMachineCapability(agent.machineId, DAEMON_CAPABILITY_MODEL_SEEN_BOUNDARY);
      if (!readPositionFollowsModelSeen && immediateAckSeqs.length > 0) {
        await channelService.markAgentLegacyAckCheckpoint(actingAgentId, immediateAckSeqs);
      }
    }
    if (ackedThirdPartyEventIds.length > 0) {
      await oauthService.markThirdPartyAgentEventsDelivered(ackedThirdPartyEventIds);
    }

    // last_seen_msgId echo: prefer the newest event's id in this delivery
    // batch; if empty, return null. This is a delivery cursor/debug echo,
    // NOT a model-seen boundary. Do not feed it into send freshness;
    // `/internal/agent-api/send` requires runtime-maintained `seenUpToSeq`.
    const newestEvent = events[events.length - 1];
    const lastSeenMsgId = newestEvent?.id ?? null;
    // In cursor mode the cursor must cover every pending row (the batch is
    // not in seq order across the buffer and the inbox, and dropped rows ride
    // along), so passing it back acknowledges the whole batch.
    const lastSeenSeq = cursorAck
      ? (cursorPendingSeqs.length > 0 ? Math.max(...cursorPendingSeqs, sinceSeq ?? 0) : sinceSeq)
      : newestEvent?.seq ?? sinceSeq;

    // reply_target hint: the canonical send target of the newest event (the
    // same `#name` / `#name:<8hex>` / `dm:@peer` / `dm:@peer:<8hex>` string the
    // CLI prints for it and `/send` resolves). Null when the batch is empty or
    // the newest event has no conversation (third-party app events).
    const replyTarget = newestEvent ? formatInboxMessageTarget(newestEvent) : null;

    // Conversations still unread beyond this batch; present while any remain.
    const inboxHint = pulled && pulled.remainingConversations > 0
      ? { unread_conversations: pulled.remainingConversations, command: "raft inbox check" as const }
      : null;

    addTraceEvent("external_agent.events.check.finished", {
      body_result: events.length > 0 ? "returned" : "empty",
      returned_count: events.length,
      queued_count: queued.length,
      filtered_count: filtered.length,
      undeliverable_count: undeliverableQueued.length,
      is_external: isExternal,
      pulled_count: durable?.length ?? 0,
      since_seq: sinceSeq,
      ack_mode: cursorAck ? "cursor" : "immediate",
      cursor_acked_count: cursorAckedSeqs.size,
    });

    sendAgentApiResponse("events", res, {
      events,
      last_seen_msgId: lastSeenMsgId,
      last_seen_seq: lastSeenSeq,
      reply_target: replyTarget,
      // Deprecated, always empty/null and read by no client. Still sent
      // because published CLIs/SDKs parse them as required; the contract
      // marks them optional so a later release can drop them.
      pending_notice_ids: [] as string[],
      wake_reason: null as string | null,
      // More to page: this response was trimmed to `limit`, or the durable
      // inbox pull itself stopped at one of its caps (per conversation, total,
      // conversations) with unread left behind.
      has_more: filtered.length > trimmed.length || pulled?.truncated === true,
      inbox_hint: inboxHint,
      ack_mode: cursorAck ? "cursor" : "immediate",
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load events",
      logPrefix: "internal.agent-api.events error:",
      err,
    });
  }
});

/**
 * Inbox push registration for the bound External Agent (raft-agent-inbox.v2;
 * see services/agentInboxPushService). Gated on `read`: a push hands over
 * exactly what `GET /events` (a `read` route) would, and acknowledges it the
 * same way, so a credential that may drain its inbox may also have it pushed.
 * The receiver supplies the signing secret; it is stored encrypted and never
 * returned by any route.
 */
function sendAgentInboxPushError(res: Response, error: AgentInboxPushError): void {
  const status = error.code === "AGENT_NOT_EXTERNAL"
    ? 403
    : error.code === "AGENT_NOT_FOUND" || error.code === "CREDENTIAL_INACTIVE"
      ? 401
      : error.code === "PUSH_UNAVAILABLE"
        ? 503
        : 400;
  res.status(status).json({ error: error.message, code: error.code });
}

registerAgentApiRoute("pushWebhookStatus", async (req, res) => {
  try {
    res.json(await getAgentInboxPushStatus({ agentId: req.actingAgentId!, serverId: req.serverId! }));
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load push webhook",
      logPrefix: "internal.agent-api.push-webhook.get error:",
      err,
    });
  }
});

registerAgentApiRoute("pushWebhookRegister", async (req, res) => {
  const body = req.body as unknown;
  if (!body || typeof body !== "object" || Array.isArray(body)
    || Object.keys(body).some((key) => key !== "url" && key !== "secret")) {
    res.status(400).json({ error: "Body must be {url, secret}", code: "INVALID_BODY" });
    return;
  }
  const { url, secret } = body as { url?: unknown; secret?: unknown };
  try {
    const status = await putAgentInboxPushRegistration({
      agentId: req.actingAgentId!,
      serverId: req.serverId!,
      credentialId: req.agentCredentialId!,
      url,
      secret,
    });
    res.json(status);
  } catch (err) {
    if (err instanceof AgentInboxPushError) {
      sendAgentInboxPushError(res, err);
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to store push webhook",
      logPrefix: "internal.agent-api.push-webhook.put error:",
      err,
    });
  }
});

registerAgentApiRoute("pushWebhookDelete", async (req, res) => {
  try {
    await deleteAgentInboxPushRegistration({ agentId: req.actingAgentId!, serverId: req.serverId! });
    res.status(204).end();
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to delete push webhook",
      logPrefix: "internal.agent-api.push-webhook.delete error:",
      err,
    });
  }
});

type AgentApiAttachmentDownloadTarget = {
  attachment: typeof attachments.$inferSelect;
  storage: NonNullable<ReturnType<typeof getStorage>>;
  contentType: string;
  contentDisposition: string;
};

/**
 * The one auth + visibility resolution behind both agent attachment download
 * routes (`attachmentDownload` streams/redirects the bytes,
 * `attachmentDownloadUrl` returns the presigned URL), so they cannot diverge.
 * Both routes are registered with the same contract capability. A malformed,
 * missing, or unreadable id answers the same uniform 404; missing storage
 * answers 503. Returns null once it has answered. `logContext.attachment` is
 * the row the caller's error log may describe.
 */
async function resolveAgentApiAttachmentDownloadTarget(
  req: Request,
  res: Response,
  logContext: { attachment: typeof attachments.$inferSelect | null },
): Promise<AgentApiAttachmentDownloadTarget | null> {
  const attachmentId = getFullUuidParam(req.params.attachmentId);
  if (!attachmentId) {
    sendAttachmentDownloadUnavailable(res);
    return null;
  }
  const [projection] = await getDb()
    .select()
    .from(attachments)
    .where(eq(attachments.id, attachmentId))
    .limit(1);
  if (!projection) {
    sendAttachmentDownloadUnavailable(res);
    return null;
  }
  logContext.attachment = projection;
  const attachment = await resolveAgentReadableAttachment(projection, req.serverId!, req.actingAgentId!);
  if (!attachment) {
    sendAttachmentDownloadUnavailable(res);
    return null;
  }
  const storage = getStorage();
  if (!storage) {
    res.status(503).json({ error: "File storage is not configured on this server" });
    return null;
  }
  return {
    attachment,
    storage,
    contentType: buildAttachmentResponseContentType(attachment.mimeType),
    contentDisposition: buildAttachmentContentDisposition(attachment.filename, attachment.mimeType),
  };
}

/**
 * Presigned GET for the target, valid AGENT_API_ATTACHMENT_DOWNLOAD_URL_TTL_SECONDS,
 * or null when the storage backend cannot presign (local disk). The URL is a
 * bearer capability: never log or trace it.
 */
function presignAgentApiAttachmentDownload(target: AgentApiAttachmentDownloadTarget): Promise<string> | null {
  if (!target.storage.getPresignedUrl) return null;
  return target.storage.getPresignedUrl(target.attachment.storageKey, {
    expiresIn: AGENT_API_ATTACHMENT_DOWNLOAD_URL_TTL_SECONDS,
    responseContentDisposition: target.contentDisposition,
    responseContentType: target.contentType,
  });
}

/**
 * Error log for the agent attachment routes: ids and row metadata plus the
 * error class only. The error message is left out on purpose because a
 * storage/presign failure may carry the signed URL.
 */
function logAgentApiAttachmentError(
  logPrefix: string,
  req: Request,
  attachment: typeof attachments.$inferSelect | null,
  err: unknown,
): void {
  console.error(logPrefix, {
    attachmentId: attachment?.id ?? req.params.attachmentId ?? null,
    channelId: attachment?.channelId ?? null,
    storageKey: attachment?.storageKey ?? null,
    sizeBytes: attachment?.sizeBytes ?? null,
    mimeType: attachment?.mimeType ?? null,
    filename: attachment?.filename ?? null,
    errorClass: errorClassOf(err),
  });
}

registerAgentApiRoute("attachmentDownload", ...agentApiRequestValidators("attachmentDownload"), async (req, res) => {
  const logContext: { attachment: typeof attachments.$inferSelect | null } = { attachment: null };
  try {
    const target = await resolveAgentApiAttachmentDownloadTarget(req, res, logContext);
    if (!target) return;
    const { attachment, storage, contentType, contentDisposition } = target;
    const presigning = presignAgentApiAttachmentDownload(target);
    if (presigning) {
      const presignedUrl = await presigning;
      res.status(302);
      res.setHeader("Cache-Control", "private, no-store");
      res.setHeader("Location", presignedUrl);
      res.setHeader("Content-Length", "0");
      res.setHeader("X-Content-Type-Options", "nosniff");
      res.end();
      return;
    }

    const stream = await storage.get(attachment.storageKey);
    const contentLength = buildAttachmentContentLengthHeader(attachment.sizeBytes);
    res.setHeader("Cache-Control", "private, immutable, max-age=31536000");
    res.setHeader("Content-Type", contentType);
    if (contentLength) {
      res.setHeader("Content-Length", contentLength);
    }
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("Content-Disposition", contentDisposition);
    await streamStorageResponse(stream, res);
  } catch (err) {
    if (res.destroyed || res.headersSent) return;
    logAgentApiAttachmentError("internal.agent-api.attachments error:", req, logContext.attachment, err);
    res.status(500).json({ error: "Failed to serve attachment" });
  }
});

// Download URL for runtimes that cannot take binary tool results (hosted
// agents): same resolution as attachmentDownload above, then the URL in a JSON
// body instead of a 302. A storage backend that cannot presign (local disk)
// answers 409 download_url_unavailable: the request is fine and the
// attachment readable, but this Server can only stream it, so the caller
// should switch to the binary route rather than retry (a 5xx would invite
// retries under the read retry policy).
registerAgentApiRoute(
  "attachmentDownloadUrl",
  ...agentApiRequestValidators("attachmentDownloadUrl"),
  async (req, res) => {
    const logContext: { attachment: typeof attachments.$inferSelect | null } = { attachment: null };
    try {
      const target = await resolveAgentApiAttachmentDownloadTarget(req, res, logContext);
      if (!target) return;
      // Stamp the expiry before minting so expiresAt never overstates the
      // URL's real lifetime.
      const expiresAt = new Date(Date.now() + AGENT_API_ATTACHMENT_DOWNLOAD_URL_TTL_SECONDS * 1000);
      const presigning = presignAgentApiAttachmentDownload(target);
      if (!presigning) {
        res.status(409).json(AGENT_API_ATTACHMENT_DOWNLOAD_URL_UNAVAILABLE_RESPONSE);
        return;
      }
      const url = await presigning;
      const { attachment } = target;
      res.setHeader("Cache-Control", "private, no-store");
      sendAgentApiResponse("attachmentDownloadUrl", res, {
        url,
        expiresAt: expiresAt.toISOString(),
        filename: attachment.filename,
        mimeType: attachment.mimeType,
      });
    } catch (err) {
      if (res.destroyed || res.headersSent) return;
      logAgentApiAttachmentError("internal.agent-api.attachments.url error:", req, logContext.attachment, err);
      res.status(500).json({ error: "Failed to mint attachment download URL" });
    }
  },
);

// --- Attachment comments (attachment-comments MVP spec, PR3 agent transport) ---
// Mirrors the user routes on /api/attachments/:id/comments. Both transports
// reuse attachmentCommentService so there is exactly one comment pipeline;
// the agent-side authorize is canAgentPostToChannel on the parent channel.

registerAgentApiRoute("attachmentCommentsList", ...agentApiRequestValidators("attachmentCommentsList"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    // Feature flag gate — agents outside the enabled server get the same
    // consistent 403 as the user APIs.
    if (!(await attachmentCommentService.attachmentCommentsEnabledForServer(serverId))) {
      res.status(403).json({ error: "Attachment comments are not enabled on this server", code: "attachment_comments_disabled" });
      return;
    }
    const attachmentId = getFullUuidParam(req.params.attachmentId);
    if (!attachmentId) {
      sendAttachmentNotFound(res);
      return;
    }
    const db = getDb();
    const [projection] = await db
      .select()
      .from(attachments)
      .where(eq(attachments.id, attachmentId))
      .limit(1);
    if (!projection) {
      sendAttachmentNotFound(res);
      return;
    }
    const attachment = await resolveAgentReadableAttachment(projection, serverId, actingAgentId);
    if (!attachment) {
      sendAttachmentNotFound(res);
      return;
    }
    const limitRaw = typeof req.query.limit === "string" ? Number(req.query.limit) : undefined;
    const result = await attachmentCommentService.listAttachmentComments(attachment.id, {
      limit: Number.isFinite(limitRaw) ? limitRaw : undefined,
    });
    // §5 resolve AX (task #40): enrich each comment with resolved status
    // so agents see which comments are actionable without reimplementing
    // the resolve rule from raw reactions.
    const parentMsg = attachment.messageId
      ? await db.select({ senderId: messages.senderId, senderType: messages.senderType })
          .from(messages).where(eq(messages.id, attachment.messageId)).limit(1)
          .then((rows) => rows[0] ?? null)
      : null;
    const enrichedComments = parentMsg
      ? attachmentCommentService.enrichCommentsWithResolveStatus(
          result.comments as Array<Record<string, unknown> & { senderId: string; reactions: Array<{ emoji: string; reactorType: string; reactorId: string; createdAt: Date }> }>,
          parentMsg,
        )
      : result.comments.map((c) => ({ ...c, resolved: false, resolvedBy: null, resolvedAt: null }));
    // Viewer resolve capability: an agent can resolve iff it is the parent
    // message author (its ✅ satisfies §5 rule 2).
    const canResolve = !!(parentMsg && actingAgentId === parentMsg.senderId);
    const body: AgentApiAttachmentCommentsResponse = {
      ...result,
      comments: enrichedComments as AgentApiAttachmentCommentsResponse["comments"],
      viewer: {
        canComment: false,
        reason: "agent_descoped" as const,
        canResolve,
        ...(canResolve ? { resolveAction: { type: "reaction", emoji: "✅" } } : {}),
      },
    };
    sendAgentApiResponse("attachmentCommentsList", res, body);
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load comments",
      logPrefix: "internal.agent-api.attachment-comments.list error:",
      err,
    });
  }
});

internalAgentApiRouter.post("/attachments/:attachmentId/comments", requireAgentCapability("send"), async (req, res) => {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const content = req.body?.content;
    const attachmentId = getFullUuidParam(req.params.attachmentId);
    if (!attachmentId) {
      sendAttachmentNotFound(res);
      return;
    }
    const agent = await agentService.getAgent(actingAgentId);
    if (!agent) {
      res.status(404).json({ error: "Agent not found" });
      return;
    }
    const io = req.app.get("io");
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
    const result = await attachmentCommentService.createAttachmentComment(io, agentOrchestrator, {
      attachmentId,
      serverId,
      senderType: "agent",
      senderId: actingAgentId,
      senderName: agent.displayName || agent.name || "Agent",
      content,
      authorize: async (parentChannelId) => {
        const canPost = await channelService.canAgentPostToChannel(parentChannelId, actingAgentId);
        if (!canPost) {
          throw new attachmentCommentService.AttachmentCommentError(
            403,
            "not_a_member",
            "Agent must be a member of this channel to comment",
          );
        }
      },
    });
    res.json(result);
  } catch (err) {
    if (err instanceof attachmentCommentService.AttachmentCommentError) {
      res.status(err.status).json({ error: err.message, code: err.code });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to create comment",
      logPrefix: "internal.agent-api.attachment-comments.create error:",
      err,
    });
  }
});

/**
 * The other side of the DM `channel` is in (a DM, or a thread under one) as
 * the agent sees them: kind, handle, description, and the zone their latest
 * message there carried. Null when not a DM, for the agent's own or an app
 * DM, and for a peer with nothing to show (no description and no zone).
 */
async function resolvePeerCard(
  serverId: string,
  agentId: string,
  channel: { id: string; type: string; parentMessageId?: string | null } | null | undefined,
): Promise<AgentApiPeerCard | null> {
  if (!channel) return null;
  let dmChannelId: string | null = channel.type === "dm" ? channel.id : null;
  if (!dmChannelId && channel.type === "thread" && channel.parentMessageId) {
    const parent = await messageService.getMessage(channel.parentMessageId);
    const parentChannel = parent ? await channelService.getChannel(parent.channelId) : null;
    if (parentChannel?.type === "dm") dmChannelId = parentChannel.id;
  }
  if (!dmChannelId) return null;
  const peer = await channelService.getDmPeerForAgent(serverId, agentId, dmChannelId);
  if (!peer) return null;
  const timezone = peer.kind === "human"
    ? await messageService.getLatestSenderTimezone([...new Set([dmChannelId, channel.id])], "user", peer.id)
    : null;
  const card: AgentApiPeerCard = { kind: peer.kind, name: peer.name, description: peer.description, timezone };
  return peerCardHasContent(card) ? card : null;
}

/** Whether this agent already committed a send with this idempotency key. */
async function isAgentSendKeyCommitted(agentId: string, idempotencyKey: string): Promise<boolean> {
  const [committed] = await getDb()
    .select({ id: messages.id })
    .from(messages)
    .where(and(
      eq(messages.senderType, "agent"),
      eq(messages.senderId, agentId),
      eq(messages.agentSendKey, idempotencyKey),
    ))
    .limit(1);
  return Boolean(committed);
}

async function handleAgentApiMessageSend(
  req: Request,
  res: Response,
  routeKey: "messageSend" | "messageSendV2",
): Promise<void> {
  try {
    const actingAgentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const sendBody = validateAgentApiBody(routeKey, req, res);
    if (!sendBody) return;
    const {
      target,
      content,
      attachmentIds,
      idempotencyKey,
      reconcileOnly,
      continue: deprecatedContinue,
      sendDraft,
      continueAnyway,
      draftReholdCount,
      draftReplacedExisting,
      seenUpToSeq,
      seenExactSeqs: requestedSeenExactSeqs,
      seenCards,
      freshnessContextMode,
    } = sendBody;
    const mentions = routeKey === "messageSendV2" && "mentions" in sendBody
      ? sendBody.mentions as messageService.StructuredMentionInput[] | undefined
      : undefined;
    const isSendDraft = sendDraft === true;
    const isContinueAnyway = continueAnyway === true;
    const withholdFreshnessContext = freshnessContextMode === "withheld";
    const parsedDraftReholdCount = typeof draftReholdCount === "number" && Number.isFinite(draftReholdCount)
      ? Math.max(0, Math.floor(draftReholdCount))
      : 0;
    const attachmentCount = Array.isArray(attachmentIds) ? attachmentIds.length : 0;
    const requestedSendFreshness = isSendDraft ||
      isContinueAnyway ||
      typeof seenUpToSeq === "number" ||
      (Array.isArray(requestedSeenExactSeqs) && requestedSeenExactSeqs.length > 0) ||
      typeof draftReholdCount === "number" ||
      draftReplacedExisting === true ||
      withholdFreshnessContext;

    addTraceEvent("agent_api_send.request.started", {
      target_kind: inferAgentApiSendTargetKind(target),
      client_kind: agentApiClientKind(req),
      content_chars: typeof content === "string" ? content.length : 0,
      attachment_count: attachmentCount,
      idempotency_key_present: typeof idempotencyKey === "string" && idempotencyKey.length > 0,
      reconcile_only: reconcileOnly === true,
      send_draft: isSendDraft,
      continue_anyway: isContinueAnyway,
      freshness_requested: requestedSendFreshness,
      freshness_exact_seen_count: requestedSeenExactSeqs?.length ?? 0,
      freshness_context_mode: freshnessContextMode ?? "inline",
    });

    if (!target || typeof target !== "string") {
      traceSendRouteFailure("bad_request", 400);
      res.status(400).json({ error: "target is required" });
      return;
    }
    if (deprecatedContinue === true) {
      traceSendRouteFailure("bad_request", 400);
      res.status(400).json({
        error: "--continue is no longer supported. Use normal message send to update a draft, or --send-draft to send the current saved draft.",
      });
      return;
    }
    if (isContinueAnyway && !isSendDraft) {
      traceSendRouteFailure("bad_request", 400);
      res.status(400).json({ error: "--send-draft --anyway requires a saved draft" });
      return;
    }

    const agent = await tracePhase(
      () => agentService.getAgent(actingAgentId),
      (_durationMs, result) => ({
        name: "agent_api_send.agent.loaded",
        attrs: {
          outcome: result && result.serverId === serverId ? "found" : "missing",
          runtime: result?.runtime ?? null,
        },
      }),
    );
    if (!agent || agent.serverId !== serverId) {
      traceSendRouteFailure("agent_not_found", 401);
      res.status(401).json({ error: "Agent no longer exists" });
      return;
    }
    if (reconcileOnly === true) {
      if (typeof idempotencyKey !== "string" || idempotencyKey.length === 0) {
        traceSendRouteFailure("bad_request", 400);
        res.status(400).json({ error: "idempotencyKey is required for reconciliation" });
        return;
      }
      if (content !== undefined) {
        traceSendRouteFailure("bad_request", 400);
        res.status(400).json({ error: "Reconciliation requests must not include content" });
        return;
      }
      const [committed] = await getDb()
        .select({ id: messages.id, seq: messages.seq })
        .from(messages)
        .where(and(
          eq(messages.senderType, "agent"),
          eq(messages.senderId, actingAgentId),
          eq(messages.agentSendKey, idempotencyKey),
        ))
        .limit(1);
      if (!committed) {
        sendAgentApiResponse(routeKey, res, {
          ok: true,
          state: "not_found",
          reconciliation: true,
        });
        return;
      }
      sendAgentApiResponse(routeKey, res, {
        ok: true,
        state: "committed",
        messageId: committed.id,
        messageSeq: committed.seq,
        reconciliation: true,
        receiptComplete: false,
      });
      return;
    }
    if (!content || typeof content !== "string") {
      traceSendRouteFailure("bad_request", 400);
      res.status(400).json({ error: "Content is required" });
      return;
    }
    const sendFreshnessEnabled = isSendFreshnessEnabled(agent.serverId, actingAgentId);
    const sendFreshnessMode = (process.env.SLOCK_ATTESTED_SEND_MODE ?? "on").trim().toLowerCase();
    const shouldRunSendFreshness = sendFreshnessEnabled &&
      (requestedSendFreshness || sendFreshnessMode === "force");
    if (!sendFreshnessEnabled && requestedSendFreshness) {
      traceSendRouteFailure("freshness_not_enabled", 403);
      res.status(403).json({ error: "Send freshness interface is not enabled for this agent." });
      return;
    }

    const resolved = await tracePhase(
      () => resolveWritableAgentTarget(serverId, actingAgentId, target),
      (_durationMs, result) => ({
        name: "agent_api_send.target.resolved",
        attrs: {
          outcome: typeof result === "string" ? result : result ? "resolved" : "not_found",
          target_type: typeof result === "object" && result ? result.type : null,
        },
      }),
    );
    if (resolved === "forbidden") {
      traceSendRouteFailure("target_forbidden", 403);
      res.status(403).json({ error: forbiddenMessageForTarget(target) });
      return;
    }
    if (resolved === "peer-not-found") {
      traceSendRouteFailure("target_not_found", 404);
      res.status(404).json({ error: `User or agent not found: @${target.slice(4)}` });
      return;
    }
    if (resolved === "self-dm") {
      traceSendRouteFailure("bad_request", 400);
      res.status(400).json({ error: "Cannot create a DM with yourself" });
      return;
    }
    if (!resolved) {
      traceSendRouteFailure("target_not_found", 404);
      res.status(404).json({ error: notFoundMessageForTarget(target) });
      return;
    }
    try {
      await tracePhase(
        () => channelService.assertChannelNotArchived(resolved.channelId),
        () => ({
          name: "agent_api_send.archive.checked",
          attrs: {
            outcome: "active",
            target_type: resolved.type,
          },
        }),
      );
    } catch (err) {
      if (err instanceof channelService.ChannelArchivedError) {
        addTraceEvent("agent_api_send.archive.checked", {
          outcome: "archived",
          target_type: resolved.type,
        });
        traceSendRouteFailure("channel_archived", 409);
        res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
        return;
      }
      throw err;
    }

    // Cards (passive AX): the target's channel card (`seenCards.channel`) and
    // a DM target's peer card (`seenCards.peer`), each only for a client that
    // names the version its model context has (null: not seen). Never seen in
    // this context: held once, like unread messages. Seen an older version:
    // sent, with the new card in the reply. Reviewer isolation sends no new
    // context, so it carries no card either.
    const seenChannelCard = withholdFreshnessContext ? undefined : seenCards?.channel;
    const seenPeerCard = withholdFreshnessContext ? undefined : seenCards?.peer;
    const targetChannel = seenChannelCard === undefined && seenPeerCard === undefined
      ? null
      : await channelService.getChannel(resolved.channelId);
    const channelCardCheck = seenChannelCard === undefined
      ? null
      : await (async () => {
        const row = await resolveChannelCardRow(targetChannel, serverId);
        if (!row) return null;
        const card = channelCardOf(row);
        return channelCardRev(card) === seenChannelCard ? null : { card, changed: seenChannelCard !== null };
      })();
    // The peer is checked for "seen in this context" only: their zone travels
    // with each message, so a version compare would call every message from
    // another place a change. A changed description reaches the agent with
    // their next message.
    const peerCardCheck = seenPeerCard !== null
      ? null
      : await (async () => {
        const card = await resolvePeerCard(serverId, actingAgentId, targetChannel);
        return card ? { card, changed: false } : null;
      })();
    const unseenCardReasons = [
      ...(channelCardCheck && !channelCardCheck.changed ? ["channel_card"] : []),
      ...(peerCardCheck && !peerCardCheck.changed ? ["peer_card"] : []),
    ];
    // A retry of a send that already committed replays its receipt below; it
    // is not held for a card the original send was not held for.
    const holdForCards = unseenCardReasons.length > 0
      && !isContinueAnyway
      && !(typeof idempotencyKey === "string" && idempotencyKey.length > 0
        && await isAgentSendKeyCommitted(actingAgentId, idempotencyKey));
    const cardFields = {
      ...(channelCardCheck ? { channelCard: channelCardCheck.card, ...(channelCardCheck.changed ? { channelCardChanged: true } : {}) } : {}),
      ...(peerCardCheck ? { peerCard: peerCardCheck.card } : {}),
    };
    for (const [name, seen, check] of [["channel", seenChannelCard, channelCardCheck], ["peer", seenPeerCard, peerCardCheck]] as const) {
      if (seen === undefined) continue;
      addTraceEvent(`agent_api_send.${name}_card.evaluated`, {
        outcome: check === null ? "seen" : check.changed ? "changed" : holdForCards ? "held" : "unseen_not_held",
        target_type: resolved.type,
      });
    }

    const io = req.app.get("io");
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
    let attestedSendActivityContext: {
      targetType: attestedSendService.AttestedSendTargetType;
      targetRef: string;
      newMessageCount: number;
    } | null = null;

    if (shouldRunSendFreshness) {
      const freshness = await tracePhase(
        async () => {
          const evaluation = await evaluateAgentFreshness({
            agentId: actingAgentId,
            channelId: resolved.channelId,
            channelType: resolved.type,
            seenUpToSeq,
            requestedSeenExactSeqs,
          });
          return { ...evaluation, shouldHoldForFreshness: !isContinueAnyway && evaluation.newMessageCount > 0 };
        },
        (_durationMs, result) => ({
          name: "agent_api_send.freshness.evaluated",
          attrs: {
            outcome: result.shouldHoldForFreshness ? "held" : "passed",
            target_type: resolved.type,
            boundary_source: result.boundarySource,
            new_message_count: result.newMessageCount,
            formal_mention_count: result.formalMentionFacts.count,
            send_draft: isSendDraft,
            continue_anyway: isContinueAnyway,
          },
        }),
      );
      attestedSendActivityContext = {
        targetType: freshness.attestedTargetType,
        targetRef: target,
        newMessageCount: freshness.newMessageCount,
      };


      if (freshness.shouldHoldForFreshness) {
        const response = await tracePhase(
          () => renderAgentFreshnessHold(req, {
            action: "send",
            agentId: actingAgentId,
            serverId,
            agentServerId: agent.serverId,
            channelId: resolved.channelId,
            channelType: resolved.type,
            target,
            withhold: withholdFreshnessContext,
            agentOrchestrator,
            evaluation: freshness,
            replacedExisting: draftReplacedExisting === true,
            reholdCount: parsedDraftReholdCount,
            isSendDraft,
          }),
          (_durationMs, result) => ({
            name: "agent_api_send.held_response.rendered",
            attrs: {
              held_message_count: result.heldMessages?.length ?? 0,
              new_message_count: result.newMessageCount ?? 0,
              shown_message_count: result.shownMessageCount ?? 0,
              omitted_message_count: result.omittedMessageCount ?? 0,
            },
          }),
        );
        addTraceEvent("response.ready", {
          status_code: 200,
          state: "held",
          held_message_count: response.heldMessages?.length ?? 0,
          new_message_count: response.newMessageCount ?? 0,
        });
        sendAgentApiResponse(routeKey, res, {
          ...response,
          ...(holdForCards ? { holdReasons: ["unread_messages", ...unseenCardReasons] } : {}),
          ...cardFields,
        });
        return;
      }
    }

    if (holdForCards) {
      // No unread messages: the cards alone hold the send. Nothing was shown
      // from the conversation, so read positions do not move.
      await recordAgentRaftAction(req, actingAgentId, {
        title: unseenCardReasons.includes("channel_card") ? "Send held for the channel card" : "Send held for the DM peer's card",
        text: `target: ${target}\nreason: ${unseenCardReasons.join(", ")} not in the agent's current context`,
      });
      addTraceEvent("response.ready", { status_code: 200, state: "held", held_message_count: 0, new_message_count: 0 });
      sendAgentApiResponse(routeKey, res, {
        ok: true,
        state: "held",
        holdReasons: unseenCardReasons,
        heldMessages: [],
        newMessageCount: 0,
        shownMessageCount: 0,
        omittedMessageCount: 0,
        ...cardFields,
      });
      return;
    }

    const sendAttention = await buildDriveByJoinedToPostAttention(actingAgentId, resolved, target);
    const enriched = await tracePhase(
      // slack-bridge-ordinary-message-producer: agent_api.send
      () => messageService.broadcastAndDeliver(io, agentOrchestrator, {
        channelId: resolved.channelId,
        senderType: "agent",
        senderId: actingAgentId,
        senderName: agent.displayName || agent.name || "Agent",
        content,
        mentions,
        mentionContract: routeKey === "messageSendV2" ? "v2" : "v1",
        attachmentIds: Array.isArray(attachmentIds) ? attachmentIds : [],
        ...(typeof idempotencyKey === "string" && idempotencyKey.length > 0
          ? { agentSendKey: idempotencyKey, rejectMismatchedAgentSendReplay: true }
          : {}),
      }),
      (_durationMs, result) => ({
        name: "agent_api_send.commit.finished",
        attrs: {
          target_type: resolved.type,
          attachment_count: attachmentCount,
          idempotency_key_present: typeof idempotencyKey === "string" && idempotencyKey.length > 0,
          message_seq_present: Number.isFinite(Number(result.seq)),
        },
      }),
    );

    if (shouldRunSendFreshness && isSendDraft && attestedSendActivityContext) {
      const result = isContinueAnyway ? "committed_anyway" : "committed";
      await tracePhase(
        async () => {
          await attestedSendService.recordContinueEvent({
            agentId: actingAgentId,
            serverId: agent.serverId,
            targetType: attestedSendActivityContext.targetType,
            targetRef: attestedSendActivityContext.targetRef,
            messageId: enriched.id,
            result,
            newMessageCount: attestedSendActivityContext.newMessageCount,
          });
          const reviewerIsolationContinuation = withholdFreshnessContext;
          const title = reviewerIsolationContinuation
            ? "Reviewer-isolation draft sent"
            : isContinueAnyway
              ? "Send draft sent anyway"
              : "Send draft sent";
          await recordAgentRaftAction(req, actingAgentId, {
            title,
            text: reviewerIsolationContinuation
              ? `${formatAttestedMessageCount(attestedSendActivityContext.newMessageCount)} withheld`
              : [
                  `target: ${target}`,
                  `freshness updates: ${formatAttestedMessageCount(attestedSendActivityContext.newMessageCount)}`,
                  isContinueAnyway
                    ? "decision: sent anyway after reviewing freshness context"
                    : "decision: saved draft freshness check passed when sent",
                ].join("\n"),
          });
        },
        () => ({
          name: "agent_api_send.continue_event.recorded",
          attrs: {
            result,
            new_message_count: attestedSendActivityContext?.newMessageCount ?? 0,
          },
        }),
      );
    }

    addTraceEvent("response.ready", {
      status_code: 200,
      state: "sent",
      target_type: resolved.type,
      message_seq_present: Number.isFinite(Number(enriched.seq)),
    });
    const pendingMentionActions = messageService.getSenderPendingMentionActions(enriched);
    const unresolvedMentionHandles = messageService.getSenderUnresolvedMentionHandles(enriched);
    sendAgentApiResponse(routeKey, res, {
      ok: true,
      state: "sent",
      messageId: enriched.id,
      messageSeq: enriched.seq,
      ...(sendAttention ? { attention: sendAttention } : {}),
      ...(pendingMentionActions.length > 0 ? { pendingMentionActions } : {}),
      ...(unresolvedMentionHandles.length > 0 ? { unresolvedMentionHandles } : {}),
      ...cardFields,
    });
  } catch (err) {
    if (routeKey === "messageSendV2" && err instanceof messageService.MentionValidationError && err.code) {
      traceSendRouteFailure("mention_validation", 400);
      res.status(400).json({ error: err.message, code: err.code });
      return;
    }
    if (err instanceof messageService.AgentSendIdempotencyConflictError) {
      // Closed-set subkind: a reused idempotency key is request misuse.
      traceSendRouteFailure("bad_request", err.status);
      res.status(err.status).json({
        error: err.message,
        code: err.code,
        mismatch: err.mismatch,
        suggestedNextAction: err.suggestedNextAction,
      });
      return;
    }
    if (err instanceof AttachmentLinkError) {
      traceSendRouteFailure("bad_request", err.status);
      res.status(err.status).json({ error: err.message, code: err.code });
      return;
    }
    if (err instanceof DmTargetResolutionError) {
      // An ambiguous same-name DM peer (409) or unknown peer kind (400) is a
      // target the caller can fix, not a server fault.
      traceSendRouteFailure("bad_request", err.status);
      res.status(err.status).json(err.toResponseBody());
      return;
    }
    if (transientTransactionSqlState(err)) {
      // The send transaction lost a deadlock/serialization race on every
      // bounded retry and rolled back: nothing was committed. The same
      // idempotencyKey is safe to resend.
      traceSendRouteCatch(err, 503);
      res.setHeader("Retry-After", "1");
      res.status(503).json(transientSendConflictBody());
      return;
    }
    // #79: classify the failure into a closed-set error_subkind (+ bounded
    // sanitized message) so the otherwise-opaque 500 is splittable in traces
    // (input-validation vs genuine server-internal). Classification is
    // structural only — never sniffs err.message. HTTP status unchanged (the
    // mention_validation 500→4xx fix is a separate PR).
    traceSendRouteCatch(err, 500);
    console.error("internal.agent-api.send error:", serializeErrorForLog(err));
    res.status(500).json({ error: "Failed to send message" });
  }
}

registerAgentApiRoute("messageSend", (req, res) => handleAgentApiMessageSend(req, res, "messageSend"));
registerAgentApiRoute("messageSendV2", (req, res) => handleAgentApiMessageSend(req, res, "messageSendV2"));

function parseAgentApiAttachmentUpload(req: Request, res: Response, next: NextFunction): void {
  runSingleAttachmentUpload(req, res, next);
}

function attachmentUploadSessionService(req: Request): AttachmentUploadSessionService | null {
  return (req.app.get("attachmentUploadSessionService") as AttachmentUploadSessionService | undefined) ?? null;
}

function limitAttachmentUploadSessionCreate(req: Request, res: Response, next: NextFunction): void {
  const limiter = req.app.get("attachmentUploadSessionCreateLimiter") as RequestHandler | undefined;
  if (!limiter) {
    res.status(500).json({ error: "Attachment upload limiter is unavailable" });
    return;
  }
  limiter(req, res, next);
}

const uploadForbiddenBody = {
  code: "UPLOAD_FORBIDDEN",
  message: "The agent cannot upload to this channel.",
  retryable: false,
} as const;

async function agentCanCreateUploadSession(
  req: Request,
  body: AgentApiRequestBodyByRoute["attachmentUploadSessionCreate"],
): Promise<boolean> {
  const agentId = req.actingAgentId!;
  const serverId = req.serverId!;
  if (!await channelService.canAgentPostToChannel(body.channelId, agentId)) return false;
  if (
    await channelService.isChannelArchived(body.channelId)
    || await isChannelReadOnlyByBillingFeature(body.channelId, serverId)
    || await isChannelReadOnlyByQuota(body.channelId, serverId)
  ) return false;
  const quota = await getFileUploadQuotaSummary(serverId);
  return body.sizeBytes <= getAttachmentFileSizeLimitBytes(quota.plan)
    && (!quota.enforced || body.sizeBytes <= quota.remainingBytes);
}

registerAgentApiRoute(
  "attachmentUploadCapabilities",
  validateAgentApiResponseMiddleware("attachmentUploadCapabilities"),
  async (req, res) => {
    const service = attachmentUploadSessionService(req);
    if (service) {
      const result = await service.capabilities({ serverId: req.serverId!, agentId: req.actingAgentId! });
      res.status(result.status).json(result.body);
      return;
    }
    const quota = await getFileUploadQuotaSummary(req.serverId!);
    res.json({
      directUploadEnabled: false,
      directUploadThresholdBytes: null,
      maxBytes: getAttachmentFileSizeLimitBytes(quota.plan),
      sessionExpiresInSeconds: null,
    });
  },
);

registerAgentApiRoute(
  "attachmentUploadSessionCreate",
  limitAttachmentUploadSessionCreate,
  ...agentApiRequestValidators("attachmentUploadSessionCreate"),
  validateAgentApiResponseMiddleware("attachmentUploadSessionCreate"),
  async (req, res) => {
    const service = attachmentUploadSessionService(req);
    if (!service) {
      res.status(404).json({ code: "UPLOAD_SESSION_NOT_FOUND", message: "Direct uploads are disabled.", retryable: false });
      return;
    }
    const body = req.body as AgentApiRequestBodyByRoute["attachmentUploadSessionCreate"];
    if (!await agentCanCreateUploadSession(req, body)) {
      res.status(403).json(uploadForbiddenBody);
      return;
    }
    const result = await service.create({ serverId: req.serverId!, agentId: req.actingAgentId! }, body);
    res.status(result.status).json(result.body);
  },
);

registerAgentApiRoute(
  "attachmentUploadSessionComplete",
  ...agentApiRequestValidators("attachmentUploadSessionComplete"),
  validateAgentApiResponseMiddleware("attachmentUploadSessionComplete"),
  async (req, res) => {
    const service = attachmentUploadSessionService(req);
    if (!service) {
      res.status(404).json({ code: "UPLOAD_SESSION_NOT_FOUND", message: "Direct uploads are disabled.", retryable: false });
      return;
    }
    const result = await service.complete(
      { serverId: req.serverId!, agentId: req.actingAgentId! },
      req.params.uploadId as string,
    );
    res.status(result.status).json(result.body);
  },
);

registerAgentApiRoute(
  "attachmentUploadSessionCancel",
  ...agentApiRequestValidators("attachmentUploadSessionCancel"),
  validateAgentApiResponseMiddleware("attachmentUploadSessionCancel"),
  async (req, res) => {
    const service = attachmentUploadSessionService(req);
    if (!service) {
      res.status(404).json({ code: "UPLOAD_SESSION_NOT_FOUND", message: "Direct uploads are disabled.", retryable: false });
      return;
    }
    const result = await service.cancel(
      { serverId: req.serverId!, agentId: req.actingAgentId! },
      req.params.uploadId as string,
    );
    res.status(result.status).json(result.body);
  },
);

registerAgentApiRoute(
  "attachmentUploadSessionStatus",
  ...agentApiRequestValidators("attachmentUploadSessionStatus"),
  validateAgentApiResponseMiddleware("attachmentUploadSessionStatus"),
  async (req, res) => {
    const service = attachmentUploadSessionService(req);
    if (!service) {
      res.status(404).json({ code: "UPLOAD_SESSION_NOT_FOUND", message: "Direct uploads are disabled.", retryable: false });
      return;
    }
    const result = await service.status(
      { serverId: req.serverId!, agentId: req.actingAgentId! },
      req.params.uploadId as string,
    );
    res.status(result.status).json(result.body);
  },
);

registerAgentApiRoute("attachmentUpload", parseAgentApiAttachmentUpload, async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;

    const file = req.file;
    if (!file) {
      res.status(400).json({ error: "No file provided" });
      return;
    }
    if (isEmptyUploadedFile(file)) {
      res.status(400).json({ error: "Empty files are not allowed" });
      return;
    }
    const maxBytes = await resolveRequestAttachmentFileSizeLimitBytes(req);
    if (isOversizedUploadedFile(file, maxBytes)) {
      res.status(413).json(buildAttachmentTooLargeResponse(maxBytes));
      return;
    }

    const channelId = typeof req.body.channelId === "string" ? req.body.channelId : undefined;
    if (!channelId) {
      res.status(400).json({ error: "channelId is required" });
      return;
    }

    const canPost = await channelService.canAgentPostToChannel(channelId, agentId);
    if (!canPost) {
      res.status(403).json({ error: "Agent must join this channel before sending or uploading" });
      return;
    }

    if (await channelService.isChannelArchived(channelId)) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }
    if (await isChannelReadOnlyByBillingFeature(channelId, serverId)) {
      res.status(403).json({ error: "This joint channel is read-only because it has more than 2 free servers. It becomes writable again when a server upgrades or a free server leaves." });
      return;
    }

    const storage = getStorage();
    if (!storage) {
      res.status(503).json({ error: "File uploads are not configured on this server" });
      return;
    }

    const normalizedFilename = normalizeAttachmentFilename(file.originalname);
    const explicitMimeType = typeof req.body.mimeType === "string" ? req.body.mimeType : undefined;
    const normalizedMimeType = normalizeUploadedMimeType(
      normalizedFilename,
      file.mimetype,
      file.buffer,
      explicitMimeType,
    );
    const cdnStorage = getCdnStorage();

    const [inserted] = await uploadAttachmentBuffers({
      serverId,
      channelId,
      uploaderId: agentId,
      uploaderType: "agent",
      files: [{ buffer: file.buffer, filename: normalizedFilename, mimeType: normalizedMimeType }],
      storage,
      cdnStorage,
      preview: {
        canGenerate: canGenerateImagePreview,
        generateThumbnail,
        isSvg: isSvgAttachmentMimeType,
        generateSvgRasterPreview,
      },
    });

    sendAgentApiResponse("attachmentUpload", res, {
      id: inserted.id,
      filename: normalizeAttachmentFilename(inserted.filename),
      mimeType: resolveAttachmentMimeType(inserted.filename, inserted.mimeType),
      sizeBytes: inserted.sizeBytes,
      thumbnailUrl: getThumbnailUrl(inserted.thumbnailKey),
    });
    recordRaftCliActivity(req, agentId, {
      command: "attachment.upload",
      summary: "Uploaded attachment",
      target: `${channelId} · ${normalizeAttachmentFilename(inserted.filename)}`,
      correlationId: inserted.id,
    });
  } catch (err) {
    console.error("internal.agent-api.upload error:", serializeErrorForLog(err));
    if (isStorageTimeoutError(err)) {
      res.status(504).json({ error: "Attachment storage timed out" });
      return;
    }
    if (err instanceof FileUploadQuotaExceededError) {
      res.status(err.status).json(await buildFileUploadQuotaExceededResponse(req.serverId!, err));
      return;
    }
    res.status(500).json({ error: "Failed to upload file" });
  }
});
registerAgentApiRoute("resolveChannel", ...agentApiRequestValidators("resolveChannel"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const { target } = req.body as AgentApiRequestBodyByRoute["resolveChannel"];

    const resolved = await resolveWritableAgentTarget(serverId, agentId, target);
    if (resolved === "forbidden") {
      setRequestTraceErrorCode(res, "target_forbidden");
      res.status(403).json({ error: forbiddenMessageForTarget(target) });
      return;
    }
    if (resolved === "peer-not-found") {
      setRequestTraceErrorCode(res, "peer_not_found");
      res.status(404).json({ error: `User or agent not found: @${target.slice(4)}` });
      return;
    }
    if (resolved === "self-dm") {
      setRequestTraceErrorCode(res, "self_dm");
      res.status(400).json({ error: "Cannot create a DM with yourself" });
      return;
    }
    if (!resolved) {
      setRequestTraceErrorCode(res, "channel_not_found");
      res.status(404).json({ error: notFoundMessageForTarget(target) });
      return;
    }

    sendAgentApiResponse("resolveChannel", res, { channelId: resolved.channelId });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to resolve channel",
      logPrefix: "internal.agent-api.resolve-channel error:",
      err,
    });
  }
});
registerAgentApiRoute("messageSearch", async (req, res) => {
  const requestAbort = bindRequestAbortSignal(req, res);
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const queryInput = validateAgentApiQuery("messageSearch", req, res);
    if (!queryInput) return;
    const query = (queryInput.q ?? "").trim();

    const channelRef = queryInput.channel;
    let channelId: string | undefined;
    if (channelRef) {
      const resolved = await channelService.resolveChannelByName(serverId, agentId, channelRef);
      if (!resolved) {
        res.status(404).json({ error: `Channel not found: ${channelRef}` });
        return;
      }
      channelId = resolved.channelId;
    }

    const limit = Math.min(
      Number(queryInput.limit) || AGENT_API_MESSAGE_SEARCH_DEFAULT_LIMIT,
      AGENT_API_MESSAGE_SEARCH_MAX_LIMIT,
    );
    const offset = Math.max(Number(queryInput.offset) || 0, 0);
    const senderFilter = await resolveSearchSenderFilter(
      serverId,
      { type: "agent", id: agentId },
      queryInput.sender,
      queryInput.senderId,
    );
    if (!senderFilter.ok) {
      res.status(senderFilter.status).json({ error: senderFilter.error, errorCode: senderFilter.errorCode });
      return;
    }
    const after = queryInput.after ? new Date(queryInput.after) : undefined;
    const before = queryInput.before ? new Date(queryInput.before) : undefined;
    if ((after && Number.isNaN(after.getTime())) || (before && Number.isNaN(before.getTime()))) {
      res.status(400).json({ error: "Invalid date filter" });
      return;
    }
    const sort = queryInput.sort === "recent" ? "recent" : "relevance";
    const cursor = queryInput.cursor ? searchService.parseMessageSearchWalkCursor(queryInput.cursor) : undefined;
    if (cursor === null || (cursor && (!query || sort !== "recent"))) {
      res.status(400).json({ error: "Invalid search cursor: pass back nextCursor unchanged, with the same query and --sort recent" });
      return;
    }
    const hasMeaningfulFilter = Boolean(channelId || senderFilter.senderId || after || before);
    if (!query && !hasMeaningfulFilter) {
      sendAgentApiResponse("messageSearch", res, { results: [], hasMore: false });
      return;
    }

    const searchResponse = await searchService.searchMessagesForAgent({
      serverId,
      agentId,
      query,
      channelId,
      senderId: senderFilter.senderId,
      after,
      before,
      sort: query ? sort : "recent",
      limit,
      offset,
      cursor,
      signal: requestAbort.signal,
    });
    const [renderedContents, renderedSnippets, taskFacts, commentScopes] = await Promise.all([
      agentPermalinkRenderService.renderAgentReadablePermalinksInTexts(
        searchResponse.results.map((result) => result.content),
        serverId,
      ),
      agentPermalinkRenderService.renderAgentReadablePermalinksInTexts(
        searchResponse.results.map((result) => result.snippet),
        serverId,
      ),
      loadCanonicalTaskFactsByMessageId(searchResponse.results.map((result) => result.id)),
      // Search rows are intentionally lean. Load only the returned ids' scope
      // projections. Agent comment access is evaluated at the server level,
      // exactly like the dedicated Agent comment routes; user-targeted rules
      // remain human-viewer policy.
      messageService.loadAgentCommentScopesForMessages(
        searchResponse.results.map((result) => result.id),
        serverId,
      ),
    ]);

    sendAgentApiResponse("messageSearch", res, {
      ...searchResponse,
      results: searchResponse.results.map((result, index) => {
        const { externalMessage, ...visibleResult } = result;
        const external = result.senderType === "external_projection";
        // Slack-bridged rows read like ordinary messages; other external content stays inert.
        const inert = external && !isSlackBridgedExternalProvider(externalMessage?.provider);
        const taskFact = external ? undefined : taskFacts.get(result.id);
        const taskProjectionFields = taskFact ? {
          taskStatus: taskFact.taskStatus,
          taskNumber: taskFact.taskNumber,
          taskCurrentProjection: {
            ...taskFact.taskCurrentProjection,
            amendedAt: taskFact.taskCurrentProjection.amendedAt?.toISOString() ?? null,
          },
        } : {};
        return {
          ...visibleResult,
          senderType: toAgentFacingActorType(result.senderType),
          ...(externalMessage ? { external_message: externalMessage } : {}),
          content: inert
            ? renderThirdPartyInertText({ field: "tool_result", value: result.content })
            : renderAgentCommentScopedContent(
                renderedContents[index] ?? result.content,
                commentScopes.get(result.id),
              ),
          snippet: inert
            ? renderThirdPartyInertText({ field: "tool_result", value: result.snippet })
            : renderedSnippets[index] ?? result.snippet,
          ...taskProjectionFields,
        };
      }),
    });
  } catch (err) {
    if (searchService.isMessageSearchPublicError(err)) {
      res.status(err.status).json({ error: err.message, errorCode: err.code });
      return;
    }
    if (requestAbort.signal.aborted || searchService.isSearchQueryAbortedError(err)) {
      return;
    }
    if (respondToDmTargetResolutionError(err, res)) return;
    console.error("internal.agent-api.search error:", serializeErrorForLog(err));
    if (!res.headersSent) {
      res.status(500).json({ error: "Failed to search messages" });
    }
  } finally {
    requestAbort.cleanup();
  }
});
registerAgentApiRoute("messageResolve", ...agentApiRequestValidators("messageResolve"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    const { msgId: rawMessageId } = req.params as AgentApiRequestParamsByRoute["messageResolve"];
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const resolved = await messageService.resolveMessageIdVisibleToAgent(serverId, agentId, rawMessageId);
    if (!resolved.ok) {
      res.status(resolved.status).json(messageResolveErrorPayload(resolved));
      return;
    }

    const payload = await resolveAgentVisibleMessagePayload(resolved.messageId, serverId, agentId);
    if (!payload) {
      res.status(404).json({ error: "Message not found", errorCode: "NOT_FOUND" });
      return;
    }

    sendAgentApiResponse("messageResolve", res, {
      message: {
        ...payload,
        parent_channel_type: payload.parent_channel_type ?? undefined,
        parent_channel_name: payload.parent_channel_name ?? undefined,
      },
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to resolve message",
      logPrefix: "internal.agent-api.message-resolve error:",
      err,
    });
  }
});
registerAgentApiRoute("channelMembers", async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const channelMembersQuery = validateAgentApiQuery("channelMembers", req, res);
    if (!channelMembersQuery) return;
    const { channel: channelRef } = channelMembersQuery;
    const resolved = await channelService.resolveChannelByName(serverId, agentId, channelRef);
    if (!resolved) {
      res.status(404).json({ error: `Channel not found: ${channelRef}` });
      return;
    }

    const members = await channelService.getChannelMembers(resolved.channelId);
    const agentRoles = await getAgentServerRolesInServer(serverId, members.agents.map((agent) => agent.id));
    const visibleAgents = members.agents.map((agent) => ({
      name: agent.name,
      status: agent.status,
      role: agentRoles.get(agent.id) ?? null,
    }));
    const visibleHumans = await filterAgentVisibleHumansForHiddenDirectory(
      serverId,
      agentId,
      members.humans,
    );
    sendAgentApiResponse("channelMembers", res, {
      channel: { ref: channelRef, type: resolved.type },
      agents: visibleAgents,
      humans: visibleHumans.map((human) => {
        const servers = channelService.agentVisibleMemberServers(human, serverId);
        return { name: human.name, description: human.description, role: human.role, ...(servers ? { servers } : {}) };
      }),
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to get channel members",
      logPrefix: "internal.agent-api.channel-members error:",
      err,
    });
  }
});
registerAgentApiRoute("threadUnfollow", ...agentApiRequestValidators("threadUnfollow"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const { thread: threadRef } = req.body as AgentApiRequestBodyByRoute["threadUnfollow"];
    const reason = typeof req.body?.reason === "string" && req.body.reason.trim()
      ? req.body.reason.trim()
      : "no longer following";

    let threadChannelId: string | null = null;
    if (UUID_RE.test(threadRef)) {
      const channel = await channelService.getChannel(threadRef);
      if (!channel || channel.serverId !== serverId) {
        res.status(404).json({ error: "Thread not found" });
        return;
      }
      const canAccess = await channelService.canAgentAccessChannel(channel.id, agentId);
      if (!canAccess) {
        res.status(404).json({ error: "Thread not found" });
        return;
      }
      if (channel.type !== "thread") {
        res.status(400).json({ error: "Target must be a thread" });
        return;
      }
      threadChannelId = channel.id;
    } else {
      const resolved = await channelService.resolveChannelByName(serverId, agentId, threadRef);
      if (!resolved) {
        res.status(404).json({ error: "Thread not found" });
        return;
      }
      if (resolved.type !== "thread") {
        res.status(400).json({ error: "Target must be a thread" });
        return;
      }
      threadChannelId = resolved.channelId;
    }

    await channelService.unfollowThreadForFollower("agent", agentId, threadChannelId);
    const agent = await agentService.getAgent(agentId);
    const io = req.app.get("io") as SocketServer | undefined;
    await emitThreadFollowersUpdated(io, threadChannelId);
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
    if (agent && io && agentOrchestrator) {
      // Joint threads: the local projection is not where reads resolve.
      // broadcastSystemMessage persists against the channelId it is handed,
      // so a receipt for a joint thread landed in the LOCAL projection while
      // every read (permalink, thread parent lookup) resolves to the
      // CANONICAL thread -- the message became unreachable, and the local
      // projection has no parent linkage, so Activity rendered a blank row.
      // ...ToLocalSurfaces resolves the projection first and falls back to
      // this exact call when there is no joint mapping.
      await messageService.broadcastSystemMessageToLocalSurfaces(
        io,
        agentOrchestrator,
        threadChannelId,
        `@${agent.name} stopped following this thread: ${messageService.summarizeForSystemMessage(reason)}`,
        {
          // Self-unfollow has a zero-person audience (the only actor who cares is
          // the agent that just left the thread). Record no inbox fact at all.
          // Tenny ruling (#8): skip-mode, not born-read.
          inboxFactPolicy: {
            mode: "skip",
            producer: "channel.self_unfollow_thread",
            reason: "self-unfollow has zero audience — no inbox fact",
          },
        },
      );
    }
    await recordAgentRaftAction(req, agentId, {
      title: "Unfollowed thread",
      text: [
        `target: ${threadRef}`,
        `threadChannelId: ${threadChannelId}`,
        `reason: ${reason}`,
      ].join("\n"),
    });
    sendAgentApiResponse("threadUnfollow", res, { ok: true });
  } catch (err: any) {
    sendJsonServerError(req, res, {
      error: "Failed to unfollow thread",
      code: "agent_api_thread_unfollow_failed",
      logPrefix: "[AgentAPI] Failed to unfollow thread",
      err,
    });
  }
});
registerAgentApiRoute("threadList", ...agentApiRequestValidators("threadList"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const threads = await channelService.listAgentFollowedThreads(serverId, agentId);
    sendAgentApiResponse("threadList", res, { threads });
  } catch (err: unknown) {
    sendJsonServerError(req, res, {
      error: "Failed to list followed threads",
      code: "agent_api_thread_list_failed",
      logPrefix: "[AgentAPI] Failed to list followed threads",
      err,
    });
  }
});
/**
 * GET /internal/agent-api/inbox/conversations?view=unread|mentions&before_seq=<n>&limit=<n>
 *
 * `raft inbox check`: the agent's durable unread conversations, newest activity
 * first (the agent counterpart of the human Activity panel). Not `/inbox`: the
 * managed-runner daemon proxy answers GET /inbox locally with its pending buffer.
 */
registerAgentApiRoute("inboxList", ...agentApiRequestValidators("inboxList"), async (req, res) => {
  const query = req.query as AgentApiInboxListQuery;
  const limit = Math.min(Number(query.limit ?? AGENT_API_INBOX_DEFAULT_LIMIT), AGENT_API_INBOX_MAX_LIMIT);
  try {
    const list = await channelService.listAgentInbox(req.actingAgentId!, req.serverId!, {
      view: query.view ?? "unread",
      beforeSeq: query.before_seq === undefined ? undefined : Number(query.before_seq),
      limit,
    });
    sendAgentApiResponse("inboxList", res, list);
  } catch (err: unknown) {
    if (err instanceof channelService.AgentInboxUnavailableError) {
      console.error("[AgentAPI] inbox list unavailable", serializeErrorForLog(err.cause ?? err));
      res.status(503).json({ error: "Inbox is temporarily unavailable", code: err.code });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to list inbox",
      code: "agent_api_inbox_list_failed",
      logPrefix: "[AgentAPI] Failed to list inbox",
      err,
    });
  }
});
/**
 * RFC 074 §2 personal assistant (v0) reads. The assistant calls with its own
 * credential and the Server answers AS ITS OWNER, minus DMs. Every request
 * re-resolves the owner (`resolveAssistantOwner`: flag on, live assistant,
 * owner still a member), and every way that fails gets the same
 * `403 assistant_not_enabled` so the reason is not observable. A DM, a thread
 * under a DM, and a channel the owner cannot see all get the same neutral 404.
 *
 * Neither read changes anyone's state: no read cursor, no activity, no
 * product event. Both reuse the human read paths (`getInboxItems`,
 * `filterUserAccessibleChannelIds`, `canUserAccessChannel`, `listMessages`).
 */
async function requireAssistantOwner(req: Request, res: Response): Promise<string | null> {
  const ownerUserId = await assistantService.resolveAssistantOwner(req.actingAgentId!, req.serverId!);
  if (!ownerUserId) {
    res.status(403).json({
      error: "Only a personal assistant can use this, and only where personal assistants are enabled.",
      code: assistantService.ASSISTANT_NOT_ENABLED_CODE,
    });
    return null;
  }
  return ownerUserId;
}

const ASSISTANT_CHANNEL_NOT_FOUND_BODY = { ...CHANNEL_NOT_FOUND_BODY, code: "channel_not_found" } as const;

registerAgentApiRoute("assistantOwnerInbox", ...agentApiRequestValidators("assistantOwnerInbox"), async (req, res) => {
  try {
    const serverId = req.serverId!;
    const ownerUserId = await requireAssistantOwner(req, res);
    if (!ownerUserId) return;
    const query = req.query as AgentApiAssistantOwnerInboxQuery;
    const filter = query.filter ?? "unread";
    const limit = Math.min(Number(query.limit ?? AGENT_API_ASSISTANT_INBOX_DEFAULT_LIMIT), AGENT_API_ASSISTANT_INBOX_MAX_LIMIT);
    const offset = Number(query.offset ?? 0);
    const historyCutoff = getHistoryCutoff(await getServerPlan(serverId));
    const loaded = await channelService.getInboxItems(serverId, ownerUserId, {
      filter,
      limit,
      offset,
      scope: "no_dms",
      historyCutoff,
    });
    // Re-check every row against the owner's access right now. The inbox
    // projection can trail a membership change; a borrowed read must not.
    const accessChannelIds = loaded.items.map((item) => item.kind === "thread" ? item.threadChannelId : item.channelId);
    const visible = await channelService.filterUserAccessibleChannelIds(accessChannelIds, ownerUserId, serverId);
    const items = loaded.items
      .filter((item) => !channelService.isDmScopedInboxItem(item))
      .filter((item) => visible.has(item.kind === "thread" ? item.threadChannelId : item.channelId))
      .map((item) => item.kind === "thread"
        ? {
            kind: "thread" as const,
            channelId: item.threadChannelId,
            channelName: item.parentChannelName,
            channelType: item.parentChannelType,
            parentChannelId: item.parentChannelId,
            parentMessageId: item.parentMessageId,
            unread: item.unreadCount,
            hasMention: item.hasMention,
            latestAt: item.lastActivityAt ?? null,
            latestSenderName: item.latestActivitySenderName ?? null,
            latestPreview: item.latestActivityPreview ?? null,
            firstUnreadMessageId: item.firstUnreadMessageId ?? null,
          }
        : {
            kind: "channel" as const,
            channelId: item.channelId,
            channelName: item.channelName,
            channelType: item.channelType,
            parentChannelId: null,
            parentMessageId: null,
            unread: item.unreadCount,
            hasMention: item.hasMention,
            latestAt: item.lastMessageAt ?? null,
            latestSenderName: item.lastMessageSenderName ?? null,
            latestPreview: item.lastMessagePreview ?? null,
            firstUnreadMessageId: item.firstUnreadMessageId ?? null,
          });
    recordTraceEvent("server.agent_api.assistant.owner_inbox", {
      agent_id: req.actingAgentId!,
      subject_user_id: ownerUserId,
      filter,
      row_count: loaded.items.length,
      returned_count: items.length,
    });
    sendAgentApiResponse("assistantOwnerInbox", res, {
      owner: { userId: ownerUserId },
      filter,
      items,
      hasMore: loaded.hasMore,
      nextOffset: loaded.hasMore ? offset + loaded.items.length : null,
    });
  } catch (err: unknown) {
    sendJsonServerError(req, res, {
      error: "Failed to list the owner's inbox",
      code: "agent_api_assistant_owner_inbox_failed",
      logPrefix: "[AgentAPI] Failed to list assistant owner inbox",
      err,
    });
  }
});

registerAgentApiRoute("assistantOwnerMessages", ...agentApiRequestValidators("assistantOwnerMessages"), async (req, res) => {
  try {
    const serverId = req.serverId!;
    const ownerUserId = await requireAssistantOwner(req, res);
    if (!ownerUserId) return;
    const query = req.query as AgentApiAssistantOwnerMessagesQuery;
    const channelId = query.channelId;
    const anchors = [query.before, query.after, query.around].filter((value) => value !== undefined && value.trim() !== "");
    if (anchors.length > 1) {
      res.status(400).json({ error: "Use at most one of before, after, or around.", code: "invalid_message_page_cursor" });
      return;
    }
    const channel = await channelService.getChannel(channelId);
    if (
      !channel
      || channel.serverId !== serverId
      || await assistantService.isDmScopedChannel(channel)
      || !await channelService.canUserAccessChannel(channelId, ownerUserId, serverId)
    ) {
      res.status(404).json(ASSISTANT_CHANNEL_NOT_FOUND_BODY);
      return;
    }
    const limit = Math.min(Number(query.limit ?? AGENT_API_ASSISTANT_MESSAGES_DEFAULT_LIMIT), AGENT_API_ASSISTANT_MESSAGES_MAX_LIMIT);
    const historyCutoff = getHistoryCutoff(await getServerPlan(serverId));
    const jointThreadProjection = channel.type === "thread"
      ? await channelService.getJointThreadProjectionByLocalThread(channelId, serverId)
      : null;
    const resolvedAccess = channel.type === "joint"
      ? await channelService.resolveChannelAccess({ serverId, channelId })
      : null;
    const storageChannelId = jointThreadProjection?.canonicalThreadChannelId
      ?? (resolvedAccess?.kind === "joint" ? resolvedAccess.canonicalChannelId : channelId);
    const projectionChannelId = jointThreadProjection?.localThreadChannelId
      ?? (resolvedAccess?.kind === "joint" ? resolvedAccess.localChannelId : channelId);
    const viewer = {
      attachmentCommentViewerUserId: ownerUserId,
      forwardedBundleViewerUserId: ownerUserId,
      forwardedBundleViewerServerId: serverId,
      includeSenderTimezone: true,
    };
    const anchorNotFound = () => res.status(404).json({ error: "No such message in this conversation.", code: "message_not_found" });
    const around = query.around?.trim();
    const before = query.before?.trim();
    const after = query.after?.trim();
    for (const anchor of [around, before, after]) {
      if (anchor && !isHistoryAnchorShape(anchor)) {
        res.status(400).json({ error: "An anchor is a message id, an 8-character short id, or a seq.", code: "invalid_message_page_cursor" });
        return;
      }
    }
    let rawMsgs;
    let hasOlder = false;
    let hasNewer = false;
    if (around) {
      const resolution = await messageService.resolveMessageSeqAnchor(storageChannelId, around, "around", historyCutoff);
      if (!resolution.ok) {
        anchorNotFound();
        return;
      }
      const beforeCount = Math.floor((limit - 1) / 2);
      const context = await messageService.getMessageContextBySeq(
        storageChannelId,
        resolution.seq,
        beforeCount,
        limit - beforeCount - 1,
        historyCutoff,
        viewer,
      );
      if (!context || context.channelId !== storageChannelId) {
        anchorNotFound();
        return;
      }
      rawMsgs = context.messages;
      hasOlder = context.hasOlder;
      hasNewer = context.hasNewer;
    } else {
      const anchor = before ?? after;
      const resolution = anchor
        ? await messageService.resolveMessageSeqAnchor(storageChannelId, anchor, "pagination", historyCutoff)
        : undefined;
      if (resolution && !resolution.ok) {
        anchorNotFound();
        return;
      }
      const beforeSeq = before && resolution?.ok ? resolution.seq : undefined;
      const afterSeq = after && resolution?.ok ? resolution.seq : undefined;
      rawMsgs = await messageService.listMessages(storageChannelId, limit + 1, beforeSeq, afterSeq, historyCutoff, viewer);
      const probed = paginateHistoryProbe(
        rawMsgs,
        limit,
        afterSeq !== undefined ? "after" : beforeSeq !== undefined ? "before" : "latest",
      );
      rawMsgs = probed.messages;
      hasOlder = probed.hasOlder;
      hasNewer = probed.hasNewer;
    }
    const pageMsgs = jointThreadProjection || resolvedAccess?.kind === "joint"
      ? await messageService.projectJointMessagesToLocalChannel(rawMsgs, projectionChannelId, serverId)
      : rawMsgs;
    const renderedContents = await agentPermalinkRenderService.renderAgentReadablePermalinksInTexts(
      pageMsgs.map((message) => message.content),
      serverId,
    );
    recordTraceEvent("server.agent_api.assistant.owner_messages", {
      agent_id: req.actingAgentId!,
      subject_user_id: ownerUserId,
      channel_id: channelId,
      channel_type: channel.type,
      message_count: pageMsgs.length,
    });
    sendAgentApiResponse("assistantOwnerMessages", res, {
      owner: { userId: ownerUserId },
      channelId,
      messages: pageMsgs.map((message, index: number) =>
        toAgentApiMessageEnvelope(
          message,
          messageService.appendAgentFacingForwardedSnapshot(
            renderAgentCommentScopedContent(renderedContents[index] ?? "", message.commentRef),
            message.actionMetadata,
          ),
          req.actingAgentId!,
        )
      ),
      hasMore: hasOlder || hasNewer,
      hasOlder,
      hasNewer,
    });
  } catch (err: unknown) {
    sendJsonServerError(req, res, {
      error: "Failed to read the owner's messages",
      code: "agent_api_assistant_owner_messages_failed",
      logPrefix: "[AgentAPI] Failed to read assistant owner messages",
      err,
    });
  }
});

registerAgentApiRoute("profileShow", async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const query = validateAgentApiQuery("profileShow", req, res);
    if (!query) return;
    // Both reading your own profile and looking up another member's need
    // only `read` (the route capability): a target lookup returns only
    // profiles this agent can already see (visibility is enforced by
    // resolveProfileViewForAgent), and member listing is already `read`.
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
    const resolved = await resolveProfileViewForAgent(serverId, agentId, query.target, agentOrchestrator);
    if (!resolved.ok) {
      res.status(resolved.status).json({ error: resolved.error });
      return;
    }
    sendAgentApiResponse("profileShow", res, resolved.profile);
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load profile",
      logPrefix: "internal.agent-api.profile-show error:",
      err,
    });
  }
});
registerAgentApiRoute("profileUpdate", ...agentApiRequestValidators("profileUpdate"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const body = req.body as AgentApiRequestBodyByRoute["profileUpdate"];
    const hasAvatarUrl = Object.prototype.hasOwnProperty.call(body, "avatarUrl");
    const hasDisplayName = Object.prototype.hasOwnProperty.call(body, "displayName");
    const hasDescription = Object.prototype.hasOwnProperty.call(body, "description");
    if (!hasAvatarUrl && !hasDisplayName && !hasDescription) {
      res.status(400).json({
        error: "Provide at least one of avatarUrl, displayName, or description",
        errorCode: "PROFILE_UPDATE_NO_FIELDS",
      });
      return;
    }
    // A hosted agent's description IS its instructions on the provider
    // (providerAgentInstructions), and the next human profile edit pushes the
    // current description there. Letting the agent write it would let it
    // rewrite its own instructions, so only a human may change it.
    if (hasDescription && await getHostedRuntimeSummary(agentId)) {
      res.status(403).json({
        error: "A cloud agent's description is its instructions and only a human can change it",
        code: "hosted_agent_description_human_only",
        suggestedNextAction: "Your description is your instructions, so only a human can change it: ask one to edit it in your Profile tab in Raft. Nothing was changed; to update your display name or avatar, retry without the description.",
      });
      return;
    }

    const fields: { avatarUrl?: string; displayName?: string; description?: string } = {};
    if (hasAvatarUrl) {
      const value = body.avatarUrl;
      if (typeof value !== "string") {
        res.status(400).json({ error: "avatarUrl must be a string" });
        return;
      }
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        res.status(400).json({ error: "avatarUrl must not be empty" });
        return;
      }
      if (!trimmed.startsWith("pixel:")) {
        res.status(400).json({
          error: "avatarUrl must be a pixel: URL",
          errorCode: "PROFILE_AVATAR_URL_BAD_FORMAT",
        });
        return;
      }
      fields.avatarUrl = trimmed;
    }
    if (hasDisplayName) {
      const value = body.displayName;
      if (typeof value !== "string") {
        res.status(400).json({ error: "displayName must be a string" });
        return;
      }
      const trimmed = value.trim();
      if (trimmed.length === 0) {
        res.status(400).json({ error: "displayName must not be empty" });
        return;
      }
      if (trimmed.length > MAX_AGENT_PROFILE_DISPLAY_NAME_LENGTH) {
        res.status(400).json({
          error: `displayName must be at most ${MAX_AGENT_PROFILE_DISPLAY_NAME_LENGTH} characters`,
        });
        return;
      }
      fields.displayName = trimmed;
    }
    if (hasDescription) {
      const value = body.description;
      if (typeof value !== "string") {
        res.status(400).json({ error: "description must be a string" });
        return;
      }
      if (value.length === 0) {
        res.status(400).json({ error: "description must not be empty" });
        return;
      }
      if (value.length > MAX_AGENT_PROFILE_DESCRIPTION_LENGTH) {
        res.status(400).json({
          error: `description must be at most ${MAX_AGENT_PROFILE_DESCRIPTION_LENGTH} characters`,
        });
        return;
      }
      fields.description = value;
    }

    const updated = await agentService.updateAgent(agentId, fields);
    if (!updated) {
      res.status(404).json({ error: "Agent not found" });
      return;
    }
    broadcastAgentUpdated(req, updated.serverId, agentId);
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
    agentOrchestrator?.evictCache(agentId);
    const profile = await buildAgentProfileView(agentId, agentId, agentOrchestrator);
    if (!profile) {
      res.status(404).json({ error: "Agent not found" });
      return;
    }
    sendAgentApiResponse("profileUpdate", res, profile);
    recordRaftCliActivity(req, agentId, {
      command: "profile.update",
      summary: "Updated profile",
      target: `@${profile.name}`,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to update profile",
      logPrefix: "internal.agent-api.profile-update error:",
      err,
    });
  }
});
registerAgentApiRoute("profileAvatarUpdate", async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const agent = await agentService.getAgent(agentId);
    if (!agent || agent.serverId !== serverId) {
      res.status(404).json({ error: "Agent not found" });
      return;
    }
    const uploaded = await runSingleAvatarUpload(profileAvatarUpload, req);
    if (!uploaded) {
      res.status(400).json({ error: "No avatar file provided" });
      return;
    }

    const avatarUrl = await storeAgentAvatar(serverId, agent.avatarUrl, uploaded.buffer);
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
    agentOrchestrator?.evictCache(agentId);
    await agentService.updateAgent(agentId, { avatarUrl });
    broadcastAgentUpdated(req, serverId, agentId);

    const profile = await buildAgentProfileView(agentId, agentId, agentOrchestrator);
    if (!profile) {
      res.status(404).json({ error: "Agent not found" });
      return;
    }

    sendAgentApiResponse("profileAvatarUpdate", res, profile);
    recordRaftCliActivity(req, agentId, {
      command: "profile.avatar.update",
      summary: "Updated profile avatar",
      target: `@${profile.name}`,
    });
  } catch (err) {
    if (err instanceof multer.MulterError && err.code === "LIMIT_FILE_SIZE") {
      res.status(400).json({
        error: PROFILE_AVATAR_TOO_LARGE_MESSAGE,
        errorCode: "PROFILE_AVATAR_TOO_LARGE",
        maxBytes: MAX_PROFILE_AVATAR_BYTES,
      });
      return;
    }
    if (err instanceof Error && err.message.includes(PROFILE_AVATAR_BAD_FORMAT_MESSAGE)) {
      res.status(400).json({
        error: PROFILE_AVATAR_BAD_FORMAT_MESSAGE,
        errorCode: "PROFILE_AVATAR_BAD_FORMAT",
      });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to update avatar",
      logPrefix: "internal.agent-api.profile-avatar-update error:",
      err,
    });
  }
});
registerAgentApiRoute("integrationList", async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const [clients, overview] = await Promise.all([
      oauthService.listAgentAvailableOAuthClients(serverId),
      oauthService.getAgentIntegrationsOverview(agentId),
    ]);
    const activeLogins = overview
      .filter((item) => item.type === "active" && !item.revokedAt)
      .map((item) => {
        const agentManifest = oauthService.resolveAgentManifest({
          agentManifestUrl: item.clientAgentManifestUrl,
          homepageUrl: item.clientHomepageUrl,
          returnUrl: item.clientReturnUrl,
        });
        return {
          id: item.id,
          serviceId: item.clientId,
          clientId: item.clientKey,
          name: item.clientName,
          description: item.clientDescription ?? null,
          homepageUrl: item.clientHomepageUrl ?? null,
          returnUrl: item.clientReturnUrl ?? null,
          agentManifestUrl: agentManifest.url,
          agentManifestUrlSource: agentManifest.source ?? null,
          scopes: item.scopes,
          createdAt: item.createdAt,
        };
      });

    sendAgentApiResponse("integrationList", res, {
      services: await Promise.all(clients.map((client) => serializeOAuthClientForAgentApi(client, serverId))),
      activeLogins,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to list agent integrations",
      logPrefix: "internal.agent-api.integrations-list error:",
      err,
    });
  }
});
registerAgentApiRoute(
  "integrationMarketplaceSearch",
  ...agentApiRequestValidators("integrationMarketplaceSearch"),
  async (req, res) => {
    try {
      const agentId = req.actingAgentId!;
      const serverId = req.serverId!;
      const query = req.query as AgentApiRequestQueryByRoute["integrationMarketplaceSearch"];
      const limit = query.limit ? Number(query.limit) : 20;
      if (!Number.isInteger(limit) || limit < 1 || limit > 50) {
        res.status(400).json({ error: "limit must be an integer from 1 to 50" });
        return;
      }
      const normalizedQuery = query.query?.trim() || null;
      const apps = await oauthService.searchPublicMarketplaceOAuthClients({
        serverId,
        query: normalizedQuery,
        limit,
      });
      sendAgentApiResponse("integrationMarketplaceSearch", res, {
        surface: "public_marketplace",
        metadataTrust: "untrusted_app_supplied",
        query: normalizedQuery,
        limit,
        apps: await Promise.all(apps.map(serializeMarketplaceOAuthClientForAgentApi)),
      });
      recordRaftCliActivity(req, agentId, {
        command: "integration.marketplace",
        summary: normalizedQuery ? "Searched public Marketplace apps" : "Listed public Marketplace apps",
        target: `${apps.length} result${apps.length === 1 ? "" : "s"}`,
      });
    } catch (err) {
      sendJsonServerError(req, res, {
        error: "Failed to search public Marketplace apps",
        logPrefix: "internal.agent-api.integrations-marketplace error:",
        err,
      });
    }
  },
);
registerAgentApiRoute("integrationToken", ...agentApiRequestValidators("integrationToken"), async (req, res) => {
  res.setHeader("Cache-Control", "no-store");
  try {
    const body = req.body as AgentApiRequestBodyByRoute["integrationToken"];
    const result = await issueAgentAudienceJwt({ serverId: req.serverId!, agentId: req.actingAgentId!, service: body.service });
    sendAgentApiResponse("integrationToken", res, result);
  } catch (error) {
    if (error instanceof AgentAudienceJwtError) {
      res.status(403).json({
        error: error.code, errorCode: error.code,
      });
      return;
    }
    sendJsonServerError(req, res, { error: "Agent JWT issuance failed", logPrefix: "internal.agent-api.integration-token error:", err: error });
  }
});
registerAgentApiRoute("integrationLogin", ...agentApiRequestValidators("integrationLogin"), async (req, res) => {
  let phase = "agent_lookup";
  let appType: oauthService.OAuthClientAppType | "unresolved" = "unresolved";
  let scopeCount = 0;
  const defaultScopesUsed = (
    req.body as AgentApiRequestBodyByRoute["integrationLogin"] | undefined
  )?.scopes == null;
  const closeTrace = (input: {
    outcome: "success" | "rejected" | "error";
    reason: string;
    httpStatus: number;
    grantStatus?: "created" | "reused" | "pending";
  }) => {
    addTraceEvent("agent_integration_login.request.closed", {
      event_kind: "agent_integration_login",
      outcome: input.outcome,
      reason: input.reason,
      phase,
      http_status: input.httpStatus,
      app_type: appType,
      requested_scope_count: scopeCount,
      default_scopes_used: defaultScopesUsed,
      ...(input.grantStatus ? { grant_status: input.grantStatus } : {}),
    });
  };
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const body = req.body as AgentApiRequestBodyByRoute["integrationLogin"];
    const agent = await agentService.getAgent(agentId);
    if (!agent || agent.serverId !== serverId) {
      closeTrace({ outcome: "rejected", reason: "agent_not_found", httpStatus: 404 });
      res.status(404).json({ error: "Agent not found" });
      return;
    }
    phase = "server_lookup";
    const server = await serverService.getServer(serverId);
    if (!server) {
      closeTrace({ outcome: "rejected", reason: "server_not_found", httpStatus: 404 });
      res.status(404).json({ error: "Server not found" });
      return;
    }

    phase = "service_list";
    const clients = await oauthService.listAgentAvailableOAuthClients(serverId);
    phase = "service_resolve";
    const resolved = resolveAgentLoginClient(clients, body.service);
    if (!resolved.ok) {
      if (resolved.status !== 404) {
        closeTrace({ outcome: "rejected", reason: "service_resolution_failed", httpStatus: resolved.status });
        res.status(resolved.status).json({ error: resolved.error });
        return;
      }

      phase = "marketplace_resolve";
      const publicClients = await oauthService.listPublicMarketplaceOAuthClients();
      const marketplaceResolved = resolveAgentLoginClient(publicClients, body.service);
      if (!marketplaceResolved.ok) {
        closeTrace({ outcome: "rejected", reason: "service_resolution_failed", httpStatus: marketplaceResolved.status });
        res.status(marketplaceResolved.status).json({ error: marketplaceResolved.error });
        return;
      }
      appType = marketplaceResolved.client.appType;
      phase = "scope_normalize";
      const scopes = oauthService.normalizeAgentRequestedScopes(
        normalizeAgentLoginScopes(body.scopes, marketplaceResolved.client),
        marketplaceResolved.client,
      );
      scopeCount = scopes.length;
      const response: AgentApiResponseByRoute["integrationLogin"] = {
        status: "install_required",
        nextAction: "install_from_marketplace",
        service: await serializeOAuthClientForAgentApi(marketplaceResolved.client, serverId),
        scopes,
        installation: {
          serverSlug: server.slug,
          serverName: server.name,
          marketplaceUrl: `${getAppUrl()}/s/${encodeURIComponent(server.slug)}/settings/applications?marketplace_app=${encodeURIComponent(marketplaceResolved.client.id)}`,
          target: null,
          actionCardMessageId: null,
        },
      };
      const target = typeof body.target === "string" ? body.target.trim() : "";
      if (target) {
        phase = "install_target";
        const resolvedTarget = await channelService.resolveChannelByName(serverId, agentId, target);
        if (!resolvedTarget) {
          closeTrace({ outcome: "rejected", reason: "install_target_not_found", httpStatus: 404 });
          res.status(404).json({ error: "Install target not found or not visible to this agent" });
          return;
        }
        phase = "install_card";
        const agentName = agent.displayName ?? agent.name;
        const clientNameBinding = actionCardsService.bindMarketplaceAppName(marketplaceResolved.client.name);
        const card = await actionCardsService.prepareActionCard({
          serverId,
          requesterAgentId: agentId,
          targetChannelId: resolvedTarget.channelId,
          action: {
            type: "integration:install_marketplace_app",
            clientId: marketplaceResolved.client.id,
            clientKey: marketplaceResolved.client.clientId,
            ...clientNameBinding,
            agentId,
            agentName,
            scopes,
            draftHint: `${agentName} requested ${clientNameBinding.clientName}, which is public in the Raft Marketplace but is not installed on this Server. Installing is a Server owner/admin action; the Agent cannot install it automatically.`,
          },
          io: (req.app.get("io") ?? null) as SocketServer | null,
        });
        response.installation = {
          ...response.installation!,
          target,
          actionCardMessageId: card.messageId,
        };
      }
      phase = "response";
      sendAgentApiResponse("integrationLogin", res, response);
      closeTrace({ outcome: "success", reason: "install_required", httpStatus: 200 });
      return;
    }
    appType = resolved.client.appType;

    phase = "scope_normalize";
    const scopes = normalizeAgentLoginScopes(body.scopes, resolved.client);
    scopeCount = scopes.length;
    phase = "access_request";
    const requested = await oauthService.requestAgentAccess({
      clientId: resolved.client.id,
      serverSlug: server.slug,
      agentName: agent.name,
      scopes,
      initiatedByAgent: true,
    });

    if (requested.grantStatus === "pending") {
      const response: AgentApiResponseByRoute["integrationLogin"] = {
        status: "approval_required",
        service: await serializeOAuthClientForAgentApi(resolved.client, serverId),
        scopes: requested.request.scopes ?? [],
        requestId: requested.request.id,
        approval: {
          requestId: requested.request.id,
          target: null,
          actionCardMessageId: null,
        },
      };

      const target = typeof body.target === "string" ? body.target.trim() : "";
      if (target) {
        phase = "approval_target";
        const resolvedTarget = await channelService.resolveChannelByName(serverId, agentId, target);
        if (!resolvedTarget) {
          closeTrace({
            outcome: "rejected",
            reason: "approval_target_not_found",
            httpStatus: 404,
            grantStatus: "pending",
          });
          res.status(404).json({ error: "Approval target not found or not visible to this agent" });
          return;
        }
        phase = "approval_card";
        const card = await actionCardsService.prepareActionCard({
          serverId,
          requesterAgentId: agentId,
          targetChannelId: resolvedTarget.channelId,
          action: {
            type: "integration:approve_agent_login",
            requestId: requested.request.id,
            agentId,
            agentName: agent.displayName ?? agent.name,
            clientId: resolved.client.id,
            clientKey: resolved.client.clientId,
            clientName: resolved.client.name,
            scopes: requested.request.scopes ?? [],
            draftHint:
              `${agent.displayName ?? agent.name} is requesting human approval to use ${resolved.client.name}. Server-installed apps do not need this approval; Marketplace apps do.`,
          },
          io: (req.app.get("io") ?? null) as SocketServer | null,
        });
        response.approval = {
          requestId: requested.request.id,
          target,
          actionCardMessageId: card.messageId,
        };
      }

      phase = "response";
      sendAgentApiResponse("integrationLogin", res, response);
      closeTrace({
        outcome: "success",
        reason: "approval_required",
        httpStatus: 200,
        grantStatus: "pending",
      });
      return;
    }

    phase = "response";
    const responseStatus = requested.grantStatus === "reused" ? "already_logged_in" : "logged_in";
    sendAgentApiResponse("integrationLogin", res, {
      status: responseStatus,
      service: await serializeOAuthClientForAgentApi(resolved.client, serverId),
      scopes: requested.request.scopes ?? [],
      requestId: requested.request.id,
    });
    closeTrace({
      outcome: "success",
      reason: responseStatus,
      httpStatus: 200,
      grantStatus: requested.grantStatus,
    });
  } catch (err) {
    if (err instanceof DmTargetResolutionError) {
      closeTrace({ outcome: "rejected", reason: err.code, httpStatus: err.status });
      res.status(err.status).json(err.toResponseBody());
      return;
    }
    const message = err instanceof Error ? err.message : "Failed to provision agent login";
    if (
      message === "scopes must be an array" ||
      message === "scopes must contain strings" ||
      message === "scope values must be non-empty" ||
      message === "at least one scope is required" ||
      message === "invalid_scope"
    ) {
      const error = message === "invalid_scope"
        ? "Requested scopes are not allowed for this service; use --scope with a scope declared by the service"
        : message;
      closeTrace({ outcome: "rejected", reason: message, httpStatus: 400 });
      res.status(400).json({
        error,
        ...(message === "invalid_scope" ? { errorCode: "INVALID_SCOPE" } : {}),
      });
      return;
    }
    if (message.includes("not found")) {
      closeTrace({ outcome: "rejected", reason: "not_found", httpStatus: 404 });
      res.status(404).json({ error: message });
      return;
    }
    closeTrace({
      outcome: "error",
      reason: "unexpected_error",
      httpStatus: 500,
    });
    console.error("internal.agent-api.integrations-login error:", serializeErrorForLog(err));
    res.status(500).json({ error: "Failed to provision agent login" });
  }
});
registerAgentApiRoute("integrationAppPrepare", ...agentApiRequestValidators("integrationAppPrepare"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const body = req.body as AgentApiRequestBodyByRoute["integrationAppPrepare"];
    if (body.mode === "update") {
      res.status(410).json({
        error: "App registration update cards are disabled; the app owner must use the direct integration app update command",
        errorCode: "LEGACY_APP_UPDATE_DISABLED",
      });
      return;
    }
    const target = optionalIntegrationString(body.target);
    if (!target) {
      res.status(400).json({ error: "target is required" });
      return;
    }
    const clientKey = optionalIntegrationString(body.clientKey);
    const resolvedTarget = await channelService.resolveChannelByName(serverId, agentId, target);
    if (!resolvedTarget) {
      res.status(404).json({ error: "Action-card target not found or not visible to this agent" });
      return;
    }

    const scopes = normalizeIntegrationAppPrepareScopes(body.scopes);
    if (scopes.some((scope) => !isRaftOAuthScope(scope))) {
      res.status(400).json({ error: "invalid_scope", errorCode: "INVALID_SCOPE" });
      return;
    }
    const rawCategory = optionalIntegrationString(body.category);
    const category = rawCategory ? canonicalizeOAuthClientCategory(rawCategory) : undefined;
    if (rawCategory && !category) {
      res.status(400).json({ error: "invalid category", errorCode: "INVALID_CATEGORY" });
      return;
    }
    const unsafeDemoUrlOverride = body.unsafeDemoUrlOverride === true;
    const draftHint = optionalIntegrationString(body.draftHint);
    const homepageUrl = optionalIntegrationString(body.homepageUrl);
    const returnUrl = optionalIntegrationString(body.returnUrl);
    const agentManifestUrl = optionalIntegrationString(body.agentManifestUrl);
    const setupUrl = optionalIntegrationString(body.setupUrl);
    const setupDescription = optionalIntegrationString(body.setupDescription);

    const name = optionalIntegrationString(body.name);
    if (!name) {
      res.status(400).json({ error: "name is required for register" });
      return;
    }
    if (!returnUrl) {
      res.status(400).json({ error: "returnUrl is required for register" });
      return;
    }
    const action: Extract<ActionCardAction, { type: "integration:register_app" }> = {
      type: "integration:register_app",
      name,
      ...(clientKey ? { clientKey } : {}),
      description: optionalIntegrationString(body.description),
      category: category ?? undefined,
      homepageUrl,
      returnUrl,
      agentManifestUrl,
      setupUrl,
      setupDescription,
      scopes,
      unsafeDemoUrlOverride,
      draftHint: draftHint ?? `Agent prepared a Login with Raft app registration for ${name}. The requester becomes the app owner after server commit and receives the initial secret through a private transient handoff; only a lost handoff should be recovered with rotate-secret --output <new-private-path>. No secret is stored in this card.`,
    };

    const card = await actionCardsService.prepareActionCard({
      serverId,
      requesterAgentId: agentId,
      targetChannelId: resolvedTarget.channelId,
      action,
      io: (req.app.get("io") ?? null) as SocketServer | null,
    });

    res.status(201);
    sendAgentApiResponse("integrationAppPrepare", res, {
      status: "prepared",
      mode: body.mode,
      target,
      actionCardMessageId: card.messageId,
      action,
    });
    recordRaftCliActivity(req, agentId, {
      command: "integration.app.prepare",
      summary: `Prepared integration app ${body.mode} card`,
      target,
      correlationId: card.messageId,
    });
  } catch (err) {
    if (err instanceof actionCardsService.ActionCardError) {
      res.status(err.status).json({ error: err.message, errorCode: err.code });
      return;
    }
    if (err instanceof Error && /scope/.test(err.message)) {
      res.status(400).json({ error: err.message });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to prepare integration app registration",
      logPrefix: "internal.agent-api.integrations-app-prepare error:",
      err,
    });
  }
});
registerAgentApiRoute("integrationAppRotateSecret", ...agentApiRequestValidators("integrationAppRotateSecret"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const body = req.body as AgentApiRequestBodyByRoute["integrationAppRotateSecret"];
    const clientKey = optionalIntegrationString(body.clientKey);
    if (!clientKey) {
      res.status(400).json({ error: "clientKey is required" });
      return;
    }

    const rotated = await oauthService.rotateClientSecretForAgent({
      serverId,
      clientKey,
      actorAgentId: agentId,
    });

    if (rotated.status === "not_found") {
      // Unknown and cross-server clients remain non-enumerating.
      res.status(404).json({ error: "Integration app not found" });
      return;
    }
    if (rotated.status === "owner_required") {
      res.status(403).json({
        error: "Only this app's owner, delegated rotate maintainer, or a current server admin can rotate its secret",
        errorCode: "APP_MANAGE_AUTHORITY_REQUIRED",
      });
      return;
    }

    sendAgentApiResponse("integrationAppRotateSecret", res, {
      clientId: rotated.value.clientId,
      clientKey: rotated.value.clientKey,
      clientName: rotated.value.clientName,
      clientSecret: rotated.value.clientSecret,
    });
    // Activity log carries only the client key — never the rotated plaintext.
    recordRaftCliActivity(req, agentId, {
      command: "integration.app.rotate-secret",
      summary: `Rotated integration app secret for ${rotated.value.clientKey}`,
      correlationId: rotated.value.clientId,
    });
  } catch (err) {
    // Never surface the secret in logs: the catch path only ran if rotation
    // threw before returning, so no plaintext is in scope here regardless.
    sendJsonServerError(req, res, {
      error: "Failed to rotate integration app secret",
      logPrefix: "internal.agent-api.integrations-app-rotate-secret error:",
      err,
    });
  }
});
registerAgentApiRoute("integrationAppTransferOwner", ...agentApiRequestValidators("integrationAppTransferOwner"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const body = req.body as AgentApiRequestBodyByRoute["integrationAppTransferOwner"];
    const clientKey = optionalIntegrationString(body.clientKey);
    const targetAgentName = optionalIntegrationString(body.targetAgent)?.replace(/^@/, "");
    if (!clientKey || !targetAgentName) {
      res.status(400).json({ error: "clientKey and targetAgent are required" });
      return;
    }
    const targetAgentId = await channelService.resolveAgentByName(serverId, targetAgentName);
    if (!targetAgentId) {
      res.status(404).json({ error: "Target agent not found" });
      return;
    }

    const transferred = await oauthService.transferClientOwnershipForAgent({
      serverId,
      clientKey,
      actorAgentId: agentId,
      targetAgentId,
    });
    if (transferred.status === "not_found") {
      res.status(404).json({ error: "Integration app not found" });
      return;
    }
    if (transferred.status === "owner_required") {
      res.status(403).json({
        error: "Only this app's owner or a current server admin can transfer it",
        errorCode: "APP_MANAGE_AUTHORITY_REQUIRED",
      });
      return;
    }
    if (transferred.status === "target_not_found") {
      res.status(404).json({ error: "Target agent not found" });
      return;
    }

    sendAgentApiResponse("integrationAppTransferOwner", res, {
      ...transferred.value,
      ownerAgentName: targetAgentName,
    });
    recordRaftCliActivity(req, agentId, {
      command: "integration.app.transfer-owner",
      summary: transferred.value.ownershipOutcome === "transferred"
        ? `Transferred integration app ${transferred.value.clientKey} to @${targetAgentName}`
        : `Confirmed integration app ${transferred.value.clientKey} is already owned by @${targetAgentName}`,
      correlationId: transferred.value.clientId,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to transfer integration app ownership",
      logPrefix: "internal.agent-api.integrations-app-transfer-owner error:",
      err,
    });
  }
});
registerAgentApiRoute("integrationAppUpdate", ...agentApiRequestValidators("integrationAppUpdate"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const body = req.body as AgentApiRequestBodyByRoute["integrationAppUpdate"];
    const clientKey = optionalIntegrationString(body.clientKey);
    if (!clientKey) {
      res.status(400).json({ error: "clientKey is required" });
      return;
    }
    const updatedFields = ["name", "description", "whenToUse", "category", "homepageUrl", "returnUrl", "agentManifestUrl", "setupUrl", "setupDescription", "scopes"]
      .filter((field) => body[field as keyof typeof body] !== undefined);
    if (updatedFields.length === 0) {
      res.status(400).json({ error: "At least one app field is required" });
      return;
    }
    if (body.returnUrl !== undefined && !body.returnUrl.trim()) {
      res.status(400).json({
        error: "returnUrl cannot be empty; OAuth apps must keep a registered callback URL",
        errorCode: "RETURN_URL_REQUIRED",
      });
      return;
    }
    const unsafeDemoUrlOverride = body.unsafeDemoUrlOverride === true;
    const updated = await oauthService.updateOAuthClientForAgent({
      serverId,
      clientKey,
      actorAgentId: agentId,
      name: body.name,
      description: body.description,
      whenToUse: body.whenToUse,
      category: body.category,
      homepageUrl: body.homepageUrl === undefined
        ? undefined
        : actionCardsService.validateIntegrationUrl(body.homepageUrl, "homepageUrl", unsafeDemoUrlOverride),
      returnUrl: body.returnUrl === undefined
        ? undefined
        : actionCardsService.validateIntegrationUrl(body.returnUrl, "returnUrl", unsafeDemoUrlOverride),
      agentManifestUrl: body.agentManifestUrl === undefined
        ? undefined
        : actionCardsService.validateIntegrationUrl(body.agentManifestUrl, "agentManifestUrl", unsafeDemoUrlOverride),
      setupUrl: body.setupUrl === undefined ? undefined : actionCardsService.validateIntegrationUrl(body.setupUrl, "setupUrl", unsafeDemoUrlOverride),
      setupDescription: body.setupDescription,
      allowedScopes: body.scopes === undefined
        ? undefined
        : (() => {
          const scopes = normalizeIntegrationAppPrepareScopes(body.scopes);
          return scopes.length > 0 ? scopes : null;
        })(),
    });
    if (updated.status === "not_found") {
      res.status(404).json({ error: "Integration app not found" });
      return;
    }
    if (updated.status === "owner_required") {
      res.status(403).json({
        error: "Only this app's owner or a current server admin can update it",
        errorCode: "APP_MANAGE_AUTHORITY_REQUIRED",
      });
      return;
    }
    if (updated.status === "invalid_return_url") {
      res.status(400).json({
        error: "returnUrl cannot be empty; OAuth apps must keep a registered callback URL",
        errorCode: "RETURN_URL_REQUIRED",
      });
      return;
    }
    sendAgentApiResponse("integrationAppUpdate", res, {
      clientId: updated.value.id,
      clientKey: updated.value.clientId,
      clientName: updated.value.name,
      updatedFields,
    });
    recordRaftCliActivity(req, agentId, {
      command: "integration.app.update",
      summary: `Updated integration app ${updated.value.clientId}: ${updatedFields.join(", ")}`,
      correlationId: updated.value.id,
    });
  } catch (err) {
    if (err instanceof Error) {
      if (err.message.includes("category")) {
        res.status(400).json({ error: err.message, errorCode: "INVALID_CATEGORY" });
        return;
      }
      if (
        err.message.includes("required") ||
        err.message.includes("agentManifestUrl") || err.message.includes("setup") ||
        err.message.includes("scope") ||
        err.message.includes("whenToUse")
      ) {
        res.status(400).json({ error: err.message, errorCode: "INVALID_APP_METADATA" });
        return;
      }
    }
    if (err instanceof actionCardsService.ActionCardError) {
      res.status(err.status).json({ error: err.message, errorCode: err.code });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to update integration app",
      logPrefix: "internal.agent-api.integrations-app-update error:",
      err,
    });
  }
});
registerAgentApiRoute("integrationAppManage", ...agentApiRequestValidators("integrationAppManage"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const body = req.body as AgentApiRequestBodyByRoute["integrationAppManage"];
    const resolved = await oauthService.resolveOAuthClientForAgentMutation({
      serverId,
      clientKey: body.clientKey,
      actorAgentId: agentId,
    });
    if (resolved.status === "not_found") {
      res.status(404).json({ error: "Integration app not found" });
      return;
    }
    if (resolved.status === "owner_required") {
      res.status(403).json({
        error: "Only this app's owner or a current server admin can manage it",
        errorCode: "APP_MANAGE_AUTHORITY_REQUIRED",
      });
      return;
    }
    const client = resolved.value.client;
    let publishStatus: string | null | undefined;
    let logoUrl: string | null | undefined;
    let shareUrl: string | null | undefined;
    let link: {
      id: string;
      expiresAt: string | null;
      revokedAt: string | null;
      lastUsedAt: string | null;
      createdAt: string;
      updatedAt: string;
    } | null | undefined;

    if (body.action === "share_link_get") {
      const current = await oauthService.getOAuthClientShareLink({
        serverId,
        clientId: client.id,
        actorAgentId: agentId,
      });
      link = current ? {
        ...current,
        expiresAt: current.expiresAt?.toISOString() ?? null,
        revokedAt: current.revokedAt?.toISOString() ?? null,
        lastUsedAt: current.lastUsedAt?.toISOString() ?? null,
        createdAt: current.createdAt.toISOString(),
        updatedAt: current.updatedAt.toISOString(),
      } : null;
    } else if (body.action === "share_link_create") {
      const created = await oauthService.createOAuthClientShareLink({
        serverId,
        clientId: client.id,
        createdByAgentId: agentId,
        expiresInDays: body.expiresInDays,
      });
      if (!created) {
        res.status(409).json({ error: "Integration app cannot create a private share link in its current state" });
        return;
      }
      link = {
        ...created.link,
        expiresAt: created.link.expiresAt?.toISOString() ?? null,
        revokedAt: created.link.revokedAt?.toISOString() ?? null,
        lastUsedAt: created.link.lastUsedAt?.toISOString() ?? null,
        createdAt: created.link.createdAt.toISOString(),
        updatedAt: created.link.updatedAt.toISOString(),
      };
      shareUrl = buildIntegrationInviteUrl(getAppUrl(), created.token);
    } else if (body.action === "share_link_revoke") {
      const revoked = await oauthService.revokeOAuthClientShareLink({
        serverId,
        clientId: client.id,
        revokedByAgentId: agentId,
      });
      if (!revoked) {
        res.status(404).json({ error: "Integration app share link not found" });
        return;
      }
      link = {
        ...revoked,
        expiresAt: revoked.expiresAt?.toISOString() ?? null,
        revokedAt: revoked.revokedAt?.toISOString() ?? null,
        lastUsedAt: revoked.lastUsedAt?.toISOString() ?? null,
        createdAt: revoked.createdAt.toISOString(),
        updatedAt: revoked.updatedAt.toISOString(),
      };
    } else if (body.action === "request_publish") {
      const updated = await oauthService.requestOAuthClientPublish({
        serverId,
        clientId: client.id,
        requestedByAgentId: agentId,
      });
      if (!updated) {
        res.status(409).json({ error: "Integration app cannot request Marketplace review in its current state" });
        return;
      }
      publishStatus = updated.publishStatus;
    } else if (body.action === "request_unpublish") {
      const updated = await oauthService.requestOAuthClientUnpublish({
        serverId,
        clientId: client.id,
        requestedByAgentId: agentId,
      });
      if (!updated) {
        res.status(409).json({ error: "Integration app is not currently published" });
        return;
      }
      publishStatus = updated.publishStatus;
    } else if (body.action === "clear_logo") {
      const updated = await oauthService.clearOAuthClientLogo({
        serverId,
        clientId: client.id,
        actorAgentId: agentId,
      });
      if (!updated) {
        res.status(404).json({ error: "Integration app not found" });
        return;
      }
      logoUrl = updated.logoUrl;
    } else {
      const deleted = await oauthService.deleteOAuthClient({
        serverId,
        clientId: client.id,
        deletedByAgentId: agentId,
      });
      if (!deleted) {
        res.status(409).json({ error: "Published integration apps must request Marketplace removal before deletion" });
        return;
      }
    }

    sendAgentApiResponse("integrationAppManage", res, {
      action: body.action,
      clientId: client.id,
      clientKey: client.clientId,
      clientName: client.name,
      publishStatus,
      logoUrl,
      shareUrl,
      link,
    });
    recordRaftCliActivity(req, agentId, {
      command: `integration.app.${body.action.replaceAll("_", "-")}`,
      summary: `Managed integration app ${client.clientId}: ${body.action}`,
      correlationId: client.id,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : "";
    if (message.includes("description")) {
      res.status(400).json({ error: message });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to manage integration app",
      logPrefix: "internal.agent-api.integrations-app-manage error:",
      err,
    });
  }
});
registerAgentApiRoute("integrationAppLogoUpdate", async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const uploaded = await runSingleAvatarUpload(integrationLogoUpload, req);
    const clientKey = optionalIntegrationString(req.body?.clientKey);
    if (!clientKey) {
      res.status(400).json({ error: "clientKey is required" });
      return;
    }
    if (!uploaded) {
      res.status(400).json({ error: "No logo file provided" });
      return;
    }
    const resolved = await oauthService.resolveOAuthClientForAgentMutation({
      serverId,
      clientKey,
      actorAgentId: agentId,
    });
    if (resolved.status === "not_found") {
      res.status(404).json({ error: "Integration app not found" });
      return;
    }
    if (resolved.status === "owner_required") {
      res.status(403).json({
        error: "Only this app's owner or a current server admin can update its logo",
        errorCode: "APP_MANAGE_AUTHORITY_REQUIRED",
      });
      return;
    }
    const updated = await oauthService.updateOAuthClientLogo({
      serverId,
      clientId: resolved.value.client.id,
      fileBuffer: uploaded.buffer,
      actorAgentId: agentId,
    });
    if (!updated?.logoUrl) {
      res.status(404).json({ error: "Integration app not found" });
      return;
    }
    sendAgentApiResponse("integrationAppLogoUpdate", res, {
      clientId: updated.id,
      clientKey: updated.clientId,
      clientName: updated.name,
      logoUrl: updated.logoUrl,
    });
    recordRaftCliActivity(req, agentId, {
      command: "integration.app.logo.update",
      summary: `Updated integration app logo for ${updated.clientId}`,
      correlationId: updated.id,
    });
  } catch (err) {
    if (err instanceof multer.MulterError && err.code === "LIMIT_FILE_SIZE") {
      res.status(400).json({
        error: PROFILE_AVATAR_TOO_LARGE_MESSAGE,
        errorCode: "APP_LOGO_TOO_LARGE",
        maxBytes: MAX_PROFILE_AVATAR_BYTES,
      });
      return;
    }
    if (err instanceof Error && err.message.includes(PROFILE_AVATAR_BAD_FORMAT_MESSAGE)) {
      res.status(400).json({
        error: PROFILE_AVATAR_BAD_FORMAT_MESSAGE,
        errorCode: "APP_LOGO_BAD_FORMAT",
      });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to update integration app logo",
      logPrefix: "internal.agent-api.integrations-app-logo-update error:",
      err,
    });
  }
});
registerAgentApiRoute("integrationAppList", async (req, res) => {
  try {
    const apps = await integrationAppQueryService.listAgentIntegrationApps({
      serverId: req.serverId!,
      agentId: req.actingAgentId!,
    });
    sendAgentApiResponse("integrationAppList", res, { apps });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to list integration apps",
      logPrefix: "internal.agent-api.integrations-app-list error:",
      err,
    });
  }
});
registerAgentApiRoute("integrationAppStatus", ...agentApiRequestValidators("integrationAppStatus"), async (req, res) => {
  try {
    const query = req.query as AgentApiRequestQueryByRoute["integrationAppStatus"];
    const card = query.card?.trim();
    const clientKey = query.client?.trim();
    if (Boolean(card) === Boolean(clientKey)) {
      res.status(400).json({ error: "Exactly one of card or client is required" });
      return;
    }
    const app = card
      ? await integrationAppQueryService.getAgentIntegrationAppByCard({
          serverId: req.serverId!,
          agentId: req.actingAgentId!,
          cardRef: card,
        })
      : await integrationAppQueryService.getAgentIntegrationAppByClient({
          serverId: req.serverId!,
          agentId: req.actingAgentId!,
          clientKey: clientKey!,
        });
    if (!app) {
      res.status(404).json({ error: "Integration app not found" });
      return;
    }
    sendAgentApiResponse("integrationAppStatus", res, { app });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to load integration app status",
      logPrefix: "internal.agent-api.integrations-app-status error:",
      err,
    });
  }
});
registerAgentApiRoute("taskList", async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const query = validateAgentApiQuery("taskList", req, res);
    if (!query) return;
    const { channel, mine, status } = query;

    let statusFilter: TaskStatus | undefined;
    if (status !== undefined && status !== "all") {
      if (typeof status !== "string" || !isTaskStatus(status)) {
        res.status(400).json({ error: "Invalid status value" });
        return;
      }
      statusFilter = status;
    }

    if (mine === "true") {
      const taskSurfaces = new Map<string, {
        channelRef: string;
        localChannel: TaskSurfaceChannel;
      }>();
      // The visible channels and their refs, then every task surface in one
      // batch. These ids are already authorized by the ref resolution, so they
      // are not turned into refs and looked up by name again (that round trip,
      // per channel under Promise.all, borrowed 172 pool connections for one
      // agent on prod).
      const agent = await agentService.getAgent(agentId);
      if (agent && agent.serverId === serverId) {
        const channelRefs = await channelService.listAgentFacingTaskChannelRefs(serverId, agentId);
        const surfaces = await resolveTaskChannelSurfaces(serverId, [...channelRefs.keys()]);
        for (const [localChannelId, channelRef] of channelRefs) {
          const resolved = surfaces.get(localChannelId);
          if (!resolved) continue;
          const surface = {
            channelRef,
            localChannel: resolved.localChannel,
          };
          // Legacy/raw imports may still carry the authorized local joint id,
          // while Task V2 writes the canonical storage id. Both project to the
          // same caller-local surface; neither id is discovered from task rows.
          taskSurfaces.set(localChannelId, surface);
          taskSurfaces.set(resolved.storageChannelId, surface);
        }
      }
      const assigned = await taskService.listTasksAssignedToAgent(
        [...taskSurfaces.keys()],
        agentId,
        status ?? undefined,
      );
      const visibleTasks = assigned.flatMap((task) => {
        const surface = taskSurfaces.get(task.channelId);
        return surface
          ? [{
              ...taskService.projectTaskToChannel(task, surface.localChannel),
              channelRef: surface.channelRef,
            }]
          : [];
      });
      sendAgentApiResponse("taskList", res, {
        tasks: visibleTasks,
        scope: "mine",
        coverage: {
          status: "incomplete",
          visibleChannelTypes: ["channel", "private", "joint", "dm"],
          includesArchived: true,
          inaccessibleScope: "not_asserted",
          reason: "Channel membership can change after assignment; inaccessible task scope is not asserted.",
        },
        pagination: { mode: "complete", truncated: false },
      });
      return;
    }

    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel!, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }

    const tasks = await taskService.listTasks(ctx.storageChannelId, statusFilter);
    sendAgentApiResponse("taskList", res, {
      tasks: taskService.projectTasksToChannel(tasks, ctx.localChannel),
      scope: "channel",
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to list tasks",
      logPrefix: "internal.agent-api.task-list error:",
      err,
    });
  }
});

async function resolveTaskCreateAssignee(
  serverId: string,
  channelId: string,
  assigneeRef: string,
  res: Response,
): Promise<(taskService.TaskCreationAssignee & {
  name: string;
}) | null> {
  // @stdrc: assigning at create time carries the SAME permission as create and
  // as assign -- "只有 delete 这种行为，是只能创建者和 admin 做". The former
  // `manageServer` gate on dispatching to another actor is therefore removed.
  //
  // That gate also served as directory-oracle protection. It is not needed for
  // this actor class: a channel member can already enumerate server humans and
  // agents through `raft server info` / channel members, so distinguishable
  // not-found / ambiguous / cannot-claim answers leak nothing they cannot
  // already read, and they are materially better diagnostics for an agent.
  const handle = assigneeRef.slice(1).trim();

  const [humanId, targetAgentId] = await Promise.all([
    channelService.resolveUserByName(serverId, handle),
    channelService.resolveAgentByName(serverId, handle),
  ]);

  if (humanId && targetAgentId) {
    res.status(409).json({ error: `Assignee @${handle} is ambiguous`, code: "assignee_ambiguous" });
    return null;
  }

  const assignee: taskService.TaskCreationAssignee | null = targetAgentId
    ? { type: "agent", id: targetAgentId }
    : humanId
      ? { type: "user", id: humanId }
      : null;
  if (!assignee) {
    res.status(404).json({ error: `Assignee @${handle} not found`, code: "assignee_not_found" });
    return null;
  }

  const canClaim = assignee.type === "agent"
    ? await channelService.canAgentPostToChannel(channelId, assignee.id)
    : await channelService.canUserPostToChannel(channelId, assignee.id);
  if (!canClaim) {
    res.status(403).json({
      error: `Assignee @${handle} cannot claim tasks in this channel`,
      code: "assignee_cannot_claim",
    });
    return null;
  }

  return {
    ...assignee,
    name: handle,
  };
}

async function resolveJointTaskCreateAssignee(
  channelId: string,
  assigneeRef: string,
  res: Response,
): Promise<(taskService.TaskCreationAssignee & {
  name: string;
}) | null> {
  const handle = assigneeRef.slice(1).trim();
  const assignee = await resolveTaskAssignAssignee(channelId, assigneeRef);
  if (!assignee) {
    // Joint resolution is intentionally scoped to the channel-visible union.
    // Missing, ambiguous, and non-member handles remain one fail-closed shape.
    res.status(404).json({
      error: `Assignee @${handle} is not assignable in this channel`,
      code: "assignee_not_assignable",
    });
    return null;
  }
  return {
    ...assignee,
    name: handle,
  };
}

/**
 * The taskCreate response body. One builder for the live response and the
 * idempotency ledger, so a keyed replay returns exactly the first response.
 */
function buildTaskCreateResponse(
  created: Awaited<ReturnType<typeof taskService.createTasks>>["tasks"],
  assignmentReceipt?: taskService.TaskAssignmentReceipt,
): AgentApiResponseByRoute["taskCreate"] {
  return {
    tasks: created.map((task) => ({
      taskNumber: task.taskNumber,
      messageId: task.messageId,
      title: task.title,
      status: task.status,
      claimedByType: task.claimedByType,
      claimedById: task.claimedById,
      claimedByName: task.claimedByName,
      claimedAt: task.claimedAt,
      requiresResourceReceipt: task.requiresResourceReceipt,
    })),
    ...(assignmentReceipt && {
      assignmentReceipt: {
        messageId: assignmentReceipt.message.id,
        content: assignmentReceipt.content,
        assignee: assignmentReceipt.assignee,
        state: assignmentReceipt.state,
      },
    }),
  };
}

registerAgentApiRoute("taskCreate", ...agentApiRequestValidators("taskCreate"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, tasks: items, assignee: assigneeRef, idempotencyKey } = req.body as AgentApiRequestBodyByRoute["taskCreate"];
    if (items.length > 50) {
      res.status(400).json({ error: "Cannot create more than 50 tasks at once" });
      return;
    }
    const idempotency = agentApiIdempotencyScope(agentId, "taskCreate", idempotencyKey, {
      channel,
      tasks: items.map((item) => ({ title: item.title.trim(), createsResource: item.creates_resource === true })),
      assignee: assigneeRef ?? null,
    });
    if (idempotency && await replayAgentApiIdempotentWrite(idempotency, res)) return;

    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (!await assertAgentApiTaskWritableChannel(ctx, agentId, res, {
      threadError: "Thread messages cannot become tasks",
    })) {
      return;
    }
    const assignee = assigneeRef
      ? ctx.surface.isJoint
        ? await resolveJointTaskCreateAssignee(ctx.channelId, assigneeRef, res)
        : await resolveTaskCreateAssignee(serverId, ctx.channelId, assigneeRef, res)
      : undefined;
    if (assigneeRef && !assignee) return;

    let created: Awaited<ReturnType<typeof taskService.createTasks>>["tasks"];
    let hostMessages: Awaited<ReturnType<typeof taskService.createTasks>>["hostMessages"];
    let persistedAssignmentReceipt: taskService.TaskAssignmentReceipt | undefined;
    try {
      if (assignee) {
        const result = await taskService.createTasksWithAssignmentReceipt(
          ctx.storageChannelId,
          "agent",
          agentId,
          items.map((item) => ({
            title: item.title.trim(),
            createsResource: item.creates_resource === true,
          })),
          assignee,
          { assigneeName: assignee.name },
          {
            ...(ctx.surface.isJoint ? { initiatingLocalChannelId: ctx.channelId } : {}),
            ...(idempotency && {
              beforeCommit: (executor, result) => idempotentAgentApiService.recordAgentApiIdempotentResponse(executor, idempotency, {
                status: 200,
                body: buildTaskCreateResponse(result.tasks, result.assignmentReceipt),
              }),
            }),
          },
        );
        created = result.tasks;
        hostMessages = result.hostMessages;
        persistedAssignmentReceipt = result.assignmentReceipt;
      } else {
        const result = await taskService.createTasks(
          ctx.storageChannelId,
          "agent",
          agentId,
          items.map((item) => ({
            title: item.title.trim(),
            createsResource: item.creates_resource === true,
          })),
          idempotency
            ? {
              beforeCommit: (executor, result) => idempotentAgentApiService.recordAgentApiIdempotentResponse(executor, idempotency, {
                status: 200,
                body: buildTaskCreateResponse(result.tasks),
              }),
            }
            : {},
        );
        created = result.tasks;
        hostMessages = result.hostMessages;
      }
    } catch (err) {
      if (idempotency && err instanceof idempotentAgentApiService.AgentApiIdempotencyRaceLostError) {
        // A concurrent request with this key committed first; this request's
        // creation rolled back. Answer with the committed first response.
        if (await replayAgentApiIdempotentWrite(idempotency, res)) return;
        throw err;
      }
      if (err instanceof taskService.TaskCreationAssigneeEligibilityError) {
        res.status(403).json({ error: err.message, code: err.code });
        return;
      }
      throw err;
    }

    const io: SocketServer = req.app.get("io");
    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
    const creatorName = created[0]?.createdByName || "Unknown";
    const fanoutCreatedTasks = async () => {
      // v1.4: host message broadcasts on the ordinary message/agent-delivery
      // path; the task fact broadcasts separately on the task board.
      const targets = await getTaskRealtimeSurfaceTargets(ctx.surface);
      for (const target of targets) {
        const projectedTasks = taskService.projectTasksToChannel(created, target.localChannel);
        for (const message of hostMessages) {
          // message-realtime-producer: task-route.new.agent-api
          emitTaskMessageNew(io, target, { ...message, channelId: target.channelId }, creatorName);
        }
        emitTaskCreated(io, target, {
          channelId: target.channelId,
          tasks: projectedTasks,
        });
        const projectedMessages = hostMessages.map((message) => ({ ...message, channelId: target.channelId }));
        await messageService.deliverMessagesToAgents(
          agentOrchestrator,
          projectedMessages,
          creatorName,
        );
      }
    };

    const taskList = created
      .map((task) => `task #${task.taskNumber} "${messageService.summarizeForSystemMessage(task.title)}"`)
      .join(", ");
    if (assignee && persistedAssignmentReceipt) {
      // The transaction committed the channel's newest durable seq. Advance
      // heartbeat catch-up before any fallible realtime/delivery fanout so a
      // missed emit remains recoverable without waiting for a later message.
      const receiptTargets = await getTaskRealtimeSurfaceTargets(ctx.surface);
      for (const target of receiptTargets) {
        messageService.updateMaxSeq(target.serverId, persistedAssignmentReceipt.message.seq);
      }
      try {
        await failpoints.hit("server.task.assignedCreate.postCommitFanout", {
          channelId: ctx.channelId,
          receiptMessageId: persistedAssignmentReceipt.message.id,
        }, async () => undefined);
        await fanoutCreatedTasks();
        await messageService.broadcastSystemMessageToLocalSurfaces(
          io,
          agentOrchestrator,
          ctx.channelId,
          persistedAssignmentReceipt.content,
          {
            inboxFactPolicy: {
              mode: "record",
              producer: "task.assignment_receipt",
              reason: "a task assignment is durable directed attention for its assignee",
              causalActor: { type: "agent", id: agentId },
            },
            personalAttentionTargets: [{ type: assignee.type, id: assignee.id, name: assignee.name }],
            persistedMessage: persistedAssignmentReceipt.message,
          },
        );
      } catch (err) {
        // Durable state already committed. A transient fanout failure must not
        // turn success into retry-inviting HTTP 500 and duplicate the tasks.
        console.error("internal.agent-api.task-create post-commit fanout failed:", serializeErrorForLog(err));
      }
    } else {
      await fanoutCreatedTasks();
      const sysContent = `📋 ${created.length} new task${created.length > 1 ? "s" : ""} created: ${taskList}`;
      messageService.broadcastSystemMessageToLocalSurfaces(io, agentOrchestrator, ctx.channelId, sysContent, {
        inboxFactPolicy: {
          mode: "record",
          producer: "task.created_summary",
          reason: "new shared tasks are channel activity",
          causalActor: { type: "agent", id: agentId },
        },
        // The agent that created the tasks should not see its own action as unread.
      }).catch(() => {});
    }

    sendAgentApiResponse("taskCreate", res, buildTaskCreateResponse(created, persistedAssignmentReceipt));
    void recordAgentRaftAction(req, agentId, {
      title: `Created ${created.length} task${created.length === 1 ? "" : "s"}`,
      text: `target: ${channel}`,
    });
  } catch (err) {
    if (respondToAgentApiIdempotencyConflict(err, res)) return;
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to create tasks",
      logPrefix: "internal.agent-api.task-create error:",
      err,
    });
  }
});

function taskClaimMode(hasTaskNumbers: boolean, hasMessageIds: boolean): "mixed" | "task_number" | "message_id" | "empty" {
  if (hasTaskNumbers && hasMessageIds) return "mixed";
  if (hasTaskNumbers) return "task_number";
  if (hasMessageIds) return "message_id";
  return "empty";
}

function taskClaimReasonBucket(reason: string | undefined): string {
  const normalized = reason?.toLowerCase() ?? "";
  if (!normalized) return "unknown_business_failure";
  if (normalized.includes("not found")) return "not_found";
  if (normalized.includes("already claimed")) return "already_claimed";
  if (normalized.includes("already converted")) return "already_converted";
  if (normalized.includes("archived")) return "channel_archived";
  if (normalized.includes("thread")) return "thread_target";
  return "unknown_business_failure";
}

function traceTaskClaimFinished(attrs: Record<string, unknown>): void {
  addTraceEvent("task_claim.finished", {
    event_kind: "task_claim",
    ...attrs,
  });
}

registerAgentApiRoute("taskClaim", ...agentApiRequestValidators("taskClaim"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      traceTaskClaimFinished({
        outcome: "rejected",
        reason: "invalid_principal",
        claim_mode: "empty",
        input_count: 0,
      });
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, task_numbers, message_ids, seenUpToSeq, seenExactSeqs, freshnessContextMode } = req.body as AgentApiRequestBodyByRoute["taskClaim"];
    const hasTaskNumbers = Array.isArray(task_numbers) && task_numbers.length > 0;
    const hasMessageIds = Array.isArray(message_ids) && message_ids.length > 0;
    const claimMode = taskClaimMode(hasTaskNumbers, hasMessageIds);
    const inputCount = (hasTaskNumbers ? task_numbers.length : 0) + (hasMessageIds ? message_ids.length : 0);
    if (!hasTaskNumbers && !hasMessageIds) {
      traceTaskClaimFinished({
        outcome: "rejected",
        reason: "invalid_body",
        claim_mode: claimMode,
        input_count: inputCount,
      });
      res.status(400).json({ error: "channel and either task_numbers or message_ids array are required" });
      return;
    }

    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      traceTaskClaimFinished({
        outcome: "rejected",
        reason: "target_not_found",
        claim_mode: claimMode,
        input_count: inputCount,
      });
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (!await assertAgentApiTaskWritableChannel(ctx, agentId, res, {
      threadError: "Thread messages cannot be claimed as tasks",
    })) {
      traceTaskClaimFinished({
        outcome: "rejected",
        reason: ctx.channelType === "thread" ? "thread_target" : "target_not_writable",
        claim_mode: claimMode,
        input_count: inputCount,
      });
      return;
    }

    const io: SocketServer = req.app.get("io");
    const results: {
      taskNumber?: number;
      messageId?: string;
      success: boolean;
      reason?: string;
      conflict?: AgentApiTaskClaimConflict;
    }[] = [];
    let successfulClaimCount = 0;

    if (hasTaskNumbers) {
      for (const taskNumber of task_numbers) {
        const existing = await taskService.getTaskByNumber(ctx.storageChannelId, taskNumber);
        if (existing && rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, existing.createdAt, res)) return;
      }
    }

    const held = await holdTaskWriteForFreshness(req, {
      action: "task_claim",
      writeKey: `task_claim:${[...(task_numbers ?? [])].sort((a, b) => a - b).join(",")}|${[...(message_ids ?? [])].sort().join(",")}`,
      agentId,
      serverId,
      ctx,
      target: channel,
      seenUpToSeq,
      seenExactSeqs,
      freshnessContextMode,
    });
    if (held) {
      traceTaskClaimFinished({
        outcome: "held",
        reason: "freshness",
        claim_mode: claimMode,
        input_count: inputCount,
      });
      sendAgentApiResponse("taskClaim", res, held);
      return;
    }

    await traceQuerySpan({
      queryName: "tasks.claim",
      phase: "agent_api_task_claim",
      dbSystem: "postgresql",
      attrs: {
        claim_mode: claimMode,
        input_count: inputCount,
      },
      successAttrs: () => ({
        result_count: results.length,
        successful_claim_count: successfulClaimCount,
      }),
    }, async () => {

    if (hasTaskNumbers) {
      const batchResults = await taskService.batchClaimTasks(ctx.storageChannelId, task_numbers, "agent", agentId, ctx.localChannel);
      for (const result of batchResults) {
        let claimedMessageId: string | undefined;
        if (result.success && result.task) {
          const facts = await emitTaskMutationToSurfaces(io, ctx.surface, result.task);
          claimedMessageId = facts.messageId ?? undefined;
          successfulClaimCount += 1;
        }
        results.push({
          taskNumber: result.taskNumber,
          messageId: claimedMessageId,
          success: result.success,
          reason: result.reason,
          conflict: result.conflict,
        });
      }
    }

    if (hasMessageIds) {
      for (const rawMsgId of message_ids) {
        const preflight = await taskService.resolveMessageInChannel(ctx.storageChannelId, rawMsgId);
        if (preflight && rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, preflight.createdAt, res)) return;
        if (preflight) {
          const existing = await taskService.resolveTaskByMessageId(preflight.id);
          if (existing && rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, existing.row.createdAt, res)) return;
        }
      }
      for (const rawMsgId of message_ids) {
        const resolved = await taskService.resolveMessageInChannel(ctx.storageChannelId, rawMsgId);
        if (!resolved) {
          results.push({ messageId: rawMsgId, success: false, reason: "message not found" });
          continue;
        }

        const msgId = resolved.id;
        if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, resolved.createdAt, res)) return;
        const converted = await taskService.convertMessageToTask(msgId, "agent", agentId, ctx.storageChannelId);
        if (typeof converted === "string") {
          if (converted === "already converted") {
            const existingOwner = await taskService.resolveTaskByMessageId(msgId);
            if (existingOwner) {
              if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, existingOwner.row.createdAt, res)) return;
              // P3: both arms of the old ownership ternary read the same field.
              const existingNumber = existingOwner.row.taskNumber;
              const rejection = await taskService.getClaimRejectionForOwner(existingOwner, "agent", agentId);
              if (rejection) {
                results.push({
                  messageId: msgId,
                  success: false,
                  reason: rejection.reason,
                  conflict: rejection.conflict ?? undefined,
                  taskNumber: existingNumber,
                });
                continue;
              }
              const claimed = await taskService.claimTaskDetailed(existingOwner.row.id, "agent", agentId);
              if (typeof claimed.result !== "string") {
                const facts = await emitTaskMutationToSurfaces(io, ctx.surface, claimed.result);
                successfulClaimCount += 1;
                results.push({ messageId: msgId, success: true, taskNumber: facts.taskNumber });
                continue;
              }
              results.push({
                messageId: msgId,
                success: false,
                reason: claimed.result,
                conflict: claimed.conflict,
                taskNumber: existingNumber,
              });
              continue;
            }
          }
          results.push({ messageId: msgId, success: false, reason: converted });
        } else {
          const claimed = await taskService.claimTaskDetailed(converted.id, "agent", agentId);
          if (typeof claimed.result !== "string") {
            // The convert left the host message untouched, so only the task
            // board learns about this — no message:updated for an unchanged row.
            const facts = await describeTaskMutation(claimed.result);
            const targets = await getTaskRealtimeSurfaceTargets(ctx.surface);
            for (const target of targets) {
              emitTaskCreated(io, target, {
                channelId: target.channelId,
                tasks: [taskService.projectTaskToChannel(facts.enriched, target.localChannel)],
              });
            }
            successfulClaimCount += 1;
            results.push({ messageId: msgId, success: true, taskNumber: facts.taskNumber });
          } else {
            results.push({ messageId: msgId, success: false, reason: claimed.result, conflict: claimed.conflict });
          }
        }
      }
    }
    });

    // No post-hoc conflict projection happens here: every `conflict` above was
    // built by the authoritative claim writer (or channel-bound pre-check)
    // from the same observed row that produced its `reason`. A cross-channel
    // message id fails channel-bound resolution and stays indistinguishable
    // from a nonexistent one — prose "message not found", zero conflict.
    const failedResults = results.filter((result) => !result.success);
    traceTaskClaimFinished({
      outcome: successfulClaimCount > 0
        ? failedResults.length > 0 ? "partial_success" : "success"
        : "business_rejected",
      reason: successfulClaimCount > 0
        ? failedResults.length > 0 ? "partial_failure" : "claimed"
        : "no_claims",
      claim_mode: claimMode,
      input_count: inputCount,
      result_count: results.length,
      successful_claim_count: successfulClaimCount,
      failed_result_count: failedResults.length,
      failure_reason_buckets: [...new Set(failedResults.map((result) => taskClaimReasonBucket(result.reason)))].join(",") || "none",
    });
    sendAgentApiResponse("taskClaim", res, { results });
    if (successfulClaimCount > 0) {
      void recordAgentRaftAction(req, agentId, {
        title: `Claimed ${successfulClaimCount} task${successfulClaimCount === 1 ? "" : "s"}`,
        text: `target: ${channel}`,
      });
    }
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    traceTaskClaimFinished({
      outcome: "internal_error",
      reason: "unexpected_throw",
      error_class: errorClassOf(err),
    });
    console.error("internal.agent-api.task-claim error:", serializeErrorForLog(err));
    res.status(500).json({ error: "Failed to claim tasks" });
  }
});

registerAgentApiRoute("taskUnclaim", ...agentApiRequestValidators("taskUnclaim"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, task_number } = req.body as AgentApiRequestBodyByRoute["taskUnclaim"];
    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (rejectAgentApiTaskWriteIfNeeded(await channelService.canAgentPostToChannel(ctx.channelId, agentId), res)) return;
    if (await channelService.isChannelArchived(ctx.channelId)) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }

    const task = await taskService.getTaskByNumber(ctx.storageChannelId, task_number);
    if (!task) {
      res.status(404).json({ error: "Task not found" });
      return;
    }
    if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, task.createdAt, res)) return;

    const result = await taskService.unclaimTask(task.id, "agent", agentId);
    if (typeof result === "string") {
      res.status(409).json({ error: result });
      return;
    }

    const io: SocketServer = req.app.get("io");
    await emitTaskMutationToSurfaces(io, ctx.surface, result);

    sendAgentApiResponse("taskUnclaim", res, { ok: true });
    void recordAgentRaftAction(req, agentId, {
      title: `Unclaimed task #${task_number}`,
      text: `target: ${channel}`,
      producerFactId: task.id,
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to unclaim task",
      logPrefix: "internal.agent-api.task-unclaim error:",
      err,
    });
  }
});

/**
 * Resolve an `@handle` for the ASSIGN path.
 *
 * Deliberately NOT `resolveTaskCreateAssignee`. That one gates assigning to
 * another actor behind `manageServer`, and @stdrc ruled that setting an assignee
 * is member-level (任何人都可以 assign 给别人). Reusing it would have silently
 * re-imposed the admin gate on the very capability this feature exists to grant.
 *
 * But the concern the create path was protecting is real and survives: handle
 * resolution can be used as a **directory oracle** — probing which names exist,
 * are ambiguous, or are hidden from you. So instead of an authority gate, this
 * closes the oracle directly: every unresolvable handle, every handle for
 * someone outside this channel, and every ambiguous handle answer with the SAME
 * message. A caller learns only "not assignable here", never why.
 *
 * ⚠️ This makes the agent assign path member-level while assigned-CREATE stays
 * admin-only. That divergence is raised with @stdrc rather than resolved here —
 * loosening the create path is a permission change to shipped behavior and is
 * not this feature's call.
 */
async function resolveTaskAssignAssignee(
  channelId: string,
  assigneeRef: string,
): Promise<taskService.TaskCreationAssignee | null> {
  const handle = assigneeRef.startsWith("@") ? assigneeRef.slice(1).trim() : assigneeRef.trim();
  if (!handle) return null;

  // Resolve only through the channel-visible directory. For a joint channel,
  // getChannelMembers returns the union of active projections, so a peer-server
  // member is assignable without turning the requester's local server directory
  // into the authority source. Logical ids are deduped before ambiguity is
  // judged: one person present on both projections is still one candidate.
  const members = await channelService.getChannelMembers(channelId);
  const humanIds = new Set(members.humans.filter((human) => human.name === handle).map((human) => human.id));
  const agentIds = new Set(members.agents.filter((agent) => agent.name === handle).map((agent) => agent.id));
  if (humanIds.size + agentIds.size !== 1) return null;
  if (agentIds.size === 1) return { type: "agent", id: [...agentIds][0]! };
  return { type: "user", id: [...humanIds][0]! };
}

async function resolveTaskResourceTeardownOwner(
  surface: TaskChannelSurface,
  localChannelId: string,
  ownerRef: string,
): Promise<{ id: string; name: string; serverId: string; targetChannelId: string } | null> {
  const handle = ownerRef.startsWith("@") ? ownerRef.slice(1).trim() : ownerRef.trim();
  if (!handle) return null;
  const members = await channelService.getChannelMembers(localChannelId);
  const matches = members.agents.filter((agent) => agent.name === handle);
  const ids = new Set(matches.map((agent) => agent.id));
  if (ids.size !== 1) return null;
  const owner = matches.find((agent) => agent.id === [...ids][0]);
  if (!owner) return null;
  const targets = await getTaskRealtimeSurfaceTargets(surface);
  const target = targets.find((candidate) => candidate.serverId === owner.serverId);
  if (!target) return null;
  return {
    id: owner.id,
    name: owner.name,
    serverId: owner.serverId,
    targetChannelId: target.channelId,
  };
}

registerAgentApiRoute("taskAssign", ...agentApiRequestValidators("taskAssign"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, task_number, assignee: assigneeRef, expected_revision } =
      req.body as AgentApiRequestBodyByRoute["taskAssign"];
    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (rejectAgentApiTaskWriteIfNeeded(await channelService.canAgentPostToChannel(ctx.channelId, agentId), res)) return;
    if (await channelService.isChannelArchived(ctx.channelId)) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }

    const task = await taskService.getTaskByNumber(ctx.storageChannelId, task_number);
    if (!task) {
      res.status(404).json({ error: "Task not found" });
      return;
    }
    if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, task.createdAt, res)) return;

    let assignee: taskService.TaskCreationAssignee | null = null;
    if (assigneeRef !== null) {
      assignee = await resolveTaskAssignAssignee(ctx.channelId, assigneeRef);
      if (!assignee) {
        res.status(404).json({
          error: `${assigneeRef} is not assignable in this channel`,
          code: "assignee_not_assignable",
        });
        return;
      }
    }

    const result = await taskService.assignTask(task.id, assignee, "agent", agentId, {
      expectedRevision: expected_revision,
    });
    if (typeof result === "string") {
      res.status(409).json({ error: result });
      return;
    }

    const io: SocketServer = req.app.get("io");
    await emitTaskMutationToSurfaces(io, ctx.surface, result);

    sendAgentApiResponse("taskAssign", res, {
      ok: true,
      revision: result.row.revision,
      assignee: assigneeRef ?? null,
    });
    void recordAgentRaftAction(req, agentId, {
      title: assigneeRef
        ? `Assigned task #${task_number} to ${assigneeRef}`
        : `Unassigned task #${task_number}`,
      text: `target: ${channel}`,
      producerFactId: task.id,
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to assign task",
      logPrefix: "internal.agent-api.task-assign error:",
      err,
    });
  }
});

registerAgentApiRoute("taskUpdateStatus", ...agentApiRequestValidators("taskUpdateStatus"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, task_number, status, seenUpToSeq, seenExactSeqs, freshnessContextMode } = req.body as AgentApiRequestBodyByRoute["taskUpdateStatus"];
    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (rejectAgentApiTaskWriteIfNeeded(await channelService.canAgentPostToChannel(ctx.channelId, agentId), res)) return;
    if (await channelService.isChannelArchived(ctx.channelId)) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }

    const task = await taskService.getTaskByNumber(ctx.storageChannelId, task_number);
    if (!task) {
      res.status(404).json({ error: "Task not found" });
      return;
    }
    if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, task.createdAt, res)) return;

    const held = await holdTaskWriteForFreshness(req, {
      action: "task_update",
      writeKey: `task_update:${task.id}:${status}`,
      agentId,
      serverId,
      ctx,
      target: channel,
      seenUpToSeq,
      seenExactSeqs,
      freshnessContextMode,
    });
    if (held) {
      sendAgentApiResponse("taskUpdateStatus", res, held);
      return;
    }

    // Same shape as the browser route: try the ordinary member-level update,
    // then let an actor holding `deleteAnyTask` force it. Agents carry a
    // `serverAgentMembers.role`, so an admin agent is a real state — this route
    // simply never consulted it, which made agents strictly weaker than human
    // admins for no stated reason (@stdrc: 人和 agent 的权限管理才一致).
    let result = await taskService.updateTaskStatus(task.id, status, agentId, "agent");
    if (typeof result === "string" && result !== "task not found") {
      if (await actorHasServerCapabilityInServer(serverId, "agent", agentId, "deleteAnyTask")) {
        result = await taskService.forceUpdateTaskStatus(task.id, status, "agent", agentId);
      }
    }
    if (typeof result === "string") {
      setRequestTraceErrorCode(res, taskStatusRejectionCode(result));
      res.status(409).json({ error: result });
      return;
    }

    const io: SocketServer = req.app.get("io");
    await emitTaskMutationToSurfaces(io, ctx.surface, result);

    // Lifecycle system message into the task thread (parity with the human
    // route): the acting agent should not see its own status change as unread.
    // No status allow-list here — status is already constrained at route
    // validation, and buildTaskStatusChangeNotice has the emoji/label fallback.
    // A hand-written list here would drift from the human path (same bug we just fixed).
    const taskMessageId = task.messageId;
    if (taskMessageId) {
      const content = messageService.buildTaskStatusChangeNotice(ctx.agent.name || "Someone", task.taskNumber, task.title, status);
      void (async () => {
        try {
          const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator | undefined;
          if (!agentOrchestrator) return;
          const thread = ctx.localChannel.deletedAt
            ? await channelService.getOrCreateThread(taskMessageId, agentId, "agent")
            : await channelService.getOrCreateThreadForChannel(ctx.localChannel.id, taskMessageId, agentId, "agent");
          await messageService.broadcastSystemMessageToLocalSurfaces(io, agentOrchestrator, thread.id, content, {
            inboxFactPolicy: {
              mode: "record",
              producer: "task.lifecycle_thread",
              reason: "task status transitions are a collaboration signal for the thread audience",
              causalActor: { type: "agent", id: agentId },
            },
          });
        } catch (err) {
          console.error("agent task status: failed to post lifecycle system message:", err);
        }
      })();
    }

    sendAgentApiResponse("taskUpdateStatus", res, { ok: true });
    void recordAgentRaftAction(req, agentId, {
      title: `Updated task #${task_number} to ${status}`,
      text: `target: ${channel}`,
      producerFactId: task.id,
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to update task status",
      logPrefix: "internal.agent-api.task-update-status error:",
      err,
    });
  }
});

registerAgentApiRoute("taskResourceReceipt", ...agentApiRequestValidators("taskResourceReceipt"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, task_number, receipt } =
      req.body as AgentApiRequestBodyByRoute["taskResourceReceipt"];
    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (rejectAgentApiTaskWriteIfNeeded(await channelService.canAgentPostToChannel(ctx.channelId, agentId), res)) return;
    if (await channelService.isChannelArchived(ctx.channelId)) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }

    const task = await taskService.getTaskByNumber(ctx.storageChannelId, task_number);
    if (!task) {
      res.status(404).json({ error: "Task not found" });
      return;
    }
    if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, task.createdAt, res)) return;
    const teardownOwner = await resolveTaskResourceTeardownOwner(
      ctx.surface,
      ctx.channelId,
      receipt.teardown_owner,
    );
    if (!teardownOwner) {
      res.status(409).json({
        error: "teardown_owner is not a unique agent in this task channel",
        code: "teardown_owner_not_assignable",
      });
      return;
    }

    const result = await taskService.recordTaskResourceReceipt({
      taskId: task.id,
      receipt,
      actorType: "agent",
      actorId: agentId,
      teardownOwnerAgentId: teardownOwner.id,
      teardownOwnerServerId: teardownOwner.serverId,
      teardownOwnerTargetChannelId: teardownOwner.targetChannelId,
      expiryFollowups: taskResourceExpiryFollowups,
    });
    if (typeof result === "string") {
      const status = result.includes("must be") ? 400 : 409;
      res.status(status).json({ error: result });
      return;
    }

    const io: SocketServer = req.app.get("io");
    if (!result.idempotent) {
      await emitTaskMutationToSurfaces(io, ctx.surface, { source: "tasks", row: result.task });
    }
    // Retry the Computer handoff even for an idempotent HTTP retry: the
    // durable transaction may have committed while the first sync failed.
    await publishTaskResourceExpiryFollowup(
      req,
      result.expiryFollowup.id,
      teardownOwner.serverId,
      !result.idempotent,
    );

    sendAgentApiResponse("taskResourceReceipt", res, {
      ok: true,
      taskNumber: result.task.taskNumber,
      revision: result.task.revision,
      receipt: { ...result.receipt },
      expiryFollowup: {
        id: result.expiryFollowup.id,
        ownerAgentId: teardownOwner.id,
        owner: `@${teardownOwner.name}`,
        fireAt: result.expiryFollowup.fireAt.toISOString(),
        msgId: result.task.messageId!,
        targetChannelId: teardownOwner.targetChannelId,
      },
    });
    void recordAgentRaftAction(req, agentId, {
      title: `Recorded resource receipt for task #${task_number}`,
      text: `target: ${channel}`,
      producerFactId: task.id,
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to record task resource receipt",
      logPrefix: "internal.agent-api.task-resource-receipt error:",
      err,
    });
  }
});

/**
 * Delete a task.
 *
 * Authorization is the browser rule verbatim: the creator, or an actor holding
 * `deleteAnyTask`. Before this route existed an agent could not delete at all --
 * not even an admin agent, and not even a task it had created itself -- while a
 * human creator could. That was the one task verb with no agent surface.
 */
registerAgentApiRoute("taskDelete", ...agentApiRequestValidators("taskDelete"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, task_number } = req.body as AgentApiRequestBodyByRoute["taskDelete"];
    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (rejectAgentApiTaskWriteIfNeeded(await channelService.canAgentPostToChannel(ctx.channelId, agentId), res)) return;
    if (await channelService.isChannelArchived(ctx.channelId)) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }

    const owner = await taskService.resolveTaskByNumber(ctx.storageChannelId, task_number);
    if (!owner) {
      res.status(404).json({ error: "Task not found" });
      return;
    }
    if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, owner.row.createdAt, res)) return;

    const isCreator = owner.row.createdByType === "agent" && owner.row.createdById === agentId;
    if (!isCreator && !await actorHasServerCapabilityInServer(serverId, "agent", agentId, "deleteAnyTask")) {
      res.status(403).json({
        error: "Only the task creator or server admins can delete",
        code: "task_delete_forbidden",
      });
      return;
    }

    const title = owner.row.title;
    await taskService.deleteTaskByOwner(owner);

    const io: SocketServer = req.app.get("io");
    const targets = await getTaskRealtimeSurfaceTargets(ctx.surface);
    for (const target of targets) {
      emitTaskDeleted(io, target, { channelId: target.channelId, taskId: owner.row.id });
    }

    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
    const actor = await agentService.getAgent(agentId);
    const actorName = actor?.displayName || actor?.name || "An agent";
    messageService.broadcastSystemMessageToLocalSurfaces(io, agentOrchestrator, ctx.channelId,
      `🗑 ${actorName} deleted #${task_number} "${messageService.summarizeForSystemMessage(title)}"`, {
        inboxFactPolicy: {
          mode: "skip",
          producer: "task.deleted_summary",
          reason: "task lifecycle churn should not move Activity unread",
        },
      }).catch(() => {});

    sendAgentApiResponse("taskDelete", res, { ok: true });
    void recordAgentRaftAction(req, agentId, {
      title: `Deleted task #${task_number}`,
      text: `target: ${channel}`,
      producerFactId: owner.row.id,
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to delete task",
      logPrefix: "internal.agent-api.task-delete error:",
      err,
    });
  }
});

/**
 * Convert a message into a task WITHOUT claiming it.
 *
 * `taskClaim --message-id` already converts, but it also assigns the result to
 * the caller, so an agent filing work for someone else had no way to express
 * "this message is a task" without first taking it. The browser has had the
 * unclaimed form (`POST /tasks/convert-message`) all along.
 */
registerAgentApiRoute("taskConvert", ...agentApiRequestValidators("taskConvert"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, message_id } = req.body as AgentApiRequestBodyByRoute["taskConvert"];
    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (rejectAgentApiTaskWriteIfNeeded(await channelService.canAgentPostToChannel(ctx.channelId, agentId), res)) return;
    if (await channelService.isChannelArchived(ctx.channelId)) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }

    const resolved = await taskService.resolveMessageInChannel(ctx.storageChannelId, message_id);
    if (!resolved) {
      res.status(404).json({ error: "Message not found" });
      return;
    }
    if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, resolved.createdAt, res)) return;

    const converted = await taskService.convertMessageToTask(resolved.id, "agent", agentId, ctx.storageChannelId);
    if (typeof converted === "string") {
      res.status(409).json({ error: converted });
      return;
    }

    const enriched = await taskService.enrichSingleLegacyTask(converted);
    const io: SocketServer = req.app.get("io");
    // Converting does not rewrite the host message, so there is no
    // `message:updated` and nothing new to deliver — the message was already
    // delivered when it was sent. Only the board learns something.
    const targets = await getTaskRealtimeSurfaceTargets(ctx.surface);
    for (const target of targets) {
      emitTaskCreated(io, target, {
        channelId: target.channelId,
        tasks: [taskService.projectTaskToChannel(enriched, target.localChannel)],
      });
    }

    const agentOrchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
    const actor = await agentService.getAgent(agentId);
    const actorName = actor?.displayName || actor?.name || "An agent";
    messageService.broadcastSystemMessageToLocalSurfaces(io, agentOrchestrator, ctx.channelId,
      `📋 ${actorName} converted a message to task #${converted.taskNumber} "${messageService.summarizeForSystemMessage(converted.title)}"`, {
        inboxFactPolicy: {
          mode: "record",
          producer: "task.converted_summary",
          reason: "newly created task is shared channel activity",
          causalActor: { type: "agent", id: agentId },
        },
      }).catch(() => {});

    sendAgentApiResponse("taskConvert", res, {
      task: {
        taskNumber: enriched.taskNumber,
        messageId: enriched.messageId,
        title: enriched.title,
        status: enriched.status,
        claimedByType: enriched.claimedByType,
        claimedById: enriched.claimedById,
        claimedByName: enriched.claimedByName,
        claimedAt: enriched.claimedAt,
        requiresResourceReceipt: enriched.requiresResourceReceipt,
      },
    });
    void recordAgentRaftAction(req, agentId, {
      title: `Converted a message to task #${converted.taskNumber}`,
      text: `target: ${channel}`,
      producerFactId: converted.id,
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to convert message to task",
      logPrefix: "internal.agent-api.task-convert error:",
      err,
    });
  }
});

registerAgentApiRoute("taskAmend", ...agentApiRequestValidators("taskAmend"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const { channel, task_number, title, description, seenUpToSeq, seenExactSeqs, freshnessContextMode } = req.body as AgentApiRequestBodyByRoute["taskAmend"];
    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    if (rejectAgentApiTaskWriteIfNeeded(await channelService.canAgentPostToChannel(ctx.channelId, agentId), res)) return;
    if (await channelService.isChannelArchived(ctx.channelId)) {
      res.status(409).json({ error: "This channel is archived", code: "channel_archived" });
      return;
    }

    const task = await taskService.getTaskByNumber(ctx.storageChannelId, task_number);
    if (!task) {
      res.status(404).json({ error: "Task not found" });
      return;
    }
    if (rejectHistoricalJointTaskWriteIfNeeded(ctx.surface, task.createdAt, res)) return;

    const held = await holdTaskWriteForFreshness(req, {
      action: "task_amend",
      writeKey: `task_amend:${task.id}`,
      agentId,
      serverId,
      ctx,
      target: channel,
      seenUpToSeq,
      seenExactSeqs,
      freshnessContextMode,
    });
    if (held) {
      sendAgentApiResponse("taskAmend", res, held);
      return;
    }

    const patch: taskService.TaskAmendPatch = {};
    if (title !== undefined) patch.title = title;
    if (description !== undefined) patch.description = description;
    const result = await taskService.amendTask(task.id, patch, "agent", agentId);
    if (typeof result === "string") {
      res.status(409).json({ error: result });
      return;
    }

    const actor = await agentService.getAgent(agentId);
    const io: SocketServer = req.app.get("io");
    await emitTaskMutationToSurfaces(
      io,
      ctx.surface,
      { source: "tasks", row: result.row },
      {
        title: result.row.title,
        description: result.row.description,
        revision: result.row.revision,
        superseded: true,
        amendedAt: result.event.createdAt.toISOString(),
        amendedByType: result.event.actorType,
        amendedByName: actor?.name ?? null,
        source: "tasks_current_projection",
      },
    );

    sendAgentApiResponse("taskAmend", res, {
      task: {
        taskNumber: result.row.taskNumber,
        title: result.row.title,
        description: result.row.description,
        revision: result.row.revision,
      },
      event: {
        id: result.event.id,
        seq: result.event.seq,
        eventType: result.event.eventType,
        actorType: result.event.actorType,
        actorName: actor?.name ?? null,
        payload: result.event.payload,
        createdAt: result.event.createdAt.toISOString(),
      },
    });
    void recordAgentRaftAction(req, agentId, {
      title: `Amended task #${task_number}`,
      text: `target: ${channel}\nrevision: ${result.row.revision}\nevent_seq: ${result.event.seq}`,
      producerFactId: result.event.id,
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to amend task",
      logPrefix: "internal.agent-api.task-amend error:",
      err,
    });
  }
});

registerAgentApiRoute("taskHistory", ...agentApiRequestValidators("taskHistory"), async (req, res) => {
  try {
    const agentId = req.actingAgentId;
    const serverId = req.serverId;
    if (!agentId || !serverId) {
      res.status(401).json({ error: "Agent credential required", code: "invalid_principal" });
      return;
    }

    const query = validateAgentApiQuery("taskHistory", req, res);
    if (!query) return;
    const { channel, task_number } = query;
    const ctx = await resolveAgentApiTaskChannel(agentId, serverId, channel, res);
    if (!ctx) {
      res.status(404).json({ error: "Agent or channel not found" });
      return;
    }
    const task = await taskService.getTaskByNumber(ctx.storageChannelId, task_number);
    if (!task) {
      res.status(404).json({ error: "Task not found" });
      return;
    }
    const history = await taskService.listTaskHistory(task.id);
    if (typeof history === "string") {
      res.status(409).json({ error: history });
      return;
    }
    const owner = await taskService.resolveTaskById(task.id);
    if (!owner || owner.source !== "tasks") {
      res.status(409).json({ error: "legacy task cards do not have canonical history" });
      return;
    }
    sendAgentApiResponse("taskHistory", res, {
      task: {
        taskNumber: owner.row.taskNumber,
        title: owner.row.title,
        description: owner.row.description,
        revision: owner.row.revision,
      },
      events: history,
    });
  } catch (err) {
    if (respondToTaskWriteError(err, res)) return;
    sendJsonServerError(req, res, {
      error: "Failed to read task history",
      logPrefix: "internal.agent-api.task-history error:",
      err,
    });
  }
});

export const REMINDERS_UNSUPPORTED_FOR_EXTERNAL_AGENTS = "reminders_unsupported_for_external_agents";
/**
 * Guards the legacy reminder writes (schedule, update, snooze). Runs before
 * request validation so the answer does not depend on the payload. Reads
 * (list, log) and cancel stay available in both cases below, and reminders
 * already queued keep firing.
 *
 * - External agent: reminders are fired by the agent's Computer (the daemon
 *   schedules them locally); an external agent has none, so a reminder it
 *   schedules would be stored and never fire.
 * - `reminder_app` on for the server: reminders moved to the Reminder app
 *   (see refuseIfRemindersMovedToApp).
 */
const refuseLegacyReminderMutation: RequestHandler = async (req, res, next) => {
  try {
    const external = typeof req.actingAgentIsExternal === "boolean"
      ? req.actingAgentIsExternal
      : isExternalAgentRuntime((await agentService.getAgent(req.actingAgentId!))?.runtime ?? "");
    if (external) {
      res.status(409).json({
        error: "Reminders are not yet supported for external agents: a reminder is fired by the agent's Raft computer, and an external agent has none. Nothing was scheduled.",
        code: REMINDERS_UNSUPPORTED_FOR_EXTERNAL_AGENTS,
      });
      return;
    }
    if (await refuseIfRemindersMovedToApp(res, req.serverId)) return;
    next();
  } catch (err) {
    next(err);
  }
};

registerAgentApiRoute("reminderList", ...agentApiRequestValidators("reminderList"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const statusFilter = parseReminderStatusFilter(req, res);
    if (!statusFilter) return;

    const rows = await reminderCrud.listAppReminders({
      serverId,
      ownerAgentId: agentId,
      status: statusFilter,
    });
    const summaries = await reminderService.toReminderSummaries(rows, serverId);
    sendAgentApiResponse("reminderList", res, { reminders: summaries });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to list reminders",
      logPrefix: "internal.agent-api.reminder-list error:",
      err,
    });
  }
});

registerAgentApiRoute("reminderCreate", refuseLegacyReminderMutation, ...agentApiRequestValidators("reminderCreate"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const body = req.body as AgentApiRequestBodyByRoute["reminderCreate"];
    const { title, fireAt, delaySeconds, msgId, payload, repeat, tz, channel } = body;
    if (typeof title !== "string" || title.trim().length === 0) {
      res.status(400).json({ error: "title is required" });
      return;
    }
    if (title.length > REMINDER_MAX_TITLE_LEN) {
      res.status(400).json({ error: `title must be at most ${REMINDER_MAX_TITLE_LEN} characters` });
      return;
    }

    const recurrence = parseReminderRecurrence(repeat, tz, res);
    if (recurrence === undefined) return;

    const hasScheduleInput = delaySeconds != null || fireAt != null;
    let fireAtDate: Date;
    let warning: string | undefined;
    if (hasScheduleInput) {
      const schedule = resolveScheduleInput({ delaySeconds, fireAt }, Date.now());
      if (!schedule.ok) {
        res.status(400).json({ error: schedule.error });
        return;
      }
      fireAtDate = schedule.fireAt;
      warning = schedule.warning;
    } else if (recurrence) {
      fireAtDate = computeNextFire(recurrence, new Date());
    } else {
      res.status(400).json({ error: "Provide delaySeconds, fireAt, or repeat" });
      return;
    }

    if (typeof msgId !== "string" || msgId.length === 0) {
      res.status(400).json({ error: "msgId is required for agent-created reminders" });
      return;
    }
    const resolved = await resolveReminderMsgIdForAgentApi(serverId, agentId, msgId);
    if (!resolved.ok) {
      res.status(resolved.status).json({ error: resolved.error });
      return;
    }

    let targetChannelId: string | null = null;
    if (channel != null) {
      if (typeof channel !== "string" || channel.length === 0) {
        res.status(400).json({ error: "channel must be a non-empty string" });
        return;
      }
      const ctx = await channelService.resolveChannelByName(serverId, agentId, channel);
      if (!ctx) {
        setRequestTraceErrorCode(res, "channel_not_found");
        res.status(404).json({ error: "Agent or channel not found" });
        return;
      }
      targetChannelId = ctx.channelId;
    }

    const row = await reminderCrud.createAppReminder({
      serverId,
      ownerAgentId: agentId,
      targetChannelId,
      msgId: resolved.messageId,
      title: title.trim(),
      fireAt: fireAtDate,
      payload: payload ?? null,
      recurrence,
      createdBy: { type: "agent", id: agentId },
    });
    await syncReminderToComputer(req, row, "upsert");
    const [summary] = await reminderService.toReminderSummaries([row], serverId);
    await publishReminderEvent(req.app.get("io"), row, { type: "reminder:scheduled", reminder: summary });

    const response: AgentApiResponseByRoute["reminderCreate"] = { reminder: summary };
    if (warning) response.warning = warning;
    res.status(201);
    sendAgentApiResponse("reminderCreate", res, response);
    recordRaftCliActivity(req, agentId, {
      command: "reminder.schedule",
      summary: "Scheduled reminder",
      target: summary.msgRef ?? row.id,
      correlationId: row.id,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to create reminder",
      logPrefix: "internal.agent-api.reminder-create error:",
      err,
    });
  }
});

registerAgentApiRoute("reminderCancel", ...agentApiRequestValidators("reminderCancel"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const { reminderId } = req.params as AgentApiRequestParamsByRoute["reminderCancel"];
    const existing = await loadOwnedReminder(reminderId, serverId, agentId);
    if (!existing) {
      setRequestTraceErrorCode(res, "reminder_not_found");
      res.status(404).json({ error: "Reminder not found" });
      return;
    }
    if (existing.status !== "scheduled" && existing.status !== "fired") {
      setRequestTraceErrorCode(res, "reminder_not_active");
      res.status(409).json({ error: `Reminder is already ${existing.status}` });
      return;
    }

    const canceled = await reminderCrud.cancelAppReminder(reminderId, {
      actor: { type: "agent", id: agentId },
      expectedVersion: existing.version,
    });
    if (!canceled) {
      setRequestTraceErrorCode(res, "reminder_state_conflict");
      res.status(409).json({ error: "Reminder state changed; refresh and retry" });
      return;
    }
    await syncReminderToComputer(req, canceled, "cancel");

    const [summary] = await reminderService.toReminderSummaries([canceled], serverId);
    await publishReminderEvent(req.app.get("io"), canceled, { type: "reminder:canceled" });

    sendAgentApiResponse("reminderCancel", res, { reminder: summary });
    recordRaftCliActivity(req, agentId, {
      command: "reminder.cancel",
      summary: "Canceled reminder",
      target: summary.msgRef ?? canceled.id,
      correlationId: canceled.id,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to cancel reminder",
      logPrefix: "internal.agent-api.reminder-cancel error:",
      err,
    });
  }
});

registerAgentApiRoute("reminderSnooze", refuseLegacyReminderMutation, ...agentApiRequestValidators("reminderSnooze"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const { reminderId } = req.params as AgentApiRequestParamsByRoute["reminderSnooze"];
    const { delaySeconds } = req.body as AgentApiRequestBodyByRoute["reminderSnooze"];
    const schedule = resolveScheduleInput({ delaySeconds }, Date.now());
    if (!schedule.ok) {
      setRequestTraceErrorCode(res, "reminder_schedule_invalid");
      res.status(400).json({ error: schedule.error });
      return;
    }

    const existing = await loadOwnedReminder(reminderId, serverId, agentId);
    if (!existing) {
      setRequestTraceErrorCode(res, "reminder_not_found");
      res.status(404).json({ error: "Reminder not found" });
      return;
    }
    if (existing.status !== "scheduled" && existing.status !== "fired") {
      setRequestTraceErrorCode(res, "reminder_not_active");
      res.status(409).json({
        error: `Reminder ${existing.id} is ${existing.status}; only scheduled or fired reminders can be snoozed`,
      });
      return;
    }

    const snoozed = await reminderCrud.snoozeAppReminder(existing.id, Number(delaySeconds), {
      actor: { type: "agent", id: agentId },
      expectedVersion: existing.version,
    });
    if (!snoozed) {
      setRequestTraceErrorCode(res, "reminder_state_conflict");
      res.status(409).json({ error: "Reminder state changed; refresh and retry" });
      return;
    }
    await syncReminderToComputer(req, snoozed, "upsert");

    const [summary] = await reminderService.toReminderSummaries([snoozed], serverId);
    await publishReminderEvent(req.app.get("io"), snoozed, { type: "reminder:scheduled", reminder: summary });
    sendAgentApiResponse("reminderSnooze", res, { reminder: summary });
    recordRaftCliActivity(req, agentId, {
      command: "reminder.snooze",
      summary: "Snoozed reminder",
      target: summary.msgRef ?? snoozed.id,
      correlationId: snoozed.id,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to snooze reminder",
      logPrefix: "internal.agent-api.reminder-snooze error:",
      err,
    });
  }
});

registerAgentApiRoute("reminderUpdate", refuseLegacyReminderMutation, ...agentApiRequestValidators("reminderUpdate"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const { reminderId } = req.params as AgentApiRequestParamsByRoute["reminderUpdate"];
    const body = req.body as AgentApiRequestBodyByRoute["reminderUpdate"];

    const existing = await loadOwnedReminder(reminderId, serverId, agentId);
    if (!existing) {
      setRequestTraceErrorCode(res, "reminder_not_found");
      res.status(404).json({ error: "Reminder not found" });
      return;
    }
    if (existing.status === "fired") {
      setRequestTraceErrorCode(res, "reminder_not_active");
      res.status(409).json({
        error: `Reminder ${existing.id} is fired; snooze it back to scheduled before updating, or schedule a new reminder`,
      });
      return;
    }
    if (existing.status !== "scheduled") {
      setRequestTraceErrorCode(res, "reminder_not_active");
      res.status(409).json({ error: `Reminder ${existing.id} is ${existing.status}` });
      return;
    }

    const { fireAt, delaySeconds, repeat, title, tz } = body;
    const mutations = [
      fireAt != null || delaySeconds != null ? "time" : null,
      repeat != null ? "repeat" : null,
      title != null ? "title" : null,
    ].filter(Boolean);
    if (mutations.length !== 1) {
      setRequestTraceErrorCode(res, "reminder_update_invalid");
      res.status(400).json({ error: "update requires exactly one of fireAt/delaySeconds, repeat, or title" });
      return;
    }

    let patch: reminderService.ReminderUpdatePatch;
    let warning: string | undefined;
    if (fireAt != null || delaySeconds != null) {
      const schedule = resolveScheduleInput({ fireAt, delaySeconds }, Date.now());
      if (!schedule.ok) {
        setRequestTraceErrorCode(res, "reminder_schedule_invalid");
        res.status(400).json({ error: schedule.error });
        return;
      }
      warning = schedule.warning;
      patch = fireAt != null
        ? { kind: "fireAt", fireAt: schedule.fireAt }
        : { kind: "delay", delaySeconds: Number(delaySeconds) };
    } else if (repeat != null) {
      const recurrence = parseReminderRecurrence(repeat, tz, res);
      if (!recurrence) return;
      patch = { kind: "recurrence", recurrence };
    } else {
      if (typeof title !== "string" || title.trim().length === 0) {
        setRequestTraceErrorCode(res, "reminder_title_invalid");
        res.status(400).json({ error: "title must be a non-empty string" });
        return;
      }
      if (title.length > REMINDER_MAX_TITLE_LEN) {
        setRequestTraceErrorCode(res, "reminder_title_invalid");
        res.status(400).json({ error: `title must be at most ${REMINDER_MAX_TITLE_LEN} characters` });
        return;
      }
      patch = { kind: "title", title: title.trim() };
    }

    const updated = await reminderCrud.updateAppReminder(existing.id, patch, {
      actor: { type: "agent", id: agentId },
      expectedVersion: existing.version,
    });
    if (!updated) {
      setRequestTraceErrorCode(res, "reminder_state_conflict");
      res.status(409).json({ error: "Reminder state changed; refresh and retry" });
      return;
    }
    await syncReminderToComputer(req, updated, "upsert");

    const [summary] = await reminderService.toReminderSummaries([updated], serverId);
    await publishReminderEvent(req.app.get("io"), updated, { type: "reminder:scheduled", reminder: summary });
    const response: AgentApiResponseByRoute["reminderUpdate"] = { reminder: summary };
    if (warning) response.warning = warning;
    sendAgentApiResponse("reminderUpdate", res, response);
    recordRaftCliActivity(req, agentId, {
      command: "reminder.update",
      summary: "Updated reminder",
      target: summary.msgRef ?? updated.id,
      correlationId: updated.id,
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to update reminder",
      logPrefix: "internal.agent-api.reminder-update error:",
      err,
    });
  }
});

registerAgentApiRoute("appSourceAck", ...agentApiRequestValidators("appSourceAck"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const body = req.body as AgentApiRequestBodyByRoute["appSourceAck"];
    const ack = await ackBuiltInAppSource({
      ...body,
      serverId,
      actingAgentId: agentId,
    });
    if (!ack.ok) {
      res.status(ack.status).json(ack.body);
      return;
    }

    sendAgentApiResponse("appSourceAck", res, ack.response);
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to acknowledge app source",
      logPrefix: "internal.agent-api.app-source-ack error:",
      err,
    });
  }
});

registerAgentApiRoute("reminderLog", ...agentApiRequestValidators("reminderLog"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;
    const { reminderId } = req.params as AgentApiRequestParamsByRoute["reminderLog"];
    const resolved = await reminderCrud.resolveAppHistoricalReminderIdForOwner(reminderId, serverId, agentId);
    if (resolved.kind === "ambiguous") {
      res.status(409).json({ error: "Reminder id prefix is ambiguous" });
      return;
    }
    if (resolved.kind !== "resolved") {
      res.status(404).json({ error: "Reminder not found" });
      return;
    }

    const events = await reminderCrud.listAppReminderEventsForOwner(resolved.reminderId, serverId, agentId);
    sendAgentApiResponse("reminderLog", res, {
      events: reminderService.toReminderEventSummaries(events),
    });
  } catch (err) {
    sendJsonServerError(req, res, {
      error: "Failed to read reminder log",
      logPrefix: "internal.agent-api.reminder-log error:",
      err,
    });
  }
});

function sendRapAppConfigError(res: Response, error: RapAppConfigError): void {
  const status = error.code === "RAP_APP_CONFIG_APP_UNKNOWN"
    ? 404
    : error.code === "RAP_APP_CONFIG_OWNER_MISMATCH"
      ? 403
      : error.code === "RAP_APP_CONFIG_REVISION_STALE"
        ? 409
        : error.code === "RAP_APP_CONFIG_STORED_INVALID"
          ? 500
          : 400;
  res.status(status).json({
    error: error.message,
    errorCode: error.code,
    ...(error.currentRevision === undefined ? {} : { currentRevision: error.currentRevision }),
  });
}

registerAgentApiRoute("appConfigGet", ...agentApiRequestValidators("appConfigGet"), async (req, res) => {
  try {
    const { appId } = req.params as AgentApiRequestParamsByRoute["appConfigGet"];
    const snapshot = await getRapAppConfig({
      serverId: req.serverId!,
      subjectAgentId: req.actingAgentId!,
      appId,
    });
    sendAgentApiResponse("appConfigGet", res, { ...snapshot });
  } catch (error) {
    if (error instanceof RapAppConfigError) {
      sendRapAppConfigError(res, error);
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to read RAP App config",
      logPrefix: "internal.agent-api.app-config-get error:",
      err: error,
    });
  }
});

registerAgentApiRoute("appConfigPatch", ...agentApiRequestValidators("appConfigPatch"), async (req, res) => {
  try {
    const serverId = req.serverId!;
    const subjectAgentId = req.actingAgentId!;
    const { appId } = req.params as AgentApiRequestParamsByRoute["appConfigPatch"];
    const body = req.body as AgentApiRequestBodyByRoute["appConfigPatch"];
    const snapshot = await patchRapAppConfig({
      serverId,
      subjectAgentId,
      appId,
      expectedRevision: body.expectedRevision,
      set: body.set,
      unset: body.unset,
    });
    const orchestrator = req.app.get("agentOrchestrator") as AgentOrchestrator;
    // Task #204: a durable config change must reach the owner's Computer. This
    // is best-effort by design — an offline machine refills on reconnect — so a
    // push failure must not fail the mutation the caller already committed.
    try {
      await pushBuiltInAppConfigForOwner({
        appId,
        serverId,
        ownerAgentId: subjectAgentId,
        orchestrator,
      });
    } catch (pushError) {
      console.error("internal.agent-api.app-config-push error:", pushError);
    }
    sendAgentApiResponse("appConfigPatch", res, { ...snapshot });
    recordRaftCliActivity(req, subjectAgentId, {
      command: "app.config",
      summary: `Updated ${appId} config`,
      target: appId,
      correlationId: `${appId}:${snapshot.revision}`,
    });
  } catch (error) {
    if (error instanceof RapAppConfigError) {
      sendRapAppConfigError(res, error);
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to update RAP App config",
      logPrefix: "internal.agent-api.app-config-patch error:",
      err: error,
    });
  }
});

registerAgentApiRoute("actionPrepare", ...agentApiRequestValidators("actionPrepare"), async (req, res) => {
  try {
    const agentId = req.actingAgentId!;
    const serverId = req.serverId!;

    const body = req.body as AgentApiRequestBodyByRoute["actionPrepare"];
    const idempotency = agentApiIdempotencyScope(agentId, "actionPrepare", body.idempotencyKey, {
      target: body.target,
      action: body.action,
    });
    if (idempotency && await replayAgentApiIdempotentWrite(idempotency, res)) return;
    const actionType = typeof body.action.type === "string" ? body.action.type : null;
    if (
      actionType === "integration:approve_agent_login"
      || actionType === "integration:install_marketplace_app"
      || actionType === "integration:register_app"
      || actionType === "integration:update_app_registration"
    ) {
      res.status(400).json({
        error: "integration action cards must be created by raft integration commands",
        errorCode: "ACTION_TYPE_NOT_PREPARABLE",
      });
      return;
    }

    const target = body.target.trim();
    const resolved = await resolveWritableAgentTarget(serverId, agentId, target);
    if (resolved === "forbidden") {
      res.status(403).json({ error: forbiddenMessageForTarget(target) });
      return;
    }
    if (resolved === "peer-not-found") {
      res.status(404).json({ error: `User or agent not found: @${target.slice(4)}` });
      return;
    }
    if (resolved === "self-dm") {
      res.status(400).json({ error: "Cannot create a DM with yourself" });
      return;
    }
    if (!resolved) {
      res.status(400).json({ error: `Could not resolve target: ${body.target}` });
      return;
    }

    const io = (req.app.get("io") ?? null) as Parameters<typeof actionCardsService.prepareActionCard>[0]["io"];
    let out: Awaited<ReturnType<typeof actionCardsService.prepareActionCard>>;
    try {
      out = await actionCardsService.prepareActionCard({
        serverId,
        requesterAgentId: agentId,
        targetChannelId: resolved.channelId,
        action: body.action,
        io: io ?? null,
        ...(idempotency && {
          beforeCommit: (executor, result) => idempotentAgentApiService.recordAgentApiIdempotentResponse(executor, idempotency, {
            status: 201,
            body: { messageId: result.messageId, metadata: { ...result.metadata } },
          }),
        }),
      });
    } catch (err) {
      if (idempotency && err instanceof idempotentAgentApiService.AgentApiIdempotencyRaceLostError) {
        // A concurrent request with this key committed first; this card
        // rolled back. Answer with the committed first response.
        if (await replayAgentApiIdempotentWrite(idempotency, res)) return;
      }
      throw err;
    }
    res.status(201);
    sendAgentApiResponse("actionPrepare", res, { messageId: out.messageId, metadata: { ...out.metadata } });
    recordRaftCliActivity(req, agentId, {
      command: "action.prepare",
      summary: "Prepared action card",
      target,
      correlationId: out.messageId,
    });
  } catch (err) {
    if (respondToAgentApiIdempotencyConflict(err, res)) return;
    if (err instanceof actionCardsService.ActionCardError) {
      res.status(err.status).json({ error: err.message, errorCode: err.code });
      return;
    }
    sendJsonServerError(req, res, {
      error: "Failed to prepare action card",
      logPrefix: "internal.agent-api.prepare-action error:",
      err,
    });
  }
});

function sendRaftbenchError(error: unknown, res: Response): void {
  if (error instanceof RaftbenchError) {
    res.status(error.status).json({ error: error.message, code: error.code, suggestedNextAction: error.suggestedNextAction, ...(error.issues ? { issues: error.issues } : {}) });
  } else {
    console.error("RaftBench request failed", serializeErrorForLog(error));
    res.status(503).json({ error: "RaftBench could not confirm this request. An interrupted submission may already have been accepted.", code: "raftbench_unavailable", suggestedNextAction: "Retry the same context request or identical submission after a short delay." });
  }
}
registerAgentApiRoute("raftbenchContext", validateAgentApiResponseMiddleware("raftbenchContext"), async (req, res) => {
  const parsed = raftbenchContextQuerySchema.safeParse(req.query);
  if (!parsed.success) {
    res.status(400).json({ error: "Invalid context request.", code: "raftbench_validation_failed", issues: parsed.error.issues, suggestedNextAction: "Use the assigned run ID and nextCursor returned by context." }); return;
  }
  try { sendAgentApiResponse("raftbenchContext", res, await readRaftbenchContext(req.serverId!, req.actingAgentId!, parsed.data)); }
  catch (error) { sendRaftbenchError(error, res); }
});
registerAgentApiRoute("raftbenchSubmit", validateAgentApiResponseMiddleware("raftbenchSubmit"), async (req, res) => {
  const parsed = raftbenchReportSchema.safeParse(req.body);
  if (!parsed.success) {
    res.status(422).json({ error: "Result validation failed; nothing has been saved.", code: "raftbench_validation_failed", issues: parsed.error.issues.map(issue => ({ path: issue.path.join("."), message: issue.message, suggestedFix: "Correct this field to match the result schema. Nothing has been saved." })), suggestedNextAction: "Correct all listed fields once. If still invalid, submit incomplete with the validation failure as its reason." }); return;
  }
  try { sendAgentApiResponse("raftbenchSubmit", res, await submitRaftbenchReport(req.serverId!, req.actingAgentId!, parsed.data)); }
  catch (error) { sendRaftbenchError(error, res); }
});
