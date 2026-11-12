import { agentApiHistoryConsumptionScopeSchema, type AgentApiHistoryConsumptionScope, type AgentMessage } from "@botiverse/raft-shared";
import type { AgentProxyVisibleMessage } from "./agentCredentialProxy";

export type AgentVisibleDeliveryConsumeSource =
  | "spawn_wake_message"
  | "spawn_wake_notice"
  | "agent_api_events_local"
  | "agent_api_events_server"
  | "agent_api_history"
  | "agent_api_send_commit"
  | "server_held_context"
  | "side_effect_preflight_context"
  | "spawn_starting_inbox_update"
  | "thread_join_context_rendered"
  | "verified_contiguous_content_consumption"
  | string;

type VisibleBucket = {
  maxSeq: number;
  boundarySeq: number;
  seqs: Set<number>;
  ids: Set<string>;
};

export type AgentVisibleDeliveryConsumption = {
  targets: string[];
  messagesCount: number;
  shouldSuppress(message: AgentMessage): boolean;
};

type SeenMessage = { seq?: number; message_id?: string; id?: string };

/**
 * Model-seen facts as a freshness decision may use them: what the model has in
 * front of it now, as evidence for a send's `seenUpToSeq` / `seenExactSeqs`
 * and the task-claim preflight. Delivery suppression keeps using the
 * process-wide queries; this view only narrows what counts as evidence.
 */
export type AgentVisibleFreshnessView = {
  getBoundary(target: string): number | undefined;
  isModelSeen(target: string, message: SeenMessage): boolean;
  getExactSeenSeqs(target: string): number[];
};

/** Facts recorded while one RFC 072 context id was current. */
type ContextScope = {
  contextId: string;
  boundaries: Map<string, number>;
  ids: Map<string, Set<string>>;
  exactSeqs: Map<string, Set<number>>;
};

const NO_FRESHNESS_EVIDENCE: AgentVisibleFreshnessView = {
  getBoundary: () => undefined,
  isModelSeen: () => false,
  getExactSeenSeqs: () => [],
};

function getMessageShortId(messageId: string): string {
  return messageId.startsWith("thread-") ? messageId.slice(7) : messageId.slice(0, 8);
}

// History accepts short or full event IDs, including uppercase hex. Keep the
// existing visible key, but validate a full address against the full event ID
// before collapsing it; two different UUIDs may share the same short prefix.
function eventIdFromTarget(target: string): string | undefined {
  return /^agent-event:([0-9a-f]{8}(?:-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})?)$/i
    .exec(target)?.[1].toLowerCase();
}

function canonicalVisibleTarget(target: string): string {
  const eventId = eventIdFromTarget(target);
  return eventId ? `agent-event:${getMessageShortId(eventId)}` : target;
}

// Single source for the agent-visible target key (project-from-single-source, FM2
// discipline). Both pending-message suppression and visible-message consumption
// delegate here so the two sides cannot disagree on the key for the same message.
function computeTarget(
  channelType: string | undefined,
  channelName: string | undefined,
  parentChannelName: string | undefined,
  parentChannelType: string | undefined,
): string {
  if (channelType === "thread" && parentChannelName) {
    const shortId = getMessageShortId(String(channelName ?? ""));
    if (parentChannelType === "dm") {
      return `dm:@${parentChannelName}:${shortId}`;
    }
    return `#${parentChannelName}:${shortId}`;
  }
  if (channelType === "dm") {
    return `dm:@${channelName}`;
  }
  return `#${channelName}`;
}

export function formatAgentMessageVisibleTarget(message: AgentMessage): string {
  return formatProxyVisibleMessageTarget(message);
}

export function formatProxyVisibleMessageTarget(message: AgentProxyVisibleMessage): string {
  // Third-party events retain synthetic DM metadata for transport, but their
  // visible scope is the event. Use the same key for consumption and suppression.
  if (message.third_party_event) {
    return `agent-event:${getMessageShortId(message.third_party_event.id.toLowerCase())}`;
  }
  return computeTarget(message.channel_type, message.channel_name, message.parent_channel_name, message.parent_channel_type);
}

function hasTargetMetadata(message: AgentProxyVisibleMessage): boolean {
  if (message.third_party_event) return true;
  if (message.channel_type === "thread") return Boolean(message.parent_channel_name && message.channel_name);
  return Boolean(message.channel_type && message.channel_name);
}

function visibleMessageId(message: AgentProxyVisibleMessage | AgentMessage): string {
  if (typeof (message as AgentProxyVisibleMessage).message_id === "string") return String((message as AgentProxyVisibleMessage).message_id);
  if (typeof (message as AgentProxyVisibleMessage).id === "string") return String((message as AgentProxyVisibleMessage).id);
  return "";
}

function isSeenIn(boundary: number | undefined, ids: ReadonlySet<string> | undefined, message: SeenMessage): boolean {
  const seq = Number(message.seq ?? 0);
  if (Number.isFinite(seq) && seq > 0 && typeof boundary === "number" && boundary >= Math.floor(seq)) return true;
  const id = visibleMessageId(message);
  return id.length > 0 && ids?.has(id) === true;
}

function sortedSeqs(seqs: ReadonlySet<number> | undefined): number[] {
  return seqs ? [...seqs].sort((a, b) => a - b) : [];
}

function addCappedExactSeqs(map: Map<string, Set<number>>, target: string, seqs: Iterable<number>): void {
  let targetSeqs = map.get(target);
  if (!targetSeqs) {
    targetSeqs = new Set<number>();
    map.set(target, targetSeqs);
  }
  for (const seq of seqs) targetSeqs.add(seq);
  if (targetSeqs.size > MAX_EXACT_SEEN_SEQS_PER_TARGET) {
    const kept = [...targetSeqs].sort((a, b) => a - b).slice(-MAX_EXACT_SEEN_SEQS_PER_TARGET);
    map.set(target, new Set(kept));
  }
}

/**
 * How much of the model's consumption the caller can prove.
 *
 * The boundary is a CONTIGUOUS claim ("everything up to this seq was
 * consumed"), so it can never be derived from a single message: only the
 * caller knows whether what it consumed was contiguous. It used to be
 * guessed from the source NAME (`verified_contiguous_content_consumption`,
 * `thread_join_context_rendered`), which meant a source name had to carry
 * two different meanings at once. The caller now states it explicitly.
 */
export type AgentVisibleDeliveryBoundaryEvidence = "none" | "contiguous" | "thread";

/**
 * What the caller knows about the model's reading of the messages (task #74).
 *
 * - `none`: an attention signal. The message was queued, announced, or
 *   written by the agent itself — nobody read its body.
 * - `rendered`: the bodies were put in front of the model (the prompt carried
 *   them). The model may have read them, but the daemon has not seen the
 *   model act on them.
 * - `consumed`: the runtime consumed the messages (an API read, a history
 *   pull). The read is conclusive.
 *
 * The distinction used to be guessed from the source name, which is how one
 * source name ended up meaning two different things and how a wake that was
 * only announced came to count as a read. The step that hands the bodies over
 * states it instead.
 */
export type AgentVisibleDeliveryReadEvidence = "none" | "rendered" | "consumed";

function canAdvanceBoundaryForTarget(evidence: AgentVisibleDeliveryBoundaryEvidence, target: string): boolean {
  // Model-seen boundary contract: daemon delivery/queue/wake signals prove
  // attention, not contiguous content consumption. A sparse high-seq mention
  // must never advance the high-water boundary and hide older unseen rows.
  if (evidence === "contiguous") return true;
  if (evidence === "thread") {
    // A rendered thread-join package shows the parent/root message and recent
    // thread replies. The recent replies are contiguous only within the thread
    // target; the parent message is an exact visible row, not proof that the
    // parent channel prefix was consumed.
    return target.includes(":");
  }
  return false;
}

/**
 * Owns the daemon's agent-visible delivery ledger.
 *
 * The ledger is the single boundary where APM turns "the model has seen this"
 * into durable process-local facts. It owns both forms of model-seen state:
 * target high-water boundaries for verified rendered/contiguous content
 * consumption, and exact message-id sets for weaker visibility sources such as
 * delivery signals, history/context reads, and self-authored commits. APM
 * should ask this component whether a message is model-seen; it should not
 * infer meaning from raw boundary/id maps or mutate those maps directly.
 *
 * Invariants:
 * - I1 boundary/id synchronization: only verified rendered/contiguous content
 *   consume sources may advance a target boundary, and boundary advancement is
 *   recorded alongside the exact message ids observed in the same target scope.
 *   Current daemon attention/delivery sources can add exact ids but must never
 *   advance a boundary.
 * - I2 visibility authorization: a visible message may enter the ledger only
 *   for the current authorized target projection. Explicit target mismatches
 *   are rejected before any state is written, keeping parent/thread/DM/channel
 *   scopes isolated.
 * - I3 consume/suppress contract: consume returns a suppression matcher for
 *   the just-consumed batch. APM applies that matcher to active inbox and
 *   start-pending buffers; the ledger does not reach out and mutate those
 *   external queues itself.
 * - No raw mutable state leak: callers get read-only boundary/id-set queries
 *   and must route every write through recordConsumed().
 */
/**
 * Cap on exact model-seen seqs kept per target (task #360). Matches the Server's
 * per-send cap on `seenExactSeqs`; the newest seqs are kept.
 */
export const MAX_EXACT_SEEN_SEQS_PER_TARGET = 2_500;

/**
 * Cap on message ids kept per target (task #73).
 *
 * The id sets exist so the delivery gate can recognise a message it already
 * handed to the model. They used to grow with the agent's whole consumed
 * traffic until a stop or a daemon restart, because the handoff no longer
 * clears them. The cap keeps the most recent ids: an evicted id can only
 * cause the server's re-push of that one message to arrive again (a repeat),
 * never a loss.
 */
export const MAX_MESSAGE_IDS_PER_TARGET = 2_500;

/**
 * Add ids to a target's id set, keeping the most recently ADDED ids once the
 * cap is hit. A Set preserves insertion order, so the tail is the newest; an
 * id that is added again keeps its original position and is not refreshed.
 * The worst that costs is one repeat: an id that fell out of the window is no
 * longer recognised, so the server's re-push of that message arrives again.
 *
 * Returns the ids the cap evicted. An evicted id can only cost one repeat:
 * the server's re-push of that message is no longer recognised as already
 * handed to the model. It can never cost a message.
 */
function addCappedMessageIds(targetIds: Set<string>, ids: Iterable<string>): Set<string> {
  const evicted = new Set<string>();
  for (const id of ids) targetIds.add(id);
  if (targetIds.size > MAX_MESSAGE_IDS_PER_TARGET) {
    const kept = new Set([...targetIds].slice(-MAX_MESSAGE_IDS_PER_TARGET));
    for (const id of targetIds) if (!kept.has(id)) evicted.add(id);
    targetIds.clear();
    for (const id of kept) targetIds.add(id);
  }
  return evicted;
}

export class AgentVisibleDeliveryLedger {
  private readonly boundaryByAgent = new Map<string, Map<string, number>>();
  private readonly messageIdsByAgent = new Map<string, Map<string, Set<string>>>();
  private readonly exactSeqsByAgent = new Map<string, Map<string, Set<number>>>();
  // The subset of the facts above that a BODY render proved: the ids and seqs
  // whose content the model actually saw. Attention signals (queued, announced)
  // are recorded in the general sets only.
  private readonly bodyIdsByAgent = new Map<string, Map<string, Set<string>>>();
  private readonly bodySeqsByAgent = new Map<string, Map<string, Set<number>>>();
  private readonly contextScopeByAgent = new Map<string, ContextScope>();

  clearAgent(agentId: string): void {
    this.boundaryByAgent.delete(agentId);
    this.messageIdsByAgent.delete(agentId);
    this.exactSeqsByAgent.delete(agentId);
    this.bodyIdsByAgent.delete(agentId);
    this.bodySeqsByAgent.delete(agentId);
    this.contextScopeByAgent.delete(agentId);
  }

  /**
   * Narrow the agent's read position to what a body render proved, for a
   * session handoff. The high-water boundary stays (only body consumption
   * advances it); the id and exact-seq sets keep only their body-proven
   * entries. Everything an attention signal left behind is dropped, so the
   * server's re-push of a merely-notified message still reaches the
   * replacement.
   */
  keepOnlyBodyFacts(agentId: string): void {
    const ids = this.messageIdsByAgent.get(agentId);
    if (ids) {
      const bodies = this.bodyIdsByAgent.get(agentId);
      for (const [target, set] of ids) {
        const bodySet = bodies?.get(target);
        for (const id of [...set]) if (!bodySet?.has(id)) set.delete(id);
      }
    }
    const seqs = this.exactSeqsByAgent.get(agentId);
    if (seqs) {
      const bodies = this.bodySeqsByAgent.get(agentId);
      for (const [target, set] of seqs) {
        const bodySet = bodies?.get(target);
        for (const seq of [...set]) if (!bodySet?.has(seq)) set.delete(seq);
      }
    }
  }

  /**
   * Freshness evidence for `agentId` (glimpse G0). `context` is the RFC 072
   * context signal the CLI honours (present when the launch has passive AX
   * on), so the daemon and the CLI scope evidence identically. With a signal,
   * only facts recorded under its id count — none when the id is unknown — so
   * after a compaction what the model saw before it no longer vouches for a
   * send or a claim. Without one, the facts stay process-wide, as before.
   * Delivery suppression never reads this view: those facts must outlive a
   * compaction.
   */
  freshnessView(agentId: string, context: { contextId: string | null } | undefined): AgentVisibleFreshnessView {
    if (!context) {
      return {
        getBoundary: (target) => this.getBoundary(agentId, target),
        isModelSeen: (target, message) => this.isModelSeen(agentId, target, message),
        getExactSeenSeqs: (target) => this.getExactSeenSeqs(agentId, target),
      };
    }
    const scope = this.contextScopeByAgent.get(agentId);
    if (!context.contextId || scope?.contextId !== context.contextId) return NO_FRESHNESS_EVIDENCE;
    return {
      getBoundary: (target) => scope.boundaries.get(canonicalVisibleTarget(target)),
      isModelSeen: (target, message) => {
        const key = canonicalVisibleTarget(target);
        return isSeenIn(scope.boundaries.get(key), scope.ids.get(key), message);
      },
      getExactSeenSeqs: (target) => sortedSeqs(scope.exactSeqs.get(canonicalVisibleTarget(target))),
    };
  }

  /**
   * Exact seqs this target's model has seen (task #360), ascending. They never
   * advance the boundary; the send preflight hands the ones above the boundary
   * to the Server as `seenExactSeqs`, which excludes exactly those rows from
   * the unread count without assuming the gaps were read.
   */
  getExactSeenSeqs(agentId: string, target: string): number[] {
    return sortedSeqs(this.exactSeqsByAgent.get(agentId)?.get(canonicalVisibleTarget(target)));
  }

  hasAgentState(agentId: string): boolean {
    return this.boundaryByAgent.has(agentId) || this.messageIdsByAgent.has(agentId);
  }

  getBoundary(agentId: string, target: string): number | undefined {
    return this.boundaryByAgent.get(agentId)?.get(canonicalVisibleTarget(target));
  }

  getMessageIdSet(agentId: string, target: string): Set<string> | undefined {
    return this.messageIdsByAgent.get(agentId)?.get(canonicalVisibleTarget(target));
  }

  isModelSeen(agentId: string, target: string, message: SeenMessage): boolean {
    return isSeenIn(this.getBoundary(agentId, target), this.getMessageIdSet(agentId, target), message);
  }

  /**
   * Whether the model READ this message's body — the high-water boundary
   * (only body consumption advances it) or a body-proven id/seq. Attention
   * signals (queued, announced) do NOT count: they say the message was
   * mentioned, not that anybody read it.
   */
  isBodyModelSeen(agentId: string, target: string, message: SeenMessage): boolean {
    const key = canonicalVisibleTarget(target);
    const boundary = this.getBoundary(agentId, key);
    const seq = Number(message.seq ?? 0);
    if (Number.isFinite(seq) && seq > 0 && typeof boundary === "number" && boundary >= Math.floor(seq)) return true;
    const id = visibleMessageId(message);
    if (id.length > 0 && this.bodyIdsByAgent.get(agentId)?.get(key)?.has(id) === true) return true;
    if (Number.isFinite(seq) && seq > 0 && this.bodySeqsByAgent.get(agentId)?.get(key)?.has(Math.floor(seq)) === true) return true;
    return false;
  }

  /**
   * `contextId` is the RFC 072 context id current when the content reached the
   * model under passive AX (see freshnessView); without one the facts are
   * process-wide only.
   */
  recordConsumed(
    agentId: string,
    input: {
      historyScope?: AgentApiHistoryConsumptionScope;
      target?: string;
      messages: AgentProxyVisibleMessage[];
      boundarySeq?: number;
      source: AgentVisibleDeliveryConsumeSource;
      contextId?: string | null;
      /**
       * What the caller knows about the model's reading (task #74). The step
       * that hands the bodies over states it; the source name never does.
       *
       * `rendered` (or `consumed`) means THIS call put the message bodies in
       * front of the model. `none` means an attention signal: queued,
       * announced, or written by the agent itself — nobody read its body. At
       * a session handoff the body facts survive and the attention facts do
       * not, so a wrong `rendered` would drop a message the replacement has
       * never seen. The rule is "may repeat, never lose".
       */
      readEvidence?: AgentVisibleDeliveryReadEvidence;
      /** What the caller can prove about contiguity; defaults to no evidence. */
      boundaryEvidence?: AgentVisibleDeliveryBoundaryEvidence;
    },
  ): AgentVisibleDeliveryConsumption | null {
    if (input.messages.length === 0 && (!input.target || typeof input.boundarySeq !== "number")) return null;
    const explicitTarget = input.target ? canonicalVisibleTarget(input.target) : undefined;
    const explicitEventId = input.target ? eventIdFromTarget(input.target) : undefined;
    const scopeResult = agentApiHistoryConsumptionScopeSchema.safeParse(input.historyScope);
    const historyScope = input.source === "agent_api_history" && scopeResult.success
      && scopeResult.data.agent_id === agentId && scopeResult.data.target === input.target
      ? scopeResult.data : undefined;
    const legacyDmIds = new Set<string>();
    if (historyScope) {
      for (const message of input.messages) {
        // The scope alone is insufficient: each returned body must belong to
        // the authorized channel. No sequence-based fallback across targets.
        if (message.channel_id === historyScope.channel_id && visibleMessageId(message)) {
          legacyDmIds.add(visibleMessageId(message));
        }
      }
    }
    const byTarget = new Map<string, VisibleBucket>();
    const ensureBucket = (target: string): VisibleBucket => {
      let bucket = byTarget.get(target);
      if (!bucket) {
        bucket = { maxSeq: 0, boundarySeq: 0, seqs: new Set<number>(), ids: new Set<string>() };
        byTarget.set(target, bucket);
      }
      return bucket;
    };

    for (const message of input.messages) {
      const messageTarget = formatProxyVisibleMessageTarget(message);
      const fullEventMismatch = explicitEventId?.length === 36
        && message.third_party_event?.id.toLowerCase() !== explicitEventId;
      if (input.target && hasTargetMetadata(message) && (messageTarget !== explicitTarget || fullEventMismatch)) {
        throw new Error(`AgentVisibleDeliveryLedger target mismatch: explicit target ${input.target} does not match visible message target ${messageTarget}`);
      }
      const target = explicitTarget ?? messageTarget;
      if (!target) continue;
      const bucket = ensureBucket(target);
      const seq = Number(message.seq ?? 0);
      if (Number.isFinite(seq) && seq > 0) {
        const normalizedSeq = Math.floor(seq);
        bucket.maxSeq = Math.max(bucket.maxSeq, normalizedSeq);
        bucket.seqs.add(normalizedSeq);
      }
      const id = visibleMessageId(message);
      if (id.length > 0) bucket.ids.add(id);
    }

    if (explicitTarget && typeof input.boundarySeq === "number" && Number.isFinite(input.boundarySeq) && input.boundarySeq > 0) {
      const bucket = ensureBucket(explicitTarget);
      bucket.boundarySeq = Math.max(bucket.boundarySeq, Math.floor(input.boundarySeq));
    }
    if (byTarget.size === 0) return null;

    const boundaryAdvancingTargets = new Set<string>();
    for (const [target, bucket] of byTarget) {
      const previousBoundary = this.getBoundary(agentId, target);
      const advancesBoundary = canAdvanceBoundaryForTarget(input.boundaryEvidence ?? "none", target);
      if (advancesBoundary) {
        boundaryAdvancingTargets.add(target);
        const highWaterSeq = Math.max(bucket.maxSeq, bucket.boundarySeq);
        const boundaryMap = this.boundaryMap(agentId);
        boundaryMap.set(target, Math.max(previousBoundary ?? 0, highWaterSeq));
      }
      const evictedIds = bucket.ids.size > 0
        ? addCappedMessageIds(this.messageIdSet(agentId, target), bucket.ids)
        : undefined;
      if (bucket.seqs.size > 0) this.recordExactSeqs(agentId, target, bucket.seqs);
      if (input.readEvidence !== undefined && input.readEvidence !== "none") {
        this.recordBodyFacts(agentId, target, bucket);
      }
      this.assertConsumedInvariants("recordConsumed", agentId, target, bucket, {
        advancesBoundary,
        previousBoundary,
        ...(evictedIds && evictedIds.size > 0 ? { evictedIds } : {}),
      });
    }
    if (input.contextId) this.recordContextScope(agentId, input.contextId, byTarget, boundaryAdvancingTargets);

    return {
      targets: [...byTarget.keys()],
      messagesCount: input.messages.length,
      shouldSuppress(message: AgentMessage): boolean {
        const target = formatAgentMessageVisibleTarget(message);
        const bucket = byTarget.get(target);
        if (historyScope) {
          return Boolean(!message.third_party_event
            && message.channel_id === historyScope.channel_id
            && message.channel_type === historyScope.channel_type
            && (message.channel_type === "dm" || message.parent_channel_type === "dm")
            && legacyDmIds.has(visibleMessageId(message)));
        }
        if (!bucket) return false;
        const seq = typeof message.seq === "number" ? Math.floor(message.seq) : 0;
        const id = visibleMessageId(message);
        const advancesBoundary = boundaryAdvancingTargets.has(target);
        return (advancesBoundary && seq > 0 && bucket.boundarySeq > 0 && seq <= bucket.boundarySeq)
          || (seq > 0 && bucket.seqs.has(seq))
          || (id.length > 0 && bucket.ids.has(id));
      },
    };
  }

  private boundaryMap(agentId: string): Map<string, number> {
    let map = this.boundaryByAgent.get(agentId);
    if (!map) {
      map = new Map<string, number>();
      this.boundaryByAgent.set(agentId, map);
    }
    return map;
  }

  private recordExactSeqs(agentId: string, target: string, seqs: Iterable<number>): void {
    let map = this.exactSeqsByAgent.get(agentId);
    if (!map) {
      map = new Map<string, Set<number>>();
      this.exactSeqsByAgent.set(agentId, map);
    }
    addCappedExactSeqs(map, target, seqs);
  }

  /** Record the ids and seqs a body render proved, per target. */
  private recordBodyFacts(agentId: string, target: string, bucket: VisibleBucket): void {
    if (bucket.ids.size > 0) {
      let map = this.bodyIdsByAgent.get(agentId);
      if (!map) {
        map = new Map<string, Set<string>>();
        this.bodyIdsByAgent.set(agentId, map);
      }
      let set = map.get(target);
      if (!set) {
        set = new Set<string>();
        map.set(target, set);
      }
      addCappedMessageIds(set, bucket.ids);
    }
    if (bucket.seqs.size > 0) {
      let map = this.bodySeqsByAgent.get(agentId);
      if (!map) {
        map = new Map<string, Set<number>>();
        this.bodySeqsByAgent.set(agentId, map);
      }
      // The same cap as the exact-seq history: this collection exists to be
      // consulted by the handoff filter, and an uncapped one would grow with
      // the agent's whole consumed traffic.
      addCappedExactSeqs(map, target, bucket.seqs);
    }
  }

  /** A new context id starts an empty scope: the previous context's facts are gone from the model. */
  private recordContextScope(
    agentId: string,
    contextId: string,
    byTarget: ReadonlyMap<string, VisibleBucket>,
    boundaryAdvancingTargets: ReadonlySet<string>,
  ): void {
    let scope = this.contextScopeByAgent.get(agentId);
    if (scope?.contextId !== contextId) {
      scope = { contextId, boundaries: new Map(), ids: new Map(), exactSeqs: new Map() };
      this.contextScopeByAgent.set(agentId, scope);
    }
    for (const [target, bucket] of byTarget) {
      if (boundaryAdvancingTargets.has(target)) {
        scope.boundaries.set(target, Math.max(scope.boundaries.get(target) ?? 0, bucket.maxSeq, bucket.boundarySeq));
      }
      if (bucket.ids.size > 0) {
        let ids = scope.ids.get(target);
        if (!ids) {
          ids = new Set<string>();
          scope.ids.set(target, ids);
        }
        // The same cap as the process-wide id set: a context id can outlive
        // many sessions (runtimes that never report a compaction reuse it),
        // so this collection must not grow with the traffic either.
        addCappedMessageIds(ids, bucket.ids);
      }
      if (bucket.seqs.size > 0) addCappedExactSeqs(scope.exactSeqs, target, bucket.seqs);
    }
  }

  private messageIdMap(agentId: string): Map<string, Set<string>> {
    let map = this.messageIdsByAgent.get(agentId);
    if (!map) {
      map = new Map<string, Set<string>>();
      this.messageIdsByAgent.set(agentId, map);
    }
    return map;
  }

  private messageIdSet(agentId: string, target: string): Set<string> {
    const map = this.messageIdMap(agentId);
    let ids = map.get(target);
    if (!ids) {
      ids = new Set<string>();
      map.set(target, ids);
    }
    return ids;
  }

  private assertConsumedInvariants(
    context: string,
    agentId: string,
    target: string,
    bucket: VisibleBucket,
    options: { advancesBoundary: boolean; previousBoundary: number | undefined; evictedIds?: ReadonlySet<string> },
  ): void {
    const boundary = this.getBoundary(agentId, target);
    if (!options.advancesBoundary && boundary !== options.previousBoundary) {
      throw new Error(`Agent visible delivery ledger invariant violation after ${context}: set-only source advanced boundary for ${agentId} ${target}`);
    }

    if (options.advancesBoundary) {
      const highWaterSeq = Math.max(bucket.maxSeq, bucket.boundarySeq);
      const normalizedBoundary = boundary ?? 0;
      if (normalizedBoundary < (options.previousBoundary ?? 0)) {
        throw new Error(`Agent visible delivery ledger invariant violation after ${context}: boundary regressed for ${agentId} ${target}`);
      }
      if (highWaterSeq > 0 && normalizedBoundary < highWaterSeq) {
        throw new Error(`Agent visible delivery ledger invariant violation after ${context}: boundary below consumed high-water for ${agentId} ${target}`);
      }
    }

    if (bucket.ids.size > 0) {
      const targetIds = this.getMessageIdSet(agentId, target);
      for (const id of bucket.ids) {
        // The id cap may have evicted this very id (a batch larger than the
        // cap). The eviction costs a repeat, never a message.
        if (options.evictedIds?.has(id) === true) continue;
        if (targetIds?.has(id) !== true) {
          throw new Error(`Agent visible delivery ledger invariant violation after ${context}: missing consumed message id for ${agentId} ${target}`);
        }
      }
    }
  }
}
