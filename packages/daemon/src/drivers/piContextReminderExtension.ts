import type { ExtensionContext, ExtensionFactory } from "@earendil-works/pi-coding-agent";

const CONTEXT_REMINDER_MARGIN_TOKENS = 32_000;
const CONTEXT_REMINDER_PREFIX = "[Raft 系统提醒]";
const CONTEXT_REMINDER_STATES = new WeakMap<object, PiContextReminderState>();

interface PiContextReminderState {
  agentId: string;
  sessionId: string;
  reminderQueued: boolean;
  pendingNotice: string | null;
}

type GetCompactionReserveTokens = (model: NonNullable<ExtensionContext["model"]>) => number;

export function createPiContextReminderExtensions(
  runtime: string | undefined,
  agentId: string,
  getCompactionReserveTokens: GetCompactionReserveTokens,
): ExtensionFactory[] {
  return runtime === "pi"
    ? [createPiContextReminderExtension(agentId, getCompactionReserveTokens)]
    : [];
}

export function createPiContextReminderExtension(
  agentId: string,
  getCompactionReserveTokens: GetCompactionReserveTokens,
): ExtensionFactory {
  return (pi) => {
    const stateFor = (context: ExtensionContext): PiContextReminderState | undefined => {
      if (!agentId) return undefined;
      const sessionId = context.sessionManager.getSessionId();
      const manager = context.sessionManager as object;
      let state = CONTEXT_REMINDER_STATES.get(manager);
      if (!state || state.agentId !== agentId || state.sessionId !== sessionId) {
        state = { agentId, sessionId, reminderQueued: false, pendingNotice: null };
        CONTEXT_REMINDER_STATES.set(manager, state);
      }
      return state;
    };

    pi.on("turn_end", (_event, context) => {
      const state = stateFor(context);
      if (!state || state.reminderQueued) return;

      try {
        const model = context.model;
        const usage = context.getContextUsage();
        const tokens = usage?.tokens;
        const contextWindow = usage?.contextWindow;
        const reserveTokens = model ? getCompactionReserveTokens(model) : undefined;
        if (
          typeof tokens !== "number" || !Number.isSafeInteger(tokens) || tokens < 0
          || typeof contextWindow !== "number" || !Number.isSafeInteger(contextWindow) || contextWindow <= 0
          || typeof reserveTokens !== "number" || !Number.isSafeInteger(reserveTokens) || reserveTokens < 0
        ) return;

        // The 32,000-token margin is provisional and needs validation against observed Pi sessions.
        const threshold = Math.min(
          0.70 * contextWindow,
          contextWindow - reserveTokens - CONTEXT_REMINDER_MARGIN_TOKENS,
        );
        if (threshold <= 0 || tokens < threshold) return;

        const percentage = Math.round((tokens / contextWindow) * 100);
        state.pendingNotice = `${CONTEXT_REMINDER_PREFIX} 上下文已用到约 ${percentage}%，请先把交接写入 MEMORY.md，再运行 raft session reset。`;
        state.reminderQueued = true;
      } catch {
        // Missing or invalid SDK usage/settings fail closed: no guessed defaults.
      }
    });

    pi.on("input", (event, context) => {
      const state = stateFor(context);
      if (!state?.pendingNotice || event.streamingBehavior !== undefined || !context.isIdle()) {
        return { action: "continue" };
      }

      const notice = state.pendingNotice;
      state.pendingNotice = null;
      return {
        action: "transform",
        text: `${notice}\n\n${event.text}`,
        ...(event.images ? { images: event.images } : {}),
      };
    });
  };
}
