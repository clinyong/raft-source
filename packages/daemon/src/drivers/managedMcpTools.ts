import type { ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { TSchema } from "typebox";
import {
  MCP_OUTCOME_UNKNOWN_TEXT,
  flattenManagedMcpPromptText,
  formatManagedMcpPromptName,
  type ManagedMcpCallResult,
  type ManagedMcpRuntimeSnapshot,
  type ManagedMcpRuntimeTool,
} from "@botiverse/raft-shared";

const MANAGED_MCP_HTTP_TIMEOUT_MS = 25_000;

class ManagedMcpHttpError extends Error {
  constructor(message: string, readonly status: number, readonly code: string | null) {
    super(message);
    this.name = "ManagedMcpHttpError";
  }
}
async function requestJson<T>(input: {
  url: URL;
  token: string;
  method?: "GET" | "POST";
  body?: unknown;
  signal?: AbortSignal;
}): Promise<T> {
  const timeout = AbortSignal.timeout(MANAGED_MCP_HTTP_TIMEOUT_MS);
  const signal = input.signal ? AbortSignal.any([input.signal, timeout]) : timeout;
  const response = await fetch(input.url, {
    method: input.method ?? "GET",
    headers: {
      Authorization: `Bearer ${input.token}`,
      "X-Slock-Agent-Active-Capabilities": "mcp",
      ...(input.body === undefined ? {} : { "Content-Type": "application/json" }),
    },
    ...(input.body === undefined ? {} : { body: JSON.stringify(input.body) }),
    signal,
  });
  const payload = await response.json().catch(() => null) as { error?: unknown; code?: unknown } | T | null;
  if (!response.ok) {
    const error = typeof (payload as { error?: unknown } | null)?.error === "string"
      ? (payload as { error: string }).error
      : `HTTP ${response.status}`;
    const code = typeof (payload as { code?: unknown } | null)?.code === "string"
      ? (payload as { code: string }).code
      : null;
    throw new ManagedMcpHttpError(error, response.status, code);
  }
  return payload as T;
}

function isRuntimeSnapshot(value: unknown): value is ManagedMcpRuntimeSnapshot {
  return typeof value === "object"
    && value !== null
    && (value as { catalogVersion?: unknown }).catalogVersion === 1
    && Array.isArray((value as { tools?: unknown }).tools);
}

/** A remote tool error's text, capped; a cut is marked so the model doesn't take it as the whole answer. */
export const MANAGED_MCP_ERROR_TEXT_LIMIT = 4_000;

function resultText(result: ManagedMcpCallResult, tool: Pick<ManagedMcpRuntimeTool, "runtimeName">): string {
  const text = result.content
    .filter((block): block is Extract<ManagedMcpCallResult["content"][number], { type: "text" }> => block.type === "text")
    .map((block) => block.text)
    .join("\n");
  // Same words as raft-sdk: name the tool.
  if (!text) return `${tool.runtimeName} failed`;
  return text.length > MANAGED_MCP_ERROR_TEXT_LIMIT ? `${text.slice(0, MANAGED_MCP_ERROR_TEXT_LIMIT)}…[truncated]` : text;
}

function buildTool(input: {
  tool: ManagedMcpRuntimeTool;
  serverUrl: string;
  token: string;
  exposure: ToolDefinition["exposure"];
}): ToolDefinition {
  const { tool } = input;
  return {
    name: tool.runtimeName,
    // Names, title and description come from the remote server (or the connector's setup):
    // names quoted on one line so they cannot break out of the text they sit in.
    label: tool.title ? flattenManagedMcpPromptText(tool.title) : `${formatManagedMcpPromptName(tool.serverName)}: ${formatManagedMcpPromptName(tool.toolName)}`,
    description: tool.description || `Call ${formatManagedMcpPromptName(tool.toolName)} on the managed MCP server ${formatManagedMcpPromptName(tool.serverName)}.`,
    promptSnippet: `${tool.runtimeName}: ${tool.description ? flattenManagedMcpPromptText(tool.description) : `Call ${formatManagedMcpPromptName(tool.toolName)} on ${formatManagedMcpPromptName(tool.serverName)}`}`,
    parameters: tool.inputSchema as TSchema,
    // Managed MCP tools are deferred by default: their full schemas are not
    // declared to the model up front; the session's tool_search loads a match
    // (and its schema) on demand before the next model call. This keeps large
    // servers (Linear, Yokii) out of the fixed prompt until actually needed.
    // The daemon still executes the call through agent-api either way. The
    // caller can opt back to "direct" (full declaration, the pre-tool-search
    // behavior) when tool_search is not available for the session.
    exposure: input.exposure ?? "deferred",
    async execute(_toolCallId, params, signal) {
      let result: ManagedMcpCallResult;
      try {
        result = await requestJson<ManagedMcpCallResult>({
          url: new URL("/internal/agent-api/mcp/call", input.serverUrl),
          token: input.token,
          method: "POST",
          body: {
            mcpServerId: tool.mcpServerId,
            toolName: tool.toolName,
            arguments: params as Record<string, unknown>,
            expectedConfigVersion: tool.configVersion,
            expectedAssignmentVersion: tool.assignmentVersion,
          },
          signal,
        });
      } catch (error) {
        // An HTTP answer with Raft's JSON `code` is Raft's own (its agent-facing
        // text). A transport failure mid-call, or a 5xx from something in between
        // (no Raft code), has an unknown outcome: same words as raft-sdk.
        if (error instanceof ManagedMcpHttpError && !(error.status >= 500 && error.code === null)) throw error;
        if (signal?.aborted) throw error;
        throw new Error(MCP_OUTCOME_UNKNOWN_TEXT);
      }
      if (result.isError) throw new Error(resultText(result, tool));
      return {
        content: result.content.map((block) => block.type === "text"
          ? { type: "text" as const, text: block.text }
          : { type: "image" as const, data: block.data, mimeType: block.mimeType }),
        details: {
          managedMcp: true,
          mcpServerId: tool.mcpServerId,
          toolName: tool.toolName,
          ...(result.structuredContent ? { structuredContent: result.structuredContent } : {}),
        },
      };
    },
  };
}

export async function createManagedMcpPiTools(input: {
  serverUrl: string;
  agentCredentialKey: string | null | undefined;
  onWarning?: (message: string) => void;
  /** Tool declaration mode: "deferred" (default) keeps schemas out of the fixed prompt until tool_search loads them; "direct" declares them fully up front. */
  exposure?: ToolDefinition["exposure"];
}): Promise<ToolDefinition[]> {
  if (!input.agentCredentialKey) return [];
  try {
    const snapshot = await requestJson<ManagedMcpRuntimeSnapshot>({
      url: new URL("/internal/agent-api/mcp/tools", input.serverUrl),
      token: input.agentCredentialKey,
    });
    if (!isRuntimeSnapshot(snapshot)) throw new Error("Managed MCP snapshot contract is invalid");
    return snapshot.tools.map((tool) => buildTool({
      tool,
      serverUrl: input.serverUrl,
      token: input.agentCredentialKey!,
      exposure: input.exposure,
    }));
  } catch (error) {
    const detail = error instanceof ManagedMcpHttpError
      ? `${error.code ?? "managed_mcp_http_error"} (${error.status})`
      : error instanceof Error ? error.message.slice(0, 200) : "unknown error";
    input.onWarning?.(`Managed MCP tools unavailable for this session: ${detail}`);
    return [];
  }
}
