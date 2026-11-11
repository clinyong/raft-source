import type { Command } from "commander";
import { defineCommand, registerCliCommand } from "../../core/command";
import type { CommandRuntimeOptions } from "../../core/context";
import { CliError } from "../../core/errors";
import { writeText, adoptCliReplyText } from "../../core/renderer";
import { createAgentApiSurfaceClient } from "../../agentApiPath";

export const sessionResetCommand = defineCommand({
  name: "reset",
  description: "Request a fresh session after your current turn ends.",
}, async (ctx) => {
  const client = ctx.createApiClient(ctx.loadAgentContext());
  const result = await createAgentApiSurfaceClient(client).self.sessionReset();
  if (!result.ok || !result.data) {
    throw new CliError({ code: "CHECK_FAILED", message: result.error ?? `HTTP ${result.status}` });
  }
  writeText(ctx.io, adoptCliReplyText("Session reset accepted; the current turn must finish first.\n"));
});

export function registerSessionResetCommand(parent: Command, runtimeOptions?: CommandRuntimeOptions): void {
  registerCliCommand(parent, sessionResetCommand, runtimeOptions);
}
