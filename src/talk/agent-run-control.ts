/**
 * Runtime adapter for realtime voice control of active OpenClaw agent runs.
 *
 * The shared module owns classification and message contracts; this adapter
 * binds those contracts to embedded-run abort, status, and steering primitives.
 */
import type { EmbeddedAgentQueueMessageOutcome } from "../agents/embedded-agent-runner/runs.js";
import { getDiagnosticSessionActivitySnapshot } from "../logging/diagnostic-run-activity.js";
import {
  buildRealtimeVoiceAgentCancelProviderResult,
  buildRealtimeVoiceAgentFollowupSteeringText,
  formatRealtimeVoiceAgentQueueRejection,
  formatRealtimeVoiceAgentStatus,
  resolveRealtimeVoiceAgentControlIntent,
  type RealtimeVoiceAgentControlResult,
  type RealtimeVoiceAgentRunActivity,
} from "./agent-run-control-shared.js";
import type { TalkEvent } from "./talk-events.js";

export {
  buildRealtimeVoiceAgentCancelProviderResult,
  buildRealtimeVoiceAgentControlSpeechMessage,
  classifyRealtimeVoiceAgentControlText,
  normalizeRealtimeVoiceAgentControlMode,
  parseRealtimeVoiceAgentControlToolArgs,
  REALTIME_VOICE_AGENT_CONTROL_MODES,
  REALTIME_VOICE_AGENT_CONTROL_TOOL,
  REALTIME_VOICE_AGENT_CONTROL_TOOL_NAME,
  resolveRealtimeVoiceAgentControlIntent,
  shouldAutoControlRealtimeVoiceAgentText,
  type RealtimeVoiceAgentControlMode,
  type RealtimeVoiceAgentControlIntent,
  type RealtimeVoiceAgentControlProviderResult,
  type RealtimeVoiceAgentControlResult,
} from "./agent-run-control-shared.js";

type RealtimeVoiceAgentControlDeps = {
  abortEmbeddedAgentRun: (sessionId: string) => boolean;
  queueEmbeddedAgentMessageWithOutcomeAsync: (
    sessionId: string,
    text: string,
    options?: {
      steeringMode?: "all";
      debounceMs?: number;
      isInboundUserMessage?: boolean;
      taskSuggestionDeliveryMode?: undefined;
    },
  ) => Promise<EmbeddedAgentQueueMessageOutcome>;
  getDiagnosticSessionActivitySnapshot: (params: {
    sessionId?: string;
    sessionKey?: string;
  }) => RealtimeVoiceAgentRunActivity;
  resolveActiveEmbeddedRunSessionId: (sessionKey: string) => string | undefined;
};

/** Apply a spoken status, cancel, steer, or follow-up request to an active run. */
export async function controlRealtimeVoiceAgentRun(
  params: {
    sessionKey: string;
    text: string;
    mode?: unknown;
    recentEvents?: readonly TalkEvent[];
  },
  providedDeps?: RealtimeVoiceAgentControlDeps,
): Promise<RealtimeVoiceAgentControlResult> {
  const sessionKey = params.sessionKey.trim();
  const text = params.text.trim();
  const intent = resolveRealtimeVoiceAgentControlIntent({ text, mode: params.mode });
  const mode = intent.mode;
  const projections =
    providedDeps ?? (await import("../agents/embedded-agent-runner/active-run-projections.js"));
  let sessionId = projections.resolveActiveEmbeddedRunSessionId(sessionKey);

  // Status and inactive-run replies do not need the mutating agent runtime.
  // Keep them available on a cold Gateway or when action dependencies cannot load.
  if (mode === "status") {
    const activity = (
      providedDeps?.getDiagnosticSessionActivitySnapshot ?? getDiagnosticSessionActivitySnapshot
    )({ sessionId, sessionKey });
    const active = Boolean(sessionId || activity.activeWorkKind || activity.hasActiveEmbeddedRun);
    return {
      ok: true,
      mode,
      sessionKey,
      ...(sessionId ? { sessionId } : {}),
      active,
      message: formatRealtimeVoiceAgentStatus({
        active,
        recentEvents: params.recentEvents,
        activity,
      }),
      speak: true,
      show: true,
      suppress: false,
    };
  }

  const noActiveRun = (): RealtimeVoiceAgentControlResult => ({
    ok: false,
    mode,
    sessionKey,
    active: false,
    ...(mode === "cancel" ? { aborted: false } : { queued: false }),
    reason: "no_active_run",
    message: `There is no active OpenClaw run to ${mode === "cancel" ? "cancel" : "steer"}.`,
    speak: true,
    show: true,
    suppress: false,
  });
  if (!sessionId) {
    return noActiveRun();
  }
  const commands = providedDeps ?? (await import("../agents/embedded-agent-runner/runs.js"));
  // Loading commands can outlive the active run; resolve the authoritative target again.
  sessionId = projections.resolveActiveEmbeddedRunSessionId(sessionKey);
  if (!sessionId) {
    return noActiveRun();
  }
  if (mode === "cancel") {
    const aborted = commands.abortEmbeddedAgentRun(sessionId);
    const message = aborted
      ? "Cancelled the active OpenClaw run."
      : "OpenClaw could not cancel the active run.";
    return {
      ok: aborted,
      mode,
      sessionKey,
      sessionId,
      active: true,
      aborted,
      ...(aborted ? {} : { reason: "abort_rejected" }),
      message,
      speak: true,
      show: true,
      suppress: false,
      ...(aborted ? { providerResult: buildRealtimeVoiceAgentCancelProviderResult(message) } : {}),
    };
  }

  // Steering and follow-up both enqueue to the active run; follow-up is wrapped
  // so the runner treats it as deferred context instead of an immediate pivot.
  const steerText = mode === "followup" ? buildRealtimeVoiceAgentFollowupSteeringText(text) : text;
  const outcome = await commands.queueEmbeddedAgentMessageWithOutcomeAsync(sessionId, steerText, {
    steeringMode: "all",
    debounceMs: 0,
    isInboundUserMessage: true,
    // Talk cannot present task suggestions, so spoken user input must not inherit
    // a capable TUI run's model-facing task tools.
    taskSuggestionDeliveryMode: undefined,
  });
  if (!outcome.queued) {
    return {
      ok: false,
      mode,
      sessionKey,
      sessionId: outcome.sessionId,
      active: true,
      queued: false,
      reason: outcome.reason,
      message: formatRealtimeVoiceAgentQueueRejection(mode, outcome.reason),
      speak: true,
      show: true,
      suppress: false,
    };
  }

  return {
    ok: true,
    mode,
    sessionKey,
    sessionId: outcome.sessionId,
    active: true,
    queued: true,
    target: outcome.target,
    message:
      mode === "followup"
        ? "Queued that follow-up for the active OpenClaw run."
        : "Got it. I steered the active run.",
    speak: true,
    show: true,
    suppress: false,
    ...(outcome.enqueuedAtMs !== undefined ? { enqueuedAtMs: outcome.enqueuedAtMs } : {}),
    ...(outcome.deliveredAtMs !== undefined ? { deliveredAtMs: outcome.deliveredAtMs } : {}),
  };
}
