import { normalizeTalkSection } from "../config/talk.js";
import type { OpenClawConfig } from "../config/types.openclaw.js";
import { createPluginRuntime } from "../plugins/runtime/index.js";
import { createLazyRuntimeModule } from "../shared/lazy-runtime.js";
import { consultRealtimeVoiceAgent } from "../talk/agent-consult-runtime.js";
import { parseRealtimeVoiceAgentConsultArgs } from "../talk/agent-consult-tool.js";
import {
  authorizeClientVoiceConfirmation,
  bindAuthorizedClientVoiceConfirmation,
} from "../talk/client-voice-confirmation.js";
import {
  assertClientVoiceSessionOpen,
  registerClientVoiceConsultRun,
} from "../talk/client-voice-session.js";
import { registerChatAbortController } from "./chat-abort.js";
import type { GatewayRequestContext } from "./server-methods/shared-types.js";
import {
  resolveTalkAgentConsultAuthority,
  type TalkAgentConsultAuthority,
} from "./talk-client-gateway-control.js";

const loadTalkAgentExecution = createLazyRuntimeModule(async () => {
  const [embeddedAgent, admission] = await Promise.all([
    import("../agents/embedded-agent.js"),
    import("../agents/admitted-run-context.js"),
  ]);
  return {
    runEmbeddedAgent: embeddedAgent.runEmbeddedAgent,
    createOperationalRunInstanceRef: admission.createOperationalRunInstanceRef,
    prepareAgentRunAdmission: admission.prepareAgentRunAdmission,
  };
});

function createTalkClientAgentRuntime(params: {
  config: OpenClawConfig;
  agentId: string;
  rawSourceRef?: string;
}) {
  const agentRuntime = createPluginRuntime().agent;
  const runEmbeddedAgent: typeof agentRuntime.runEmbeddedAgent = async (runParams) => {
    runParams.abortSignal?.throwIfAborted();
    const execution = await loadTalkAgentExecution();
    runParams.abortSignal?.throwIfAborted();
    const preparedRunAdmission = execution.prepareAgentRunAdmission({
      cfg: params.config,
      operationalRunInstance: execution.createOperationalRunInstanceRef(runParams.runId),
      facts: {
        runId: runParams.runId,
        agentId: runParams.sessionTarget?.agentId ?? runParams.agentId ?? params.agentId,
        ingress: {
          kind: "gateway-client",
          boundary: "talk-agent-consult",
          state: "present",
          ...(params.rawSourceRef ? { rawSourceRef: params.rawSourceRef } : {}),
        },
      },
    });
    let closed = false;
    const close = () => {
      if (!closed) {
        closed = true;
        preparedRunAdmission.close();
      }
    };
    // Abort owns authority revocation independently of core completion; the
    // post-registration check closes the prepare-to-listener race.
    runParams.abortSignal?.addEventListener("abort", close, { once: true });
    try {
      runParams.abortSignal?.throwIfAborted();
      return await execution.runEmbeddedAgent({ ...runParams, preparedRunAdmission });
    } finally {
      runParams.abortSignal?.removeEventListener("abort", close);
      close();
    }
  };
  Object.defineProperty(agentRuntime, "runEmbeddedAgent", {
    configurable: true,
    enumerable: true,
    value: runEmbeddedAgent,
  });
  return agentRuntime;
}

export function createTalkClientAgentConsultRunner(params: {
  config: OpenClawConfig;
  context: Pick<GatewayRequestContext, "chatAbortControllers" | "logGateway">;
  agentId: string;
  sessionKey: string;
  ownerConnId?: string;
  authority?: TalkAgentConsultAuthority;
  getVoiceSessionId: () => string | undefined;
  initialItems: Array<{ role: "user" | "assistant"; text: string }>;
  runIdPrefix?: string;
  surface?: string;
  registerRun?: (params: { runId: string }) => void;
}) {
  const authority = params.authority ?? resolveTalkAgentConsultAuthority(undefined);
  let agentRuntime: ReturnType<typeof createPluginRuntime>["agent"] | undefined;
  const runArgs = async (args: unknown, signal?: AbortSignal) => {
    const parsedArgs = parseRealtimeVoiceAgentConsultArgs(args);
    const voiceSessionId = params.getVoiceSessionId();
    if (!voiceSessionId) {
      throw new Error("Realtime browser voice session is not ready for agent consult");
    }
    // Relays own admission before their lazy record registration. Browser callbacks
    // must validate the durable call before accepting a new run.
    if (!params.registerRun) {
      assertClientVoiceSessionOpen({
        agentId: params.agentId,
        sessionKey: params.sessionKey,
        voiceSessionId,
      });
    }
    const confirmationGrant = parsedArgs.confirmationId
      ? authorizeClientVoiceConfirmation({
          agentId: params.agentId,
          voiceSessionId,
          confirmationId: parsedArgs.confirmationId,
        })
      : undefined;
    agentRuntime ??= createTalkClientAgentRuntime({
      config: params.config,
      agentId: params.agentId,
      ...(params.ownerConnId ? { rawSourceRef: params.ownerConnId } : {}),
    });
    const talkConfig = normalizeTalkSection(params.config.talk);
    return await consultRealtimeVoiceAgent({
      cfg: params.config,
      agentRuntime,
      logger: params.context.logGateway,
      agentId: params.agentId,
      sessionKey: params.sessionKey,
      messageProvider: "webchat",
      lane: "talk",
      runIdPrefix: params.runIdPrefix ?? "talk-realtime-consult",
      args: parsedArgs,
      transcript: params.initialItems,
      surface: params.surface ?? "a browser Talk session",
      userLabel: "User",
      questionSourceLabel: "user",
      thinkLevel: talkConfig?.consultThinkingLevel,
      fastMode: talkConfig?.consultFastMode,
      ...authority,
      abortSignal: signal,
      onRunStarted: ({ runId, sessionId, timeoutMs }) => {
        if (params.registerRun) {
          params.registerRun({ runId });
        } else {
          registerClientVoiceConsultRun({
            agentId: params.agentId,
            sessionKey: params.sessionKey,
            voiceSessionId,
            runId,
            config: params.config,
          });
        }
        if (confirmationGrant) {
          bindAuthorizedClientVoiceConfirmation({ grant: confirmationGrant, runId });
        }
        if (!params.ownerConnId) {
          return undefined;
        }
        const registration = registerChatAbortController({
          chatAbortControllers: params.context.chatAbortControllers,
          runId,
          sessionId,
          sessionKey: params.sessionKey,
          agentId: params.agentId,
          timeoutMs,
          ownerConnId: params.ownerConnId,
          controlUiVisible: false,
          kind: "chat-send",
        });
        return { abortSignal: registration.controller.signal, cleanup: registration.cleanup };
      },
    });
  };
  return {
    runArgs,
    runPrompt: async ({ prompt, signal }: { prompt: string; signal?: AbortSignal }) =>
      await runArgs({ question: prompt }, signal),
  };
}
