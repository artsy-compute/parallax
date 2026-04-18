/* eslint-disable react-refresh/only-export-components */
import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useState,
  type Dispatch,
  type FC,
  type PropsWithChildren,
  type SetStateAction,
} from 'react';
import {
  API_BASE_URL,
  type AppAvailableTool,
  deleteChatHistoryConversation,
  getAgentRunDetailByConversation,
  getAgentRunsByConversation,
  getChatHistoryDetail,
  getChatHistoryList,
  type AgentRunSummary,
  type ChatHistorySummary,
} from './api';
import { useConst, useRefCallback } from '../hooks';
import { useCluster } from './cluster';
import { parseGenerationGpt, parseGenerationQwen } from './chat-helper';

const debugLog = async (...args: any[]) => {
  if (import.meta.env.DEV) {
    console.log('%c chat.tsx ', 'color: white; background: orange;', ...args);
  }
};

const STORAGE_KEY = 'parallax.chat.conversation_id';
const FIRST_TOKEN_TIMEOUT_MS = 120000;
const NO_PROGRESS_TIMEOUT_MS = 90000;
const ERROR_RECOVERY_ATTEMPTS = 5;
const ERROR_RECOVERY_POLL_INTERVAL_MS = 1500;
const ACTIVE_RUN_POLL_INTERVAL_MS = 2500;

const createConversationId = () =>
  globalThis.crypto?.randomUUID?.() || `conv-${Date.now()}-${Math.random().toString(36).slice(2)}`;

const isActiveRunStatus = (status: string) =>
  ['queued', 'running', 'paused', 'waiting_for_approval'].includes(String(status || '').trim());

const enabledToolNameSet = (tools: readonly AppAvailableTool[]) =>
  new Set(
    tools
      .filter((tool) => tool.enabled_by_default)
      .map((tool) => String(tool.name || '').trim()),
  );

const detectAutoTaskMode = (prompt: string, tools: readonly AppAvailableTool[]): ChatTaskMode | null => {
  const text = String(prompt || '').trim();
  if (!text) {
    return null;
  }

  const availableToolNames = enabledToolNameSet(tools);
  const hasWebTools = availableToolNames.has('fetch_url') || availableToolNames.has('fetch_json');
  const hasWorkspaceTools =
    availableToolNames.has('read_file')
    || availableToolNames.has('list_files')
    || availableToolNames.has('search_files');
  const hasClusterTools =
    availableToolNames.has('get_cluster_status')
    || availableToolNames.has('list_nodes')
    || availableToolNames.has('list_models')
    || availableToolNames.has('get_join_command')
    || availableToolNames.has('get_nodes_overview')
    || availableToolNames.has('get_model_details');

  if (
    hasWebTools
    && (
      /\bhttps?:\/\/\S+/i.test(text)
      || /\b(?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/\S*)?\b/i.test(text)
      || /\b(latest|today|current|recent|price|weather|news|live|update|updated|market|stock|score)\b/i.test(text)
    )
  ) {
    return 'live_data';
  }

  if (
    hasWorkspaceTools
    && (
      /\b(file|files|folder|directory|repo|repository|workspace|project|codebase|source|read|search|find)\b/i.test(text)
      || /(?:^|[\s"'`])(?:\.{0,2}\/|\/)[^\s"'`]+/.test(text)
      || /\b\w+\.(?:ts|tsx|js|jsx|py|rs|md|json|toml|yaml|yml|txt|sh)\b/i.test(text)
    )
  ) {
    return 'workspace';
  }

  if (
    hasClusterTools
    && /\b(cluster|node|nodes|scheduler|model|models|join command|topology|gpu|vram|status)\b/i.test(text)
  ) {
    return 'cluster';
  }

  return null;
};

const requestOptionsForMode = (mode: ChatTaskMode | null): RequestExecutionOptions => {
  if (mode === 'live_data') {
    return {
      serverTools: {
        enabled: true,
        allow_web_fetch: true,
        allow_file_read: false,
      },
      toolChoice: 'required',
      extraMessages: [
        {
          id: `system-live-${Date.now()}`,
          role: 'system',
          content:
            'Use the available web-fetch tools to gather current live information before answering. Do not answer from memory when current information is requested. If fetching fails, briefly explain the failure.',
        },
      ],
    };
  }

  if (mode === 'workspace') {
    return {
      serverTools: {
        enabled: true,
        allow_web_fetch: false,
        allow_file_read: true,
      },
      toolChoice: 'auto',
      extraMessages: [
        {
          id: `system-workspace-${Date.now()}`,
          role: 'system',
          content:
            'Use the available file and workspace tools to inspect the local project when that will produce a better answer. Prefer grounded workspace facts over generic guesses.',
        },
      ],
    };
  }

  if (mode === 'cluster') {
    return {
      serverTools: {
        enabled: true,
        allow_web_fetch: false,
        allow_file_read: false,
      },
      toolChoice: 'auto',
      extraMessages: [
        {
          id: `system-cluster-${Date.now()}`,
          role: 'system',
          content:
            'Use the available Parallax cluster tools to inspect live cluster, node, model, or join-command state before answering. Prefer observed system state over generic guidance.',
        },
      ],
    };
  }

  if (mode === 'task') {
    return {
      serverTools: {
        enabled: true,
        allow_web_fetch: false,
        allow_file_read: true,
      },
      toolChoice: 'auto',
      extraMessages: [
        {
          id: `system-task-${Date.now()}`,
          role: 'system',
          content:
            'You may use the available tools if they help complete the task. Prefer concrete results over generic disclaimers.',
        },
      ],
    };
  }

  return {
    serverTools: {
      enabled: false,
    },
  };
};

export type ChatMessageRole = 'user' | 'assistant';

export type ChatMessageStatus = 'waiting' | 'thinking' | 'generating' | 'done' | 'error';

export interface ChatMessage {
  readonly id: string;
  readonly role: ChatMessageRole;
  readonly status: ChatMessageStatus;

  /**
   * The content from user input or assistant generating.
   */
  readonly content: string;

  /**
   * The raw content from model response.
   */
  readonly raw?: string;

  /**
   * The thinking content in assistant generating.
   */
  readonly thinking?: string;
  readonly createdAt: number;
}

export type ChatStatus = 'closed' | 'opened' | 'generating' | 'error';

export interface ChatStates {
  readonly input: string;
  readonly status: ChatStatus;
  readonly messages: readonly ChatMessage[];
  readonly conversationId: string;
  readonly history: readonly ChatHistorySummary[];
  readonly historyLoading: boolean;
  readonly activeRun: AgentRunSummary | null;
  readonly runsByMessageId: Readonly<Record<string, AgentRunSummary>>;
  readonly inputTruncationNotice: {
    readonly truncated: boolean;
    readonly originalPromptTokens: number;
    readonly keptPromptTokens: number;
    readonly maxSequenceLength: number;
    readonly maxNewTokens: number;
  } | null;
  readonly requestHealthNotice: {
    readonly severity: 'warning' | 'error';
    readonly message: string;
  } | null;
  readonly promptBudgetNotice: {
    readonly inputBudgetTokens: number;
    readonly reservedOutputTokens: number;
    readonly estimatedInputTokens: number;
    readonly recentMessagesCount: number;
    readonly memorySectionsCount: number;
    readonly memoryBudgetTokens: number;
    readonly recentTurnTokens: number;
    readonly summaryTokens: number;
    readonly snippetTokens: number;
    readonly requestedOutputTokens: number;
    readonly adjustedOutputTokens: number;
    readonly outputTokensReduced: number;
    readonly adaptedOutputBudget: boolean;
  } | null;
}

export type ChatTaskMode = 'task' | 'live_data' | 'workspace' | 'cluster';

export interface ChatActions {
  readonly setInput: Dispatch<SetStateAction<string>>;
  readonly generate: (message?: ChatMessage) => void;
  readonly runTask: (message: ChatMessage, mode?: ChatTaskMode) => void;
  readonly stop: () => void;
  readonly clear: () => void;
  readonly refreshHistory: () => Promise<void>;
  readonly loadConversation: (conversationId: string) => Promise<void>;
  readonly deleteConversation: (conversationId: string) => Promise<void>;
  readonly startNewConversation: () => void;
  readonly focusInput: () => void;
  readonly registerInputFocus: (focusFn: (() => void) | null) => void;
}

export const ChatProvider: FC<PropsWithChildren> = ({ children }) => {
  const [
    {
      config: { availableTools },
      clusterInfo: { status: clusterStatus, modelName },
    },
  ] = useCluster();

  const [input, setInput] = useState<string>('');

  const [status, _setStatus] = useState<ChatStatus>('closed');
  const setStatus = useRefCallback<typeof _setStatus>((value) => {
    _setStatus((prev) => {
      const next = typeof value === 'function' ? value(prev) : value;
      if (next !== prev) {
        debugLog('setStatus', 'status', next);
      }
      return next;
    });
  });

  const [messages, setMessages] = useState<readonly ChatMessage[]>([]);
  const [conversationId, setConversationId] = useState<string>(() => {
    const stored = globalThis.localStorage?.getItem(STORAGE_KEY);
    return stored || createConversationId();
  });
  const [history, setHistory] = useState<readonly ChatHistorySummary[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [activeRun, setActiveRun] = useState<AgentRunSummary | null>(null);
  const [runsByMessageId, setRunsByMessageId] = useState<Readonly<Record<string, AgentRunSummary>>>({});
  const [inputTruncationNotice, setInputTruncationNotice] = useState<ChatStates['inputTruncationNotice']>(null);
  const [requestHealthNotice, setRequestHealthNotice] = useState<ChatStates['requestHealthNotice']>(null);
  const [promptBudgetNotice, setPromptBudgetNotice] = useState<ChatStates['promptBudgetNotice']>(null);
  const inputFocusRef = useConst<{ current: (() => void) | null }>(() => ({ current: null }));
  const firstTokenTimeoutRef = useConst<{ current: ReturnType<typeof setTimeout> | null }>(() => ({ current: null }));
  const noProgressTimeoutRef = useConst<{ current: ReturnType<typeof setTimeout> | null }>(() => ({ current: null }));
  const timedOutBeforeFirstTokenRef = useConst<{ current: boolean }>(() => ({ current: false }));
  const timedOutDuringGenerationRef = useConst<{ current: boolean }>(() => ({ current: false }));
  const streamEndedWithErrorRef = useConst<{ current: boolean }>(() => ({ current: false }));
  const firstTokenSeenRef = useConst<{ current: boolean }>(() => ({ current: false }));
  const completedRunRecoveryRef = useConst<{ current: string | null }>(() => ({ current: null }));
  const suppressNextCloseRef = useConst<{ current: boolean }>(() => ({ current: false }));

  const clearFirstTokenTimeout = useRefCallback(() => {
    if (firstTokenTimeoutRef.current) {
      clearTimeout(firstTokenTimeoutRef.current);
      firstTokenTimeoutRef.current = null;
    }
  });

  const clearNoProgressTimeout = useRefCallback(() => {
    if (noProgressTimeoutRef.current) {
      clearTimeout(noProgressTimeoutRef.current);
      noProgressTimeoutRef.current = null;
    }
  });

  const syncActiveRun = useRefCallback((run: AgentRunSummary | null) => {
    setActiveRun(run);
  });

  const syncConversationRuns = useRefCallback((runs: readonly AgentRunSummary[]) => {
    const nextMap: Record<string, AgentRunSummary> = {};
    runs.forEach((run) => {
      const requestId = String(run.request_id || '').trim();
      if (requestId) {
        nextMap[requestId] = run;
      }
    });
    setRunsByMessageId(nextMap);
  });

  const refreshConversationRuns = useRefCallback(async (targetConversationId?: string) => {
    const nextConversationId = str(targetConversationId || conversationId || '').trim();
    if (!nextConversationId) {
      setRunsByMessageId({});
      return [];
    }
    try {
      const runs = await getAgentRunsByConversation(nextConversationId);
      syncConversationRuns(runs);
      return runs;
    } catch (error) {
      setRunsByMessageId({});
      return [];
    }
  });

  const refreshActiveRun = useRefCallback(async (targetConversationId?: string, minimumUpdatedAt?: number) => {
    const nextConversationId = str(targetConversationId || conversationId || '').trim();
    if (!nextConversationId) {
      syncActiveRun(null);
      return null;
    }
    try {
      const run = await getAgentRunDetailByConversation(nextConversationId);
      if (typeof minimumUpdatedAt === 'number' && Number.isFinite(minimumUpdatedAt) && Number(run.updated_at || 0) < minimumUpdatedAt) {
        return null;
      }
      syncActiveRun(run);
      return run;
    } catch (error) {
      syncActiveRun(null);
      return null;
    }
  });

  const waitForActiveRun = useRefCallback(async (targetConversationId?: string, minimumUpdatedAt?: number) => {
    const nextConversationId = str(targetConversationId || conversationId || '').trim();
    if (!nextConversationId) {
      return null;
    }
    for (let attempt = 0; attempt < 8; attempt += 1) {
      const run = await refreshActiveRun(nextConversationId, minimumUpdatedAt);
      if (run) {
        return run;
      }
      await new Promise((resolve) => {
        globalThis.setTimeout(resolve, 350);
      });
    }
    return null;
  });

  const armNoProgressTimeout = useRefCallback(() => {
    clearNoProgressTimeout();
    noProgressTimeoutRef.current = setTimeout(() => {
      debugLog('SSE NO PROGRESS TIMEOUT');
      timedOutDuringGenerationRef.current = true;
      sse.disconnect();
    }, NO_PROGRESS_TIMEOUT_MS);
  });

  const focusInput = useRefCallback<ChatActions['focusInput']>(() => {
    inputFocusRef.current?.();
  });

  const registerInputFocus = useRefCallback<ChatActions['registerInputFocus']>((focusFn) => {
    inputFocusRef.current = focusFn;
  });

  useEffect(() => {
    globalThis.localStorage?.setItem(STORAGE_KEY, conversationId);
  }, [conversationId]);

  const refreshHistory = useRefCallback(async () => {
    setHistoryLoading(true);
    try {
      const next = await getChatHistoryList();
      setHistory(next.items);
    } catch (error) {
      console.error('getChatHistoryList error', error);
    } finally {
      setHistoryLoading(false);
    }
  });

  const hydrateConversationFromDetail = useRefCallback((
    detail: {
      conversation_id: string;
      messages: readonly {
        id: string;
        role: ChatMessageRole;
        content: string;
        created_at: number;
      }[];
    },
    nextStatus: ChatStatus = 'closed',
  ) => {
    setConversationId(detail.conversation_id || conversationId);
    setMessages(
      detail.messages.map((message) => ({
        id: message.id,
        role: message.role,
        status: 'done' as const,
        content: message.content,
        raw: message.content,
        createdAt: message.created_at,
      })),
    );
    setStatus(nextStatus);
  });

  const loadConversation = useRefCallback(async (nextConversationId: string) => {
    if (!nextConversationId) {
      return;
    }
    setHistoryLoading(true);
    try {
      const detail = await getChatHistoryDetail(nextConversationId);
      setInputTruncationNotice(null);
      setRequestHealthNotice(null);
      setPromptBudgetNotice(null);
      hydrateConversationFromDetail(detail, 'closed');
      try {
        const run = await getAgentRunDetailByConversation(detail.conversation_id || nextConversationId);
        syncActiveRun(run);
      } catch {
        syncActiveRun(null);
      }
      await refreshConversationRuns(detail.conversation_id || nextConversationId);
    } catch (error) {
      console.error('getChatHistoryDetail error', error);
    } finally {
      setHistoryLoading(false);
    }
  });

  const recoverConversationAfterStreamError = useRefCallback(async () => {
    const expectedUserMessageCount = messages.filter((message) => message.role === 'user').length;
    if (!conversationId || expectedUserMessageCount <= 0) {
      return false;
    }

    for (let attempt = 0; attempt < ERROR_RECOVERY_ATTEMPTS; attempt += 1) {
      try {
        const detail = await getChatHistoryDetail(conversationId);
        const persistedUserMessageCount = detail.messages.filter((message) => message.role === 'user').length;
        const lastPersistedMessage = detail.messages[detail.messages.length - 1];

        if (
          persistedUserMessageCount >= expectedUserMessageCount
          && lastPersistedMessage?.role === 'assistant'
          && Boolean(lastPersistedMessage.content?.trim())
        ) {
          setInputTruncationNotice(null);
          setRequestHealthNotice(null);
          setPromptBudgetNotice(null);
          hydrateConversationFromDetail(detail, 'closed');
          await refreshActiveRun(detail.conversation_id || conversationId);
          await refreshConversationRuns(detail.conversation_id || conversationId);
          refreshHistory();
          return true;
        }
      } catch (error) {
        console.error('recoverConversationAfterStreamError error', error);
      }

      if (attempt < ERROR_RECOVERY_ATTEMPTS - 1) {
        await new Promise((resolve) => {
          globalThis.setTimeout(resolve, ERROR_RECOVERY_POLL_INTERVAL_MS);
        });
      }
    }

    return false;
  });

  useEffect(() => {
    refreshHistory();
  }, []);

  useEffect(() => {
    if (!history.length) {
      return;
    }
    const exists = history.some((item) => item.conversation_id === conversationId);
    if (!exists) {
      return;
    }
    if (messages.length > 0) {
      return;
    }
    loadConversation(conversationId);
  }, [history, conversationId]);

  useEffect(() => {
    const shouldPoll =
      Boolean(conversationId)
      && (status === 'opened' || status === 'generating' || Boolean(activeRun && isActiveRunStatus(activeRun.status)));
    if (!shouldPoll) {
      return;
    }

    const intervalId = globalThis.setInterval(() => {
      void refreshActiveRun(conversationId);
      void refreshConversationRuns(conversationId);
    }, ACTIVE_RUN_POLL_INTERVAL_MS);

    return () => {
      globalThis.clearInterval(intervalId);
    };
  }, [activeRun, conversationId, refreshActiveRun, refreshConversationRuns, status]);

  const sse = useConst(() =>
    createSSE({
      onOpen: () => {
        debugLog('SSE OPEN');
        timedOutBeforeFirstTokenRef.current = false;
        timedOutDuringGenerationRef.current = false;
        streamEndedWithErrorRef.current = false;
        firstTokenSeenRef.current = false;
        clearFirstTokenTimeout();
        clearNoProgressTimeout();
        firstTokenTimeoutRef.current = setTimeout(() => {
          debugLog('SSE FIRST TOKEN TIMEOUT');
          timedOutBeforeFirstTokenRef.current = true;
          setRequestHealthNotice({
            severity: 'warning',
            message: 'Prompt processing is taking longer than expected. The request is still running and may produce output shortly.',
          });
        }, FIRST_TOKEN_TIMEOUT_MS);
        setStatus('opened');
        void refreshActiveRun();
      },
      onClose: () => {
        debugLog('SSE CLOSE');
        clearFirstTokenTimeout();
        clearNoProgressTimeout();
        if (suppressNextCloseRef.current) {
          suppressNextCloseRef.current = false;
          return;
        }
        const timedOutBeforeFirstToken = timedOutBeforeFirstTokenRef.current;
        const timedOutDuringGeneration = timedOutDuringGenerationRef.current;
        const streamEndedWithError = streamEndedWithErrorRef.current;
        setMessages((prev) => {
          const lastMessage = prev[prev.length - 1];
          if (!lastMessage) {
            return prev;
          }
          const { id, raw, thinking, content } = lastMessage;
          debugLog('GENERATING DONE', 'lastMessage:', lastMessage);
          debugLog('GENERATING DONE', 'id:', id);
          debugLog('GENERATING DONE', 'raw:', raw);
          debugLog('GENERATING DONE', 'thinking:', thinking);
          debugLog('GENERATING DONE', 'content:', content);
          return [
            ...prev.slice(0, -1),
            {
              ...lastMessage,
              status: (timedOutBeforeFirstToken || timedOutDuringGeneration || streamEndedWithError) ? 'error' : 'done',
            },
          ];
        });
        if (timedOutBeforeFirstToken) {
          setRequestHealthNotice((prev) =>
            prev ?? {
              severity: 'warning',
              message: 'Prompt processing took longer than expected before the stream closed.',
            },
          );
        } else if (timedOutDuringGeneration) {
          setRequestHealthNotice({
            severity: 'warning',
            message: 'Generation stalled after it started. You can retry the request or shorten the prompt.',
          });
        }
        setStatus(timedOutBeforeFirstToken || timedOutDuringGeneration || streamEndedWithError ? 'error' : 'closed');
        void refreshActiveRun();
        timedOutBeforeFirstTokenRef.current = false;
        timedOutDuringGenerationRef.current = false;
        streamEndedWithErrorRef.current = false;
        refreshHistory();
      },
      onError: (error) => {
        clearFirstTokenTimeout();
        clearNoProgressTimeout();
        debugLog('SSE ERROR', error);
        void (async () => {
          const recovered = await recoverConversationAfterStreamError();
          if (recovered) {
            return;
          }

          setMessages((prev) => {
            const lastMessage = prev[prev.length - 1];
            if (!lastMessage) {
              return prev;
            }
            const { id, raw, thinking, content } = lastMessage;
            debugLog('GENERATING ERROR', 'lastMessage:', lastMessage);
            debugLog('GENERATING ERROR', 'id:', id);
            debugLog('GENERATING ERROR', 'raw:', raw);
            debugLog('GENERATING ERROR', 'thinking:', thinking);
            debugLog('GENERATING ERROR', 'content:', content);
            return [
              ...prev.slice(0, -1),
              {
                ...lastMessage,
                status: 'error',
              },
            ];
          });
          if (!timedOutBeforeFirstTokenRef.current && !timedOutDuringGenerationRef.current) {
            setRequestHealthNotice({
              severity: 'error',
              message: 'The request ended unexpectedly. You can retry it from the current conversation.',
            });
          }
          debugLog('SSE ERROR', error);
          setStatus('error');
          await refreshActiveRun();
          refreshHistory();
        })();
      },
      onMessage: (message) => {
        // debugLog('onMessage', message);
        // const example = {
        //   id: 'd410014e-3308-450d-bbd2-0ec4e0c0a345',
        //   object: 'chat.completion.chunk',
        //   model: 'default',
        //   created: 1758842801.822061,
        //   choices: [
        //     {
        //       index: 0,
        //       logprobs: null,
        //       finish_reason: null,
        //       matched_stop: null,
        //       delta: { role: null, content: ' the' },
        //     },
        //   ],
        //   usage: null,
        // };
        const {
          data: { id, object, model, created, choices, usage, input_truncation, prompt_budget, error },
        } = message;
        if (input_truncation?.truncated) {
          setInputTruncationNotice({
            truncated: true,
            originalPromptTokens: input_truncation.original_prompt_tokens || 0,
            keptPromptTokens: input_truncation.kept_prompt_tokens || 0,
            maxSequenceLength: input_truncation.max_sequence_length || 0,
            maxNewTokens: input_truncation.max_new_tokens || 0,
          });
        }
        if (prompt_budget) {
          setPromptBudgetNotice({
            inputBudgetTokens: prompt_budget.input_budget_tokens || 0,
            reservedOutputTokens: prompt_budget.reserved_output_tokens || 0,
            estimatedInputTokens: prompt_budget.estimated_input_tokens || 0,
            recentMessagesCount: prompt_budget.recent_messages_count || 0,
            memorySectionsCount: prompt_budget.memory_sections_count || 0,
            memoryBudgetTokens: prompt_budget.memory_budget_tokens || 0,
            recentTurnTokens: prompt_budget.recent_turn_tokens || 0,
            summaryTokens: prompt_budget.summary_tokens || 0,
            snippetTokens: prompt_budget.snippet_tokens || 0,
            requestedOutputTokens: prompt_budget.requested_output_tokens || 0,
            adjustedOutputTokens: prompt_budget.adjusted_output_tokens || 0,
            outputTokensReduced: prompt_budget.output_tokens_reduced || 0,
            adaptedOutputBudget: !!prompt_budget.adapted_output_budget,
          });
        }
        if (object === 'chat.completion.error') {
          streamEndedWithErrorRef.current = true;
          setMessages((prev) => {
            const lastMessage = prev[prev.length - 1];
            if (!lastMessage) {
              return prev;
            }
            return [
              ...prev.slice(0, -1),
              {
                ...lastMessage,
                status: 'error',
              },
            ];
          });
          setRequestHealthNotice({
            severity: 'error',
            message: error?.message || 'The active node disconnected while serving this request. You can retry from the current conversation.',
          });
          setStatus('error');
          return;
        }
        if (object === 'chat.completion.chunk' && choices?.length > 0) {
          if (choices[0].delta.content) {
            timedOutBeforeFirstTokenRef.current = false;
            clearFirstTokenTimeout();
            armNoProgressTimeout();
            setRequestHealthNotice(null);
            setStatus('generating');
            if (!firstTokenSeenRef.current) {
              firstTokenSeenRef.current = true;
              void refreshActiveRun();
            }
          }
          setMessages((prev) => {
            let next = prev;
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            choices.forEach(({ delta: { role, content: rawDelta } = {} }: any) => {
              if (typeof rawDelta !== 'string' || !rawDelta) {
                return;
              }
              role = role || 'assistant';
              let lastMessage = next[next.length - 1];
              if (lastMessage && lastMessage.role === role) {
                const raw = lastMessage.raw + rawDelta;
                lastMessage = {
                  ...lastMessage,
                  raw: raw,
                  content: raw,
                };
                next = [...next.slice(0, -1), lastMessage];
              } else {
                lastMessage = {
                  id,
                  role,
                  status: 'thinking',
                  raw: rawDelta,
                  content: rawDelta,
                  createdAt: created,
                };
                next = [...next, lastMessage];
              }
              // debugLog('onMessage', 'update last message', lastMessage.content);
            });

            // Parse generation and extract thinking and content
            if (next !== prev && typeof model === 'string') {
              let lastMessage = next[next.length - 1];
              let thinking = '';
              let content = '';
              const modelLowerCase = model.toLowerCase();
              if (modelLowerCase.includes('gpt-oss')) {
                ({ analysis: thinking, final: content } = parseGenerationGpt(
                  lastMessage.raw || '',
                ));
              } else if (modelLowerCase.includes('qwen')) {
                ({ think: thinking, content } = parseGenerationQwen(lastMessage.raw || ''));
              } else {
                content = lastMessage.raw || '';
              }
              lastMessage = {
                ...lastMessage,
                status: (content && 'generating') || 'thinking',
                thinking,
                content,
              };
              next = [...next.slice(0, -1), lastMessage];
            }

            return next;
          });
        }
      },
    }),
  );

  useEffect(() => {
    const shouldPollHistory = Boolean(conversationId) && (status === 'opened' || status === 'generating');
    if (!shouldPollHistory) {
      return;
    }

    const intervalId = globalThis.setInterval(() => {
      void (async () => {
        try {
          const detail = await getChatHistoryDetail(conversationId);
          const lastPersistedMessage = detail.messages[detail.messages.length - 1];
          const lastVisibleMessage = messages[messages.length - 1];
          if (
            lastPersistedMessage?.role === 'assistant'
            && Boolean(lastPersistedMessage.content?.trim())
            && (
              !lastVisibleMessage
              || lastVisibleMessage.role !== 'assistant'
              || lastVisibleMessage.content !== lastPersistedMessage.content
            )
          ) {
            clearFirstTokenTimeout();
            clearNoProgressTimeout();
            suppressNextCloseRef.current = true;
            sse.disconnect();
            hydrateConversationFromDetail(detail, 'closed');
            await refreshActiveRun(detail.conversation_id || conversationId);
            await refreshConversationRuns(detail.conversation_id || conversationId);
            refreshHistory();
          }
        } catch (error) {
          console.error('poll persisted conversation during active run error', error);
        }
      })();
    }, ACTIVE_RUN_POLL_INTERVAL_MS);

    return () => {
      globalThis.clearInterval(intervalId);
    };
  }, [
    clearFirstTokenTimeout,
    clearNoProgressTimeout,
    conversationId,
    hydrateConversationFromDetail,
    messages,
    refreshActiveRun,
    refreshConversationRuns,
    refreshHistory,
    sse,
    status,
  ]);

  useEffect(() => {
    if (!activeRun || !conversationId) {
      return;
    }
    if (status !== 'opened' && status !== 'generating') {
      return;
    }
    if (!['completed', 'failed', 'cancelled'].includes(String(activeRun.status || '').trim())) {
      return;
    }
    if (completedRunRecoveryRef.current === activeRun.id) {
      return;
    }
    completedRunRecoveryRef.current = activeRun.id;

    void (async () => {
      try {
        const detail = await getChatHistoryDetail(conversationId);
        if (detail.messages.length > 0) {
          clearFirstTokenTimeout();
          clearNoProgressTimeout();
          suppressNextCloseRef.current = true;
          sse.disconnect();
          hydrateConversationFromDetail(detail, activeRun.status === 'completed' ? 'closed' : 'error');
          await refreshActiveRun(detail.conversation_id || conversationId);
          await refreshConversationRuns(detail.conversation_id || conversationId);
          refreshHistory();
        }
      } catch (error) {
        console.error('recover completed run from history error', error);
      }
    })();
  }, [activeRun, clearFirstTokenTimeout, clearNoProgressTimeout, conversationId, hydrateConversationFromDetail, refreshActiveRun, refreshConversationRuns, refreshHistory, sse, status]);

  const generate = useRefCallback<ChatActions['generate']>((message) => {
    if (clusterStatus !== 'available' || status === 'opened' || status === 'generating') {
      return;
    }

    if (!modelName) {
      return;
    }

    let nextMessages: readonly ChatMessage[] = messages;
    setInputTruncationNotice(null);
    setRequestHealthNotice(null);
    setPromptBudgetNotice(null);
    if (message) {
      // Regenerate
      const finalMessageIndex = messages.findIndex((m) => m.id === message.id);
      const finalMessage = messages[finalMessageIndex];
      if (!finalMessage) {
        return;
      }
      nextMessages = nextMessages.slice(
        0,
        finalMessageIndex + (finalMessage.role === 'user' ? 1 : 0),
      );
      debugLog('generate', 'regenerate', nextMessages);
    } else {
      // Generate for new input
      const finalInput = input.trim();
      if (!finalInput) {
        return;
      }
      setInput('');
      const now = performance.now();
      nextMessages = [
        ...nextMessages,
        { id: now.toString(), role: 'user', status: 'done', content: finalInput, createdAt: now },
      ];
      debugLog('generate', 'new', nextMessages);
    }
    setMessages(nextMessages);
    refreshHistory();
    const requestStartedAt = Date.now() / 1000;
    const autoMode = message ? null : detectAutoTaskMode(nextMessages[nextMessages.length - 1]?.content || '', availableTools);
    sse.connect(
      modelName,
      conversationId,
      nextMessages.map(({ id, role, content }) => ({ id, role, content })),
      requestOptionsForMode(autoMode),
    );
    void waitForActiveRun(conversationId, requestStartedAt - 1);
  });

  const runTask = useRefCallback<ChatActions['runTask']>((message, mode = 'task') => {
    if (clusterStatus !== 'available' || status === 'opened' || status === 'generating') {
      return;
    }

    if (!modelName) {
      return;
    }

    const finalMessageIndex = messages.findIndex((m) => m.id === message.id);
    const finalMessage = messages[finalMessageIndex];
    if (!finalMessage) {
      return;
    }

    const nextMessages = messages.slice(
      0,
      finalMessageIndex + (finalMessage.role === 'user' ? 1 : 0),
    );
    setInputTruncationNotice(null);
    setRequestHealthNotice(null);
    setPromptBudgetNotice(null);
    setMessages(nextMessages);
    refreshHistory();
    const requestStartedAt = Date.now() / 1000;
    sse.connect(
      modelName,
      conversationId,
      nextMessages.map(({ id, role, content }) => ({ id, role, content })),
      requestOptionsForMode(mode),
    );
    void waitForActiveRun(conversationId, requestStartedAt - 1);
  });

  const stop = useRefCallback<ChatActions['stop']>(() => {
    debugLog('stop', 'status', status);
    if (status === 'closed' || status === 'error') {
      return;
    }
    clearFirstTokenTimeout();
    clearNoProgressTimeout();
    sse.disconnect();
  });

  const deleteConversation = useRefCallback<ChatActions['deleteConversation']>(async (targetConversationId) => {
    if (!targetConversationId) {
      return;
    }
    if (status === 'opened' || status === 'generating') {
      stop();
    }
    await deleteChatHistoryConversation(targetConversationId);
    if (targetConversationId === conversationId) {
      setInputTruncationNotice(null);
      setRequestHealthNotice(null);
      setPromptBudgetNotice(null);
      syncActiveRun(null);
      setRunsByMessageId({});
      setMessages([]);
      setStatus('closed');
      setConversationId(createConversationId());
    }
    await refreshHistory();
  });

  const startNewConversation = useRefCallback<ChatActions['startNewConversation']>(() => {
    stop();
    setInputTruncationNotice(null);
    setRequestHealthNotice(null);
    setPromptBudgetNotice(null);
    syncActiveRun(null);
    setRunsByMessageId({});
    setMessages([]);
    setStatus('closed');
    setConversationId(createConversationId());
    refreshHistory();
    requestAnimationFrame(() => {
      focusInput();
    });
  });

  const clear = useRefCallback<ChatActions['clear']>(() => {
    debugLog('clear', 'status', status);
    stop();
    if (status === 'opened' || status === 'generating') {
      return;
    }
    setInputTruncationNotice(null);
    setRequestHealthNotice(null);
    setPromptBudgetNotice(null);
    syncActiveRun(null);
    setRunsByMessageId({});
    setMessages([]);
    setConversationId(createConversationId());
    refreshHistory();
  });

  const actions = useConst<ChatActions>({
    setInput,
    generate,
    runTask,
    stop,
    clear,
    refreshHistory,
    loadConversation,
    deleteConversation,
    startNewConversation,
    focusInput,
    registerInputFocus,
  });

  const value = useMemo<readonly [ChatStates, ChatActions]>(
    () => [
      {
        input,
        status,
        messages,
        conversationId,
        history,
        historyLoading,
        activeRun,
        runsByMessageId,
        inputTruncationNotice,
        requestHealthNotice,
        promptBudgetNotice,
      },
      actions,
    ],
    [input, status, messages, conversationId, history, historyLoading, activeRun, runsByMessageId, inputTruncationNotice, requestHealthNotice, promptBudgetNotice, actions],
  );

  return <context.Provider value={value}>{children}</context.Provider>;
};

const context = createContext<readonly [ChatStates, ChatActions] | undefined>(undefined);

export const useChat = (): readonly [ChatStates, ChatActions] => {
  const value = useContext(context);
  if (!value) {
    throw new Error('useChat must be used within a ChatProvider');
  }
  return value;
};

// ================================================================
// SSE

interface SSEOptions {
  onOpen?: () => void;
  onClose?: () => void;
  onError?: (error: Error) => void;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  onMessage?: (message: { event: string; id?: string; data: any }) => void;
}

interface RequestMessage {
  readonly id: string;
  readonly role: ChatMessageRole | 'system';
  readonly content: string;
}

interface RequestExecutionOptions {
  readonly serverTools?: {
    readonly enabled: boolean;
    readonly allow_web_fetch?: boolean;
    readonly allow_file_read?: boolean;
  };
  readonly toolChoice?: 'auto' | 'required';
  readonly extraMessages?: readonly RequestMessage[];
}

const createSSE = (options: SSEOptions) => {
  const { onOpen, onClose, onError, onMessage } = options;

  const decoder = new TextDecoder();
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
  let abortController: AbortController | undefined;

  const connect = (
    model: string,
    conversationId: string,
    messages: readonly RequestMessage[],
    requestOptions?: RequestExecutionOptions,
  ) => {
    abortController = new AbortController();
    const url = `${API_BASE_URL}/v1/chat/completions`;

    onOpen?.();

    fetch(url, {
      method: 'POST',
      body: JSON.stringify({
        stream: true,
        model,
        conversation_id: conversationId,
        messages: [...(requestOptions?.extraMessages || []), ...messages],
        max_tokens: 2048,
        server_tools: requestOptions?.serverTools ?? {
          enabled: false,
        },
        tool_choice: requestOptions?.toolChoice,
        sampling_params: {
          top_k: 3,
        },
      }),
      signal: abortController.signal,
    })
      .then(async (response) => {
        const statusCode = response.status;
        const contentType = response.headers.get('Content-Type');
        if (statusCode !== 200) {
          onError?.(new Error(`[SSE] Failed to connect: ${statusCode}`));
          return;
        }
        if (!contentType?.includes('text/event-stream')) {
          onError?.(new Error(`[SSE] Invalid content type: ${contentType}`));
          return;
        }

        reader = response.body?.getReader();
        if (!reader) {
          onError?.(new Error(`[SSE] Failed to get reader`));
          return;
        }

        let buffer = '';

        const processLines = (lines: string[]) => {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const message: { event: string; id?: string; data: any } = {
            event: 'message',
            data: undefined,
          };
          lines.forEach((line) => {
            const colonIndex = line.indexOf(':');
            if (colonIndex <= 0) {
              // No colon, skip
              return;
            }

            const field = line.slice(0, colonIndex).trim();
            const value = line.slice(colonIndex + 1).trim();

            if (value.startsWith(':')) {
              // Comment line
              return;
            }

            switch (field) {
              case 'event':
                message.event = value;
                break;
              case 'id':
                message.id = value;
                break;
              case 'data':
                try {
                  // Try to parse as JSON object
                  const data = JSON.parse(value);
                  // eslint-disable-next-line @typescript-eslint/no-explicit-any
                  const walk = (data: any) => {
                    if (!data) {
                      return;
                    }
                    if (Array.isArray(data)) {
                      data.forEach((item, i) => {
                        if (item === null) {
                          data[i] = undefined;
                        } else {
                          walk(item);
                        }
                      });
                    } else if (typeof data === 'object') {
                      Object.keys(data).forEach((key) => {
                        if (data[key] === null) {
                          delete data[key];
                        } else {
                          walk(data[key]);
                        }
                      });
                    }
                  };
                  walk(data);
                  message.data = data;
                } catch (error) {
                  // Parse failed, use original data
                  message.data = value;
                }
                break;
            }

            if (message.data !== undefined) {
              onMessage?.(message);
            }
          });
        };

        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            onClose?.();
            return;
          }

          const chunk = decoder.decode(value);
          buffer += chunk;

          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          processLines(lines);
        }
      })
      .catch((error: Error) => {
        if (error instanceof Error && error.name === 'AbortError') {
          onClose?.();
          return;
        }
        onError?.(error);
      });
  };

  const disconnect = () => {
    reader?.cancel();
    reader = undefined;
    abortController?.abort('stop');
    abortController = undefined;

    onClose?.();
  };

  return { connect, disconnect };
};
