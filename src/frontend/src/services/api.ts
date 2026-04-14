import { createHttpStreamFactory } from './http-stream';

export const API_BASE_URL = import.meta.env.DEV ? '/proxy-api' : '';

const parseJsonResponse = async (response: Response) => {
  const raw = await response.text();
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error(raw || `HTTP ${response.status}`);
  }
};

export const getModelList = async (): Promise<readonly any[]> => {
  const response = await fetch(`${API_BASE_URL}/model/list`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'model_list') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const initScheduler = async (params: {
  model_name: string;
  init_nodes_num: number;
  is_local_network: boolean;
}): Promise<void> => {
  const response = await fetch(`${API_BASE_URL}/scheduler/init`, {
    method: 'POST',
    body: JSON.stringify(params),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'scheduler_init') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const createStreamClusterStatus = createHttpStreamFactory({
  url: `${API_BASE_URL}/cluster/status`,
  method: 'GET',
});


export const rebalanceCluster = async (): Promise<{ ok: boolean; message: string }> => {
  const response = await fetch(`${API_BASE_URL}/cluster/rebalance`, { method: 'POST' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'cluster_rebalance') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};


export interface ChatHistorySummary {
  readonly conversation_id: string;
  readonly title: string;
  readonly summary: string;
  readonly summary_source?: 'none' | 'model' | 'heuristic';
  readonly message_count: number;
  readonly created_at: number;
  readonly updated_at: number;
  readonly last_message: string;
}

export interface ChatHistoryMessage {
  readonly id: string;
  readonly role: 'user' | 'assistant';
  readonly content: string;
  readonly created_at: number;
}

export interface ChatHistoryDetail {
  readonly conversation_id: string;
  readonly summary_text: string;
  readonly summary_source?: 'none' | 'model' | 'heuristic';
  readonly created_at?: number;
  readonly updated_at?: number;
  readonly messages: readonly ChatHistoryMessage[];
}

export const getChatHistoryList = async (): Promise<readonly ChatHistorySummary[]> => {
  const response = await fetch(`${API_BASE_URL}/chat/history`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'chat_history_list') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const getChatHistoryDetail = async (
  conversationId: string,
): Promise<ChatHistoryDetail> => {
  const response = await fetch(`${API_BASE_URL}/chat/history/${conversationId}`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'chat_history_detail') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const deleteChatHistoryConversation = async (
  conversationId: string,
): Promise<{ deleted: boolean; conversation_id: string }> => {
  const response = await fetch(`${API_BASE_URL}/chat/history/${conversationId}`, { method: 'DELETE' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'chat_history_delete') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};


export interface NodeOverviewHost {
  readonly id: string;
  readonly display_name: string;
  readonly ssh_target: string;
  readonly hostname_hint: string;
  readonly inventory_source: 'configured' | 'live_only';
  readonly joined: boolean;
  readonly ssh_reachable: boolean | null;
  readonly last_ping_ok: boolean | null;
  readonly last_ping_message: string;
  readonly runtime: {
    readonly node_id?: string | null;
    readonly status?: string | null;
    readonly hostname?: string | null;
    readonly gpu_name?: string | null;
    readonly gpu_memory?: number | null;
    readonly gpu_num?: number | null;
    readonly start_layer?: number | null;
    readonly end_layer?: number | null;
    readonly total_layers?: number | null;
    readonly approx_remaining_context?: number | null;
  };
  readonly system: {
    readonly cpu_percent?: number | null;
    readonly ram_used_gb?: number | null;
    readonly ram_total_gb?: number | null;
    readonly ram_used_percent?: number | null;
    readonly disk_used_gb?: number | null;
    readonly disk_total_gb?: number | null;
    readonly disk_used_percent?: number | null;
  };
  readonly host_process?: {
    readonly running: boolean;
    readonly confirmed_running?: boolean;
    readonly pid?: string;
    readonly source?: string;
    readonly message?: string;
    readonly checked_at?: number;
  };
  readonly actions: {
    readonly can_ping: boolean;
    readonly can_start: boolean;
    readonly can_stop: boolean;
    readonly can_restart: boolean;
    readonly can_tail_logs: boolean;
  };
}

export interface NodesOverview {
  readonly summary: {
    readonly configured_hosts: number;
    readonly joined_hosts: number;
    readonly unjoined_configured_hosts: number;
    readonly live_only_hosts: number;
  };
  readonly hosts: readonly NodeOverviewHost[];
}

export const getNodesOverview = async (): Promise<NodesOverview> => {
  const response = await fetch(`${API_BASE_URL}/nodes/overview`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'nodes_overview') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const pingNodeHost = async (sshTarget: string): Promise<{ ok: boolean; message: string; ssh_target: string; latency_ms?: number; return_code?: number }> => {
  const response = await fetch(`${API_BASE_URL}/nodes/ping`, {
    method: 'POST',
    body: JSON.stringify({ ssh_target: sshTarget }),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'node_ping') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};


const postNodeAction = async (
  path: string,
  expectedType: string,
  sshTarget: string,
): Promise<{ ok: boolean; message: string; ssh_target: string; action?: string; latency_ms?: number; return_code?: number }> => {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: 'POST',
    body: JSON.stringify({ ssh_target: sshTarget }),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== expectedType) {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const startNodeHost = async (sshTarget: string) => postNodeAction('/nodes/start', 'node_start', sshTarget);
export const stopNodeHost = async (sshTarget: string) => postNodeAction('/nodes/stop', 'node_stop', sshTarget);
export const restartNodeHost = async (sshTarget: string) => postNodeAction('/nodes/restart', 'node_restart', sshTarget);

export const getNodeLogs = async (sshTarget: string, lines = 200): Promise<{ ok: boolean; message: string; ssh_target: string; source?: string; content: string; stderr?: string; latency_ms?: number; return_code?: number }> => {
  const response = await fetch(`${API_BASE_URL}/nodes/logs`, {
    method: 'POST',
    body: JSON.stringify({ ssh_target: sshTarget, lines }),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'node_logs') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};
