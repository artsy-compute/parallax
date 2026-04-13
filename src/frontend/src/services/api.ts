import { createHttpStreamFactory } from './http-stream';

export const API_BASE_URL = import.meta.env.DEV ? '/proxy-api' : '';

export const getModelList = async (): Promise<readonly any[]> => {
  const response = await fetch(`${API_BASE_URL}/model/list`, { method: 'GET' });
  const message = await response.json();
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
  const message = await response.json();
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
  const message = await response.json();
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
  const message = await response.json();
  if (message.type !== 'chat_history_list') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const getChatHistoryDetail = async (
  conversationId: string,
): Promise<ChatHistoryDetail> => {
  const response = await fetch(`${API_BASE_URL}/chat/history/${conversationId}`, { method: 'GET' });
  const message = await response.json();
  if (message.type !== 'chat_history_detail') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const deleteChatHistoryConversation = async (
  conversationId: string,
): Promise<{ deleted: boolean; conversation_id: string }> => {
  const response = await fetch(`${API_BASE_URL}/chat/history/${conversationId}`, { method: 'DELETE' });
  const message = await response.json();
  if (message.type !== 'chat_history_delete') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};
