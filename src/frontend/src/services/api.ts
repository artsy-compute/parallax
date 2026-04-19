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

export interface CustomModelRecord {
  readonly id: string;
  readonly source_type: 'huggingface' | 'scheduler_root' | 'url' | 'local_path';
  readonly source_value: string;
  readonly display_name: string;
  readonly enabled: boolean;
  readonly validation_status: string;
  readonly validation_message: string;
  readonly detected_model_type?: string;
  readonly supports_sharding?: boolean | number;
  readonly vram_gb?: number;
  readonly metadata_json?: string;
  readonly created_at?: number;
  readonly updated_at?: number;
}

export interface CustomModelSearchResult {
  readonly source_type: 'huggingface';
  readonly source_value: string;
  readonly display_name: string;
  readonly validation_status: string;
  readonly validation_message: string;
  readonly detected_model_type?: string;
  readonly supports_sharding?: boolean;
  readonly vram_gb?: number;
}

export interface CustomModelSourceRoot {
  readonly id: string;
  readonly label: string;
  readonly path: string;
}

export interface CustomModelSourceOption {
  readonly root_id: string;
  readonly root_label: string;
  readonly relative_path: string;
  readonly source_value: string;
  readonly label: string;
  readonly path: string;
}

export const getCustomModelSources = async (): Promise<{
  supported_source_types: readonly ('huggingface' | 'scheduler_root' | 'url')[];
  allowed_local_roots: readonly CustomModelSourceRoot[];
  allowed_local_model_options: readonly CustomModelSourceOption[];
}> => {
  const response = await fetch(`${API_BASE_URL}/model/custom/sources`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'custom_model_sources') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const getCustomModelList = async (): Promise<readonly CustomModelRecord[]> => {
  const response = await fetch(`${API_BASE_URL}/model/custom`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'custom_model_list') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const searchCustomModels = async (query: string, limit = 8, offset = 0): Promise<{
  items: readonly CustomModelSearchResult[];
  next_offset: number;
  has_more: boolean;
}> => {
  const response = await fetch(`${API_BASE_URL}/model/custom/search?${new URLSearchParams({ query, limit: String(limit), offset: String(offset) })}`, {
    method: 'GET',
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'custom_model_search') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  if (Array.isArray(message.data)) {
    const items = message.data as readonly CustomModelSearchResult[];
    return {
      items,
      next_offset: offset + items.length,
      has_more: items.length >= limit,
    };
  }
  return {
    items: Array.isArray(message.data?.items) ? message.data.items : [],
    next_offset: Number(message.data?.next_offset || 0),
    has_more: Boolean(message.data?.has_more),
  };
};

export const addCustomModel = async (params: {
  source_type: 'huggingface' | 'scheduler_root' | 'url';
  source_value: string;
  display_name?: string;
}): Promise<CustomModelRecord> => {
  const response = await fetch(`${API_BASE_URL}/model/custom`, {
    method: 'POST',
    body: JSON.stringify(params),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'custom_model_add') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const deleteCustomModel = async (modelId: string): Promise<{ deleted: boolean; model_id: string }> => {
  const response = await fetch(`${API_BASE_URL}/model/custom/${encodeURIComponent(modelId)}`, {
    method: 'DELETE',
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'custom_model_delete') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export interface AppClusterSettings {
  readonly id?: string;
  readonly name?: string;
  readonly model_name: string;
  readonly init_nodes_num: number;
  readonly assigned_node_ids?: readonly string[];
  readonly is_local_network: boolean;
  readonly network_type: 'local' | 'remote';
  readonly advanced?: Record<string, unknown>;
}

export interface AppClusterProfile extends AppClusterSettings {
  readonly id: string;
  readonly name: string;
}

export interface AppAvailableTool {
  readonly name: string;
  readonly description: string;
  readonly enabled_by_default: boolean;
  readonly kind?: string;
  readonly plugin_name?: string;
  readonly allowed_roots?: readonly string[];
}

export interface AppSettingsPayload {
  readonly cluster_settings: AppClusterSettings;
  readonly clusters: readonly AppClusterProfile[];
  readonly active_cluster_id: string;
  readonly available_tools?: readonly AppAvailableTool[];
}

export const getAppSettings = async (): Promise<AppSettingsPayload> => {
  const response = await fetch(`${API_BASE_URL}/settings`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'app_settings') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const updateAppSettings = async (params: {
  cluster_settings?: Partial<AppClusterSettings>;
  clusters?: readonly Partial<AppClusterProfile>[];
  active_cluster_id?: string;
}): Promise<AppSettingsPayload> => {
  const response = await fetch(`${API_BASE_URL}/settings`, {
    method: 'PUT',
    body: JSON.stringify(params),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'app_settings_update') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export interface SettingsExportBundle {
  readonly schema_version: number;
  readonly cluster_settings: AppClusterSettings;
  readonly clusters?: readonly AppClusterProfile[];
  readonly active_cluster_id?: string;
  readonly managed_node_hosts: readonly {
    id?: string;
    ssh_target: string;
    parallax_path: string;
    hostname_hint?: string;
    line_number?: number;
    joined?: boolean;
    linked_cluster_ids?: readonly string[];
    linked_cluster_names?: readonly string[];
    linked_cluster_count?: number;
  }[];
  readonly custom_models: readonly { source_type: 'huggingface' | 'scheduler_root' | 'url' | 'local_path'; source_value: string; display_name?: string }[];
}

export const exportSettingsBundle = async (): Promise<SettingsExportBundle> => {
  const response = await fetch(`${API_BASE_URL}/settings/export`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'settings_export') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const importSettingsBundle = async (bundle: SettingsExportBundle): Promise<any> => {
  const response = await fetch(`${API_BASE_URL}/settings/import`, {
    method: 'POST',
    body: JSON.stringify(bundle),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'settings_import') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
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

export const getChatHistoryList = async (
  limit = 20,
  offset = 0,
): Promise<{
  items: readonly ChatHistorySummary[];
  total: number;
  limit: number;
  offset: number;
}> => {
  const response = await fetch(`${API_BASE_URL}/chat/history?${new URLSearchParams({ limit: String(limit), offset: String(offset) })}`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'chat_history_list') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (Array.isArray(message.data)) {
    const items = message.data as readonly ChatHistorySummary[];
    return {
      items,
      total: items.length,
      limit,
      offset,
    };
  }
  return {
    items: Array.isArray(message.data?.items) ? message.data.items : [],
    total: Number(message.data?.total || 0),
    limit: Number(message.data?.limit || limit),
    offset: Number(message.data?.offset || offset),
  };
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

export const deleteAllChatHistory = async (): Promise<{ deleted: number }> => {
  const response = await fetch(`${API_BASE_URL}/chat/history`, { method: 'DELETE' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'chat_history_delete_all') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export interface KnowledgeHealth {
  readonly ok: boolean;
  readonly workspace_id: string;
  readonly workspace_root: string;
  readonly storage_root: string;
  readonly embeddings: {
    readonly configured_provider: string;
    readonly active_provider: string;
  };
  readonly vector_backend: string;
  readonly counts: {
    readonly sources: number;
    readonly documents: number;
    readonly chunks: number;
    readonly jobs: number;
  };
  readonly error?: string;
}

export interface KnowledgeSourceSummary {
  readonly id: string;
  readonly workspace_id: string;
  readonly source_type: 'workspace_path' | 'url';
  readonly title: string;
  readonly canonical_uri: string;
  readonly root_path: string;
  readonly status: 'queued' | 'ready' | 'failed';
  readonly document_count: number;
  readonly last_error: string;
  readonly created_at: number;
  readonly updated_at: number;
}

export interface KnowledgeDocumentSummary {
  readonly id: string;
  readonly source_id: string;
  readonly document_uri: string;
  readonly title: string;
  readonly mime_type: string;
  readonly sha256: string;
  readonly byte_size: number;
  readonly text_length: number;
  readonly chunk_count: number;
  readonly created_at: number;
  readonly updated_at: number;
}

export interface KnowledgeSourceDetail extends KnowledgeSourceSummary {
  readonly documents: readonly KnowledgeDocumentSummary[];
}

export interface KnowledgeJob {
  readonly id: string;
  readonly workspace_id: string;
  readonly job_type: string;
  readonly status: 'queued' | 'running' | 'completed' | 'failed';
  readonly progress: number;
  readonly summary: string;
  readonly error: string;
  readonly created_at: number;
  readonly updated_at: number;
  readonly completed_at: number | null;
}

export interface KnowledgeSearchResult {
  readonly chunk_id: string;
  readonly document_id: string;
  readonly document_title: string;
  readonly document_uri: string;
  readonly source_id: string;
  readonly source_title: string;
  readonly source_type: string;
  readonly canonical_uri: string;
  readonly snippet: string;
  readonly chunk_position: number;
  readonly fused_rank: number;
  readonly lexical_rank: number | null;
  readonly semantic_rank: number | null;
}

export interface KnowledgeSearchResponse {
  readonly query: string;
  readonly items: readonly KnowledgeSearchResult[];
  readonly total: number;
}

export interface KnowledgeDocumentDetail extends KnowledgeDocumentSummary {
  readonly content: string;
  readonly source_title: string;
  readonly source_type: string;
  readonly canonical_uri: string;
  readonly chunks: readonly {
    readonly id: string;
    readonly position: number;
    readonly text: string;
    readonly token_estimate: number;
    readonly char_count: number;
  }[];
}

export interface KnowledgeCreateResponse {
  readonly source: KnowledgeSourceDetail;
  readonly job: KnowledgeJob;
  readonly vector_status: {
    readonly backend: string;
    readonly provider_name: string;
    readonly count: number;
    readonly dim: number;
  };
}

export interface KnowledgePageSummary {
  readonly id: string;
  readonly workspace_id: string;
  readonly parent_page_id: string | null;
  readonly source_id: string;
  readonly source_ids: readonly string[];
  readonly page_type: string;
  readonly aliases: readonly string[];
  readonly updated_from_job_id: string;
  readonly title: string;
  readonly slug: string;
  readonly summary: string;
  readonly sort_order: number;
  readonly is_home: boolean;
  readonly status: string;
  readonly child_count: number;
  readonly created_at: number;
  readonly updated_at: number;
}

export interface KnowledgePageDetail extends KnowledgePageSummary {
  readonly content: string;
}

export interface KnowledgePageListResponse {
  readonly home_page_id: string | null;
  readonly items: readonly KnowledgePageSummary[];
}

export interface KnowledgeGeneratePagesResponse {
  readonly home_page_id: string | null;
  readonly pages_created: number;
  readonly job: KnowledgeJob | null;
  readonly pages: KnowledgePageListResponse;
}

export interface KnowledgeRegeneratePageResponse {
  readonly page: KnowledgePageDetail | null;
  readonly job: KnowledgeJob | null;
  readonly pages: KnowledgePageListResponse;
}

export interface KnowledgeLintWikiResponse {
  readonly report_markdown: string;
  readonly job: KnowledgeJob | null;
  readonly pages: KnowledgePageListResponse;
}

export interface KnowledgeDeletePagesResponse {
  readonly deleted_pages: number;
  readonly deleted_log_entries: number;
  readonly job: KnowledgeJob | null;
  readonly pages: KnowledgePageListResponse;
}

export interface KnowledgeDeleteSourceResponse {
  readonly source_id: string;
  readonly deleted_documents: number;
  readonly deleted_chunks: number;
  readonly vector_status: {
    readonly backend: string;
    readonly provider_name: string;
    readonly count: number;
    readonly dim?: number;
  };
}

export const getKnowledgeHealth = async (): Promise<KnowledgeHealth> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/health`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_health') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error && !message.data?.ok) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const getKnowledgeSources = async (): Promise<readonly KnowledgeSourceSummary[]> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/sources`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_sources') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return Array.isArray(message.data?.items) ? message.data.items : [];
};

export const createKnowledgeLocalSource = async (path: string): Promise<KnowledgeCreateResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/sources/local`, {
    method: 'POST',
    body: JSON.stringify({ path }),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_source_create') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const createKnowledgeUrlSource = async (url: string): Promise<KnowledgeCreateResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/sources/url`, {
    method: 'POST',
    body: JSON.stringify({ url }),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_source_create') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const createKnowledgeUploadedSource = async (file: File): Promise<KnowledgeCreateResponse> => {
  const formData = new FormData();
  formData.append('file', file);
  const response = await fetch(`${API_BASE_URL}/knowledge/sources/upload`, {
    method: 'POST',
    body: formData,
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_source_create') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const deleteKnowledgeSource = async (sourceId: string): Promise<KnowledgeDeleteSourceResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/sources/${encodeURIComponent(sourceId)}`, {
    method: 'DELETE',
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_source_delete') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const searchKnowledge = async (query: string, limit = 10): Promise<KnowledgeSearchResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/search?${new URLSearchParams({ q: query, limit: String(limit) })}`, {
    method: 'GET',
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_search') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const getKnowledgeDocument = async (documentId: string): Promise<KnowledgeDocumentDetail> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/documents/${encodeURIComponent(documentId)}`, {
    method: 'GET',
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_document_detail') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const getKnowledgePages = async (): Promise<KnowledgePageListResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/pages`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_pages') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const deleteKnowledgePages = async (): Promise<KnowledgeDeletePagesResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/pages`, { method: 'DELETE' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_pages_delete') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const getKnowledgePage = async (pageId: string): Promise<KnowledgePageDetail> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/pages/${encodeURIComponent(pageId)}`, {
    method: 'GET',
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_page_detail') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const generateKnowledgePages = async (): Promise<KnowledgeGeneratePagesResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/pages/generate`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_pages_generate') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const lintKnowledgeWiki = async (): Promise<KnowledgeLintWikiResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/wiki/lint`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_wiki_lint') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const regenerateKnowledgePage = async (pageId: string): Promise<KnowledgeRegeneratePageResponse> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/pages/${encodeURIComponent(pageId)}/generate`, {
    method: 'POST',
    body: JSON.stringify({}),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_page_generate') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return message.data;
};

export const getKnowledgeJobs = async (): Promise<readonly KnowledgeJob[]> => {
  const response = await fetch(`${API_BASE_URL}/knowledge/jobs`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'knowledge_jobs') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  if (message.error) {
    throw new Error(String(message.error));
  }
  return Array.isArray(message.data?.items) ? message.data.items : [];
};

export interface ConfiguredNodeInventoryHost {
  readonly id: string;
  readonly display_name: string;
  readonly ssh_target: string;
  readonly hostname_hint: string;
  readonly line_number: number;
  readonly parallax_path: string;
  readonly joined: boolean;
  readonly management_mode: 'ssh_managed' | 'manual';
  readonly network_scope: 'local' | 'remote';
  readonly hardware?: {
    readonly gpu_name?: string;
    readonly gpu_num?: number;
    readonly gpu_memory_gb?: number;
    readonly ram_total_gb?: number;
    readonly updated_at?: number;
  };
  readonly linked_clusters: readonly { id: string; name: string }[];
  readonly linked_cluster_ids: readonly string[];
  readonly linked_cluster_names: readonly string[];
  readonly linked_cluster_count: number;
}

export const getNodesInventory = async (): Promise<{ hosts: readonly ConfiguredNodeInventoryHost[] }> => {
  const response = await fetch(`${API_BASE_URL}/nodes/inventory`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'nodes_inventory') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};

export const updateNodesInventory = async (hosts: readonly {
  id?: string;
  display_name?: string;
  ssh_target: string;
  parallax_path: string;
  hostname_hint?: string;
  management_mode?: 'ssh_managed' | 'manual';
  network_scope?: 'local' | 'remote';
  hardware?: {
    gpu_name?: string;
    gpu_num?: number;
    gpu_memory_gb?: number;
    ram_total_gb?: number;
    updated_at?: number;
  };
}[]) => {
  const response = await fetch(`${API_BASE_URL}/nodes/inventory`, {
    method: 'PUT',
    body: JSON.stringify({ hosts }),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'nodes_inventory_update') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data as { ok: boolean; message: string; hosts: readonly ConfiguredNodeInventoryHost[] };
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
  readonly lifecycle?: {
    readonly summary?: string;
    readonly management?: {
      readonly mode?: string;
      readonly last_action_state?: string;
      readonly last_action_message?: string;
      readonly checked_at?: number;
    };
    readonly process?: {
      readonly state?: string;
      readonly pid?: string;
      readonly source?: string;
      readonly message?: string;
      readonly checked_at?: number;
    };
    readonly scheduler?: {
      readonly membership?: string;
      readonly node_id?: string | null;
      readonly status?: string | null;
      readonly joined?: boolean;
    };
    readonly serving?: {
      readonly state?: string;
      readonly start_layer?: number | null;
      readonly end_layer?: number | null;
      readonly total_layers?: number | null;
    };
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

export const probeNodeHost = async (sshTarget: string, parallaxPath: string): Promise<{
  ok: boolean;
  message: string;
  ssh_target: string;
  parallax_path: string;
  ssh_reachable: boolean;
  stdout?: string;
  stderr?: string;
  return_code?: number | null;
  os_name?: string;
  remote_user?: string;
  remote_host?: string;
  ram_total_gb?: number;
  gpu_name?: string;
  gpu_num?: number;
  gpu_memory_gb?: number;
  path_exists?: boolean;
  has_venv_activate?: boolean;
  has_parallax_bin?: boolean;
  notes?: readonly string[];
}> => {
  const response = await fetch(`${API_BASE_URL}/nodes/probe`, {
    method: 'POST',
    body: JSON.stringify({ ssh_target: sshTarget, parallax_path: parallaxPath }),
  });
  const message = await parseJsonResponse(response);
  if (message.type !== 'node_probe') {
    if (message?.detail === 'Not Found' || (response.status === 404 && message?.type === undefined)) {
      throw new Error('SSH probe endpoint is unavailable. Restart the backend to load /nodes/probe.');
    }
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

export const getFrontendBuildStatus = async (): Promise<any> => {
  const response = await fetch(`${API_BASE_URL}/frontend/build_status`, { method: 'GET' });
  const message = await parseJsonResponse(response);
  if (message.type !== 'frontend_build_status') {
    throw new Error(`Invalid message type: ${message.type}.`);
  }
  return message.data;
};
