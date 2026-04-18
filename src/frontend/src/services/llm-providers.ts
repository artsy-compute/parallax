export type LlmProviderId = 'local_cluster' | 'openai' | 'anthropic' | 'compatible';

export interface LlmProviderConfig {
  id: LlmProviderId;
  enabled: boolean;
  displayName: string;
  defaultModel: string;
  apiKey: string;
  baseUrl: string;
  useActiveCluster: boolean;
  clusterId: string;
}

export interface KnowledgeGenerationConfig {
  provider: LlmProviderId;
  model: string;
}

export const LLM_PROVIDER_LABELS: Record<LlmProviderId, string> = {
  local_cluster: 'Local cluster',
  openai: 'OpenAI',
  anthropic: 'Anthropic',
  compatible: 'Compatible endpoint',
};

const DEFAULT_PROVIDERS: Record<LlmProviderId, LlmProviderConfig> = {
  local_cluster: {
    id: 'local_cluster',
    enabled: true,
    displayName: LLM_PROVIDER_LABELS.local_cluster,
    defaultModel: '',
    apiKey: '',
    baseUrl: '',
    useActiveCluster: true,
    clusterId: '',
  },
  openai: {
    id: 'openai',
    enabled: false,
    displayName: LLM_PROVIDER_LABELS.openai,
    defaultModel: '',
    apiKey: '',
    baseUrl: 'https://api.openai.com/v1',
    useActiveCluster: false,
    clusterId: '',
  },
  anthropic: {
    id: 'anthropic',
    enabled: false,
    displayName: LLM_PROVIDER_LABELS.anthropic,
    defaultModel: '',
    apiKey: '',
    baseUrl: 'https://api.anthropic.com',
    useActiveCluster: false,
    clusterId: '',
  },
  compatible: {
    id: 'compatible',
    enabled: false,
    displayName: LLM_PROVIDER_LABELS.compatible,
    defaultModel: '',
    apiKey: '',
    baseUrl: '',
    useActiveCluster: false,
    clusterId: '',
  },
};

const normalizeProvider = (id: LlmProviderId, raw: unknown): LlmProviderConfig => {
  const value = raw && typeof raw === 'object' ? (raw as Record<string, unknown>) : {};
  const defaults = DEFAULT_PROVIDERS[id];
  return {
    id,
    enabled: value.enabled === undefined ? defaults.enabled : Boolean(value.enabled),
    displayName: String(value.display_name || defaults.displayName).trim() || defaults.displayName,
    defaultModel: String(value.default_model || '').trim(),
    apiKey: String(value.api_key || '').trim(),
    baseUrl: String(value.base_url || defaults.baseUrl || '').trim(),
    useActiveCluster: value.use_active_cluster === undefined ? defaults.useActiveCluster : Boolean(value.use_active_cluster),
    clusterId: String(value.cluster_id || '').trim(),
  };
};

export const parseLlmProviderConfigs = (advanced: Record<string, unknown> | undefined): Record<LlmProviderId, LlmProviderConfig> => {
  const raw = advanced && typeof advanced.llm_providers === 'object' && advanced.llm_providers
    ? (advanced.llm_providers as Record<string, unknown>)
    : {};
  return {
    local_cluster: normalizeProvider('local_cluster', raw.local_cluster),
    openai: normalizeProvider('openai', raw.openai),
    anthropic: normalizeProvider('anthropic', raw.anthropic),
    compatible: normalizeProvider('compatible', raw.compatible),
  };
};

export const enabledLlmProviders = (providers: Record<LlmProviderId, LlmProviderConfig>): LlmProviderConfig[] => (
  (Object.keys(LLM_PROVIDER_LABELS) as LlmProviderId[])
    .map((id) => providers[id])
    .filter((provider) => provider.enabled)
);

export const parseKnowledgeGenerationConfig = (
  advanced: Record<string, unknown> | undefined,
  providers: Record<LlmProviderId, LlmProviderConfig>,
): KnowledgeGenerationConfig => {
  const raw = advanced && typeof advanced.knowledge_generation === 'object' && advanced.knowledge_generation
    ? (advanced.knowledge_generation as Record<string, unknown>)
    : {};
  const enabledProviders = enabledLlmProviders(providers);
  const defaultProvider = enabledProviders[0]?.id || 'local_cluster';
  const provider = (Object.keys(LLM_PROVIDER_LABELS) as LlmProviderId[]).includes(raw.provider as LlmProviderId)
    ? (raw.provider as LlmProviderId)
    : defaultProvider;
  const normalizedProvider = providers[provider]?.enabled ? provider : defaultProvider;
  return {
    provider: normalizedProvider,
    model: String(raw.model || providers[normalizedProvider]?.defaultModel || '').trim(),
  };
};

export const mergeAdvancedLlmSettings = (
  advanced: Record<string, unknown> | undefined,
  providers: Record<LlmProviderId, LlmProviderConfig>,
  generation: KnowledgeGenerationConfig | null,
): Record<string, unknown> => {
  const nextAdvanced = { ...(advanced || {}) };
  nextAdvanced.llm_providers = {
    local_cluster: {
      enabled: providers.local_cluster.enabled,
      display_name: providers.local_cluster.displayName,
      default_model: providers.local_cluster.defaultModel,
      use_active_cluster: providers.local_cluster.useActiveCluster,
      cluster_id: providers.local_cluster.clusterId,
    },
    openai: {
      enabled: providers.openai.enabled,
      display_name: providers.openai.displayName,
      default_model: providers.openai.defaultModel,
      base_url: providers.openai.baseUrl,
      api_key: providers.openai.apiKey,
    },
    anthropic: {
      enabled: providers.anthropic.enabled,
      display_name: providers.anthropic.displayName,
      default_model: providers.anthropic.defaultModel,
      base_url: providers.anthropic.baseUrl,
      api_key: providers.anthropic.apiKey,
    },
    compatible: {
      enabled: providers.compatible.enabled,
      display_name: providers.compatible.displayName,
      default_model: providers.compatible.defaultModel,
      base_url: providers.compatible.baseUrl,
      api_key: providers.compatible.apiKey,
    },
  };
  if (generation) {
    nextAdvanced.knowledge_generation = {
      provider: generation.provider,
      model: generation.model,
    };
  }
  return nextAdvanced;
};

export const maskSecret = (value: string) => {
  const normalized = String(value || '').trim();
  if (!normalized) {
    return 'Not configured';
  }
  if (normalized.length <= 8) {
    return 'Configured';
  }
  return `${normalized.slice(0, 4)}…${normalized.slice(-4)}`;
};
