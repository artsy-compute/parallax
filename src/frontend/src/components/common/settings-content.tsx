import { useEffect, useRef, useState, type ChangeEvent, type FC } from 'react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Checkbox,
  Chip,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  InputAdornment,
  IconButton,
  MenuItem,
  Pagination,
  Stack,
  Tab,
  Tabs,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import {
  IconAdjustments,
  IconCheck,
  IconCirclesRelation,
  IconDownload,
  IconMessageCircle,
  IconInfoCircle,
  IconLoader,
  IconPlus,
  IconSettings2,
  IconStack3,
  IconTransfer,
  IconTrash,
  IconX,
} from '@tabler/icons-react';
import { useCluster, useHost } from '../../services';
import {
  addCustomModel,
  deleteChatHistoryConversation,
  deleteCustomModel,
  deleteAllChatHistory,
  exportSettingsBundle,
  getChatHistoryDetail,
  getChatHistoryList,
  getCustomModelList,
  getCustomModelSources,
  getNodesInventory,
  importSettingsBundle,
  probeNodeHost,
  searchCustomModels,
  updateNodesInventory,
  updateAppSettings,
  type SettingsExportBundle,
  type AppClusterProfile,
  type ChatHistorySummary,
  type CustomModelRecord,
  type CustomModelSearchResult,
  type CustomModelSourceOption,
  type CustomModelSourceRoot,
} from '../../services/api';
import { useRefCallback } from '../../hooks';
import { NodeManagementContent } from './node-management-content';
import { JoinCommand, ModelSelect } from '../inputs';
import { AlertDialog } from '../mui';

type SettingsSectionKey = 'general' | 'cluster' | 'custom-models' | 'nodes' | 'chat' | 'transfer';

const SETTINGS_SECTIONS: ReadonlyArray<{ key: SettingsSectionKey; label: string; icon: FC<{ size?: number }> }> = [
  { key: 'general', label: 'Overview', icon: IconSettings2 },
  { key: 'nodes', label: 'Nodes', icon: IconCirclesRelation },
  { key: 'cluster', label: 'Clusters', icon: IconStack3 },
  { key: 'custom-models', label: 'Custom Models', icon: IconAdjustments },
  { key: 'chat', label: 'Chats', icon: IconMessageCircle },
  { key: 'transfer', label: 'Import & Export', icon: IconTransfer },
];

const CHAT_HISTORY_PAGE_SIZE = 20;
const CHAT_HISTORY_LABEL_MAX_CHARS = 56;

const cleanChatHistoryLabel = (value: string) =>
  (value || '')
    .replace(/<think>[\s\S]*?<\/think>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const truncateChatHistoryLabel = (value: string, maxChars = CHAT_HISTORY_LABEL_MAX_CHARS) => {
  if (value.length <= maxChars) {
    return value;
  }
  return value.slice(0, Math.max(0, maxChars - 1)).trimEnd() + '…';
};

const renderModelVramRequirement = (vram?: number) => (
  Number(vram || 0) > 0 ? (
    <Alert severity='warning' icon={false}>
      <Box component='span' sx={{ whiteSpace: 'nowrap' }}>
        You&apos;ll need a <strong>{`minimum of ${Number(vram)} GB of total VRAM`}</strong> to host this model.
      </Box>
    </Alert>
  ) : null
);

export const SettingsContent: FC<{ routeSection?: string }> = ({ routeSection = 'cluster' }) => {
  const navigate = useNavigate();
  const [{ type: hostType }] = useHost();
  const [
    {
      config: { modelInfo, modelInfoList, networkType, initNodesNumber, modelName: selectedModelName, activeClusterId, clusterProfiles },
      clusterInfo: {
        status: clusterStatus,
        topologyChangeAdvisory,
        initNodesNumber: clusterInitNodesNumber,
        modelName: clusterModelName,
      },
      nodeInfoList,
    },
    {
      config: { setNetworkType, setInitNodesNumber },
      rebalanceTopology,
      refreshModelList,
      init,
      reloadSettings,
      setActiveCluster,
      saveClusterProfiles,
    },
  ] = useCluster();

  const [customModels, setCustomModels] = useState<readonly CustomModelRecord[]>([]);
  const [customModelLoading, setCustomModelLoading] = useState(false);
  const [customModelError, setCustomModelError] = useState('');
  const [customModelSourceType, setCustomModelSourceType] = useState<'huggingface' | 'scheduler_root' | 'url'>('huggingface');
  const [customModelSourceValue, setCustomModelSourceValue] = useState('');
  const [customModelDisplayName, setCustomModelDisplayName] = useState('');
  const [customModelSourceRoots, setCustomModelSourceRoots] = useState<readonly CustomModelSourceRoot[]>([]);
  const [customModelSourceOptions, setCustomModelSourceOptions] = useState<readonly CustomModelSourceOption[]>([]);
  const [customModelSubmitting, setCustomModelSubmitting] = useState(false);
  const [customModelDeletingId, setCustomModelDeletingId] = useState('');
  const [pendingDeleteCustomModel, setPendingDeleteCustomModel] = useState<null | { id: string; label: string }>(null);
  const [customModelEditorOpen, setCustomModelEditorOpen] = useState(false);
  const [customModelSearchLoading, setCustomModelSearchLoading] = useState(false);
  const [customModelSearchResults, setCustomModelSearchResults] = useState<readonly CustomModelSearchResult[]>([]);
  const [customModelSearchOpen, setCustomModelSearchOpen] = useState(false);
  const [customModelSearchHasMore, setCustomModelSearchHasMore] = useState(false);
  const [customModelSearchLoadingMore, setCustomModelSearchLoadingMore] = useState(false);
  const [customModelSearchNextOffset, setCustomModelSearchNextOffset] = useState(0);
  const customModelSearchCacheRef = useRef<Record<string, {
    items: readonly CustomModelSearchResult[];
    nextOffset: number;
    hasMore: boolean;
  }>>({});
  const customModelSearchRequestIdRef = useRef(0);
  const [rebalancingTopology, setRebalancingTopology] = useState(false);
  const [initializingCluster, setInitializingCluster] = useState(false);
  const [chatHistoryItems, setChatHistoryItems] = useState<readonly ChatHistorySummary[]>([]);
  const [chatHistoryCount, setChatHistoryCount] = useState(0);
  const [chatHistoryPage, setChatHistoryPage] = useState(1);
  const [chatHistoryLoading, setChatHistoryLoading] = useState(false);
  const [clearingChatHistory, setClearingChatHistory] = useState(false);
  const [chatHistoryError, setChatHistoryError] = useState('');
  const [chatHistoryExportingId, setChatHistoryExportingId] = useState('');
  const [chatHistoryDeletingId, setChatHistoryDeletingId] = useState('');
  const [pendingDeleteConversation, setPendingDeleteConversation] = useState<null | { id: string; label: string }>(null);
  const [pendingDeleteCluster, setPendingDeleteCluster] = useState<null | { id: string; label: string }>(null);
  const [pendingDeleteNode, setPendingDeleteNode] = useState<null | { id: string; label: string }>(null);
  const [nodesInventory, setNodesInventory] = useState<Array<{
    local_id: string;
    id: string;
    display_name: string;
    ssh_target: string;
    parallax_path: string;
    hostname_hint: string;
    joined: boolean;
    management_mode: 'ssh_managed' | 'manual';
    network_scope: 'local' | 'remote';
    hardware?: {
      gpu_name?: string;
      gpu_num?: number;
      gpu_memory_gb?: number;
      ram_total_gb?: number;
      updated_at?: number;
    };
    linked_clusters: readonly { id: string; name: string }[];
    linked_cluster_ids: readonly string[];
    linked_cluster_names: readonly string[];
    linked_cluster_count: number;
  }>>([]);
  const [nodesInventoryLoading, setNodesInventoryLoading] = useState(false);
  const [nodesInventorySaving, setNodesInventorySaving] = useState(false);
  const [nodesInventoryMessage, setNodesInventoryMessage] = useState('');
  const [nodesOverviewRefreshToken, setNodesOverviewRefreshToken] = useState(0);
  const [nodeEditorOpen, setNodeEditorOpen] = useState(false);
  const [nodeEditorMode, setNodeEditorMode] = useState<'add' | 'edit'>('add');
  const [nodeEditingLocalId, setNodeEditingLocalId] = useState('');
  const [nodeEditorTab, setNodeEditorTab] = useState<'discovered' | 'manual'>('manual');
  const [nodeDraft, setNodeDraft] = useState<{
    display_name: string;
    ssh_target: string;
    parallax_path: string;
    hostname_hint: string;
    management_mode: 'ssh_managed' | 'manual';
  }>({
    display_name: '',
    ssh_target: '',
    parallax_path: '',
    hostname_hint: '',
    management_mode: 'ssh_managed',
  });
  const [nodeDraftProbeLoading, setNodeDraftProbeLoading] = useState(false);
  const [nodeDraftProbeResult, setNodeDraftProbeResult] = useState<null | {
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
  }>(null);
  const [nodeDraftProbeError, setNodeDraftProbeError] = useState('');
  const [clusterNameDraft, setClusterNameDraft] = useState('');
  const [clusterModalOpen, setClusterModalOpen] = useState(false);
  const [clusterModalMode, setClusterModalMode] = useState<'add' | 'configure'>('configure');
  const [clusterDraftName, setClusterDraftName] = useState('');
  const [clusterDraftModelName, setClusterDraftModelName] = useState('');
  const [clusterDraftAssignedNodeIds, setClusterDraftAssignedNodeIds] = useState<readonly string[]>([]);
  const [transferMessage, setTransferMessage] = useState('');
  const [transferError, setTransferError] = useState('');
  const [importingSettings, setImportingSettings] = useState(false);
  const [exportingSettings, setExportingSettings] = useState(false);
  const importFileInputRef = useRef<HTMLInputElement | null>(null);
  const inventoryRowIdRef = useRef(0);

  const nextInventoryRowId = () => {
    inventoryRowIdRef.current += 1;
    return `inventory-row-${inventoryRowIdRef.current}`;
  };
  const normalizeHostname = (value: string) => {
    let text = String(value || '').trim().toLowerCase();
    if (!text) {
      return '';
    }
    if (text.includes('@')) {
      text = text.split('@', 2)[1] || '';
    }
    if (text.startsWith('[')) {
      const closing = text.indexOf(']');
      if (closing > 0) {
        return text.slice(1, closing).trim().toLowerCase();
      }
    }
    if (text.split(':').length === 2) {
      const [host, port] = text.split(':');
      if (port && /^\d+$/.test(port)) {
        text = host || '';
      }
    }
    return text.trim().toLowerCase();
  };
  const mapInventoryHost = (host: Awaited<ReturnType<typeof getNodesInventory>>['hosts'][number]) => ({
    local_id: nextInventoryRowId(),
    id: host.id || '',
    display_name: host.display_name || host.ssh_target || host.hostname_hint || '',
    ssh_target: host.ssh_target || '',
    parallax_path: host.parallax_path || '',
    hostname_hint: host.hostname_hint || '',
    joined: !!host.joined,
    management_mode: host.management_mode || 'ssh_managed',
    network_scope: host.network_scope || 'remote',
    hardware: host.hardware || {},
    linked_clusters: host.linked_clusters || [],
    linked_cluster_ids: host.linked_cluster_ids || [],
    linked_cluster_names: host.linked_cluster_names || [],
    linked_cluster_count: Number(host.linked_cluster_count || 0),
  });
  const loadCustomModels = async () => {
    try {
      setCustomModelLoading(true);
      setCustomModelError('');
      setCustomModels(await getCustomModelList());
    } catch (error) {
      setCustomModelError(error instanceof Error ? error.message : 'Failed to load custom models');
    } finally {
      setCustomModelLoading(false);
    }
  };

  const loadCustomModelSources = useRefCallback(async () => {
    const data = await getCustomModelSources();
    setCustomModelSourceRoots(data.allowed_local_roots || []);
    setCustomModelSourceOptions(data.allowed_local_model_options || []);
    if (
      customModelSourceType === 'scheduler_root'
      && !customModelSourceValue
      && (data.allowed_local_model_options || []).length > 0
    ) {
      setCustomModelSourceValue(String(data.allowed_local_model_options?.[0]?.source_value || ''));
    }
  });

  const activeSection = SETTINGS_SECTIONS.some((item) => item.key === routeSection)
    ? (routeSection as SettingsSectionKey)
    : 'cluster';

  useEffect(() => {
    if (hostType !== 'node') {
      loadCustomModels();
      loadCustomModelSources().catch((error) => {
        console.error('getCustomModelSources error', error);
      });
    }
  }, [hostType, loadCustomModelSources]);

  useEffect(() => {
    if (hostType === 'node' || activeSection !== 'custom-models' || !customModelEditorOpen) {
      return;
    }
    loadCustomModelSources().catch((error) => {
      console.error('getCustomModelSources error', error);
    });
  }, [hostType, activeSection, customModelEditorOpen, loadCustomModelSources]);

  const selectedCluster = clusterProfiles.find((item) => item.id === activeClusterId) || clusterProfiles[0];
  const selectedClusterAssignedNodeIds = new Set(selectedCluster?.assigned_node_ids || []);
  const availableNodeByHostname = new Map(
    nodeInfoList
      .filter((node) => node.status === 'available')
      .map((node) => [normalizeHostname(node.hostname), node] as const)
      .filter(([hostname]) => !!hostname),
  );
  const getHostGpuMemoryGb = (
    host: { hardware?: { gpu_memory_gb?: number; ram_total_gb?: number } },
    node?: { gpuMemory?: number; ramTotalGb?: number },
  ) => Math.max(0, Number(node?.gpuMemory || host.hardware?.gpu_memory_gb || 0));
  const getHostClusterMemoryGb = (
    host: { hardware?: { gpu_memory_gb?: number; ram_total_gb?: number } },
    node?: { gpuMemory?: number; ramTotalGb?: number },
  ) => {
    const gpuMemoryGb = getHostGpuMemoryGb(host, node);
    if (gpuMemoryGb > 0) {
      return gpuMemoryGb;
    }
    return Math.max(0, Number(node?.ramTotalGb || host.hardware?.ram_total_gb || 0));
  };
  const assignedHosts = nodesInventory.filter((host) => selectedClusterAssignedNodeIds.has(host.id));
  const assignedHostDetails = assignedHosts.map((host) => ({
    host,
    node: availableNodeByHostname.get(normalizeHostname(host.hostname_hint || host.ssh_target)),
  }));
  const availableAssignedHosts = assignedHosts.filter((host) => host.joined);
  const assignedTotalMemory = assignedHostDetails
    .reduce((sum, item) => sum + getHostClusterMemoryGb(item.host, item.node), 0);
  const knownAssignedHardwareCount = assignedHostDetails.filter((item) => getHostClusterMemoryGb(item.host, item.node) > 0).length;
  const remainingMemoryGap = Math.max(0, Number(modelInfo?.vram || 0) - assignedTotalMemory);
  const formatChatTimestamp = (value?: number) => {
    if (!value) {
      return '';
    }
    try {
      return new Date(value * 1000).toLocaleString();
    } catch {
      return '';
    }
  };
  const configuredHostnames = new Set(
    nodesInventory
      .map((host) => normalizeHostname(host.hostname_hint || host.ssh_target))
      .filter((hostname) => !!hostname),
  );
  const draftAssignedHosts = nodesInventory.filter((host) => clusterDraftAssignedNodeIds.includes(host.id));
  const draftAvailableAssignedHosts = draftAssignedHosts.filter((host) =>
    nodeInfoList.some((node) => node.id === host.id && node.status === 'available'),
  );
  const draftAssignedHostDetails = draftAssignedHosts.map((host) => ({
    host,
    node: nodeInfoList.find((node) => node.id === host.id),
  }));
  const draftAvailableAssignedMemory = draftAssignedHostDetails
    .reduce((sum, item) => sum + getHostClusterMemoryGb(item.host, item.node), 0);
  const draftKnownAssignedHardwareCount = draftAssignedHostDetails.filter((item) => getHostClusterMemoryGb(item.host, item.node) > 0).length;
  const draftModelInfo = modelInfoList.find((item) => item.name === clusterDraftModelName);
  const draftRemainingMemoryGap = Math.max(0, Number(draftModelInfo?.vram || 0) - draftAvailableAssignedMemory);
  const clusterModalCapacityAlert = (() => {
    if (clusterModalMode === 'add') {
      if (!draftModelInfo || draftModelInfo.vram <= 0) {
        return <Alert severity='info'>Model memory requirement is unknown, so Parallax cannot confirm cluster capacity yet.</Alert>;
      }
      if (clusterDraftAssignedNodeIds.length === 0) {
        return <Alert severity='warning'>No nodes are assigned to this cluster yet.</Alert>;
      }
      if (draftAvailableAssignedMemory >= draftModelInfo.vram) {
        return <Alert severity='success'>Assigned nodes currently report enough memory for this model.</Alert>;
      }
      return (
        <Alert severity='warning'>
          Assigned nodes currently report {draftAvailableAssignedMemory} GB, about {draftRemainingMemoryGap} GB short of the model requirement.
        </Alert>
      );
    }
    if (!modelInfo || modelInfo.vram <= 0) {
      return <Alert severity='info'>Model memory requirement is unknown, so Parallax cannot confirm cluster capacity yet.</Alert>;
    }
    if (selectedClusterAssignedNodeIds.size === 0) {
      return <Alert severity='warning'>No nodes are assigned to this cluster yet.</Alert>;
    }
    if (assignedTotalMemory >= modelInfo.vram) {
      return <Alert severity='success'>Assigned nodes currently provide enough memory for this model.</Alert>;
    }
    return (
      <Alert severity='warning'>
        Assigned nodes currently provide {assignedTotalMemory} GB, about {remainingMemoryGap} GB short of the model requirement.
      </Alert>
    );
  })();
  const canProceedWithAddCluster = !!draftModelInfo && draftModelInfo.vram > 0 && draftAvailableAssignedMemory >= draftModelInfo.vram;
  const canProceedWithConfiguredCluster = !!modelInfo && modelInfo.vram > 0 && assignedTotalMemory >= modelInfo.vram;
  const discoveredNodeCandidates = nodeInfoList
    .filter((node) => !!normalizeHostname(node.hostname))
    .filter((node) => !configuredHostnames.has(normalizeHostname(node.hostname)));

  useEffect(() => {
    if (!nodeEditorOpen) {
      return;
    }
    if (discoveredNodeCandidates.length === 0 && nodeEditorTab === 'discovered') {
      setNodeEditorTab('manual');
    }
  }, [discoveredNodeCandidates.length, nodeEditorOpen, nodeEditorTab]);

  useEffect(() => {
    if (!nodeEditorOpen || (nodeEditorMode === 'add' && nodeEditorTab !== 'manual')) {
      setNodeDraftProbeLoading(false);
      setNodeDraftProbeResult(null);
      setNodeDraftProbeError('');
      return;
    }
    if (nodeDraft.management_mode !== 'ssh_managed') {
      setNodeDraftProbeLoading(false);
      setNodeDraftProbeResult(null);
      setNodeDraftProbeError('');
      return;
    }
    const sshTarget = nodeDraft.ssh_target.trim();
    const parallaxPath = nodeDraft.parallax_path.trim();
    if (!sshTarget || !parallaxPath) {
      setNodeDraftProbeLoading(false);
      setNodeDraftProbeResult(null);
      setNodeDraftProbeError('');
      return;
    }
    setNodeDraftProbeResult(null);
    setNodeDraftProbeError('');
    let cancelled = false;
    const timeoutId = window.setTimeout(async () => {
      try {
        setNodeDraftProbeLoading(true);
        const result = await probeNodeHost(sshTarget, parallaxPath);
        if (cancelled) {
          return;
        }
        setNodeDraftProbeResult(result);
      } catch (error) {
        if (cancelled) {
          return;
        }
        setNodeDraftProbeResult(null);
        setNodeDraftProbeError(error instanceof Error ? error.message : 'Failed to probe SSH node');
      } finally {
        if (!cancelled) {
          setNodeDraftProbeLoading(false);
        }
      }
    }, 500);
    return () => {
      cancelled = true;
      window.clearTimeout(timeoutId);
    };
  }, [nodeDraft.management_mode, nodeDraft.parallax_path, nodeDraft.ssh_target, nodeEditorMode, nodeEditorOpen, nodeEditorTab]);
  const customModelUrlValidationError = (() => {
    if (customModelSourceType !== 'url') {
      return '';
    }
    const raw = customModelSourceValue.trim();
    if (!raw) {
      return '';
    }
    try {
      const parsed = new URL(raw);
      if (!['http:', 'https:'].includes(parsed.protocol)) {
        return 'URL source must use http or https.';
      }
      if (!parsed.hostname) {
        return 'URL source must include a host.';
      }
      const filename = parsed.pathname.split('/').filter(Boolean).pop() || '';
      if (!filename) {
        return 'URL source must point to a downloadable archive file.';
      }
      const lower = filename.toLowerCase();
      if (!['.zip', '.tar', '.tar.gz', '.tgz', '.tar.bz2', '.tar.xz'].some((suffix) => lower.endsWith(suffix))) {
        return 'URL source must point to a .zip or .tar archive.';
      }
      return '';
    } catch {
      return 'Enter a valid archive URL.';
    }
  })();
  const customModelSearchStatus = (() => {
    if (customModelSourceType !== 'huggingface') {
      return null;
    }
    const query = customModelSourceValue.trim();
    if (!query) {
      return {
        severity: 'info' as const,
        message: 'Type a Hugging Face repo id or keyword to search. The scheduler validates matches by fetching model config metadata.',
      };
    }
    if (customModelSearchLoading) {
      return {
        severity: 'info' as const,
        message: `Searching Hugging Face for "${query}" and validating matching model configs on the scheduler…`,
      };
    }
    if (customModelSearchResults.length > 0) {
      return {
        severity: 'success' as const,
        message: `Found ${customModelSearchResults.length} validated match${customModelSearchResults.length === 1 ? '' : 'es'}${customModelSearchHasMore ? ' so far' : ''}. Pick one below or keep typing to refine.`,
      };
    }
    if (query && customModelSearchResults.length === 0) {
      return {
        severity: 'warning' as const,
        message: `No validated Hugging Face matches found for "${query}".`,
      };
    }
    return null;
  })();

  const loadChatHistoryPage = useRefCallback(async (page: number) => {
    const normalizedPage = Math.max(1, Number(page || 1));
    try {
      setChatHistoryLoading(true);
      setChatHistoryError('');
      const result = await getChatHistoryList(
        CHAT_HISTORY_PAGE_SIZE,
        (normalizedPage - 1) * CHAT_HISTORY_PAGE_SIZE,
      );
      setChatHistoryItems(result.items || []);
      setChatHistoryCount(Number(result.total || 0));
      const maxPage = Math.max(1, Math.ceil(Math.max(0, Number(result.total || 0)) / CHAT_HISTORY_PAGE_SIZE));
      if (normalizedPage > maxPage) {
        setChatHistoryPage(maxPage);
      }
    } catch (error) {
      console.error('getChatHistoryList error', error);
      setChatHistoryItems([]);
      setChatHistoryCount(0);
      setChatHistoryError(error instanceof Error ? error.message : 'Failed to load chat history');
    } finally {
      setChatHistoryLoading(false);
    }
  });

  useEffect(() => {
    if (activeSection !== 'chat') {
      return;
    }
    loadChatHistoryPage(chatHistoryPage);
  }, [activeSection, chatHistoryPage, loadChatHistoryPage]);

  useEffect(() => {
    setClusterNameDraft(selectedCluster?.name || '');
  }, [selectedCluster?.id, selectedCluster?.name]);

  const loadInventory = useRefCallback(async () => {
    try {
      setNodesInventoryLoading(true);
      setNodesInventoryMessage('');
      const result = await getNodesInventory();
      setNodesInventory((result.hosts || []).map(mapInventoryHost));
    } catch (error) {
      setNodesInventoryMessage(error instanceof Error ? error.message : 'Failed to load configured node inventory');
    } finally {
      setNodesInventoryLoading(false);
    }
  });

  useEffect(() => {
    loadInventory();
  }, [loadInventory]);

  useEffect(() => {
    if (hostType === 'node' || activeSection !== 'nodes') {
      return;
    }
    loadInventory();
  }, [hostType, activeSection, loadInventory]);

  useEffect(() => {
    if (hostType === 'node' || customModelSourceType !== 'huggingface') {
      return;
    }
    const query = customModelSourceValue.trim();
    if (!query) {
      setCustomModelSearchResults([]);
      setCustomModelSearchLoading(false);
      setCustomModelSearchOpen(false);
      setCustomModelSearchHasMore(false);
      setCustomModelSearchNextOffset(0);
      return;
    }
    const timeoutId = window.setTimeout(async () => {
      const cached = customModelSearchCacheRef.current[query];
      if (cached) {
        setCustomModelError('');
        setCustomModelSearchResults(cached.items);
        setCustomModelSearchHasMore(cached.hasMore);
        setCustomModelSearchNextOffset(cached.nextOffset);
        setCustomModelSearchLoading(false);
        setCustomModelSearchOpen(cached.items.length > 0);
        return;
      }
      const requestId = ++customModelSearchRequestIdRef.current;
      try {
        setCustomModelSearchLoading(true);
        setCustomModelError('');
        const result = await searchCustomModels(query, 3, 0);
        if (requestId !== customModelSearchRequestIdRef.current) {
          return;
        }
        customModelSearchCacheRef.current[query] = {
          items: result.items,
          nextOffset: result.next_offset,
          hasMore: result.has_more,
        };
        setCustomModelSearchResults(result.items);
        setCustomModelSearchHasMore(result.has_more);
        setCustomModelSearchNextOffset(result.next_offset);
        setCustomModelSearchOpen(result.items.length > 0);
      } catch (error) {
        if (requestId !== customModelSearchRequestIdRef.current) {
          return;
        }
        setCustomModelError(error instanceof Error ? error.message : 'Failed to search Hugging Face models');
        setCustomModelSearchResults([]);
        setCustomModelSearchOpen(false);
        setCustomModelSearchHasMore(false);
        setCustomModelSearchNextOffset(0);
      } finally {
        if (requestId === customModelSearchRequestIdRef.current) {
          setCustomModelSearchLoading(false);
        }
      }
    }, 300);
    return () => window.clearTimeout(timeoutId);
  }, [hostType, customModelSourceType, customModelSourceValue]);

  useEffect(() => {
    if (customModelSourceType !== 'scheduler_root') {
      return;
    }
    if (!customModelSourceValue && customModelSourceOptions.length > 0) {
      setCustomModelSourceValue(String(customModelSourceOptions[0]?.source_value || ''));
      return;
    }
    if (
      customModelSourceValue
      && !customModelSourceOptions.some((option) => option.source_value === customModelSourceValue)
    ) {
      setCustomModelSourceValue(String(customModelSourceOptions[0]?.source_value || ''));
    }
  }, [customModelSourceType, customModelSourceOptions, customModelSourceValue]);

  const onClickRebalanceTopology = async () => {
    if (rebalancingTopology) {
      return;
    }
    try {
      setRebalancingTopology(true);
      await rebalanceTopology();
    } catch (error) {
      console.error('rebalanceTopology error', error);
    } finally {
      setRebalancingTopology(false);
    }
  };

  const onSaveClusterSettings = useRefCallback(async () => {
    const shouldInit =
      clusterStatus === 'idle'
      || clusterStatus === 'failed'
      || clusterInitNodesNumber !== initNodesNumber
      || clusterModelName !== selectedModelName;
    if (!shouldInit) {
      closeClusterModal();
      return;
    }
    try {
      setInitializingCluster(true);
      await init();
      closeClusterModal();
    } catch (error) {
      console.error('init error', error);
    } finally {
      setInitializingCluster(false);
    }
  });

  const onAddCustomModel = async () => {
    if (customModelSubmitting) {
      return;
    }
    try {
      setCustomModelSubmitting(true);
      setCustomModelError('');
      await addCustomModel({
        source_type: customModelSourceType,
        source_value: customModelSourceValue.trim(),
        display_name: customModelDisplayName.trim(),
      });
      await Promise.all([loadCustomModels(), refreshModelList()]);
      closeCustomModelEditor();
    } catch (error) {
      setCustomModelError(error instanceof Error ? error.message : 'Failed to add custom model');
    } finally {
      setCustomModelSubmitting(false);
    }
  };

  const onDeleteCustomModel = async (modelId: string) => {
    if (!modelId || customModelDeletingId) {
      return;
    }
    try {
      setCustomModelDeletingId(modelId);
      setCustomModelError('');
      await deleteCustomModel(modelId);
      await Promise.all([loadCustomModels(), refreshModelList()]);
    } catch (error) {
      setCustomModelError(error instanceof Error ? error.message : 'Failed to remove custom model');
    } finally {
      setCustomModelDeletingId('');
    }
  };

  const confirmDeleteCustomModel = async () => {
    if (!pendingDeleteCustomModel) {
      return;
    }
    await onDeleteCustomModel(pendingDeleteCustomModel.id);
    setPendingDeleteCustomModel(null);
  };

  const onLoadMoreCustomModelSearchResults = useRefCallback(async () => {
    const query = customModelSourceValue.trim();
    if (
      customModelSourceType !== 'huggingface'
      || !query
      || customModelSearchLoading
      || customModelSearchLoadingMore
      || !customModelSearchHasMore
    ) {
      return;
    }
    try {
      setCustomModelSearchLoadingMore(true);
      setCustomModelError('');
      const result = await searchCustomModels(query, 3, customModelSearchNextOffset);
      setCustomModelSearchResults((prev) => {
        const merged = [
          ...prev,
          ...result.items.filter((item) => !prev.some((existing) => existing.source_value === item.source_value)),
        ];
        customModelSearchCacheRef.current[query] = {
          items: merged,
          nextOffset: result.next_offset,
          hasMore: result.has_more,
        };
        return merged;
      });
      setCustomModelSearchHasMore(result.has_more);
      setCustomModelSearchNextOffset(result.next_offset);
      setCustomModelSearchOpen(result.items.length > 0);
    } catch (error) {
      setCustomModelError(error instanceof Error ? error.message : 'Failed to load more Hugging Face matches');
    } finally {
      setCustomModelSearchLoadingMore(false);
    }
  });

  const closeCustomModelEditor = () => {
    setCustomModelEditorOpen(false);
    setCustomModelError('');
    setCustomModelSourceValue('');
    setCustomModelDisplayName('');
    setCustomModelSearchOpen(false);
    setCustomModelSearchResults([]);
    setCustomModelSearchHasMore(false);
    setCustomModelSearchNextOffset(0);
    setCustomModelSearchLoadingMore(false);
  };

  const renderValidationChip = (status: string) => {
    const normalized = String(status || '').toLowerCase();
    if (normalized === 'verified') {
      return <Chip size='small' color='success' icon={<IconCheck size={14} />} label='Verified' />;
    }
    if (normalized === 'config_only') {
      return <Chip size='small' color='warning' label='Config only' />;
    }
    if (normalized === 'pending') {
      return <Chip size='small' color='info' icon={<IconLoader size={14} />} label='Pending' />;
    }
    return <Chip size='small' color='default' label={normalized || 'Unknown'} />;
  };

  const openSection = (key: SettingsSectionKey) => {
    navigate(`/settings/${key}`);
  };

  const applyClusterProfilesState = async (nextClusters: readonly AppClusterProfile[], nextActiveClusterId: string) => {
    await saveClusterProfiles(nextClusters, nextActiveClusterId);
  };

  const onSelectClusterProfile = async (clusterId: string) => {
    if (!clusterId || clusterId === activeClusterId) {
      return;
    }
    await setActiveCluster(clusterId);
  };

  const onCreateClusterProfile = async (params?: {
    name?: string;
    model_name?: string;
    assigned_node_ids?: readonly string[];
  }) => {
    const nextIndex = clusterProfiles.length + 1;
    const source = selectedCluster || {
      model_name: selectedModelName,
      init_nodes_num: initNodesNumber,
      assigned_node_ids: [],
      is_local_network: networkType === 'local',
      network_type: networkType,
      advanced: {},
    };
    const assignedNodeIds = [...(params?.assigned_node_ids || source.assigned_node_ids || [])];
    const newCluster: AppClusterProfile = {
      id: `cluster-${Date.now()}`,
      name: String(params?.name || '').trim() || `Cluster ${nextIndex}`,
      model_name: String(params?.model_name || source.model_name || ''),
      init_nodes_num: Math.max(1, Number(assignedNodeIds.length || source.init_nodes_num || 1)),
      assigned_node_ids: assignedNodeIds,
      is_local_network: source.network_type === 'remote' ? false : true,
      network_type: source.network_type === 'remote' ? 'remote' : 'local',
      advanced: { ...(source.advanced || {}) },
    };
    await applyClusterProfilesState([...clusterProfiles, newCluster], newCluster.id);
  };

  const closeClusterModal = () => {
    setClusterModalOpen(false);
    setClusterDraftName('');
    setClusterDraftModelName('');
    setClusterDraftAssignedNodeIds([]);
  };

  const onSubmitCreateClusterProfile = async () => {
    await onCreateClusterProfile({
      name: clusterDraftName,
      model_name: clusterDraftModelName,
      assigned_node_ids: clusterDraftAssignedNodeIds,
    });
    closeClusterModal();
  };

  const onSaveClusterName = async () => {
    if (!selectedCluster) {
      return;
    }
    const nextName = clusterNameDraft.trim();
    if (!nextName || nextName === selectedCluster.name) {
      return;
    }
    await applyClusterProfilesState(
      clusterProfiles.map((item) => item.id === selectedCluster.id ? { ...item, name: nextName } : item),
      selectedCluster.id,
    );
  };

  const onDeleteClusterProfile = async (clusterId?: string) => {
    const targetId = String(clusterId || selectedCluster?.id || '').trim();
    if (!targetId || clusterProfiles.length <= 1) {
      return;
    }
    const remaining = clusterProfiles.filter((item) => item.id !== targetId);
    const nextActiveId = targetId === activeClusterId
      ? (remaining[0]?.id || '')
      : (activeClusterId || remaining[0]?.id || '');
    await applyClusterProfilesState(remaining, nextActiveId);
    if (targetId === activeClusterId) {
      setClusterModalOpen(false);
    }
  };

  const openClusterConfig = async (clusterId: string) => {
    if (!clusterId) {
      return;
    }
    if (clusterId !== activeClusterId) {
      await onSelectClusterProfile(clusterId);
    }
    setClusterModalMode('configure');
    setClusterModalOpen(true);
  };

  const openClusterEditor = () => {
    const nextIndex = clusterProfiles.length + 1;
    const source = selectedCluster || {
      model_name: selectedModelName,
      assigned_node_ids: [],
    };
    setClusterDraftName(`Cluster ${nextIndex}`);
    setClusterDraftModelName(String(source.model_name || modelInfoList[0]?.name || ''));
    setClusterDraftAssignedNodeIds([...(source.assigned_node_ids || [])]);
    setClusterModalMode('add');
    setClusterModalOpen(true);
  };

  const onToggleDraftAssignedNode = (hostId: string) => {
    if (!hostId) {
      return;
    }
    setClusterDraftAssignedNodeIds((prev) => (
      prev.includes(hostId)
        ? prev.filter((item) => item !== hostId)
        : [...prev, hostId]
    ));
  };

  const confirmDeleteClusterProfile = async () => {
    if (!pendingDeleteCluster) {
      return;
    }
    await onDeleteClusterProfile(pendingDeleteCluster.id);
    setPendingDeleteCluster(null);
  };

  const onClearAllChatHistory = async () => {
    try {
      setClearingChatHistory(true);
      setChatHistoryError('');
      const result = await deleteAllChatHistory();
      setChatHistoryItems([]);
      setChatHistoryCount(0);
      setChatHistoryPage(1);
      if (result.deleted >= 0) {
        console.log('Deleted chat history conversations:', result.deleted);
      }
    } catch (error) {
      console.error('deleteAllChatHistory error', error);
      setChatHistoryError(error instanceof Error ? error.message : 'Failed to clear chat history');
    } finally {
      setClearingChatHistory(false);
    }
  };

  const onExportChatConversation = async (conversation: ChatHistorySummary) => {
    try {
      setChatHistoryExportingId(conversation.conversation_id);
      setChatHistoryError('');
      const detail = await getChatHistoryDetail(conversation.conversation_id);
      const payload = {
        conversation_id: detail.conversation_id,
        title: conversation.title,
        summary: conversation.summary,
        summary_source: detail.summary_source || conversation.summary_source || 'none',
        created_at: detail.created_at ?? conversation.created_at,
        updated_at: detail.updated_at ?? conversation.updated_at,
        message_count: conversation.message_count,
        messages: detail.messages,
      };
      const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      const safeTitle = String(conversation.title || conversation.conversation_id || 'conversation')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '')
        .slice(0, 48) || 'conversation';
      link.href = url;
      link.download = `${safeTitle || 'conversation'}-${conversation.conversation_id.slice(0, 8)}.json`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error('getChatHistoryDetail error', error);
      setChatHistoryError(error instanceof Error ? error.message : 'Failed to export conversation');
    } finally {
      setChatHistoryExportingId('');
    }
  };

  const onDeleteChatConversation = async (conversationId: string) => {
    try {
      setChatHistoryDeletingId(conversationId);
      setChatHistoryError('');
      const result = await deleteChatHistoryConversation(conversationId);
      if (!result.deleted) {
        return;
      }
      const nextCount = Math.max(0, chatHistoryCount - 1);
      const maxPage = Math.max(1, Math.ceil(nextCount / CHAT_HISTORY_PAGE_SIZE));
      const nextPage = Math.min(chatHistoryPage, maxPage);
      setChatHistoryCount(nextCount);
      if (nextPage !== chatHistoryPage) {
        setChatHistoryPage(nextPage);
      } else {
        setChatHistoryItems((prev) => prev.filter((item) => item.conversation_id !== conversationId));
        if (chatHistoryItems.length <= 1 && nextPage > 1) {
          setChatHistoryPage(nextPage - 1);
        } else {
          loadChatHistoryPage(nextPage);
        }
      }
    } catch (error) {
      console.error('deleteChatHistoryConversation error', error);
      setChatHistoryError(error instanceof Error ? error.message : 'Failed to delete conversation');
    } finally {
      setChatHistoryDeletingId('');
    }
  };

  const confirmDeleteChatConversation = async () => {
    if (!pendingDeleteConversation) {
      return;
    }
    await onDeleteChatConversation(pendingDeleteConversation.id);
    setPendingDeleteConversation(null);
  };

  const closeNodeEditor = () => {
    setNodeEditorOpen(false);
    setNodeEditorMode('add');
    setNodeEditingLocalId('');
    setNodeDraftProbeLoading(false);
    setNodeDraftProbeResult(null);
    setNodeDraftProbeError('');
    setNodeDraft({
      display_name: '',
      ssh_target: '',
      parallax_path: '',
      hostname_hint: '',
      management_mode: 'ssh_managed',
    });
  };

  const persistNodesInventory = async (
    nextInventory: Array<{
      local_id: string;
      id: string;
      display_name: string;
      ssh_target: string;
      parallax_path: string;
      hostname_hint: string;
      joined: boolean;
      management_mode: 'ssh_managed' | 'manual';
      network_scope: 'local' | 'remote';
      hardware?: {
        gpu_name?: string;
        gpu_num?: number;
        gpu_memory_gb?: number;
        ram_total_gb?: number;
        updated_at?: number;
      };
      linked_clusters: readonly { id: string; name: string }[];
      linked_cluster_ids: readonly string[];
      linked_cluster_names: readonly string[];
      linked_cluster_count: number;
    }>,
    successMessage: string,
  ) => {
    try {
      setNodesInventorySaving(true);
      setNodesInventoryMessage('');
      const result = await updateNodesInventory(
        nextInventory.map((item) => ({
          id: item.id || undefined,
          display_name: item.display_name || undefined,
          ssh_target: item.ssh_target.trim(),
          parallax_path: item.parallax_path.trim(),
          hostname_hint: item.hostname_hint || undefined,
          management_mode: item.management_mode,
          network_scope: item.network_scope,
          hardware: item.hardware,
        })),
      );
      setNodesInventory((result.hosts || []).map(mapInventoryHost));
      setNodesOverviewRefreshToken((prev) => prev + 1);
      await reloadSettings();
      setNodesInventoryMessage('');
    } catch (error) {
      setNodesInventoryMessage(error instanceof Error ? error.message : 'Failed to update node inventory');
    } finally {
      setNodesInventorySaving(false);
    }
  };

  const onAddNodeDraft = async () => {
    const sshTarget = nodeDraft.ssh_target.trim();
    const parallaxPath = nodeDraft.parallax_path.trim();
    if (!sshTarget) {
      return;
    }
    const nextItem: (typeof nodesInventory)[number] = {
      local_id: nextInventoryRowId(),
      id: '',
      display_name: nodeDraft.display_name.trim() || sshTarget,
      ssh_target: sshTarget,
      parallax_path: parallaxPath,
      hostname_hint: normalizeHostname(sshTarget),
      joined: false,
      management_mode: 'ssh_managed',
      network_scope: 'remote',
      hardware: {
        gpu_name: nodeDraftProbeResult?.gpu_name || '',
        gpu_num: Number(nodeDraftProbeResult?.gpu_num || 0),
        gpu_memory_gb: Number(nodeDraftProbeResult?.gpu_memory_gb || 0),
        ram_total_gb: Number(nodeDraftProbeResult?.ram_total_gb || 0),
        updated_at: Date.now() / 1000,
      },
      linked_clusters: [],
      linked_cluster_ids: [],
      linked_cluster_names: [],
      linked_cluster_count: 0,
    };
    await persistNodesInventory([...nodesInventory, nextItem], 'Node added to configured inventory');
    closeNodeEditor();
  };

  const onSaveNodeDraft = async () => {
    if (!nodeEditingLocalId) {
      return;
    }
    const nextInventory = nodesInventory.map((item) => {
      if (item.local_id !== nodeEditingLocalId) {
        return item;
      }
      if (nodeDraft.management_mode === 'manual') {
        return {
          ...item,
          display_name: nodeDraft.display_name.trim() || nodeDraft.hostname_hint.trim() || item.display_name,
          ssh_target: '',
          parallax_path: '',
          hostname_hint: normalizeHostname(nodeDraft.hostname_hint),
          management_mode: 'manual' as const,
        };
      }
      return {
        ...item,
        display_name: nodeDraft.display_name.trim() || nodeDraft.ssh_target.trim() || item.display_name,
        ssh_target: nodeDraft.ssh_target.trim(),
        parallax_path: nodeDraft.parallax_path.trim(),
        hostname_hint: normalizeHostname(nodeDraft.ssh_target),
        management_mode: 'ssh_managed' as const,
        hardware: {
          ...(item.hardware || {}),
          gpu_name: nodeDraftProbeResult?.gpu_name || item.hardware?.gpu_name || '',
          gpu_num: Number(nodeDraftProbeResult?.gpu_num || item.hardware?.gpu_num || 0),
          gpu_memory_gb: Number(nodeDraftProbeResult?.gpu_memory_gb || item.hardware?.gpu_memory_gb || 0),
          ram_total_gb: Number(nodeDraftProbeResult?.ram_total_gb || item.hardware?.ram_total_gb || 0),
          updated_at: nodeDraftProbeResult ? Date.now() / 1000 : item.hardware?.updated_at,
        },
      };
    });
    await persistNodesInventory(nextInventory, 'Configured node updated');
    closeNodeEditor();
  };

  const onAddDiscoveredNode = async (node: (typeof nodeInfoList)[number]) => {
    const hostnameHint = normalizeHostname(node.hostname);
    if (!hostnameHint) {
      return;
    }
    await persistNodesInventory(
      [
        ...nodesInventory,
        {
          local_id: nextInventoryRowId(),
          id: '',
          display_name: node.hostname || node.id,
          ssh_target: '',
          parallax_path: '',
          hostname_hint: hostnameHint,
          joined: node.status === 'available',
          management_mode: 'manual',
          network_scope: 'remote',
          hardware: {
            gpu_name: node.gpuName || '',
            gpu_num: Number(node.gpuNumber || 0),
            gpu_memory_gb: Number(node.gpuMemory || 0),
            ram_total_gb: Number(node.ramTotalGb || 0),
            updated_at: Date.now() / 1000,
          },
          linked_clusters: [],
          linked_cluster_ids: [],
          linked_cluster_names: [],
          linked_cluster_count: 0,
        },
      ],
      'Discovered node added to configured inventory',
    );
    closeNodeEditor();
  };

  const onRemoveInventoryNode = async (localId: string) => {
    await persistNodesInventory(
      nodesInventory.filter((item) => item.local_id !== localId),
      'Node removed from configured inventory',
    );
  };

  const onRemoveConfiguredOverviewHost = async (host: { id: string; ssh_target: string; hostname_hint: string }) => {
    const targetLocalId = nodesInventory.find((item) => item.id && item.id === host.id)?.local_id
      || nodesInventory.find((item) => item.ssh_target && item.ssh_target === host.ssh_target)?.local_id
      || nodesInventory.find((item) => item.hostname_hint && item.hostname_hint === normalizeHostname(host.hostname_hint))?.local_id
      || '';
    if (!targetLocalId) {
      setNodesInventoryMessage('Configured node could not be matched in inventory');
      return;
    }
    await onRemoveInventoryNode(targetLocalId);
  };

  const openConfiguredNodeEditor = (host: {
    id: string;
    display_name: string;
    ssh_target: string;
    hostname_hint: string;
    management_mode?: 'ssh_managed' | 'manual';
    parallax_path?: string;
  }) => {
    const target = nodesInventory.find((item) => item.id === host.id)
      || nodesInventory.find((item) => item.ssh_target === host.ssh_target)
      || nodesInventory.find((item) => item.hostname_hint === normalizeHostname(host.hostname_hint));
    if (!target) {
      setNodesInventoryMessage('Configured node could not be matched in inventory');
      return;
    }
    setNodeEditorMode('edit');
    setNodeEditingLocalId(target.local_id);
    setNodeEditorTab('manual');
    setNodeDraft({
      display_name: target.display_name || '',
      ssh_target: target.ssh_target || '',
      parallax_path: target.parallax_path || '',
      hostname_hint: target.hostname_hint || '',
      management_mode: target.management_mode || 'ssh_managed',
    });
    setNodeDraftProbeResult(null);
    setNodeDraftProbeError('');
    setNodeEditorOpen(true);
  };

  const confirmDeleteNode = async () => {
    if (!pendingDeleteNode) {
      return;
    }
    const target = nodesInventory.find((item) => item.id === pendingDeleteNode.id)
      || nodesInventory.find((item) => item.local_id === pendingDeleteNode.id);
    if (target) {
      await onRemoveInventoryNode(target.local_id);
    }
    setPendingDeleteNode(null);
  };

  const onExportSettings = async () => {
    try {
      setExportingSettings(true);
      setTransferError('');
      setTransferMessage('');
      const bundle = await exportSettingsBundle();
      const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = 'parallax-settings.json';
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      URL.revokeObjectURL(url);
      setTransferMessage('Exported current settings to parallax-settings.json');
    } catch (error) {
      setTransferError(error instanceof Error ? error.message : 'Failed to export settings');
    } finally {
      setExportingSettings(false);
    }
  };

  const onImportSettingsFile = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file) {
      return;
    }
    try {
      setImportingSettings(true);
      setTransferError('');
      setTransferMessage('');
      const raw = await file.text();
      const bundle = JSON.parse(raw) as SettingsExportBundle;
      await importSettingsBundle(bundle);
      await Promise.all([
        reloadSettings(),
        refreshModelList(),
        loadCustomModels(),
        loadInventory(),
      ]);
      setTransferMessage('Imported settings JSON and replaced the previous saved configuration.');
    } catch (error) {
      setTransferError(error instanceof Error ? error.message : 'Failed to import settings');
    } finally {
      setImportingSettings(false);
    }
  };

  const onToggleAssignedNode = async (hostId: string) => {
    if (!selectedCluster || !hostId) {
      return;
    }
    const nextAssignedIds = new Set(selectedCluster.assigned_node_ids || []);
    if (nextAssignedIds.has(hostId)) {
      nextAssignedIds.delete(hostId);
    } else {
      nextAssignedIds.add(hostId);
    }
    await applyClusterProfilesState(
      clusterProfiles.map((cluster) => (
        cluster.id === selectedCluster.id
          ? {
            ...cluster,
            assigned_node_ids: Array.from(nextAssignedIds),
            init_nodes_num: Math.max(1, nextAssignedIds.size),
          }
          : cluster
      )),
      selectedCluster.id,
    );
  };

  const renderSectionContent = () => {
    if (activeSection === 'general') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Typography variant='h2'>Overview</Typography>
          <Typography variant='body2' color='text.secondary'>
            Settings now separates planning from day-to-day operations. Define the available machine pool in <strong>Nodes</strong>, manage the shared model library in <strong>Custom Models</strong>, then configure each saved <strong>Cluster</strong> with its own model and startup capacity.
          </Typography>
          <Alert severity='info'>
            Each saved cluster profile represents one scheduler configuration. The runtime still serves one active cluster at a time, but you can keep multiple cluster definitions and switch between them.
          </Alert>
          <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1, flexWrap: 'wrap' }}>
            <Button variant='outlined' onClick={() => openSection('nodes')}>Define available nodes</Button>
            <Button variant='contained' onClick={() => openSection('cluster')}>Configure cluster</Button>
          </Stack>
        </Stack>
      );
    }

    if (activeSection === 'cluster') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
            <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
              <Typography variant='h2'>Cluster</Typography>
              <Tooltip
                title='Saved clusters are reusable scheduler configurations. Each one keeps its own model choice, assigned nodes, and startup planning.'
                placement='right'
                slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
              >
                <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                  <IconInfoCircle size={16} />
                </IconButton>
              </Tooltip>
            </Stack>
            <Stack direction='row' sx={{ gap: 1, alignItems: 'center' }}>
              {topologyChangeAdvisory.show && topologyChangeAdvisory.canRebalance && (
                <Button size='small' variant='outlined' color='warning' disabled={rebalancingTopology} onClick={onClickRebalanceTopology}>
                  {rebalancingTopology ? 'Rebalancing...' : 'Rebalance now'}
                </Button>
              )}
              <Button
                variant='outlined'
                onClick={openClusterEditor}
                startIcon={<IconPlus size={16} />}
              >
                Add cluster
              </Button>
            </Stack>
          </Stack>
          <Typography variant='body2' color='text.secondary'>
            Saved clusters are reusable scheduler configurations. Open one when you want to change its model, assigned nodes, or startup planning.
          </Typography>
          <Stack sx={{ gap: 0.75 }}>
            {clusterProfiles.map((cluster) => (
              <Stack
                key={cluster.id}
                direction='row'
                sx={{
                  alignItems: 'flex-start',
                  justifyContent: 'space-between',
                  gap: 1.5,
                  px: 1.25,
                  py: 1,
                  borderRadius: 2,
                  border: '1px solid',
                  borderColor: 'divider',
                  bgcolor: 'background.paper',
                }}
              >
                <Stack sx={{ minWidth: 0, gap: 0.25, flex: 1 }}>
                  <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75, flexWrap: 'wrap' }}>
                    <Typography variant='body2' sx={{ fontWeight: 600 }}>
                      {cluster.name}
                    </Typography>
                    {cluster.model_name && (
                      <Chip size='small' variant='outlined' label={cluster.model_name} />
                    )}
                  </Stack>
                  <Stack direction='row' sx={{ gap: 0.5, flexWrap: 'wrap' }}>
                    <Typography variant='caption' color='text.secondary'>
                      {(cluster.assigned_node_ids || []).length} assigned node{(cluster.assigned_node_ids || []).length === 1 ? '' : 's'}
                    </Typography>
                    <Typography variant='caption' color='text.secondary'>
                      Startup target {Math.max(1, Number(cluster.init_nodes_num || (cluster.assigned_node_ids || []).length || 1))}
                    </Typography>
                    {!cluster.model_name && (
                      <Typography variant='caption' color='text.secondary'>
                        No model selected
                      </Typography>
                    )}
                  </Stack>
                </Stack>
                <Stack direction='row' sx={{ gap: 0.25, alignItems: 'center', flex: 'none' }}>
                  <Tooltip title='Configure cluster'>
                    <IconButton size='small' onClick={() => void openClusterConfig(cluster.id)}>
                      <IconAdjustments size={16} />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title={clusterProfiles.length <= 1 ? 'At least one cluster is required' : 'Delete cluster'}>
                    <span>
                      <IconButton
                        size='small'
                        color='error'
                        disabled={clusterProfiles.length <= 1}
                        onClick={() => setPendingDeleteCluster({ id: cluster.id, label: cluster.name })}
                      >
                        <IconTrash size={16} />
                      </IconButton>
                    </span>
                  </Tooltip>
                </Stack>
              </Stack>
            ))}
          </Stack>
          <Dialog open={clusterModalOpen} onClose={closeClusterModal} fullWidth maxWidth='md'>
            <DialogTitle sx={{ pr: 6 }}>
              {clusterModalMode === 'add' ? 'Add Cluster' : 'Configure Cluster'}
              <IconButton
                onClick={closeClusterModal}
                aria-label='Close cluster dialog'
                sx={{ position: 'absolute', right: 16, top: 16 }}
              >
                <IconX size={18} />
              </IconButton>
            </DialogTitle>
            <DialogContent dividers>
              <Stack sx={{ gap: 1.25, pt: 0.5 }}>
                {clusterModalMode === 'add' ? (
                  <Typography variant='body2' color='text.secondary'>
                    New clusters start from the current cluster’s model and assigned nodes, but you can adjust them before creating the cluster.
                  </Typography>
                ) : null}
                <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
                  <TextField
                    label='Cluster name'
                    size='small'
                    fullWidth
                    value={clusterModalMode === 'add' ? clusterDraftName : clusterNameDraft}
                    onChange={(event) => {
                      if (clusterModalMode === 'add') {
                        setClusterDraftName(event.target.value);
                      } else {
                        setClusterNameDraft(event.target.value);
                      }
                    }}
                    placeholder={clusterModalMode === 'add' ? `Cluster ${clusterProfiles.length + 1}` : undefined}
                  />
                  {clusterModalMode === 'configure' && (
                    <>
                      <Button variant='outlined' onClick={() => void onSaveClusterName()} disabled={!selectedCluster || !clusterNameDraft.trim() || clusterNameDraft.trim() === selectedCluster.name}>
                        Save name
                      </Button>
                      <Button variant='text' color='error' onClick={() => setPendingDeleteCluster({ id: selectedCluster?.id || '', label: selectedCluster?.name || 'this cluster' })} disabled={clusterProfiles.length <= 1 || !selectedCluster?.id}>
                        Delete cluster
                      </Button>
                    </>
                  )}
                </Stack>
                <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
                  <Typography variant='body1'>Model</Typography>
                  <Tooltip
                    title={clusterModalMode === 'add' ? 'Choose the model this new cluster should host.' : 'Choose the model this saved cluster should host, then size startup capacity around it.'}
                    placement='right'
                    slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
                  >
                    <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                      <IconInfoCircle size={16} />
                    </IconButton>
                  </Tooltip>
                </Stack>
                {clusterModalMode === 'add' ? (
                  <TextField
                    select
                    label='Model'
                    size='small'
                    fullWidth
                    value={clusterDraftModelName}
                    onChange={(event) => setClusterDraftModelName(String(event.target.value))}
                    sx={{
                      '& .MuiOutlinedInput-root': {
                        minHeight: '4rem',
                        borderRadius: 3,
                        px: 0.5,
                      },
                      '& .MuiSelect-select': {
                        display: 'flex',
                        alignItems: 'center',
                        minHeight: '4rem !important',
                        py: '0.375rem !important',
                        pl: '0.25rem !important',
                      },
                    }}
                    SelectProps={{
                      renderValue: (value) => {
                        const selected = modelInfoList.find((item) => item.name === value);
                        if (!selected) {
                          return value as string;
                        }
                        return (
                          <Stack direction='row' sx={{ alignItems: 'center', gap: 1, minWidth: 0 }}>
                            <Box
                              component='img'
                              src={selected.logoUrl}
                              alt=''
                              sx={{
                                width: '2.25rem',
                                height: '2.25rem',
                                borderRadius: '0.5rem',
                                border: '1px solid',
                                borderColor: 'divider',
                                objectFit: 'cover',
                                flex: 'none',
                              }}
                            />
                            <Stack sx={{ minWidth: 0, flex: 1, gap: 0.125 }}>
                              <Typography variant='subtitle2' sx={{ fontSize: '0.875rem', lineHeight: '1.125rem', fontWeight: 300 }}>
                                {selected.displayName}
                              </Typography>
                              <Typography variant='body2' color='text.secondary' sx={{ fontSize: '0.75rem', lineHeight: '1rem', fontWeight: 300 }}>
                                {selected.name}
                              </Typography>
                            </Stack>
                            {selected.vram > 0 && (
                              <Chip size='small' variant='outlined' label={`${selected.vram} GB`} />
                            )}
                          </Stack>
                        );
                      },
                    }}
                  >
                    {modelInfoList.map((item) => (
                      <MenuItem key={item.name} value={item.name}>
                        <Stack direction='row' sx={{ alignItems: 'center', gap: 1, minWidth: 0, width: '100%' }}>
                          <Box
                            component='img'
                            src={item.logoUrl}
                            alt=''
                            sx={{
                              width: '2.25rem',
                              height: '2.25rem',
                              borderRadius: '0.5rem',
                              border: '1px solid',
                              borderColor: 'divider',
                              objectFit: 'cover',
                              flex: 'none',
                            }}
                          />
                          <Stack sx={{ minWidth: 0, flex: 1, gap: 0.125 }}>
                            <Typography variant='subtitle2' sx={{ fontSize: '0.875rem', lineHeight: '1.125rem', fontWeight: 300 }}>
                              {item.displayName}
                            </Typography>
                            <Typography variant='body2' color='text.secondary' sx={{ fontSize: '0.75rem', lineHeight: '1rem', fontWeight: 300 }}>
                              {item.name}
                            </Typography>
                          </Stack>
                          {item.vram > 0 && (
                            <Chip size='small' variant='outlined' label={`${item.vram} GB`} />
                          )}
                        </Stack>
                      </MenuItem>
                    ))}
                  </TextField>
                ) : (
                  <ModelSelect autoCommit />
                )}
                {renderModelVramRequirement(clusterModalMode === 'add' ? draftModelInfo?.vram : modelInfo?.vram)}
                <Typography variant='body2' color='text.secondary'>
                  Define the model for this saved cluster, assign which nodes it may use, and then set the initial number of joined nodes the scheduler should plan around.
                </Typography>
                <Stack sx={{ gap: 1 }}>
                  <Typography variant='body1'>Assigned nodes</Typography>
                  <Typography variant='body2' color='text.secondary'>
                    Only assigned nodes are intended to serve this cluster. Unassigned nodes stay out of the shard pool for this saved cluster.
                  </Typography>
                  {nodesInventory.length === 0 && (
                    <Alert severity='info'>
                      No managed hosts are defined yet. Add nodes in <strong>Settings &gt; Nodes</strong> before assigning them to this cluster.
                    </Alert>
                  )}
                  {nodesInventory.length > 0 && (
                    <Stack sx={{ gap: 1, maxHeight: '18rem', overflowY: 'auto', pr: 0.5 }}>
                      {nodesInventory.map((host) => {
                        const linkedOtherClusters = host.linked_clusters.filter((cluster) => cluster.id !== selectedCluster?.id);
                        const assignedHere = clusterModalMode === 'add'
                          ? clusterDraftAssignedNodeIds.includes(host.id)
                          : selectedClusterAssignedNodeIds.has(host.id);
                        return (
                          <Stack
                            key={host.id || host.local_id}
                            direction='row'
                            sx={{
                              alignItems: 'flex-start',
                              justifyContent: 'space-between',
                              gap: 1,
                              px: 1.25,
                              py: 1,
                              borderRadius: 2,
                              border: '1px solid',
                              borderColor: assignedHere ? 'primary.main' : 'divider',
                              bgcolor: assignedHere ? 'action.selected' : 'background.paper',
                            }}
                          >
                            <Stack sx={{ minWidth: 0, flex: 1, gap: 0.5 }}>
                              <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75, flexWrap: 'wrap' }}>
                                <Checkbox
                                  size='small'
                                  checked={assignedHere}
                                  onChange={() => {
                                    if (clusterModalMode === 'add') {
                                      onToggleDraftAssignedNode(host.id);
                                    } else {
                                      void onToggleAssignedNode(host.id);
                                    }
                                  }}
                                  sx={{ p: 0, mr: 0.5 }}
                                />
                                <Typography variant='body2' sx={{ fontWeight: 600 }}>
                                  {host.display_name || host.ssh_target || host.hostname_hint || 'Unnamed host'}
                                </Typography>
                                <Chip size='small' variant='outlined' label={host.management_mode === 'manual' ? 'Self-joining' : 'SSH managed'} />
                                {getHostGpuMemoryGb(host, availableNodeByHostname.get(normalizeHostname(host.hostname_hint || host.ssh_target))) > 0 && (
                                  <Chip
                                    size='small'
                                    variant='outlined'
                                    label={`${getHostGpuMemoryGb(host, availableNodeByHostname.get(normalizeHostname(host.hostname_hint || host.ssh_target)))} GB VRAM`}
                                  />
                                )}
                                {Number(host.hardware?.ram_total_gb || 0) > 0 && (
                                  <Chip size='small' variant='outlined' label={`${Number(host.hardware?.ram_total_gb || 0)} GB RAM`} />
                                )}
                                {host.joined
                                  ? <Chip size='small' color='success' label='Online' />
                                  : <Chip size='small' variant='outlined' label='Offline' />}
                                {linkedOtherClusters.length > 0 && (
                                  <Chip
                                    size='small'
                                    color='warning'
                                    variant='outlined'
                                    label={linkedOtherClusters.length === 1 ? 'Used elsewhere' : `Used by ${linkedOtherClusters.length} clusters`}
                                  />
                                )}
                              </Stack>
                              {host.parallax_path && (
                                <Typography variant='caption' color='text.secondary'>
                                  PARALLAX_PATH: {host.parallax_path}
                                </Typography>
                              )}
                              {host.management_mode === 'manual' && (
                                <Typography variant='caption' color='text.secondary'>
                                  Self-joining node: Parallax does not SSH into or restart this node.
                                </Typography>
                              )}
                              <Stack direction='row' sx={{ gap: 0.5, flexWrap: 'wrap' }}>
                                {host.linked_cluster_count === 0 && <Chip size='small' variant='outlined' label='Unassigned' />}
                                {host.linked_clusters.map((cluster) => (
                                  <Chip
                                    key={`${host.id}-${cluster.id}`}
                                    size='small'
                                    color={cluster.id === selectedCluster?.id ? 'primary' : 'default'}
                                    variant={cluster.id === selectedCluster?.id ? 'filled' : 'outlined'}
                                    label={cluster.name}
                                  />
                                ))}
                              </Stack>
                            </Stack>
                          </Stack>
                        );
                      })}
                    </Stack>
                  )}
                </Stack>
                <Stack sx={{ gap: 0.75 }}>
                  <Typography variant='body1'>Capacity summary</Typography>
                  <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1, flexWrap: 'wrap' }}>
                    <Chip color='default' variant='outlined' label={`${clusterModalMode === 'add' ? draftAssignedHosts.length : assignedHosts.length} assigned`} />
                    <Chip color={(clusterModalMode === 'add' ? draftAvailableAssignedHosts.length : availableAssignedHosts.length) > 0 ? 'success' : 'default'} variant='outlined' label={`${clusterModalMode === 'add' ? draftAvailableAssignedHosts.length : availableAssignedHosts.length} online now`} />
                    <Chip color='default' variant='outlined' label={`startup target ${Math.max(1, clusterModalMode === 'add' ? draftAssignedHosts.length : assignedHosts.length)}`} />
                    <Chip color='info' variant='outlined' label={`${clusterModalMode === 'add' ? draftAvailableAssignedMemory : assignedTotalMemory} GB total memory`} />
                    {clusterModalMode === 'add'
                      ? draftModelInfo && draftModelInfo.vram > 0 && <Chip color='warning' variant='outlined' label={`${draftModelInfo.vram} GB required`} />
                      : modelInfo && modelInfo.vram > 0 && <Chip color='warning' variant='outlined' label={`${modelInfo.vram} GB required`} />}
                  </Stack>
                  {(clusterModalMode === 'add' ? clusterDraftAssignedNodeIds.length : selectedClusterAssignedNodeIds.size) > 0 && (
                    <Typography variant='caption' color='text.secondary'>
                      Using live telemetry when available and stored node hardware otherwise. Hardware is known for {clusterModalMode === 'add' ? draftKnownAssignedHardwareCount : knownAssignedHardwareCount}/{clusterModalMode === 'add' ? draftAssignedHosts.length : assignedHosts.length} assigned node{(clusterModalMode === 'add' ? draftAssignedHosts.length : assignedHosts.length) === 1 ? '' : 's'}.
                    </Typography>
                  )}
                  {clusterModalCapacityAlert}
                </Stack>
                <Alert severity='info'>
                  Startup planning now follows the assigned-node list automatically. If more assigned nodes arrive later, Parallax can extend capacity, but adding nodes may trigger layer movement and temporary performance churn while the scheduler rebalances.
                </Alert>
                {topologyChangeAdvisory.show && <Alert severity='warning'>{topologyChangeAdvisory.message}</Alert>}
                <Stack direction='row' sx={{ justifyContent: 'flex-end', gap: 1 }}>
                  <Button variant='text' onClick={closeClusterModal}>
                    {clusterModalMode === 'add' ? 'Cancel' : 'Close'}
                  </Button>
                  {clusterModalMode === 'add' ? (
                    <Button
                      variant='contained'
                      onClick={() => void onSubmitCreateClusterProfile()}
                      disabled={!clusterDraftModelName.trim() || !canProceedWithAddCluster}
                    >
                      Add cluster
                    </Button>
                  ) : (
                    <Button variant='contained' onClick={onSaveClusterSettings} disabled={initializingCluster || !canProceedWithConfiguredCluster}>
                      {initializingCluster ? 'Saving...' : 'Save'}
                    </Button>
                  )}
                </Stack>
              </Stack>
            </DialogContent>
          </Dialog>
          <AlertDialog
            open={!!pendingDeleteCluster}
            onClose={() => setPendingDeleteCluster(null)}
            color='warning'
            title='Delete cluster'
            content={
              <Typography variant='body2'>
                Delete {pendingDeleteCluster ? `"${truncateChatHistoryLabel(pendingDeleteCluster.label)}"` : 'this cluster'}? This cannot be undone.
              </Typography>
            }
            cancelLabel='Cancel'
            confirmLabel='Delete'
            autoFocusAction='cancel'
            onConfirm={confirmDeleteClusterProfile}
          />
        </Stack>
      );
    }

    if (activeSection === 'custom-models') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
            <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
              <Typography variant='h2'>Custom Models</Typography>
              <Tooltip
                title='Custom models are a shared library for all clusters. Supported in this version: Hugging Face repo ids, approved scheduler-local model roots, and ad hoc archive URLs imported onto the scheduler.'
                placement='right'
                slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
              >
                <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                  <IconInfoCircle size={16} />
                </IconButton>
              </Tooltip>
            </Stack>
            {!customModelEditorOpen && (
              <Button
                variant='outlined'
                onClick={() => setCustomModelEditorOpen(true)}
                startIcon={<IconPlus size={16} />}
                sx={{ whiteSpace: 'nowrap' }}
              >
                Add model
              </Button>
            )}
          </Stack>
          {customModelError && <Alert severity='warning'>{customModelError}</Alert>}
          <Stack sx={{ gap: 1, maxHeight: '24rem', overflow: 'auto' }}>
            {customModelLoading && <Typography variant='body2' color='text.secondary'>Loading custom models…</Typography>}
            {!customModelLoading && customModels.length === 0 && <Typography variant='body2' color='text.secondary'>No custom models added yet.</Typography>}
            {customModels.map((model) => (
              <Stack key={model.id} direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1.5, px: 1.25, py: 1, borderRadius: 2, border: '1px solid', borderColor: 'divider', bgcolor: 'background.paper' }}>
                <Stack sx={{ minWidth: 0, gap: 0.25 }}>
                  <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75, flexWrap: 'wrap' }}>
                    <Typography variant='body2' sx={{ fontWeight: 600 }}>{model.display_name || model.source_value}</Typography>
                    {renderValidationChip(model.validation_status)}
                    <Chip
                      size='small'
                      variant='outlined'
                      label={
                        model.source_type === 'huggingface'
                          ? 'HF'
                          : model.source_type === 'scheduler_root'
                            ? 'Local root'
                            : 'URL'
                      }
                    />
                  </Stack>
                  <Typography variant='caption' color='text.secondary' sx={{ wordBreak: 'break-all' }}>{model.source_value}</Typography>
                  {model.validation_message && <Typography variant='caption' color='text.secondary'>{model.validation_message}</Typography>}
                </Stack>
                <IconButton
                  size='small'
                  color='error'
                  disabled={customModelDeletingId === model.id}
                  onClick={() => setPendingDeleteCustomModel({ id: model.id, label: model.display_name || model.source_value })}
                >
                  <IconTrash size={16} />
                </IconButton>
              </Stack>
            ))}
          </Stack>
          <Dialog open={customModelEditorOpen} onClose={closeCustomModelEditor} fullWidth maxWidth='md'>
            <DialogTitle sx={{ pr: 6 }}>
              Add Custom Model
              <IconButton
                onClick={closeCustomModelEditor}
                aria-label='Close add custom model dialog'
                sx={{ position: 'absolute', right: 16, top: 16 }}
              >
                <IconX size={18} />
              </IconButton>
            </DialogTitle>
            <DialogContent dividers>
              <Stack sx={{ gap: 1.25, pt: 0.5 }}>
              <TextField
                select
                label='Source'
                size='small'
                value={customModelSourceType}
                onChange={(event) => {
                  const nextType = event.target.value as 'huggingface' | 'scheduler_root' | 'url';
                  setCustomModelSourceType(nextType);
                  setCustomModelError('');
                  setCustomModelSearchOpen(false);
                  setCustomModelSearchResults([]);
                  setCustomModelSearchHasMore(false);
                  setCustomModelSearchNextOffset(0);
                  if (nextType === 'scheduler_root') {
                    setCustomModelSourceValue(String(customModelSourceOptions[0]?.source_value || ''));
                  } else {
                    setCustomModelSourceValue('');
                  }
                }}
                sx={{ minWidth: { sm: '10rem' } }}
              >
                <MenuItem value='huggingface'>Hugging Face</MenuItem>
                <MenuItem value='scheduler_root' disabled={customModelSourceRoots.length === 0}>Approved local root</MenuItem>
                <MenuItem value='url'>URL</MenuItem>
              </TextField>
                {customModelSourceType === 'huggingface' ? (
                  <Stack sx={{ gap: 1 }}>
                    <Autocomplete
                      freeSolo
                      fullWidth
                      openOnFocus
                      autoHighlight
                      disablePortal
                      clearOnBlur={false}
                      options={customModelSearchResults}
                      loading={customModelSearchLoading}
                      open={!!customModelSourceValue.trim() && (customModelSearchOpen || customModelSearchLoading || customModelSearchResults.length > 0)}
                      loadingText='Searching Hugging Face…'
                      noOptionsText={customModelSourceValue.trim() ? 'No matching models found' : 'Start typing a repo id'}
                      onOpen={() => {
                        if (customModelSourceValue.trim()) setCustomModelSearchOpen(true);
                      }}
                      onClose={() => setCustomModelSearchOpen(false)}
                      filterOptions={(options) => options}
                      getOptionLabel={(option) => typeof option === 'string' ? option : option.source_value}
                      inputValue={customModelSourceValue}
                      onInputChange={(_, value, reason) => {
                        if (reason !== 'reset' && !customModelSearchLoading) {
                          setCustomModelSourceValue(value);
                          setCustomModelSearchOpen(!!value.trim());
                        }
                      }}
                      onChange={(_, value) => {
                        if (typeof value === 'string') {
                          setCustomModelSourceValue(value);
                          setCustomModelSearchOpen(false);
                          return;
                        }
                        if (value) {
                          setCustomModelSourceValue(value.source_value);
                          setCustomModelDisplayName(value.display_name);
                          setCustomModelSearchOpen(false);
                        }
                      }}
                      renderInput={(params) => (
                        <TextField
                          {...params}
                          label='Repo id'
                          size='small'
                          placeholder='org/model-name'
                          helperText='Searches Hugging Face shortly after you stop typing.'
                          InputProps={{
                            ...params.InputProps,
                            endAdornment: (
                              <>
                                {customModelSearchLoading ? <CircularProgress color='inherit' size={16} /> : null}
                                {params.InputProps.endAdornment}
                              </>
                            ),
                          }}
                        />
                      )}
                      renderOption={(props, option) => (
                        <Box component='li' {...props} sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
                          <Stack sx={{ minWidth: 0, gap: 0.25 }}>
                            <Typography variant='body2' sx={{ fontWeight: 600 }}>{option.display_name}</Typography>
                            <Typography variant='caption' color='text.secondary'>{option.validation_message}</Typography>
                          </Stack>
                          <Stack direction='row' sx={{ gap: 0.75, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                            {renderValidationChip(option.validation_status)}
                            {typeof option.vram_gb === 'number' && option.vram_gb > 0 && <Chip size='small' variant='outlined' label={`${option.vram_gb} GB`} />}
                          </Stack>
                        </Box>
                      )}
                    />
                    {customModelSearchStatus && (
                      <Alert severity={customModelSearchStatus.severity}>
                        {customModelSearchStatus.message}
                      </Alert>
                    )}
                    {customModelSearchLoading && (
                      <Stack direction='row' sx={{ alignItems: 'center', gap: 1, px: 0.5, py: 0.25 }}>
                        <CircularProgress size={18} />
                        <Typography variant='body2' color='text.secondary'>
                          Waiting for Hugging Face matches…
                        </Typography>
                      </Stack>
                    )}
                    {customModelSearchResults.length > 0 && (
                      <Stack sx={{ gap: 0.75 }}>
                        {customModelSearchResults.map((option) => (
                          <Stack
                            key={option.source_value}
                            direction='row'
                            onClick={() => {
                              setCustomModelSourceValue(option.source_value);
                              setCustomModelDisplayName(option.display_name);
                              setCustomModelSearchOpen(false);
                            }}
                            sx={{
                              alignItems: 'center',
                              justifyContent: 'space-between',
                              gap: 1,
                              px: 1.25,
                              py: 1,
                              borderRadius: 2,
                              border: '1px solid',
                              borderColor: option.source_value === customModelSourceValue ? 'primary.main' : 'divider',
                              bgcolor: option.source_value === customModelSourceValue ? 'action.selected' : 'background.paper',
                              cursor: 'pointer',
                              '&:hover': {
                                bgcolor: 'action.hover',
                              },
                            }}
                          >
                            <Stack sx={{ minWidth: 0, gap: 0.25, flex: 1 }}>
                              <Typography variant='body2' sx={{ fontWeight: 600 }}>
                                {option.display_name}
                              </Typography>
                              <Typography variant='caption' color='text.secondary'>
                                {option.source_value}
                              </Typography>
                              <Typography variant='caption' color='text.secondary'>
                                {option.validation_message}
                              </Typography>
                            </Stack>
                            <Stack direction='row' sx={{ gap: 0.75, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                              {renderValidationChip(option.validation_status)}
                              {typeof option.vram_gb === 'number' && option.vram_gb > 0 && (
                                <Chip size='small' variant='outlined' label={`${option.vram_gb} GB`} />
                              )}
                            </Stack>
                          </Stack>
                        ))}
                        {customModelSearchHasMore && (
                          <Stack direction='row' sx={{ justifyContent: 'flex-end' }}>
                            <Button
                              variant='outlined'
                              onClick={() => void onLoadMoreCustomModelSearchResults()}
                              disabled={customModelSearchLoadingMore}
                              startIcon={customModelSearchLoadingMore ? <IconLoader size={16} /> : undefined}
                            >
                              {customModelSearchLoadingMore ? 'Loading more…' : 'Load more matches'}
                            </Button>
                          </Stack>
                        )}
                      </Stack>
                    )}
                  </Stack>
                ) : customModelSourceType === 'scheduler_root' ? (
                <TextField
                  select
                  label='Model directory'
                  size='small'
                  fullWidth
                  value={customModelSourceValue}
                  onChange={(event) => setCustomModelSourceValue(String(event.target.value))}
                  helperText={
                    customModelSourceOptions.length === 0
                      ? 'No approved model directories with config.json were found under the scheduler roots.'
                      : (customModelSourceOptions.find((option) => option.source_value === customModelSourceValue)?.path || 'Select an approved model directory')
                  }
                  disabled={customModelSourceOptions.length === 0}
                >
                  {customModelSourceOptions.map((option) => (
                    <MenuItem key={option.source_value} value={option.source_value}>
                      {option.label}
                    </MenuItem>
                  ))}
                </TextField>
              ) : (
                <TextField
                  label='Archive URL'
                  size='small'
                  fullWidth
                  value={customModelSourceValue}
                  onChange={(event) => setCustomModelSourceValue(event.target.value)}
                  placeholder='https://example.com/model.tar.gz'
                  error={!!customModelUrlValidationError}
                  helperText={customModelUrlValidationError || 'The scheduler downloads and imports the archive into the approved local model root.'}
                />
              )}
                <TextField
                  label='Display name'
                  size='small'
                  value={customModelDisplayName}
                  onChange={(event) => setCustomModelDisplayName(event.target.value)}
                  placeholder='Optional'
                  sx={{ maxWidth: { sm: '20rem' } }}
                />
                <Stack direction='row' sx={{ justifyContent: 'flex-end', gap: 1 }}>
                  <Button variant='text' onClick={closeCustomModelEditor}>
                    Cancel
                  </Button>
                  <Button
                    variant='contained'
                    onClick={onAddCustomModel}
                    disabled={
                      customModelSubmitting
                      || (
                        customModelSourceType === 'huggingface'
                          ? !customModelSourceValue.trim()
                          : customModelSourceType === 'url'
                            ? !!customModelUrlValidationError
                            : !customModelSourceValue.trim()
                      )
                    }
                    startIcon={customModelSubmitting ? <IconLoader size={16} /> : <IconCheck size={16} />}
                  >
                    Add model
                  </Button>
                </Stack>
              </Stack>
            </DialogContent>
          </Dialog>
          <AlertDialog
            open={!!pendingDeleteCustomModel}
            onClose={() => setPendingDeleteCustomModel(null)}
            color='warning'
            title='Delete custom model'
            content={
              <Typography variant='body2'>
                Delete {pendingDeleteCustomModel ? `"${truncateChatHistoryLabel(pendingDeleteCustomModel.label)}"` : 'this custom model'}? This cannot be undone.
              </Typography>
            }
            cancelLabel='Cancel'
            confirmLabel='Delete'
            autoFocusAction='cancel'
            onConfirm={confirmDeleteCustomModel}
          />
        </Stack>
      );
    }

    if (activeSection === 'nodes') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
            <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
              <Typography variant='h2'>Nodes</Typography>
              <Tooltip
                title='Manage the available machine pool here. Add SSH-managed hosts when Parallax can control them directly, or add manual remote nodes when they will join on their own and this cluster cannot SSH into them.'
                placement='right'
                slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
              >
                <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                  <IconInfoCircle size={16} />
                </IconButton>
              </Tooltip>
            </Stack>
              <Button
                variant='outlined'
                onClick={() => {
                  setNodeEditorMode('add');
                  setNodeEditorTab(discoveredNodeCandidates.length > 0 ? 'discovered' : 'manual');
                  setNodeEditorOpen(true);
                }}
              startIcon={<IconPlus size={16} />}
            >
              Add node
            </Button>
          </Stack>
          {nodesInventoryMessage && <Alert severity='info'>{nodesInventoryMessage}</Alert>}
          <Dialog open={nodeEditorOpen} onClose={closeNodeEditor} fullWidth maxWidth='md'>
            <DialogTitle sx={{ pr: 6 }}>
              {nodeEditorMode === 'edit' ? 'Configure Node' : 'Add Node'}
              <IconButton
                onClick={closeNodeEditor}
                aria-label='Close add node dialog'
                sx={{ position: 'absolute', right: 16, top: 16 }}
              >
                <IconX size={18} />
              </IconButton>
            </DialogTitle>
            <DialogContent dividers>
              <Stack sx={{ gap: 1.5, pt: 0.5, minHeight: '32rem' }}>
                {nodeEditorMode === 'add' && (
                  <Tabs
                    value={nodeEditorTab}
                    onChange={(_, value) => setNodeEditorTab(value)}
                    variant='fullWidth'
                  >
                    <Tab
                      value='discovered'
                      label={
                        <Stack direction='row' sx={{ alignItems: 'center', gap: 0.5 }}>
                          <span>{`Discovered nodes${discoveredNodeCandidates.length > 0 ? ` (${discoveredNodeCandidates.length})` : ''}`}</span>
                          <Tooltip
                            title='Add currently joined nodes directly into configured inventory.'
                            placement='top'
                            slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
                          >
                            <IconButton size='small' sx={{ color: 'inherit', p: 0.25 }}>
                              <IconInfoCircle size={14} />
                            </IconButton>
                          </Tooltip>
                        </Stack>
                      }
                      disabled={discoveredNodeCandidates.length === 0}
                    />
                    <Tab
                      value='manual'
                      label={
                        <Stack direction='row' sx={{ alignItems: 'center', gap: 0.5 }}>
                          <span>Managed via SSH</span>
                          <Tooltip
                            title='Add a node Parallax can reach and control directly over SSH.'
                            placement='top'
                            slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
                          >
                            <IconButton size='small' sx={{ color: 'inherit', p: 0.25 }}>
                              <IconInfoCircle size={14} />
                            </IconButton>
                          </Tooltip>
                        </Stack>
                      }
                    />
                  </Tabs>
                )}
                <Box sx={{ minHeight: '26rem', display: 'flex', flexDirection: 'column', pt: 2 }}>
                  {nodeEditorMode === 'add' && nodeEditorTab === 'discovered' ? (
                    discoveredNodeCandidates.length > 0 ? (
                      <Stack sx={{ gap: 0.75, minHeight: 0, flex: 1 }}>
                        <Stack sx={{ gap: 0.75, maxHeight: '18rem', overflowY: 'auto', minHeight: 0, flex: 1 }}>
                          {discoveredNodeCandidates.map((node) => (
                            <Stack
                              key={node.id}
                              direction='row'
                              sx={{
                                alignItems: 'center',
                                justifyContent: 'space-between',
                                gap: 1,
                                px: 1.25,
                                py: 1,
                                borderRadius: 2,
                                border: '1px solid',
                                borderColor: 'divider',
                                bgcolor: 'background.paper',
                              }}
                            >
                              <Stack sx={{ minWidth: 0, gap: 0.25, flex: 1 }}>
                                <Typography variant='body2' sx={{ fontWeight: 600 }}>
                                  {node.hostname || node.id}
                                </Typography>
                                <Typography variant='caption' color='text.secondary' sx={{ wordBreak: 'break-all' }}>
                                  {node.id}
                                </Typography>
                              </Stack>
                              <Stack direction='row' sx={{ gap: 0.5, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                                <Chip size='small' variant='outlined' label='Self-joining' />
                                <Chip size='small' color={node.status === 'available' ? 'success' : 'default'} variant='outlined' label={node.status === 'available' ? 'Online' : node.status} />
                                <Button
                                  variant='outlined'
                                  disabled={nodesInventorySaving}
                                  onClick={() => void onAddDiscoveredNode(node)}
                                >
                                  Add
                                </Button>
                              </Stack>
                            </Stack>
                          ))}
                        </Stack>
                      </Stack>
                    ) : (
                      <Alert severity='info'>
                        No discovered node candidates are available right now. Switch to Manual to add one yourself.
                      </Alert>
                    )
                  ) : (
                    <Stack sx={{ gap: 1.5, minHeight: 0, flex: 1 }}>
                    <Stack sx={{ gap: 1, minHeight: 0, flex: 1 }}>
                      <TextField
                        label='Node name'
                        size='small'
                        fullWidth
                        value={nodeDraft.display_name}
                        onChange={(event) => setNodeDraft((prev) => ({ ...prev, display_name: event.target.value }))}
                        placeholder={nodeDraft.management_mode === 'manual' ? 'Worker node' : 'user@host'}
                      />
                      {nodeDraft.management_mode === 'manual' ? (
                        <TextField
                          label='Hostname hint'
                          size='small'
                          fullWidth
                          value={nodeDraft.hostname_hint}
                          onChange={(event) => setNodeDraft((prev) => ({ ...prev, hostname_hint: event.target.value }))}
                          placeholder='node-12'
                        />
                      ) : (
                      <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
                        <TextField
                          label='SSH target'
                          size='small'
                          fullWidth
                          value={nodeDraft.ssh_target}
                          onChange={(event) => setNodeDraft((prev) => ({ ...prev, ssh_target: event.target.value }))}
                          placeholder='user@host'
                          InputProps={{
                            endAdornment: (
                              <InputAdornment position='end'>
                                <Tooltip
                                  title='Format: user@host or user@host:port'
                                  placement='top'
                                  slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
                                >
                                  <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                                    <IconInfoCircle size={14} />
                                  </IconButton>
                                </Tooltip>
                              </InputAdornment>
                            ),
                          }}
                        />
                        <TextField
                          label='PARALLAX_PATH'
                          size='small'
                          fullWidth
                          value={nodeDraft.parallax_path}
                          onChange={(event) => setNodeDraft((prev) => ({ ...prev, parallax_path: event.target.value }))}
                          placeholder='/path/to/parallax'
                        />
                      </Stack>
                      )}
                      {(nodeDraftProbeLoading || nodeDraftProbeResult || nodeDraftProbeError) && (
                        nodeDraft.management_mode === 'ssh_managed' ? (
                        <Stack sx={{ gap: 0.75 }}>
                          {nodeDraftProbeLoading && (
                            <Alert severity='info' icon={<CircularProgress size={16} color='inherit' />}>
                              Checking SSH access and remote node environment…
                            </Alert>
                          )}
                          {nodeDraftProbeError && !nodeDraftProbeLoading && (
                            <Alert severity='warning'>
                              {nodeDraftProbeError}
                            </Alert>
                          )}
                          {nodeDraftProbeResult && !nodeDraftProbeLoading && (
                            <Alert severity={nodeDraftProbeResult.ok ? 'success' : (nodeDraftProbeResult.ssh_reachable ? 'warning' : 'error')}>
                              <Stack sx={{ gap: 0.5 }}>
                                <Typography variant='body2'>
                                  {nodeDraftProbeResult.message}
                                </Typography>
                                {nodeDraftProbeResult.os_name && (
                                  <Typography variant='caption' color='text.secondary'>
                                    Remote OS: {nodeDraftProbeResult.os_name}
                                  </Typography>
                                )}
                                {(nodeDraftProbeResult.remote_user || nodeDraftProbeResult.remote_host) && (
                                  <Typography variant='caption' color='text.secondary'>
                                    Remote session: {(nodeDraftProbeResult.remote_user || 'unknown')}@{(nodeDraftProbeResult.remote_host || 'unknown')}
                                  </Typography>
                                )}
                                {(Number(nodeDraftProbeResult.gpu_memory_gb || 0) > 0 || Number(nodeDraftProbeResult.ram_total_gb || 0) > 0) && (
                                  <Typography variant='caption' color='text.secondary'>
                                    Detected hardware: {Number(nodeDraftProbeResult.gpu_memory_gb || 0) > 0 ? `${Number(nodeDraftProbeResult.gpu_memory_gb || 0)} GB VRAM` : 'GPU unknown'}{Number(nodeDraftProbeResult.ram_total_gb || 0) > 0 ? ` · ${Number(nodeDraftProbeResult.ram_total_gb || 0)} GB RAM` : ''}
                                  </Typography>
                                )}
                                <Stack direction='row' sx={{ gap: 0.5, flexWrap: 'wrap' }}>
                                  <Chip
                                    size='small'
                                    color={nodeDraftProbeResult.ssh_reachable ? 'success' : 'default'}
                                    variant='outlined'
                                    label={nodeDraftProbeResult.ssh_reachable ? 'SSH reachable' : 'SSH unreachable'}
                                  />
                                  <Chip
                                    size='small'
                                    color={nodeDraftProbeResult.path_exists ? 'success' : 'default'}
                                    variant='outlined'
                                    label={nodeDraftProbeResult.path_exists ? 'Path found' : 'Path missing'}
                                  />
                                  <Chip
                                    size='small'
                                    color={nodeDraftProbeResult.has_venv_activate ? 'success' : 'default'}
                                    variant='outlined'
                                    label={nodeDraftProbeResult.has_venv_activate ? 'venv ok' : 'venv missing'}
                                  />
                                  <Chip
                                    size='small'
                                    color={nodeDraftProbeResult.has_parallax_bin ? 'success' : 'default'}
                                    variant='outlined'
                                    label={nodeDraftProbeResult.has_parallax_bin ? 'parallax ok' : 'parallax missing'}
                                  />
                                </Stack>
                                {(nodeDraftProbeResult.notes || []).map((note, index) => (
                                  <Typography key={`${nodeDraftProbeResult.os_name || 'note'}-${index}`} variant='caption' color='text.secondary'>
                                    {note}
                                  </Typography>
                                ))}
                                {(!nodeDraftProbeResult.ok || !!nodeDraftProbeResult.stderr) && (nodeDraftProbeResult.stderr || nodeDraftProbeResult.stdout) && (
                                  <Box
                                    component='pre'
                                    sx={{
                                      m: 0,
                                      mt: 0.5,
                                      px: 1,
                                      py: 0.75,
                                      borderRadius: 1.5,
                                      bgcolor: 'rgba(0,0,0,0.06)',
                                      fontFamily: 'monospace',
                                      fontSize: '0.78rem',
                                      lineHeight: 1.4,
                                      whiteSpace: 'pre-wrap',
                                      wordBreak: 'break-word',
                                    }}
                                  >
                                    {nodeDraftProbeResult.stderr || nodeDraftProbeResult.stdout}
                                  </Box>
                                )}
                              </Stack>
                            </Alert>
                          )}
                        </Stack>
                        ) : null
                      )}
                      {nodeEditorMode === 'add' && nodeDraft.management_mode === 'ssh_managed' && (
                      <Stack sx={{ gap: 0.75, mt: 2.5 }}>
                        <Typography variant='body2' color='text.secondary'>
                          For nodes Parallax does not SSH into, run this command on the remote machine and then add the node from the <strong>Discovered</strong> tab after it appears.
                        </Typography>
                        <JoinCommand />
                      </Stack>
                      )}
                    </Stack>
                      <Stack direction='row' sx={{ justifyContent: 'flex-end', gap: 1, mt: 'auto' }}>
                        <Button variant='text' onClick={closeNodeEditor}>
                          Cancel
                        </Button>
                        <Button
                          variant='contained'
                          onClick={() => void (nodeEditorMode === 'edit' ? onSaveNodeDraft() : onAddNodeDraft())}
                          disabled={
                            nodesInventorySaving
                            || (
                              nodeDraft.management_mode === 'ssh_managed'
                                ? (
                                  nodeDraftProbeLoading
                                  || !nodeDraft.ssh_target.trim()
                                  || !nodeDraft.parallax_path.trim()
                                  || !nodeDraftProbeResult?.ok
                                )
                                : !nodeDraft.hostname_hint.trim()
                            )
                          }
                        >
                          {nodesInventorySaving ? (nodeEditorMode === 'edit' ? 'Saving...' : 'Adding...') : (nodeEditorMode === 'edit' ? 'Save node' : 'Add node')}
                        </Button>
                      </Stack>
                    </Stack>
                  )}
                </Box>
              </Stack>
            </DialogContent>
          </Dialog>
          {nodesInventoryLoading && <Typography variant='body2' color='text.secondary'>Loading node inventory…</Typography>}
          <NodeManagementContent
            embedded
            showLiveOnlyHosts={false}
            refreshToken={nodesOverviewRefreshToken}
            onConfigureConfiguredHost={openConfiguredNodeEditor}
            onRemoveConfiguredHost={(host) => {
              setPendingDeleteNode({
                id: host.id,
                label: host.display_name || host.ssh_target || host.hostname_hint || 'this node',
              });
            }}
          />
          <AlertDialog
            open={!!pendingDeleteNode}
            onClose={() => setPendingDeleteNode(null)}
            color='warning'
            title='Delete node'
            content={
              <Typography variant='body2'>
                Delete {pendingDeleteNode ? `"${truncateChatHistoryLabel(pendingDeleteNode.label)}"` : 'this node'}? This cannot be undone.
              </Typography>
            }
            cancelLabel='Cancel'
            confirmLabel='Delete'
            autoFocusAction='cancel'
            onConfirm={confirmDeleteNode}
          />
        </Stack>
      );
    }

    if (activeSection === 'chat') {
      const chatHistoryPageCount = Math.max(1, Math.ceil(chatHistoryCount / CHAT_HISTORY_PAGE_SIZE));
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Typography variant='h2'>Chat</Typography>
          <Typography variant='body2' color='text.secondary'>
            Manage persisted chat history for the scheduler instance.
          </Typography>
          {chatHistoryError && <Alert severity='warning'>{chatHistoryError}</Alert>}
          <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 2 }}>
            <Typography variant='body2' color='text.secondary'>
              {chatHistoryLoading ? 'Loading chat history…' : `${chatHistoryCount} saved conversation${chatHistoryCount === 1 ? '' : 's'}`}
            </Typography>
            <Button color='error' variant='outlined' onClick={onClearAllChatHistory} disabled={clearingChatHistory || chatHistoryCount === 0}>
              {clearingChatHistory ? 'Clearing...' : 'Clear all history'}
            </Button>
          </Stack>
          <Stack sx={{ gap: 0.75 }}>
            {chatHistoryLoading && (
              <Typography variant='body2' color='text.secondary'>Loading conversation page…</Typography>
            )}
            {!chatHistoryLoading && chatHistoryItems.length === 0 && (
              <Typography variant='body2' color='text.secondary'>No saved conversations yet.</Typography>
            )}
            {chatHistoryItems.map((conversation) => (
              <Stack
                key={conversation.conversation_id}
                sx={{
                  gap: 0.5,
                  px: 1.25,
                  py: 1,
                  borderRadius: 2,
                  border: '1px solid',
                  borderColor: 'divider',
                  bgcolor: 'background.paper',
                }}
              >
                <Stack direction='row' sx={{ alignItems: 'flex-start', justifyContent: 'space-between', gap: 1.5 }}>
                  <Stack sx={{ minWidth: 0, gap: 0.25, flex: 1 }}>
                    <Typography
                      variant='body2'
                      sx={{
                        minWidth: 0,
                        display: 'flex',
                        alignItems: 'baseline',
                        gap: 0.75,
                        whiteSpace: 'nowrap',
                        overflow: 'hidden',
                      }}
                    >
                      <Box component='span' sx={{ fontWeight: 600, flex: 'none' }}>
                        {conversation.title || `Conversation ${conversation.conversation_id.slice(0, 8)}`}
                      </Box>
                      {(conversation.summary || conversation.last_message) && (
                        <>
                          <Box component='span' sx={{ color: 'text.disabled', flex: 'none' }}>|</Box>
                          <Box
                            component='span'
                            sx={{
                              color: 'text.secondary',
                              minWidth: 0,
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                            }}
                          >
                            {conversation.summary || conversation.last_message}
                          </Box>
                        </>
                      )}
                    </Typography>
                    <Stack direction='row' sx={{ gap: 0.5, flexWrap: 'wrap', pt: 0.25 }}>
                      <Typography variant='caption' color='text.secondary'>
                        {conversation.message_count} message{conversation.message_count === 1 ? '' : 's'}
                      </Typography>
                      {conversation.summary_source && conversation.summary_source !== 'none' && (
                        <Typography variant='caption' color='text.secondary'>
                          Summary: {conversation.summary_source}
                        </Typography>
                      )}
                      {conversation.updated_at > 0 && (
                        <Typography variant='caption' color='text.secondary'>
                          Updated {formatChatTimestamp(conversation.updated_at)}
                        </Typography>
                      )}
                    </Stack>
                  </Stack>
                  <Stack direction='row' sx={{ gap: 0.25, alignItems: 'center', flex: 'none' }}>
                    <Tooltip title={chatHistoryExportingId === conversation.conversation_id ? 'Exporting conversation' : 'Export conversation JSON'}>
                      <span>
                        <IconButton
                          size='small'
                          disabled={chatHistoryExportingId === conversation.conversation_id}
                          onClick={() => void onExportChatConversation(conversation)}
                        >
                          <IconDownload size={16} />
                        </IconButton>
                      </span>
                    </Tooltip>
                    <Tooltip title={chatHistoryDeletingId === conversation.conversation_id ? 'Deleting conversation' : 'Delete conversation'}>
                      <span>
                        <IconButton
                          size='small'
                          color='error'
                          disabled={chatHistoryDeletingId === conversation.conversation_id}
                          onClick={() => {
                            const label = cleanChatHistoryLabel(conversation.title || conversation.last_message || '') || 'Untitled conversation';
                            setPendingDeleteConversation({ id: conversation.conversation_id, label });
                          }}
                        >
                          <IconTrash size={16} />
                        </IconButton>
                      </span>
                    </Tooltip>
                  </Stack>
                </Stack>
              </Stack>
            ))}
          </Stack>
          {chatHistoryPageCount > 1 && (
            <Stack direction='row' sx={{ justifyContent: 'center', pt: 0.5 }}>
              <Pagination
                count={chatHistoryPageCount}
                page={Math.min(chatHistoryPage, chatHistoryPageCount)}
                onChange={(_, page) => setChatHistoryPage(page)}
                color='primary'
                shape='rounded'
              />
            </Stack>
          )}
          <AlertDialog
            open={!!pendingDeleteConversation}
            onClose={() => setPendingDeleteConversation(null)}
            color='warning'
            title='Delete conversation'
            content={
              <Typography variant='body2'>
                Delete {pendingDeleteConversation ? `"${truncateChatHistoryLabel(pendingDeleteConversation.label)}"` : 'this conversation'}? This cannot be undone.
              </Typography>
            }
            cancelLabel='Cancel'
            confirmLabel='Delete'
            autoFocusAction='cancel'
            onConfirm={confirmDeleteChatConversation}
          />
        </Stack>
      );
    }

    if (activeSection === 'transfer') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Typography variant='h2'>Import &amp; Export</Typography>
          <Typography variant='body2' color='text.secondary'>
            Export the current saved configuration as JSON, or import a JSON bundle to replace the existing saved settings. Import replaces the old configuration instead of merging it.
          </Typography>
          <Alert severity='warning'>
            Import replaces saved model selection, cluster capacity settings, advanced settings, managed node inventory, and custom models.
          </Alert>
          {transferMessage && <Alert severity='success'>{transferMessage}</Alert>}
          {transferError && <Alert severity='warning'>{transferError}</Alert>}
          <input
            ref={importFileInputRef}
            type='file'
            accept='application/json,.json'
            onChange={onImportSettingsFile}
            style={{ display: 'none' }}
          />
          <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1, flexWrap: 'wrap' }}>
            <Button variant='contained' onClick={onExportSettings} disabled={exportingSettings}>
              {exportingSettings ? 'Exporting...' : 'Export settings JSON'}
            </Button>
            <Button variant='outlined' color='warning' onClick={() => importFileInputRef.current?.click()} disabled={importingSettings}>
              {importingSettings ? 'Importing...' : 'Import settings JSON'}
            </Button>
          </Stack>
        </Stack>
      );
    }

    return null;
  };

  return (
    <Stack direction={{ xs: 'column', lg: 'row' }} sx={{ gap: 3, minHeight: 0, overflow: 'hidden' }}>
      <Stack
        sx={{
          width: { xs: '100%', lg: '14rem' },
          flex: 'none',
          gap: 1,
          position: { lg: 'sticky' },
          top: 0,
          alignSelf: { lg: 'flex-start' },
        }}
      >
        <Stack sx={{ gap: 0.75 }}>
          {SETTINGS_SECTIONS.map((section) => (
            <Button
              key={section.key}
              variant={activeSection === section.key ? 'contained' : 'text'}
              color={activeSection === section.key ? 'primary' : 'inherit'}
              onClick={() => openSection(section.key)}
              sx={{
                justifyContent: 'flex-start',
                borderRadius: 2,
                textTransform: 'none',
                fontSize: '0.95rem',
                lineHeight: 1.25,
                fontWeight: activeSection === section.key ? 700 : 500,
                letterSpacing: 0,
                px: 1.5,
                py: 1,
                minHeight: '2.5rem',
                color: activeSection === section.key ? 'primary.contrastText' : 'text.secondary',
                transition: 'background-color .15s ease, color .15s ease, transform .15s ease',
                '&.MuiButton-root': {
                  fontSize: '0.95rem',
                  lineHeight: 1.25,
                },
                '&:hover': {
                  bgcolor: activeSection === section.key ? undefined : 'action.hover',
                  transform: 'translateX(2px)',
                },
              }}
            >
              {section.label}
            </Button>
          ))}
        </Stack>
      </Stack>
      <Stack sx={{ minWidth: 0, flex: 1, overflowY: 'auto', pr: { lg: 1 } }}>
        {renderSectionContent()}
      </Stack>
    </Stack>
  );
};
