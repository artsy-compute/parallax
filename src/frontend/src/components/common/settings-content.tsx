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
  IconButton,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import {
  IconCheck,
  IconInfoCircle,
  IconLoader,
  IconTrash,
} from '@tabler/icons-react';
import { useCluster, useHost } from '../../services';
import {
  addCustomModel,
  deleteCustomModel,
  deleteAllChatHistory,
  exportSettingsBundle,
  getChatHistoryList,
  getCustomModelList,
  getFrontendBuildStatus,
  getNodesInventory,
  importSettingsBundle,
  searchCustomModels,
  updateNodesInventory,
  updateAppSettings,
  type SettingsExportBundle,
  type AppClusterProfile,
  type CustomModelRecord,
  type CustomModelSearchResult,
} from '../../services/api';
import { useRefCallback } from '../../hooks';
import { NodeManagementContent } from './node-management-content';
import { JoinCommand, ModelSelect } from '../inputs';

type SettingsSectionKey = 'general' | 'cluster' | 'custom-models' | 'nodes' | 'chat' | 'advanced' | 'transfer' | 'about';

const SETTINGS_SECTIONS: ReadonlyArray<{ key: SettingsSectionKey; label: string }> = [
  { key: 'general', label: 'Overview' },
  { key: 'nodes', label: 'Nodes' },
  { key: 'cluster', label: 'Cluster' },
  { key: 'custom-models', label: 'Custom Models' },
  { key: 'chat', label: 'Chat' },
  { key: 'advanced', label: 'Advanced' },
  { key: 'transfer', label: 'Import & Export' },
  { key: 'about', label: 'About' },
];

export const SettingsContent: FC<{ routeSection?: string }> = ({ routeSection = 'cluster' }) => {
  const navigate = useNavigate();
  const [{ type: hostType }] = useHost();
  const [
    {
      config: { modelInfo, networkType, initNodesNumber, modelName: selectedModelName, activeClusterId, clusterProfiles },
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
  const [customModelSourceType, setCustomModelSourceType] = useState<'huggingface' | 'local_path'>('huggingface');
  const [customModelSourceValue, setCustomModelSourceValue] = useState('');
  const [customModelDisplayName, setCustomModelDisplayName] = useState('');
  const [customModelSubmitting, setCustomModelSubmitting] = useState(false);
  const [customModelDeletingId, setCustomModelDeletingId] = useState('');
  const [customModelEditorOpen, setCustomModelEditorOpen] = useState(false);
  const [customModelSearchLoading, setCustomModelSearchLoading] = useState(false);
  const [customModelSearchResults, setCustomModelSearchResults] = useState<readonly CustomModelSearchResult[]>([]);
  const [customModelSearchOpen, setCustomModelSearchOpen] = useState(false);
  const customModelSearchCacheRef = useRef<Record<string, readonly CustomModelSearchResult[]>>({});
  const customModelSearchRequestIdRef = useRef(0);
  const [rebalancingTopology, setRebalancingTopology] = useState(false);
  const [initializingCluster, setInitializingCluster] = useState(false);
  const [chatHistoryCount, setChatHistoryCount] = useState(0);
  const [chatHistoryLoading, setChatHistoryLoading] = useState(false);
  const [clearingChatHistory, setClearingChatHistory] = useState(false);
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
    linked_clusters: readonly { id: string; name: string }[];
    linked_cluster_ids: readonly string[];
    linked_cluster_names: readonly string[];
    linked_cluster_count: number;
  }>>([]);
  const [nodesInventoryLoading, setNodesInventoryLoading] = useState(false);
  const [nodesInventorySaving, setNodesInventorySaving] = useState(false);
  const [nodesInventoryMessage, setNodesInventoryMessage] = useState('');
  const [hostEditorOpen, setHostEditorOpen] = useState(false);
  const [hostDraft, setHostDraft] = useState<{ ssh_target: string; parallax_path: string }>({ ssh_target: '', parallax_path: '' });
  const [manualNodeEditorOpen, setManualNodeEditorOpen] = useState(false);
  const [manualNodeDraft, setManualNodeDraft] = useState<{ display_name: string; hostname_hint: string; network_scope: 'local' | 'remote' }>({
    display_name: '',
    hostname_hint: '',
    network_scope: 'remote',
  });
  const [buildStatus, setBuildStatus] = useState<any | null>(null);
  const [buildStatusLoading, setBuildStatusLoading] = useState(false);
  const [clusterNameDraft, setClusterNameDraft] = useState('');
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

  useEffect(() => {
    if (hostType !== 'node') {
      loadCustomModels();
    }
  }, [hostType]);

  const activeSection = SETTINGS_SECTIONS.some((item) => item.key === routeSection)
    ? (routeSection as SettingsSectionKey)
    : 'cluster';

  const selectedCluster = clusterProfiles.find((item) => item.id === activeClusterId) || clusterProfiles[0];
  const selectedClusterAssignedNodeIds = new Set(selectedCluster?.assigned_node_ids || []);
  const availableNodeByHostname = new Map(
    nodeInfoList
      .filter((node) => node.status === 'available')
      .map((node) => [normalizeHostname(node.hostname), node] as const)
      .filter(([hostname]) => !!hostname),
  );
  const assignedHosts = nodesInventory.filter((host) => selectedClusterAssignedNodeIds.has(host.id));
  const assignedHostDetails = assignedHosts.map((host) => ({
    host,
    node: availableNodeByHostname.get(normalizeHostname(host.hostname_hint || host.ssh_target)),
  }));
  const availableAssignedHosts = assignedHosts.filter((host) => host.joined);
  const availableAssignedVram = assignedHostDetails
    .filter((item) => item.host.joined)
    .reduce((sum, item) => sum + Math.max(0, Number(item.node?.gpuMemory || 0)), 0);
  const knownAssignedHardwareCount = assignedHostDetails.filter((item) => Number(item.node?.gpuMemory || 0) > 0).length;
  const remainingVramGap = Math.max(0, Number(modelInfo?.vram || 0) - availableAssignedVram);

  useEffect(() => {
    const loadChatHistory = async () => {
      try {
        setChatHistoryLoading(true);
        const history = await getChatHistoryList();
        setChatHistoryCount(history.length);
      } catch (error) {
        console.error('getChatHistoryList error', error);
      } finally {
        setChatHistoryLoading(false);
      }
    };
    loadChatHistory();
  }, []);

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
    const loadBuildStatus = async () => {
      try {
        setBuildStatusLoading(true);
        setBuildStatus(await getFrontendBuildStatus());
      } catch (error) {
        console.error('getFrontendBuildStatus error', error);
      } finally {
        setBuildStatusLoading(false);
      }
    };
    loadBuildStatus();
  }, []);

  useEffect(() => {
    if (hostType === 'node' || customModelSourceType !== 'huggingface') {
      return;
    }
    const query = customModelSourceValue.trim();
    if (!query) {
      setCustomModelSearchResults([]);
      setCustomModelSearchLoading(false);
      setCustomModelSearchOpen(false);
      return;
    }
    const timeoutId = window.setTimeout(async () => {
      const cached = customModelSearchCacheRef.current[query];
      if (cached) {
        setCustomModelError('');
        setCustomModelSearchResults(cached);
        setCustomModelSearchLoading(false);
        setCustomModelSearchOpen(cached.length > 0);
        return;
      }
      const requestId = ++customModelSearchRequestIdRef.current;
      try {
        setCustomModelSearchLoading(true);
        setCustomModelError('');
        const results = await searchCustomModels(query, 8);
        if (requestId !== customModelSearchRequestIdRef.current) {
          return;
        }
        customModelSearchCacheRef.current[query] = results;
        setCustomModelSearchResults(results);
        setCustomModelSearchOpen(results.length > 0);
      } catch (error) {
        if (requestId !== customModelSearchRequestIdRef.current) {
          return;
        }
        setCustomModelError(error instanceof Error ? error.message : 'Failed to search Hugging Face models');
        setCustomModelSearchResults([]);
        setCustomModelSearchOpen(false);
      } finally {
        if (requestId === customModelSearchRequestIdRef.current) {
          setCustomModelSearchLoading(false);
        }
      }
    }, 1000);
    return () => window.clearTimeout(timeoutId);
  }, [hostType, customModelSourceType, customModelSourceValue]);

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

  const onContinueToJoin = useRefCallback(async () => {
    const shouldInit =
      clusterStatus === 'idle'
      || clusterStatus === 'failed'
      || clusterInitNodesNumber !== initNodesNumber
      || clusterModelName !== selectedModelName;
    if (!shouldInit) {
      navigate('/join');
      return;
    }
    try {
      setInitializingCluster(true);
      await init();
      navigate('/join');
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
      setCustomModelSourceValue('');
      setCustomModelDisplayName('');
      setCustomModelEditorOpen(false);
      await Promise.all([loadCustomModels(), refreshModelList()]);
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

  const onCreateClusterProfile = async () => {
    const nextIndex = clusterProfiles.length + 1;
    const source = selectedCluster || {
      model_name: selectedModelName,
      init_nodes_num: initNodesNumber,
      assigned_node_ids: [],
      is_local_network: networkType === 'local',
      network_type: networkType,
      advanced: {},
    };
    const newCluster: AppClusterProfile = {
      id: `cluster-${Date.now()}`,
      name: `Cluster ${nextIndex}`,
      model_name: String(source.model_name || ''),
      init_nodes_num: Math.max(1, Number((source.assigned_node_ids || []).length || source.init_nodes_num || 1)),
      assigned_node_ids: [...(source.assigned_node_ids || [])],
      is_local_network: source.network_type === 'remote' ? false : true,
      network_type: source.network_type === 'remote' ? 'remote' : 'local',
      advanced: { ...(source.advanced || {}) },
    };
    await applyClusterProfilesState([...clusterProfiles, newCluster], newCluster.id);
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

  const onDeleteClusterProfile = async () => {
    if (!selectedCluster || clusterProfiles.length <= 1) {
      return;
    }
    const remaining = clusterProfiles.filter((item) => item.id !== selectedCluster.id);
    const nextActiveId = remaining[0]?.id || '';
    await applyClusterProfilesState(remaining, nextActiveId);
  };

  const onClearAllChatHistory = async () => {
    try {
      setClearingChatHistory(true);
      const result = await deleteAllChatHistory();
      setChatHistoryCount(0);
      if (result.deleted >= 0) {
        console.log('Deleted chat history conversations:', result.deleted);
      }
    } catch (error) {
      console.error('deleteAllChatHistory error', error);
    } finally {
      setClearingChatHistory(false);
    }
  };

  const updateInventoryRow = (localId: string, patch: Partial<{ ssh_target: string; parallax_path: string }>) => {
    setNodesInventory((prev) =>
      prev.map((item) => (
        item.local_id === localId
          ? {
            ...item,
            ...patch,
            display_name: patch.ssh_target !== undefined && item.management_mode === 'ssh_managed' ? patch.ssh_target : item.display_name,
            hostname_hint: patch.ssh_target !== undefined ? normalizeHostname(patch.ssh_target) : item.hostname_hint,
          }
          : item
      )),
    );
  };

  const onAddInventoryHostDraft = () => {
    const sshTarget = hostDraft.ssh_target.trim();
    const parallaxPath = hostDraft.parallax_path.trim();
    if (!sshTarget) {
      return;
    }
    setNodesInventory((prev) => [
      ...prev,
      {
        local_id: nextInventoryRowId(),
        id: '',
        display_name: sshTarget,
        ssh_target: sshTarget,
        parallax_path: parallaxPath,
        hostname_hint: normalizeHostname(sshTarget),
        joined: false,
        management_mode: 'ssh_managed',
        network_scope: 'remote',
        linked_clusters: [],
        linked_cluster_ids: [],
        linked_cluster_names: [],
        linked_cluster_count: 0,
      },
    ]);
    setHostDraft({ ssh_target: '', parallax_path: '' });
    setHostEditorOpen(false);
  };

  const onAddManualNodeDraft = () => {
    const hostnameHint = normalizeHostname(manualNodeDraft.hostname_hint);
    const displayName = manualNodeDraft.display_name.trim() || hostnameHint;
    if (!hostnameHint) {
      return;
    }
    setNodesInventory((prev) => [
      ...prev,
      {
        local_id: nextInventoryRowId(),
        id: '',
        display_name: displayName,
        ssh_target: '',
        parallax_path: '',
        hostname_hint: hostnameHint,
        joined: false,
        management_mode: 'manual',
        network_scope: manualNodeDraft.network_scope,
        linked_clusters: [],
        linked_cluster_ids: [],
        linked_cluster_names: [],
        linked_cluster_count: 0,
      },
    ]);
    setManualNodeDraft({ display_name: '', hostname_hint: '', network_scope: 'remote' });
    setManualNodeEditorOpen(false);
  };

  const onSaveNodesInventory = async () => {
    try {
      setNodesInventorySaving(true);
      setNodesInventoryMessage('');
      const result = await updateNodesInventory(
        nodesInventory.map((item) => ({
          id: item.id || undefined,
          display_name: item.display_name || undefined,
          ssh_target: item.ssh_target.trim(),
          parallax_path: item.parallax_path.trim(),
          hostname_hint: item.hostname_hint || undefined,
          management_mode: item.management_mode,
          network_scope: item.network_scope,
        })),
      );
      setNodesInventory((result.hosts || []).map(mapInventoryHost));
      await reloadSettings();
      setNodesInventoryMessage(result.message || 'Configured node inventory saved');
    } catch (error) {
      setNodesInventoryMessage(error instanceof Error ? error.message : 'Failed to save node inventory');
    } finally {
      setNodesInventorySaving(false);
    }
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
            <Typography variant='h2'>Cluster</Typography>
            {topologyChangeAdvisory.show && topologyChangeAdvisory.canRebalance && (
              <Button size='small' variant='outlined' color='warning' disabled={rebalancingTopology} onClick={onClickRebalanceTopology}>
                {rebalancingTopology ? 'Rebalancing...' : 'Rebalance now'}
              </Button>
            )}
          </Stack>
          <Typography variant='body2' color='text.secondary'>
            A saved cluster profile combines model choice, assigned nodes, and startup capacity into one scheduler configuration. Parallax runs one active cluster at a time, but you can keep multiple cluster definitions here and switch between them.
          </Typography>
          <Stack sx={{ gap: 1 }}>
            <Typography variant='body1'>Saved clusters</Typography>
            <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
              {clusterProfiles.map((cluster) => (
                <Button
                  key={cluster.id}
                  variant={cluster.id === activeClusterId ? 'contained' : 'outlined'}
                  onClick={() => void onSelectClusterProfile(cluster.id)}
                >
                  {cluster.name}
                </Button>
              ))}
              <Button variant='outlined' onClick={() => void onCreateClusterProfile()}>New cluster</Button>
            </Stack>
            <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
              <TextField
                label='Cluster name'
                size='small'
                fullWidth
                value={clusterNameDraft}
                onChange={(event) => setClusterNameDraft(event.target.value)}
              />
              <Button variant='outlined' onClick={() => void onSaveClusterName()} disabled={!selectedCluster || !clusterNameDraft.trim() || clusterNameDraft.trim() === selectedCluster.name}>
                Save name
              </Button>
              <Button variant='text' color='error' onClick={() => void onDeleteClusterProfile()} disabled={clusterProfiles.length <= 1}>
                Delete cluster
              </Button>
            </Stack>
          </Stack>
          <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
            <Typography variant='body1'>Model</Typography>
            <Tooltip
              title='Choose the model this saved cluster should host, then size startup capacity around it.'
              placement='right'
              slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
            >
              <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                <IconInfoCircle size={16} />
              </IconButton>
            </Tooltip>
          </Stack>
          <ModelSelect autoCommit />
          {!!modelInfo && modelInfo.vram > 0 && (
            <Alert severity='warning' icon={false}>
              <Box component='span' sx={{ whiteSpace: 'nowrap' }}>
                You&apos;ll need a <strong>{`minimum of ${modelInfo.vram} GB of total VRAM`}</strong> to host this model.
              </Box>
            </Alert>
          )}
          <Typography variant='body2' color='text.secondary'>
            Define the model for this saved cluster, assign which nodes it may use, and then set the initial number of joined nodes the scheduler should plan around. Node overlap across saved clusters is allowed, but it is your decision to keep or remove that overlap.
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
                  const assignedHere = selectedClusterAssignedNodeIds.has(host.id);
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
                            onChange={() => void onToggleAssignedNode(host.id)}
                            sx={{ p: 0, mr: 0.5 }}
                          />
                          <Typography variant='body2' sx={{ fontWeight: 600 }}>
                            {host.display_name || host.ssh_target || host.hostname_hint || 'Unnamed host'}
                          </Typography>
                          <Chip size='small' variant='outlined' label={host.management_mode === 'manual' ? 'Manual' : 'SSH managed'} />
                          <Chip size='small' variant='outlined' label={host.network_scope === 'local' ? 'Local' : 'Remote'} />
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
                            Manual node: Parallax cannot SSH into or restart this node.
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
              <Chip color='default' variant='outlined' label={`${assignedHosts.length} assigned`} />
              <Chip color={availableAssignedHosts.length > 0 ? 'success' : 'default'} variant='outlined' label={`${availableAssignedHosts.length} online now`} />
              <Chip color='default' variant='outlined' label={`startup target ${Math.max(1, assignedHosts.length)}`} />
              <Chip color='info' variant='outlined' label={`${availableAssignedVram} GB live VRAM`} />
              {modelInfo && modelInfo.vram > 0 && <Chip color='warning' variant='outlined' label={`${modelInfo.vram} GB required`} />}
            </Stack>
            {selectedClusterAssignedNodeIds.size > 0 && (
              <Typography variant='caption' color='text.secondary'>
                Using currently reporting live node hardware for VRAM totals. Hardware is known for {knownAssignedHardwareCount}/{assignedHosts.length} assigned node{assignedHosts.length === 1 ? '' : 's'}.
              </Typography>
            )}
            {!modelInfo || modelInfo.vram <= 0 ? (
              <Alert severity='info'>Model VRAM requirement is unknown, so Parallax cannot confirm cluster capacity yet.</Alert>
            ) : selectedClusterAssignedNodeIds.size === 0 ? (
              <Alert severity='warning'>No nodes are assigned to this cluster yet.</Alert>
            ) : availableAssignedVram >= modelInfo.vram ? (
              <Alert severity='success'>Assigned live nodes currently report enough VRAM for this model.</Alert>
            ) : (
              <Alert severity='warning'>
                Assigned live nodes currently report {availableAssignedVram} GB, about {remainingVramGap} GB short of the model requirement.
              </Alert>
            )}
          </Stack>
          <Alert severity='info'>
            Startup planning now follows the assigned-node list automatically. If more assigned nodes arrive later, Parallax can extend capacity, but adding nodes may trigger layer movement and temporary performance churn while the scheduler rebalances.
          </Alert>
          {topologyChangeAdvisory.show && <Alert severity='warning'>{topologyChangeAdvisory.message}</Alert>}
          <Stack direction='row' sx={{ justifyContent: 'flex-end' }}>
            <Button variant='contained' onClick={onContinueToJoin} disabled={initializingCluster}>
              {initializingCluster ? 'Starting cluster...' : 'Continue to node join'}
            </Button>
          </Stack>
        </Stack>
      );
    }

    if (activeSection === 'custom-models') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
            <Typography variant='h2'>Custom Models</Typography>
            <Tooltip
              title='Add Hugging Face repo ids or local model paths. These models are shared across clusters and appear in each cluster model selector.'
              placement='right'
              slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
            >
              <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                <IconInfoCircle size={16} />
              </IconButton>
            </Tooltip>
          </Stack>
          <Typography variant='body2' color='text.secondary'>
            Custom models are a shared library for all clusters. Supported in this version: Hugging Face repo ids and local filesystem paths. Arbitrary website URLs are intentionally not accepted.
          </Typography>
          {customModelError && <Alert severity='warning'>{customModelError}</Alert>}
          {!customModelEditorOpen && (
            <Stack direction='row' sx={{ justifyContent: 'flex-start' }}>
              <Button variant='outlined' onClick={() => setCustomModelEditorOpen(true)}>Add model</Button>
            </Stack>
          )}
          {customModelEditorOpen && (
            <Stack direction={{ xs: 'column', sm: 'row' }} sx={{ gap: 1 }}>
              <TextField
                select
                label='Source'
                size='small'
                value={customModelSourceType}
                onChange={(event) => setCustomModelSourceType(event.target.value as 'huggingface' | 'local_path')}
                sx={{ minWidth: { sm: '10rem' } }}
              >
                <MenuItem value='huggingface'>Hugging Face</MenuItem>
                <MenuItem value='local_path'>Local path</MenuItem>
              </TextField>
              {customModelSourceType === 'huggingface' ? (
                <Autocomplete
                  freeSolo
                  fullWidth
                  options={customModelSearchResults}
                  loading={customModelSearchLoading}
                  open={customModelSearchOpen}
                  onOpen={() => {
                    if (customModelSearchResults.length > 0) setCustomModelSearchOpen(true);
                  }}
                  onClose={() => setCustomModelSearchOpen(false)}
                  filterOptions={(options) => options}
                  getOptionLabel={(option) => typeof option === 'string' ? option : option.source_value}
                  inputValue={customModelSourceValue}
                  onInputChange={(_, value, reason) => {
                    if (reason !== 'reset' && !customModelSearchLoading) {
                      setCustomModelSourceValue(value);
                      if (!value.trim()) setCustomModelSearchOpen(false);
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
                      helperText='Searches Hugging Face after 1 second of inactivity.'
                      InputProps={{
                        ...params.InputProps,
                        readOnly: customModelSearchLoading,
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
              ) : (
                <TextField
                  label='Absolute path'
                  size='small'
                  fullWidth
                  value={customModelSourceValue}
                  onChange={(event) => setCustomModelSourceValue(event.target.value)}
                  placeholder='/path/to/model'
                />
              )}
              <TextField
                label='Display name'
                size='small'
                value={customModelDisplayName}
                onChange={(event) => setCustomModelDisplayName(event.target.value)}
                placeholder='Optional'
                sx={{ minWidth: { sm: '12rem' } }}
              />
              <Button variant='contained' onClick={onAddCustomModel} disabled={customModelSubmitting || !customModelSourceValue.trim()} sx={{ alignSelf: { xs: 'stretch', sm: 'center' }, whiteSpace: 'nowrap' }}>
                {customModelSubmitting ? 'Adding...' : 'Add model'}
              </Button>
              <Button
                variant='text'
                onClick={() => {
                  setCustomModelEditorOpen(false);
                  setCustomModelError('');
                  setCustomModelSourceValue('');
                  setCustomModelDisplayName('');
                  setCustomModelSearchOpen(false);
                }}
                sx={{ alignSelf: { xs: 'stretch', sm: 'center' }, whiteSpace: 'nowrap' }}
              >
                Cancel
              </Button>
            </Stack>
          )}
          <Stack sx={{ gap: 1, maxHeight: '24rem', overflow: 'auto' }}>
            {customModelLoading && <Typography variant='body2' color='text.secondary'>Loading custom models…</Typography>}
            {!customModelLoading && customModels.length === 0 && <Typography variant='body2' color='text.secondary'>No custom models added yet.</Typography>}
            {customModels.map((model) => (
              <Stack key={model.id} direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1.5, px: 1.25, py: 1, borderRadius: 2, border: '1px solid', borderColor: 'divider', bgcolor: 'background.paper' }}>
                <Stack sx={{ minWidth: 0, gap: 0.25 }}>
                  <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75, flexWrap: 'wrap' }}>
                    <Typography variant='body2' sx={{ fontWeight: 600 }}>{model.display_name || model.source_value}</Typography>
                    {renderValidationChip(model.validation_status)}
                    <Chip size='small' variant='outlined' label={model.source_type === 'huggingface' ? 'HF' : 'Local'} />
                  </Stack>
                  <Typography variant='caption' color='text.secondary' sx={{ wordBreak: 'break-all' }}>{model.source_value}</Typography>
                  {model.validation_message && <Typography variant='caption' color='text.secondary'>{model.validation_message}</Typography>}
                </Stack>
                <IconButton size='small' color='error' disabled={customModelDeletingId === model.id} onClick={() => onDeleteCustomModel(model.id)}>
                  <IconTrash size={16} />
                </IconButton>
              </Stack>
            ))}
          </Stack>
        </Stack>
      );
    }

    if (activeSection === 'nodes') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Typography variant='h2'>Nodes</Typography>
          <Typography variant='body2' color='text.secondary'>
            Manage the available machine pool here. Add SSH-managed hosts when Parallax can control them directly, or add manual remote nodes when they will join on their own and this cluster cannot SSH into them.
          </Typography>
          <Stack sx={{ gap: 0.75 }}>
            <Typography variant='body1'>Join command</Typography>
            <Typography variant='body2' color='text.secondary'>
              Use this command to bring additional nodes into the currently active cluster or to reconnect nodes during recovery.
            </Typography>
            <JoinCommand />
          </Stack>
          {nodesInventoryMessage && <Alert severity='info'>{nodesInventoryMessage}</Alert>}
          <Stack sx={{ gap: 1 }}>
            {nodesInventoryLoading && <Typography variant='body2' color='text.secondary'>Loading configured node inventory…</Typography>}
            {!nodesInventoryLoading && nodesInventory.length === 0 && <Typography variant='body2' color='text.secondary'>No configured node hosts yet.</Typography>}
            {nodesInventory.map((host) => (
              <Stack key={host.local_id} sx={{ gap: 0.75 }}>
                <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
                  {host.management_mode === 'ssh_managed' ? (
                    <>
                      <TextField label='SSH target' size='small' fullWidth value={host.ssh_target} onChange={(event) => updateInventoryRow(host.local_id, { ssh_target: event.target.value })} placeholder='user@host' />
                      <TextField label='PARALLAX_PATH' size='small' fullWidth value={host.parallax_path} onChange={(event) => updateInventoryRow(host.local_id, { parallax_path: event.target.value })} placeholder='/path/to/parallax' />
                    </>
                  ) : (
                    <>
                      <TextField
                        label='Node name'
                        size='small'
                        fullWidth
                        value={host.display_name}
                        onChange={(event) => setNodesInventory((prev) => prev.map((item) => item.local_id === host.local_id ? { ...item, display_name: event.target.value } : item))}
                        placeholder='Remote GPU node'
                      />
                      <TextField
                        label='Hostname hint'
                        size='small'
                        fullWidth
                        value={host.hostname_hint}
                        onChange={(event) => setNodesInventory((prev) => prev.map((item) => item.local_id === host.local_id ? { ...item, hostname_hint: normalizeHostname(event.target.value) } : item))}
                        placeholder='node-12'
                      />
                    </>
                  )}
                  <Button color='error' variant='text' onClick={() => setNodesInventory((prev) => prev.filter((item) => item.local_id !== host.local_id))}>Remove</Button>
                </Stack>
                <Stack direction='row' sx={{ gap: 0.5, flexWrap: 'wrap', pl: 0.25 }}>
                  <Chip size='small' variant='outlined' label={host.management_mode === 'manual' ? 'Manual' : 'SSH managed'} />
                  <Chip size='small' variant='outlined' label={host.network_scope === 'local' ? 'Local' : 'Remote'} />
                  {host.linked_cluster_count === 0 && <Chip size='small' variant='outlined' label='Unassigned' />}
                  {host.linked_clusters.map((cluster) => (
                    <Chip
                      key={`${host.id || host.local_id}-${cluster.id}`}
                      size='small'
                      color={cluster.id === activeClusterId ? 'primary' : 'default'}
                      variant={cluster.id === activeClusterId ? 'filled' : 'outlined'}
                      label={cluster.name}
                    />
                  ))}
                  {host.joined && <Chip size='small' color='success' variant='outlined' label='Online' />}
                </Stack>
              </Stack>
            ))}
            <Stack direction='row' sx={{ gap: 1, justifyContent: 'space-between' }}>
              <Stack sx={{ gap: 1, flex: 1 }}>
                {!hostEditorOpen && (
                  <Stack direction='row' sx={{ justifyContent: 'flex-start' }}>
                    <Button variant='outlined' onClick={() => setHostEditorOpen(true)}>Add host</Button>
                  </Stack>
                )}
                {hostEditorOpen && (
                  <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
                    <TextField
                      label='SSH target'
                      size='small'
                      fullWidth
                      value={hostDraft.ssh_target}
                      onChange={(event) => setHostDraft((prev) => ({ ...prev, ssh_target: event.target.value }))}
                      placeholder='user@host'
                    />
                    <TextField
                      label='PARALLAX_PATH'
                      size='small'
                      fullWidth
                      value={hostDraft.parallax_path}
                      onChange={(event) => setHostDraft((prev) => ({ ...prev, parallax_path: event.target.value }))}
                      placeholder='/path/to/parallax'
                    />
                    <Button variant='contained' onClick={onAddInventoryHostDraft} disabled={!hostDraft.ssh_target.trim()}>
                      Add host
                    </Button>
                    <Button
                      variant='text'
                      onClick={() => {
                        setHostEditorOpen(false);
                        setHostDraft({ ssh_target: '', parallax_path: '' });
                      }}
                    >
                      Cancel
                    </Button>
                  </Stack>
                )}
                {!manualNodeEditorOpen && (
                  <Stack direction='row' sx={{ justifyContent: 'flex-start' }}>
                    <Button variant='outlined' onClick={() => setManualNodeEditorOpen(true)}>Add manual remote node</Button>
                  </Stack>
                )}
                {manualNodeEditorOpen && (
                  <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
                    <TextField
                      label='Node name'
                      size='small'
                      fullWidth
                      value={manualNodeDraft.display_name}
                      onChange={(event) => setManualNodeDraft((prev) => ({ ...prev, display_name: event.target.value }))}
                      placeholder='Remote GPU node'
                    />
                    <TextField
                      label='Hostname hint'
                      size='small'
                      fullWidth
                      value={manualNodeDraft.hostname_hint}
                      onChange={(event) => setManualNodeDraft((prev) => ({ ...prev, hostname_hint: event.target.value }))}
                      placeholder='node-12'
                    />
                    <Stack direction='row' sx={{ gap: 1 }}>
                      <Button variant={manualNodeDraft.network_scope === 'remote' ? 'contained' : 'outlined'} onClick={() => setManualNodeDraft((prev) => ({ ...prev, network_scope: 'remote' }))}>
                        Remote
                      </Button>
                      <Button variant={manualNodeDraft.network_scope === 'local' ? 'contained' : 'outlined'} onClick={() => setManualNodeDraft((prev) => ({ ...prev, network_scope: 'local' }))}>
                        Local
                      </Button>
                    </Stack>
                    <Button variant='contained' onClick={onAddManualNodeDraft} disabled={!manualNodeDraft.hostname_hint.trim()}>
                      Add remote node
                    </Button>
                    <Button
                      variant='text'
                      onClick={() => {
                        setManualNodeEditorOpen(false);
                        setManualNodeDraft({ display_name: '', hostname_hint: '', network_scope: 'remote' });
                      }}
                    >
                      Cancel
                    </Button>
                  </Stack>
                )}
              </Stack>
              <Button variant='contained' onClick={onSaveNodesInventory} disabled={nodesInventorySaving}>
                {nodesInventorySaving ? 'Saving...' : 'Save inventory'}
              </Button>
            </Stack>
          </Stack>
          <NodeManagementContent embedded />
        </Stack>
      );
    }

    if (activeSection === 'chat') {
      return (
        <Stack sx={{ gap: 1 }}>
          <Typography variant='h2'>Chat</Typography>
          <Typography variant='body2' color='text.secondary'>
            Manage persisted chat history for the scheduler instance.
          </Typography>
          <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 2 }}>
            <Typography variant='body2' color='text.secondary'>
              {chatHistoryLoading ? 'Loading chat history…' : `${chatHistoryCount} saved conversation${chatHistoryCount === 1 ? '' : 's'}`}
            </Typography>
            <Button color='error' variant='outlined' onClick={onClearAllChatHistory} disabled={clearingChatHistory || chatHistoryCount === 0}>
              {clearingChatHistory ? 'Clearing...' : 'Clear all history'}
            </Button>
          </Stack>
        </Stack>
      );
    }

    if (activeSection === 'advanced') {
      return (
        <Stack sx={{ gap: 1 }}>
          <Typography variant='h2'>Advanced</Typography>
          <Typography variant='body2' color='text.secondary'>
            Utilities for refreshing cluster metadata and navigating to operational tooling. Advanced runtime values are included in settings export/import even when they are not edited directly here yet.
          </Typography>
          <Stack sx={{ gap: 1 }}>
            <Typography variant='body1'>Cluster networking mode</Typography>
            <Typography variant='body2' color='text.secondary'>
              This remains a cluster-wide startup setting for now. Local is for same-network discovery; remote uses the relay-assisted path.
            </Typography>
            <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
              <Button variant={networkType === 'local' ? 'contained' : 'outlined'} onClick={() => setNetworkType('local')} sx={{ minWidth: '5rem' }}>Local</Button>
              <Button variant={networkType === 'remote' ? 'contained' : 'outlined'} onClick={() => setNetworkType('remote')} sx={{ minWidth: '5rem' }}>Remote</Button>
            </Stack>
          </Stack>
          <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
            <Button variant='outlined' onClick={() => refreshModelList()}>Refresh model catalog</Button>
            <Button component={RouterLink} to='/join' variant='outlined'>Open reconnect flow</Button>
            <Button onClick={() => openSection('nodes')} variant='outlined'>Open nodes</Button>
          </Stack>
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

    return (
      <Stack sx={{ gap: 1 }}>
        <Typography variant='h2'>About</Typography>
        <Typography variant='body2' color='text.secondary'>
          Frontend/runtime diagnostics and documentation links.
        </Typography>
        <Alert severity={buildStatus?.stale ? 'warning' : 'info'}>
          {buildStatusLoading
            ? 'Loading frontend build status…'
            : buildStatus?.stale
              ? `Frontend build is stale: ${buildStatus.reason || 'dist is older than source files'}`
              : 'Frontend build is up to date.'}
        </Alert>
        <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
          <Button component='a' href='https://github.com/openai/parallax/blob/main/docs/settings_page_spec.md' target='_blank' rel='noreferrer' variant='text'>
            Settings spec
          </Button>
          <Button component='a' href='https://github.com/openai/parallax/blob/main/docs/user_guide/quick_start.md' target='_blank' rel='noreferrer' variant='text'>
            Quick start
          </Button>
        </Stack>
      </Stack>
    );
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
