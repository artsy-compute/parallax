import { useEffect, useRef, useState, type ChangeEvent, type FC } from 'react';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
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
import { JoinCommand, ModelSelect, NumberInput } from '../inputs';

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
  const [nodesInventory, setNodesInventory] = useState<Array<{ local_id: string; ssh_target: string; parallax_path: string }>>([]);
  const [nodesInventoryLoading, setNodesInventoryLoading] = useState(false);
  const [nodesInventorySaving, setNodesInventorySaving] = useState(false);
  const [nodesInventoryMessage, setNodesInventoryMessage] = useState('');
  const [hostEditorOpen, setHostEditorOpen] = useState(false);
  const [hostDraft, setHostDraft] = useState<{ ssh_target: string; parallax_path: string }>({ ssh_target: '', parallax_path: '' });
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
      setNodesInventory(
        (result.hosts || []).map((host) => ({
          local_id: nextInventoryRowId(),
          ssh_target: host.ssh_target || '',
          parallax_path: host.parallax_path || '',
        })),
      );
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
      is_local_network: networkType === 'local',
      network_type: networkType,
      advanced: {},
    };
    const newCluster: AppClusterProfile = {
      id: `cluster-${Date.now()}`,
      name: `Cluster ${nextIndex}`,
      model_name: String(source.model_name || ''),
      init_nodes_num: Math.max(1, Number(source.init_nodes_num || 1)),
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
      prev.map((item) => (item.local_id === localId ? { ...item, ...patch } : item)),
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
      { local_id: nextInventoryRowId(), ssh_target: sshTarget, parallax_path: parallaxPath },
    ]);
    setHostDraft({ ssh_target: '', parallax_path: '' });
    setHostEditorOpen(false);
  };

  const onSaveNodesInventory = async () => {
    try {
      setNodesInventorySaving(true);
      setNodesInventoryMessage('');
      const result = await updateNodesInventory(
        nodesInventory.map((item) => ({
          ssh_target: item.ssh_target.trim(),
          parallax_path: item.parallax_path.trim(),
        })),
      );
      setNodesInventory(
        (result.hosts || []).map((host) => ({
          local_id: nextInventoryRowId(),
          ssh_target: host.ssh_target || '',
          parallax_path: host.parallax_path || '',
        })),
      );
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

  const renderSectionContent = () => {
    if (activeSection === 'general') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Typography variant='h6'>Overview</Typography>
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
            <Typography variant='h6'>Cluster</Typography>
            {topologyChangeAdvisory.show && topologyChangeAdvisory.canRebalance && (
              <Button size='small' variant='outlined' color='warning' disabled={rebalancingTopology} onClick={onClickRebalanceTopology}>
                {rebalancingTopology ? 'Rebalancing...' : 'Rebalance now'}
              </Button>
            )}
          </Stack>
          <Typography variant='body2' color='text.secondary'>
            A saved cluster profile combines model choice and startup capacity into one scheduler configuration. Parallax runs one active cluster at a time, but you can keep multiple cluster definitions here and switch between them.
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
            Define the model for this saved cluster, then set the initial number of joined nodes the scheduler should plan around from the available node pool you defined in Nodes. Additional nodes can join later, but topology expansion is not free and may require rebalancing.
          </Typography>
          <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'center', gap: 2 }}>
            <Typography color='text.secondary'>Startup node target</Typography>
            <NumberInput
              sx={{ width: '10rem', boxShadow: 'none', bgcolor: 'transparent' }}
              slotProps={{
                root: { sx: { bgcolor: 'transparent', '&:hover': { bgcolor: 'transparent' }, '&:focus-within': { bgcolor: 'transparent' } } },
                input: { sx: { bgcolor: 'transparent !important', '&:focus': { outline: 'none' } } },
              }}
              value={initNodesNumber}
              onChange={(e) => setInitNodesNumber(Number(e.target.value))}
            />
          </Stack>
          <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'center', gap: 2 }}>
            <Typography color='text.secondary'>Are your nodes within the same local network?</Typography>
            <Stack direction='row' sx={{ gap: 1 }}>
              <Button variant={networkType === 'local' ? 'contained' : 'outlined'} onClick={() => setNetworkType('local')} sx={{ minWidth: '5rem' }}>Local</Button>
              <Button variant={networkType === 'remote' ? 'contained' : 'outlined'} onClick={() => setNetworkType('remote')} sx={{ minWidth: '5rem' }}>Remote</Button>
            </Stack>
          </Stack>
          <Alert severity='info'>
            Startup node target is used for initial allocation. If more nodes arrive later, Parallax can extend capacity, but adding nodes may trigger layer movement and temporary performance churn while the scheduler rebalances.
          </Alert>
          {topologyChangeAdvisory.show && <Alert severity='warning'>{topologyChangeAdvisory.message}</Alert>}
          <JoinCommand />
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
            <Typography variant='h6'>Custom Models</Typography>
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
          <Typography variant='h6'>Nodes</Typography>
          <Typography variant='body2' color='text.secondary'>
            Manage the available machine pool here. The inventory editor defines which SSH-reachable hosts the scheduler can use, and the live host rows below are the operational view of that same pool.
          </Typography>
          {nodesInventoryMessage && <Alert severity='info'>{nodesInventoryMessage}</Alert>}
          <Stack sx={{ gap: 1 }}>
            {nodesInventoryLoading && <Typography variant='body2' color='text.secondary'>Loading configured node inventory…</Typography>}
            {!nodesInventoryLoading && nodesInventory.length === 0 && <Typography variant='body2' color='text.secondary'>No configured node hosts yet.</Typography>}
            {nodesInventory.map((host, index) => (
              <Stack key={host.local_id} direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
                <TextField label='SSH target' size='small' fullWidth value={host.ssh_target} onChange={(event) => updateInventoryRow(host.local_id, { ssh_target: event.target.value })} placeholder='user@host' />
                <TextField label='PARALLAX_PATH' size='small' fullWidth value={host.parallax_path} onChange={(event) => updateInventoryRow(host.local_id, { parallax_path: event.target.value })} placeholder='/path/to/parallax' />
                <Button color='error' variant='text' onClick={() => setNodesInventory((prev) => prev.filter((item) => item.local_id !== host.local_id))}>Remove</Button>
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
          <Typography variant='h6'>Chat</Typography>
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
          <Typography variant='h6'>Advanced</Typography>
          <Typography variant='body2' color='text.secondary'>
            Utilities for refreshing cluster metadata and navigating to operational tooling. Advanced runtime values are included in settings export/import even when they are not edited directly here yet.
          </Typography>
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
          <Typography variant='h6'>Import &amp; Export</Typography>
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
        <Typography variant='h6'>About</Typography>
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
        <Typography variant='overline' color='text.secondary'>Categories</Typography>
        <Stack sx={{ gap: 0.75 }}>
          {SETTINGS_SECTIONS.map((section) => (
            <Button
              key={section.key}
              variant={activeSection === section.key ? 'contained' : 'text'}
              color={activeSection === section.key ? 'primary' : 'inherit'}
              onClick={() => openSection(section.key)}
              sx={{ justifyContent: 'flex-start', borderRadius: 2 }}
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
