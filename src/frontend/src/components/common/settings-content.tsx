import { useEffect, useRef, useState, type FC } from 'react';
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
  getChatHistoryList,
  getCustomModelList,
  getFrontendBuildStatus,
  getNodesInventory,
  searchCustomModels,
  updateNodesInventory,
  type CustomModelRecord,
  type CustomModelSearchResult,
} from '../../services/api';
import { useRefCallback } from '../../hooks';
import { NodeManagementContent } from './node-management-content';
import { JoinCommand, ModelSelect, NumberInput } from '../inputs';

type SettingsSectionKey = 'general' | 'models' | 'cluster' | 'nodes' | 'chat' | 'advanced' | 'about';

const SETTINGS_SECTIONS: ReadonlyArray<{ key: SettingsSectionKey; label: string }> = [
  { key: 'general', label: 'Overview' },
  { key: 'models', label: 'Models' },
  { key: 'cluster', label: 'Cluster Capacity' },
  { key: 'nodes', label: 'Node Inventory' },
  { key: 'chat', label: 'Chat' },
  { key: 'advanced', label: 'Advanced' },
  { key: 'about', label: 'About' },
];

export const SettingsContent: FC<{ routeSection?: string }> = ({ routeSection = 'models' }) => {
  const navigate = useNavigate();
  const [{ type: hostType }] = useHost();
  const [
    {
      config: { modelInfo, networkType, initNodesNumber, modelName: selectedModelName },
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
  const [nodesInventory, setNodesInventory] = useState<Array<{ ssh_target: string; parallax_path: string }>>([]);
  const [nodesInventoryLoading, setNodesInventoryLoading] = useState(false);
  const [nodesInventorySaving, setNodesInventorySaving] = useState(false);
  const [nodesInventoryMessage, setNodesInventoryMessage] = useState('');
  const [buildStatus, setBuildStatus] = useState<any | null>(null);
  const [buildStatusLoading, setBuildStatusLoading] = useState(false);
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
    : 'models';

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
    const loadInventory = async () => {
      try {
        setNodesInventoryLoading(true);
        setNodesInventoryMessage('');
        const result = await getNodesInventory();
        setNodesInventory(
          (result.hosts || []).map((host) => ({
            ssh_target: host.ssh_target || '',
            parallax_path: host.parallax_path || '',
          })),
        );
      } catch (error) {
        setNodesInventoryMessage(error instanceof Error ? error.message : 'Failed to load configured node inventory');
      } finally {
        setNodesInventoryLoading(false);
      }
    };
    loadInventory();
  }, []);

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

  const updateInventoryRow = (index: number, patch: Partial<{ ssh_target: string; parallax_path: string }>) => {
    setNodesInventory((prev) =>
      prev.map((item, itemIndex) => (itemIndex === index ? { ...item, ...patch } : item)),
    );
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

  const renderSectionContent = () => {
    if (activeSection === 'general') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Typography variant='h6'>Overview</Typography>
          <Typography variant='body2' color='text.secondary'>
            Settings now separates cluster planning from day-to-day operations. Choose what to run in <strong>Models</strong>, decide startup capacity and recovery behavior in <strong>Cluster Capacity</strong>, configure managed hosts in <strong>Node Inventory</strong>, and use <strong>Node Operations</strong> inside the Nodes section for live start, stop, restart, and logs.
          </Typography>
          <Alert severity='info'>
            Startup capacity is not a permanent hard limit. More nodes can join later, but adding or removing nodes may trigger topology changes and temporary layer reallocation before throughput improves.
          </Alert>
          <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1, flexWrap: 'wrap' }}>
            <Button variant='contained' onClick={() => openSection('models')}>Choose model</Button>
            <Button variant='outlined' onClick={() => openSection('cluster')}>Plan startup capacity</Button>
            <Button variant='outlined' onClick={() => openSection('nodes')}>Open nodes</Button>
          </Stack>
        </Stack>
      );
    }

    if (activeSection === 'models') {
      return (
        <Stack sx={{ gap: 1 }}>
          <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
            <Typography variant='h6'>Model Selection</Typography>
            <Tooltip
              title='Choose the model the scheduler should host first, then size the cluster around it.'
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
            <Alert severity='warning'>
              You&apos;ll need a <strong>{`minimum of ${modelInfo.vram} GB of total VRAM`}</strong> to host this model.
            </Alert>
          )}
          {!!modelInfo && (
            <Typography variant='body2' color='text.secondary'>
              Pick the model first. The startup node target in Cluster Capacity is only a planning value for initial placement; the cluster can grow later if more nodes join and you rebalance topology.
            </Typography>
          )}
          <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
            <Typography variant='body1'>Custom Models</Typography>
            <Tooltip
              title='Add Hugging Face repo ids or local model paths. Parallax validates config metadata before listing them in the shared model selector.'
              placement='right'
              slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
            >
              <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                <IconInfoCircle size={16} />
              </IconButton>
            </Tooltip>
          </Stack>
          <Typography variant='body2' color='text.secondary'>
            Supported in this version: Hugging Face repo ids and local filesystem paths. Arbitrary website URLs are intentionally not accepted.
          </Typography>
          {customModelError && <Alert severity='warning'>{customModelError}</Alert>}
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
          </Stack>
          <Stack sx={{ gap: 1, maxHeight: '16rem', overflow: 'auto' }}>
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

    if (activeSection === 'cluster') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
            <Typography variant='h6'>Cluster Capacity</Typography>
            {topologyChangeAdvisory.show && topologyChangeAdvisory.canRebalance && (
              <Button size='small' variant='outlined' color='warning' disabled={rebalancingTopology} onClick={onClickRebalanceTopology}>
                {rebalancingTopology ? 'Rebalancing...' : 'Rebalance now'}
              </Button>
            )}
          </Stack>
          <Typography variant='body2' color='text.secondary'>
            Set the startup target the scheduler should plan around, then reconnect or add nodes as capacity changes. Additional nodes can join later, but topology expansion is not free and may require rebalancing.
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

    if (activeSection === 'nodes') {
      return (
        <Stack sx={{ gap: 1.25 }}>
          <Typography variant='h6'>Nodes</Typography>
          <Typography variant='body2' color='text.secondary'>
            Manage host definitions and live operations together here. The inventory editor keeps the managed SSH target and install path current, and the host rows below are the live operational view.
          </Typography>
          {nodesInventoryMessage && <Alert severity='info'>{nodesInventoryMessage}</Alert>}
          <Stack sx={{ gap: 1 }}>
            {nodesInventoryLoading && <Typography variant='body2' color='text.secondary'>Loading configured node inventory…</Typography>}
            {!nodesInventoryLoading && nodesInventory.length === 0 && <Typography variant='body2' color='text.secondary'>No configured node hosts yet.</Typography>}
            {nodesInventory.map((host, index) => (
              <Stack key={`${host.ssh_target}-${index}`} direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
                <TextField label='SSH target' size='small' fullWidth value={host.ssh_target} onChange={(event) => updateInventoryRow(index, { ssh_target: event.target.value })} placeholder='user@host' />
                <TextField label='PARALLAX_PATH' size='small' fullWidth value={host.parallax_path} onChange={(event) => updateInventoryRow(index, { parallax_path: event.target.value })} placeholder='/path/to/parallax' />
                <Button color='error' variant='text' onClick={() => setNodesInventory((prev) => prev.filter((_, itemIndex) => itemIndex !== index))}>Remove</Button>
              </Stack>
            ))}
            <Stack direction='row' sx={{ gap: 1, justifyContent: 'space-between' }}>
              <Button variant='outlined' onClick={() => setNodesInventory((prev) => [...prev, { ssh_target: '', parallax_path: '' }])}>Add host</Button>
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
            Utilities for refreshing cluster metadata and navigating to operational tooling.
          </Typography>
          <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
            <Button variant='outlined' onClick={() => refreshModelList()}>Refresh model catalog</Button>
            <Button component={RouterLink} to='/join' variant='outlined'>Open reconnect flow</Button>
            <Button onClick={() => openSection('nodes')} variant='outlined'>Open nodes</Button>
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
