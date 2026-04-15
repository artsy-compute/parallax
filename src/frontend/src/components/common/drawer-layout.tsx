import { useEffect, useRef, useState, type FC, type PropsWithChildren } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  IconButton,
  MenuItem,
  Stack,
  styled,
  TextField,
  Tooltip,
  Typography,
  useMediaQuery,
  useTheme,
} from '@mui/material';
import { useCluster, useHost } from '../../services';
import { addCustomModel, deleteCustomModel, getCustomModelList, searchCustomModels, type CustomModelRecord, type CustomModelSearchResult } from '../../services/api';
import { AlertDialog, useAlertDialog } from '../mui';
import { IconBrandGradient } from '../brand';
import {
  IconCheck,
  IconCirclePlus,
  IconLoader,
  IconInfoCircle,
  IconLayoutSidebarLeftCollapse,
  IconLayoutSidebarLeftExpand,
  IconTrash,
  IconSettings,
} from '@tabler/icons-react';
import { ConversationHistory, JoinCommand, ModelSelect, NodeList } from '../inputs';

const DrawerLayoutRoot = styled(Stack)(({ theme }) => {
  const { spacing } = theme;
  return {
    width: '100%',
    height: '100%',
    justifyContent: 'flex-start',
    alignItems: 'stretch',
    overflow: 'hidden',
  };
});

const DrawerLayoutSide = styled(Stack)(({ theme }) => {
  const { palette, spacing } = theme;
  return {
    height: '100%',
    paddingBlock: spacing(2),
    paddingInline: spacing(2),
    gap: spacing(3),
    overflow: 'hidden',
    transition: 'width 0.3s ease-in-out',
    backgroundColor: palette.grey[200],
  };
});

const DrawerLayoutHeader = styled(Stack)(({ theme }) => {
  const { spacing } = theme;
  return {
    width: '100%',
    height: '2.5rem',
    flex: 'none',
    marginTop: spacing(1),
    paddingBlock: spacing(1),
    paddingInline: spacing(4),
    overflow: 'hidden',
  };
});

const DrawerLayoutContainer = styled(Stack)(({ theme }) => {
  const { palette, spacing } = theme;
  return {
    flex: 1,
    alignItems: 'center',
    overflow: 'hidden',
    backgroundColor: palette.grey[100],
  };
});

const DrawerLayoutContent = styled(Stack, {
  shouldForwardProp: (prop) => prop !== 'contentWidth',
})<{ contentWidth?: 'default' | 'wide' }>(({ theme, contentWidth = 'default' }) => {
  const { spacing } = theme;
  return {
    width: contentWidth === 'wide' ? '72rem' : '48.75rem',
    maxWidth: '100%',
    height: '100%',
    gap: spacing(2),
    paddingBlock: spacing(1),
    paddingInline: spacing(4),
    overflow: 'hidden',
  };
});

export const DrawerLayout: FC<PropsWithChildren<{ contentWidth?: 'default' | 'wide' }>> = ({ children, contentWidth = 'default' }) => {
  const [{ type: hostType }] = useHost();
  const theme = useTheme();
  const narrowWindow = useMediaQuery(theme.breakpoints.down('lg'));

  const [
    {
      config: { modelInfo, modelName: configModelName },
      clusterInfo: { status: clusterStatus, needMoreNodes, topologyChangeAdvisory, modelName: clusterModelName },
      nodeInfoList,
    },
    { rebalanceTopology, refreshModelList },
  ] = useCluster();

  const [dialogWaiting, { open: openWaiting }] = useAlertDialog({
    color: 'primary',
    titleIcon: <IconInfoCircle />,
    title: 'Reconnect your nodes',
    content: (
      <Stack sx={{ gap: 7 }}>
        <Stack sx={{ gap: 1 }}>
          <Typography variant='body1'>Run join command on your new Node</Typography>
          <JoinCommand />
        </Stack>
        <Stack sx={{ gap: 1 }}>
          <Typography variant='body1'>Check your live node status</Typography>
          <Typography variant='body2' color='text.disabled'>
            After you successfully start the server on the nodes, you should see them show up on the
            below dashboard.
          </Typography>
          <NodeList />
        </Stack>
      </Stack>
    ),
    confirmLabel: 'Finish',
  });
  useEffect(() => {
    if (hostType === 'cluster' && clusterStatus === 'waiting') {
      openWaiting();
    }
  }, [clusterStatus, openWaiting]);

  const [dialogRebalancing, { open: openRebalancing }] = useAlertDialog({
    color: 'primary',
    title: '',
    content: (
      <>
        <Typography variant='body1'>Cluster rebalancing</Typography>
        <Typography variant='body2' color='text.disabled'>
          We have noticed one of your nodes has been disconnected. We are now rebalancing your
          inference requests onto working nodes. Please wait a few seconds for the cluster to
          rebalance itself.
        </Typography>
        <NodeList variant='menu' />
      </>
    ),
    confirmLabel: 'Finish',
  });
  useEffect(() => {
    if (clusterStatus === 'rebalancing') {
      openRebalancing();
    }
  }, [clusterStatus, openRebalancing]);

  const [dialogNeedMoreNodes, { open: openDialogNeedMoreNodes }] = useAlertDialog({
    color: 'primary',
    title: '',
    content: (
      <>
        <Typography variant='body1'>
          Your selected model requires more nodes.
          {(!!modelInfo
            && modelInfo.vram > 0 && [
              ` You’ll need a `,
              <strong>{`minimum of ${modelInfo.vram} GB of total VRAM`}</strong>,
              ` to host this model.`,
            ])
            || ''}
        </Typography>
      </>
    ),
    confirmLabel: 'Finish',
  });
  useEffect(() => {
    if (needMoreNodes) {
      openDialogNeedMoreNodes();
    }
  }, [needMoreNodes, openDialogNeedMoreNodes]);

  const [dialogFailed, { open: openFailed }] = useAlertDialog({
    color: 'primary',
    title: '',
    content: (
      <>
        <Typography variant='body1'>Scheduler restart</Typography>
        <Typography variant='body2' color='text.disabled'>
          We have noticed that your scheduler has been disconnected (this would be the computer that
          ran the <strong>parallax run</strong> command). You would need to restart the scheduler,
          reconfigure the cluster, and your chat will be back up again!
        </Typography>
      </>
    ),
    confirmLabel: 'Finish',
  });
  useEffect(() => {
    if (clusterStatus === 'failed') {
      openFailed();
      return;
    }
    if (clusterStatus === 'idle') {
      // Delay trigger, due to the cluster init status is 'idle' before connecting to the scheduler.
      const timeoutId = setTimeout(() => openFailed(), 1000);
      return () => clearTimeout(timeoutId);
    }
  }, [clusterStatus, openFailed]);

  const [sidebarExpanded, setMenuOpen] = useState(!narrowWindow);
  const wideSidebarPreferenceRef = useRef(!narrowWindow);

  useEffect(() => {
    if (narrowWindow) {
      wideSidebarPreferenceRef.current = sidebarExpanded;
      setMenuOpen(false);
      return;
    }
    setMenuOpen(wideSidebarPreferenceRef.current);
  }, [narrowWindow]);

  const [clusterSettingsOpen, setClusterSettingsOpen] = useState(false);
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

  const activeNodes = nodeInfoList.filter((node) => node.status === 'available').length;
  const inactiveNodes = nodeInfoList.length - activeNodes;

  const [rebalancingTopology, setRebalancingTopology] = useState(false);

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
    if (clusterSettingsOpen && hostType !== 'node') {
      loadCustomModels();
    }
  }, [clusterSettingsOpen, hostType]);

  useEffect(() => {
    if (!clusterSettingsOpen || hostType === 'node' || customModelSourceType !== 'huggingface') {
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
  }, [clusterSettingsOpen, hostType, customModelSourceType, customModelSourceValue]);

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

  return (
    <DrawerLayoutRoot direction='row'>
      <DrawerLayoutSide
        sx={{
          width: sidebarExpanded ? '16.25rem' : '3.5rem',
          paddingInline: sidebarExpanded ? 2 : 2,
        }}
      >
        <Stack direction='row' sx={{ justifyContent: 'flex-end', alignItems: 'center', gap: 2 }}>
          {sidebarExpanded ?
            <>
              <IconBrandGradient />
              <Box sx={{ flex: 1 }} />
              <Tooltip
                title='Collapse Sidebar'
                placement='right'
                slotProps={{
                  tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } },
                }}
              >
                <IconButton
                  size='em'
                  sx={{
                    fontSize: '1.5rem',
                    borderRadius: '8px',
                    color: '#808080FF',
                    '&:hover': { bgcolor: 'action.hover' },
                  }}
                  onClick={() => {
                    setMenuOpen((prev) => {
                      const next = !prev;
                      if (!narrowWindow) {
                        wideSidebarPreferenceRef.current = next;
                      }
                      return next;
                    });
                  }}
                >
                  <IconLayoutSidebarLeftCollapse />
                </IconButton>
              </Tooltip>
            </>
          : <>
              <Box
                sx={{
                  position: 'relative',
                  width: 28,
                  height: 28,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  '&:hover .logo': { opacity: 0 },
                  '&:hover .toggle': { opacity: 1, pointerEvents: 'auto', transform: 'scale(1)' },
                }}
              >
                <Box
                  className='logo'
                  sx={{
                    position: 'absolute',
                    inset: 0,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    transition: 'opacity .15s ease',
                    opacity: 1,
                  }}
                >
                  <IconBrandGradient />
                </Box>

                <Tooltip
                  title='Expand Sidebar'
                  placement='right'
                  slotProps={{
                    tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } },
                  }}
                >
                  <IconButton
                    className='toggle'
                    size='em'
                    sx={{
                      position: 'absolute',
                      opacity: 0,
                      pointerEvents: 'none',
                      fontSize: '1.5rem',
                      transition: 'opacity .15s ease, transform .15s ease',
                      '&:hover': { bgcolor: 'action.hover' },
                    }}
                    aria-label='Expand Sidebar'
                    onClick={() => {
                      setMenuOpen((prev) => {
                        const next = !prev;
                        if (!narrowWindow) {
                          wideSidebarPreferenceRef.current = next;
                        }
                        return next;
                      });
                    }}
                  >
                    <IconLayoutSidebarLeftExpand />
                  </IconButton>
                </Tooltip>
              </Box>
            </>
          }
        </Stack>
        {sidebarExpanded && (
          <Stack sx={{ minHeight: 0, flex: 1, gap: 2, overflow: 'hidden' }}>
            <ConversationHistory />
            <Button
              color='inherit'
              variant='text'
              startIcon={<IconSettings size={18} />}
              onClick={() => setClusterSettingsOpen(true)}
              sx={{
                mt: 'auto',
                justifyContent: 'flex-start',
                color: 'text.secondary',
                borderRadius: 2,
                px: 1,
                py: 0.75,
                '&:hover': { bgcolor: 'rgba(255,255,255,0.5)' },
              }}
            >
              Settings
            </Button>
          </Stack>
        )}
        {!sidebarExpanded && (
          <Tooltip
            title='Settings'
            placement='right'
            slotProps={{
              tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } },
            }}
          >
            <IconButton
              sx={{
                mt: 'auto',
                color: 'text.secondary',
                borderRadius: '10px',
                '&:hover': { bgcolor: 'action.hover' },
              }}
              onClick={() => setClusterSettingsOpen(true)}
            >
              <IconSettings size={18} />
            </IconButton>
          </Tooltip>
        )}
      </DrawerLayoutSide>
      <DrawerLayoutContainer>
        <DrawerLayoutHeader direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1.5 }}>
          <Stack sx={{ minWidth: 0, flex: 1 }}>
            <Typography variant='caption' color='text.secondary'>Cluster model</Typography>
            <Typography variant='body2' sx={{ fontWeight: 600 }} noWrap>
              {clusterModelName || configModelName || 'No model selected'}
            </Typography>
          </Stack>
          <Stack direction='row' sx={{ gap: 0.5, flexWrap: 'wrap', alignItems: 'center', justifyContent: 'flex-end', flex: 'none' }}>
            <Box
              component='span'
              sx={{
                px: 0.75,
                py: 0.25,
                borderRadius: 999,
                bgcolor: 'rgba(46, 125, 50, 0.12)',
                color: 'success.dark',
                fontSize: '0.75rem',
                fontWeight: 600,
                lineHeight: 1.2,
                whiteSpace: 'nowrap',
              }}
            >
              {activeNodes} up
            </Box>
            {inactiveNodes > 0 && (
              <Box
                component='span'
                sx={{
                  px: 0.75,
                  py: 0.25,
                  borderRadius: 999,
                  bgcolor: 'rgba(237, 108, 2, 0.12)',
                  color: 'warning.dark',
                  fontSize: '0.75rem',
                  fontWeight: 600,
                  lineHeight: 1.2,
                  whiteSpace: 'nowrap',
                }}
              >
                {inactiveNodes} down
              </Box>
            )}
          </Stack>
        </DrawerLayoutHeader>
        <DrawerLayoutContent contentWidth={contentWidth}>
          {topologyChangeAdvisory.show && (
            <Stack
              direction='row'
              sx={{
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 2,
                px: 2,
                py: 1.5,
                borderRadius: 2,
                border: '1px solid',
                borderColor: 'warning.light',
                backgroundColor: 'rgba(255, 244, 229, 0.7)',
              }}
            >
              <Stack sx={{ minWidth: 0 }}>
                <Typography variant='body2' sx={{ fontWeight: 600, color: 'warning.dark' }}>
                  Cluster topology changed
                </Typography>
                <Typography variant='caption' color='text.secondary'>
                  {topologyChangeAdvisory.message}
                </Typography>
              </Stack>
              {topologyChangeAdvisory.canRebalance && (
                <Button
                  size='small'
                  variant='outlined'
                  color='warning'
                  disabled={rebalancingTopology}
                  onClick={onClickRebalanceTopology}
                  sx={{ flex: 'none', whiteSpace: 'nowrap' }}
                >
                  {rebalancingTopology ? 'Rebalancing...' : 'Rebalance now'}
                </Button>
              )}
            </Stack>
          )}
          {children}
        </DrawerLayoutContent>
      </DrawerLayoutContainer>
      <AlertDialog
        open={clusterSettingsOpen}
        onClose={() => setClusterSettingsOpen(false)}
        color='primary'
        titleIcon={<IconSettings />}
        title='Cluster Settings'
        fullWidth
        maxWidth='md'
        content={
          <Stack sx={{ gap: 4.5 }}>
            <Stack sx={{ gap: 1 }}>
              <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
                <Typography variant='body1'>Model</Typography>
                <Tooltip
                  title='Choose the model hosted by the scheduler and review its memory requirement.'
                  placement='right'
                  slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
                >
                  <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                    <IconInfoCircle size={16} />
                  </IconButton>
                </Tooltip>
              </Stack>
              <ModelSelect autoCommit />
            </Stack>
            <Stack sx={{ gap: 1.25 }}>
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
                      if (customModelSearchResults.length > 0) {
                        setCustomModelSearchOpen(true);
                      }
                    }}
                    onClose={() => setCustomModelSearchOpen(false)}
                    filterOptions={(options) => options}
                    getOptionLabel={(option) => typeof option === 'string' ? option : option.source_value}
                    inputValue={customModelSourceValue}
                    onInputChange={(_, value, reason) => {
                      if (reason !== 'reset' && !customModelSearchLoading) {
                        setCustomModelSourceValue(value);
                        if (!value.trim()) {
                          setCustomModelSearchOpen(false);
                        }
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
                          <Typography variant='body2' sx={{ fontWeight: 600 }}>
                            {option.display_name}
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
                <Button
                  variant='contained'
                  onClick={onAddCustomModel}
                  disabled={customModelSubmitting || !customModelSourceValue.trim()}
                  sx={{ alignSelf: { xs: 'stretch', sm: 'center' }, whiteSpace: 'nowrap' }}
                >
                  {customModelSubmitting ? 'Adding...' : 'Add model'}
                </Button>
              </Stack>
              <Stack sx={{ gap: 1, maxHeight: '16rem', overflow: 'auto' }}>
                {customModelLoading && (
                  <Typography variant='body2' color='text.secondary'>Loading custom models…</Typography>
                )}
                {!customModelLoading && customModels.length === 0 && (
                  <Typography variant='body2' color='text.secondary'>No custom models added yet.</Typography>
                )}
                {customModels.map((model) => (
                  <Stack
                    key={model.id}
                    direction='row'
                    sx={{
                      alignItems: 'center',
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
                    <Stack sx={{ minWidth: 0, gap: 0.25 }}>
                      <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75, flexWrap: 'wrap' }}>
                        <Typography variant='body2' sx={{ fontWeight: 600 }}>
                          {model.display_name || model.source_value}
                        </Typography>
                        {renderValidationChip(model.validation_status)}
                        <Chip size='small' variant='outlined' label={model.source_type === 'huggingface' ? 'HF' : 'Local'} />
                      </Stack>
                      <Typography variant='caption' color='text.secondary' sx={{ wordBreak: 'break-all' }}>
                        {model.source_value}
                      </Typography>
                      {model.validation_message && (
                        <Typography variant='caption' color='text.secondary'>
                          {model.validation_message}
                        </Typography>
                      )}
                    </Stack>
                    <IconButton
                      size='small'
                      color='error'
                      disabled={customModelDeletingId === model.id}
                      onClick={() => onDeleteCustomModel(model.id)}
                    >
                      <IconTrash size={16} />
                    </IconButton>
                  </Stack>
                ))}
              </Stack>
            </Stack>
            <Stack sx={{ gap: 1 }}>
              <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
                <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
                  <Typography variant='body1'>Live nodes</Typography>
                  <Tooltip
                    title='Check current node status, verify the cluster is healthy, or open the full node management page.'
                    placement='right'
                    slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
                  >
                    <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                      <IconInfoCircle size={16} />
                    </IconButton>
                  </Tooltip>
                </Stack>
                <Button
                  component={RouterLink}
                  to='/nodes'
                  variant='text'
                  size='small'
                  onClick={() => setClusterSettingsOpen(false)}
                  sx={{ alignSelf: 'center', minWidth: 0, px: 0.5 }}
                >
                  Node Management
                </Button>
              </Stack>
              <NodeList sx={{ maxHeight: '18rem' }} />
            </Stack>
            <Stack sx={{ gap: 1 }}>
              <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75 }}>
                <Typography variant='body1'>Add nodes</Typography>
                <Tooltip
                  title='Start new nodes with this command and watch them appear above.'
                  placement='right'
                  slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
                >
                  <IconButton size='small' sx={{ color: 'text.secondary', p: 0.25 }}>
                    <IconInfoCircle size={16} />
                  </IconButton>
                </Tooltip>
              </Stack>
              <JoinCommand />
            </Stack>
          </Stack>
        }
        confirmLabel='Close'
      />
      {dialogWaiting}
      {dialogRebalancing}
      {dialogFailed}
      {dialogNeedMoreNodes}
    </DrawerLayoutRoot>
  );
};
