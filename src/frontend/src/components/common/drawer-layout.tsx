import { useEffect, useRef, useState, type FC, type PropsWithChildren } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Alert,
  Box,
  Button,
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
import { AlertDialog, useAlertDialog } from '../mui';
import { IconBrandGradient } from '../brand';
import {
  IconInfoCircle,
  IconLayoutSidebarLeftCollapse,
  IconLayoutSidebarLeftExpand,
  IconSettings,
} from '@tabler/icons-react';
import { ConversationHistory, JoinCommand, NodeList } from '../inputs';

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
    minHeight: '3.25rem',
    flex: 'none',
    marginTop: spacing(1),
    paddingBlock: spacing(0.75),
    paddingInline: spacing(4),
    overflow: 'hidden',
  };
});

const DrawerLayoutContainer = styled(Stack)(({ theme }) => {
  const { palette } = theme;
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

export const DrawerLayout: FC<PropsWithChildren<{ contentWidth?: 'default' | 'wide'; hideConversationHistory?: boolean }>> = ({
  children,
  contentWidth = 'default',
  hideConversationHistory = false,
}) => {
  const [{ type: hostType }] = useHost();
  const theme = useTheme();
  const narrowWindow = useMediaQuery(theme.breakpoints.down('lg'));

  const [
    {
      config: { modelInfo, modelName: configModelName, activeClusterId, clusterProfiles, modelInfoList },
      clusterInfo: { status: clusterStatus, needMoreNodes, topologyChangeAdvisory, modelName: clusterModelName },
      nodeInfoList,
    },
    { rebalanceTopology, setActiveCluster },
  ] = useCluster();

  const [dialogRecovery, { open: openRecovery, close: closeRecovery }] = useAlertDialog({
    color: 'error',
    titleIcon: <IconInfoCircle />,
    title: 'Reconnect your nodes',
    content: (
      <Stack sx={{ gap: 3 }}>
        <Typography variant='body2' color='text.secondary'>
          The cluster is not ready. Run the join command on the remaining nodes, then confirm they appear in the live node list below.
        </Typography>
        <Stack sx={{ gap: 1 }}>
          <Typography variant='body1'>Join command</Typography>
          <JoinCommand />
        </Stack>
        <Stack sx={{ gap: 1 }}>
          <Typography variant='body1'>Live node status</Typography>
          <NodeList />
        </Stack>
      </Stack>
    ),
    confirmLabel: 'Close',
  });

  const [dialogRebalancing, { open: openRebalancing }] = useAlertDialog({
    color: 'primary',
    title: '',
    content: (
      <>
        <Typography variant='body1'>Cluster rebalancing</Typography>
        <Typography variant='body2' color='text.disabled'>
          We have noticed one of your nodes has been disconnected. We are now rebalancing your inference requests onto working nodes. Please wait a few seconds for the cluster to rebalance itself.
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
      <Typography variant='body1'>
        Your selected model requires more nodes.
        {(!!modelInfo && modelInfo.vram > 0 && [
          ` You’ll need a `,
          <strong>{`minimum of ${modelInfo.vram} GB of total VRAM`}</strong>,
          ` to host this model.`,
        ]) || ''}
      </Typography>
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
          We have noticed that your scheduler has been disconnected (this would be the computer that ran the <strong>parallax run</strong> command). You would need to restart the scheduler, reconfigure the cluster, and your chat will be back up again!
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
      const timeoutId = setTimeout(() => openFailed(), 1000);
      return () => clearTimeout(timeoutId);
    }
  }, [clusterStatus, openFailed]);

  const [sidebarExpanded, setMenuOpen] = useState(!narrowWindow);
  const wideSidebarPreferenceRef = useRef(!narrowWindow);

  useEffect(() => {
    if (hideConversationHistory) {
      setMenuOpen(false);
      return;
    }
    if (narrowWindow) {
      wideSidebarPreferenceRef.current = sidebarExpanded;
      setMenuOpen(false);
      return;
    }
    setMenuOpen(wideSidebarPreferenceRef.current);
  }, [hideConversationHistory, narrowWindow, sidebarExpanded]);

  const activeNodes = nodeInfoList.filter((node) => node.status === 'available').length;
  const inactiveNodes = nodeInfoList.length - activeNodes;
  const [rebalancingTopology, setRebalancingTopology] = useState(false);

  const getClusterStatusCounts = (clusterId: string) => {
    if (clusterId === activeClusterId) {
      return { up: activeNodes, down: inactiveNodes, total: activeNodes + inactiveNodes };
    }
    const cluster = clusterProfiles.find((item) => item.id === clusterId);
    const plannedNodes = Math.max(0, Number(cluster?.init_nodes_num || 0));
    return { up: 0, down: plannedNodes, total: plannedNodes };
  };

  const renderClusterCountBadge = (counts: { up: number; down: number; total: number }) => {
    const fullyUp = counts.total > 0 && counts.up >= counts.total;
    const label = counts.total > 0 ? `${counts.up}/${counts.total} up` : '0 up';
    return (
      <Box
        component='span'
        sx={{
          px: 0.55,
          py: 0.125,
          borderRadius: 999,
          bgcolor: fullyUp ? 'rgba(46, 125, 50, 0.12)' : counts.up > 0 ? 'rgba(237, 108, 2, 0.12)' : 'rgba(158, 158, 158, 0.12)',
          color: fullyUp ? 'success.dark' : counts.up > 0 ? 'warning.dark' : 'text.secondary',
          fontSize: '0.68rem',
          fontWeight: 600,
          lineHeight: 1.2,
          whiteSpace: 'nowrap',
          flex: 'none',
        }}
      >
        {label}
      </Box>
    );
  };

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

  return (
    <DrawerLayoutRoot direction='row'>
      {!hideConversationHistory && (
        <DrawerLayoutSide
          sx={{
            width: sidebarExpanded ? '16.25rem' : '3.5rem',
            paddingInline: sidebarExpanded ? 2 : 2,
          }}
        >
        <Stack direction='row' sx={{ justifyContent: 'flex-end', alignItems: 'center', gap: 2 }}>
          {sidebarExpanded ? (
            <>
              <IconBrandGradient />
              <Box sx={{ flex: 1 }} />
              <Tooltip
                title='Collapse Sidebar'
                placement='right'
                slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
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
          ) : (
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
                slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
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
          )}
        </Stack>

        {sidebarExpanded && (
          <Stack sx={{ minHeight: 0, flex: 1, gap: 2, overflow: 'hidden' }}>
            {!hideConversationHistory && <ConversationHistory />}
            {hideConversationHistory && <Box sx={{ flex: 1 }} />}
            <Button
              component={RouterLink}
              to='/settings'
              color='inherit'
              variant='text'
              startIcon={<IconSettings size={18} />}
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
            slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
          >
            <IconButton
              component={RouterLink}
              to='/settings'
              sx={{
                mt: 'auto',
                color: 'text.secondary',
                borderRadius: '10px',
                '&:hover': { bgcolor: 'action.hover' },
              }}
            >
              <IconSettings size={18} />
            </IconButton>
          </Tooltip>
        )}
        </DrawerLayoutSide>
      )}

      <DrawerLayoutContainer>
        <DrawerLayoutHeader direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1.5 }}>
          {hideConversationHistory && (
            <Stack direction='row' sx={{ alignItems: 'center', gap: 1.25, flex: 'none', minWidth: 0 }}>
              <Box component={RouterLink} to='/chat' sx={{ display: 'inline-flex', alignItems: 'center', color: 'inherit' }}>
                <IconBrandGradient />
              </Box>
            </Stack>
          )}
          {!hideConversationHistory ? (
            <Stack sx={{ minWidth: 0, flex: 1 }}>
              <Typography variant='caption' color='text.secondary'>Cluster</Typography>
              {clusterProfiles.length > 0 ? (
              <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75, minWidth: 0 }}>
                <TextField
                  select
                  size='small'
                  value={activeClusterId || clusterProfiles[0]?.id || ''}
                  onChange={(event) => void setActiveCluster(String(event.target.value))}
                  sx={{
                    mt: 0.25,
                    minWidth: '18rem',
                    width: 'fit-content',
                    maxWidth: 'min(42rem, 100%)',
                    '& .MuiInputBase-root': {
                      fontSize: '0.875rem',
                      fontWeight: 600,
                      minHeight: '2.25rem',
                      pr: 1,
                    },
                    '& .MuiSelect-select': {
                      display: 'flex',
                      alignItems: 'center',
                      minHeight: 'unset',
                      py: 0.5,
                    },
                  }}
                  slotProps={{
                    select: {
                      displayEmpty: true,
                      autoWidth: true,
                      MenuProps: {
                        PaperProps: {
                          sx: {
                            minWidth: '24rem',
                            maxWidth: 'min(48rem, calc(100vw - 2rem))',
                          },
                        },
                      },
                      renderValue: (value) => {
                        const selectedCluster = clusterProfiles.find((item) => item.id === value) || clusterProfiles[0];
                        const label = selectedCluster?.name || 'Select cluster';
                        const model = clusterModelName || configModelName;
                        const counts = getClusterStatusCounts(String(selectedCluster?.id || value || ''));
                        return (
                          <Stack direction='row' sx={{ alignItems: 'center', gap: 0.75, minWidth: 0 }}>
                            <Typography variant='inherit' noWrap sx={{ fontWeight: 600 }}>
                              {model ? `${label} · ${model}` : label}
                            </Typography>
                            {renderClusterCountBadge(counts)}
                          </Stack>
                        );
                      },
                    },
                  }}
                >
                  {clusterProfiles.map((cluster) => {
                    const counts = getClusterStatusCounts(cluster.id);
                    const clusterModel = modelInfoList.find((item) => item.name === cluster.model_name);
                    const isUnavailable = counts.total === 0 || counts.up === 0;
                    return (
                      <MenuItem
                        key={cluster.id}
                        value={cluster.id}
                        sx={{
                          opacity: isUnavailable ? 0.55 : 1,
                        }}
                      >
                        <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1, width: '100%' }}>
                          <Stack sx={{ minWidth: 0, flex: 1, gap: 0.125 }}>
                            <Typography variant='body2' sx={{ fontWeight: 600, color: isUnavailable ? 'text.secondary' : 'text.primary' }} noWrap>
                              {cluster.name}
                            </Typography>
                            <Typography variant='caption' color='text.secondary' noWrap>
                              {cluster.model_name || 'No model selected'}
                            </Typography>
                          </Stack>
                          <Stack direction='row' sx={{ gap: 0.5, alignItems: 'center', flex: 'none' }}>
                            {clusterModel && clusterModel.vram > 0 && (
                              <Box
                                component='span'
                                sx={{
                                  px: 0.55,
                                  py: 0.125,
                                  borderRadius: 999,
                                  bgcolor: 'rgba(25, 118, 210, 0.1)',
                                  color: 'info.dark',
                                  fontSize: '0.68rem',
                                  fontWeight: 600,
                                  lineHeight: 1.2,
                                  whiteSpace: 'nowrap',
                                }}
                              >
                                {clusterModel.vram} GB
                              </Box>
                            )}
                            {renderClusterCountBadge(counts)}
                          </Stack>
                        </Stack>
                      </MenuItem>
                    );
                  })}
                </TextField>
                <Tooltip
                  title='Open cluster settings'
                  placement='bottom'
                  slotProps={{ tooltip: { sx: { bgcolor: 'primary.main', color: 'common.white' } } }}
                >
                  <IconButton
                    component={RouterLink}
                    to='/settings/cluster'
                    size='small'
                    sx={{
                      mt: 0.25,
                      color: 'text.secondary',
                      borderRadius: 1.5,
                      '&:hover': { bgcolor: 'action.hover', color: 'text.primary' },
                    }}
                  >
                    <IconSettings size={16} />
                  </IconButton>
                </Tooltip>
              </Stack>
              ) : (
                <Typography variant='body2' sx={{ fontWeight: 600 }} noWrap>
                  {clusterProfiles.find((item) => item.id === activeClusterId)?.name || clusterModelName || configModelName || 'No cluster selected'}
                </Typography>
              )}
            </Stack>
          ) : (
            <Box sx={{ flex: 1 }} />
          )}
        </DrawerLayoutHeader>

        <DrawerLayoutContent contentWidth={contentWidth}>
          {hostType === 'cluster' && clusterStatus === 'waiting' && (
            <Alert
              severity='error'
              action={
                <Button onClick={openRecovery} color='inherit' size='small'>
                  Open recovery
                </Button>
              }
            >
              Some nodes are not connected yet. Reconnect your nodes to finish bringing the cluster online.
            </Alert>
          )}
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

      {dialogRebalancing}
      {dialogFailed}
      {dialogNeedMoreNodes}
      {dialogRecovery}
    </DrawerLayoutRoot>
  );
};
