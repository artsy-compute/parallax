import { useEffect, useRef, useState, type FC, type PropsWithChildren } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import {
  Box,
  Button,
  Divider,
  IconButton,
  Stack,
  styled,
  Tooltip,
  Typography,
  useMediaQuery,
  useTheme,
} from '@mui/material';
import { useCluster, useHost } from '../../services';
import { AlertDialog, useAlertDialog } from '../mui';
import { IconBrandGradient } from '../brand';
import {
  IconCirclePlus,
  IconInfoCircle,
  IconLayoutSidebarLeftCollapse,
  IconLayoutSidebarLeftExpand,
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
      config: { modelInfo },
      clusterInfo: { status: clusterStatus, needMoreNodes, topologyChangeAdvisory },
    },
    { rebalanceTopology },
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

  const [rebalancingTopology, setRebalancingTopology] = useState(false);

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
        <DrawerLayoutHeader direction='row'>
          <ModelSelect
            variant='text'
            autoCommit
            showNodeCounts
            onNodeCountsClick={() => setClusterSettingsOpen(true)}
          />
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
        content={
          <Stack sx={{ gap: 4.5 }}>
            <Stack sx={{ gap: 1 }}>
              <Typography variant='body1'>Model</Typography>
              <Typography variant='body2' color='text.disabled'>
                Choose the model hosted by the scheduler and review its memory requirement.
              </Typography>
              <ModelSelect autoCommit />
            </Stack>
            <Stack sx={{ gap: 1 }}>
              <Typography variant='body1'>Live nodes</Typography>
              <Typography variant='body2' color='text.disabled'>
                Check current node status and verify the cluster is healthy.
              </Typography>
              <NodeList sx={{ maxHeight: '18rem' }} />
            </Stack>
            <Stack sx={{ gap: 1 }}>
              <Typography variant='body1'>Add nodes</Typography>
              <Typography variant='body2' color='text.disabled'>
                Start new nodes with this command and watch them appear above.
              </Typography>
              <JoinCommand />
            </Stack>
            <Stack sx={{ gap: 1 }}>
              <Typography variant='body1'>Node management</Typography>
              <Typography variant='body2' color='text.disabled'>
                Open the dedicated node management page for configured hosts, ping, and runtime overview.
              </Typography>
              <Button
                component={RouterLink}
                to='/nodes'
                variant='outlined'
                onClick={() => setClusterSettingsOpen(false)}
                sx={{ alignSelf: 'flex-start' }}
              >
                Open Node Management
              </Button>
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
