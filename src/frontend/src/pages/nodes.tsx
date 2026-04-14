import { useEffect, useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { Alert, Box, Button, Chip, Dialog, DialogContent, DialogTitle, IconButton, Paper, Stack, Tooltip, Typography } from '@mui/material';
import {
  IconArrowLeft,
  IconBrandApple,
  IconCpu,
  IconFileText,
  IconPlayerPause,
  IconPlayerPlay,
  IconPlugConnected,
  IconRefresh,
  IconRotateClockwise2,
} from '@tabler/icons-react';
import { DrawerLayout } from '../components/common';
import { getNodeLogs, getNodesOverview, pingNodeHost, type NodeOverviewHost, type NodesOverview } from '../services/api';

const SummaryCard = ({ label, value }: { label: string; value: number }) => (
  <Paper variant='outlined' sx={{ p: 2, borderRadius: 2, minWidth: 0, flex: 1 }}>
    <Typography variant='caption' color='text.secondary'>{label}</Typography>
    <Typography variant='h6'>{value}</Typography>
  </Paper>
);

const StatusChip = ({ label, color = 'default' }: { label: string; color?: 'default' | 'success' | 'warning' | 'info' }) => (
  <Chip size='small' label={label} color={color === 'default' ? undefined : color} variant={color === 'default' ? 'outlined' : 'filled'} />
);


const HardwareIcon = ({ gpuName }: { gpuName?: string | null }) => {
  const name = String(gpuName || '').toLowerCase();
  if (name.includes('apple') || name.includes('m1') || name.includes('m2') || name.includes('m3') || name.includes('m4')) {
    return <IconBrandApple size={18} />;
  }
  if (name.includes('nvidia') || name.includes('rtx') || name.includes('gtx') || name.includes('a100') || name.includes('h100')) {
    return <Typography component='span' sx={{ fontSize: '0.72rem', fontWeight: 700, lineHeight: 1 }}>NV</Typography>;
  }
  if (name.includes('amd') || name.includes('radeon') || name.includes('instinct')) {
    return <Typography component='span' sx={{ fontSize: '0.72rem', fontWeight: 700, lineHeight: 1 }}>AMD</Typography>;
  }
  return <IconCpu size={18} />;
};


const getLogLineStyle = (line: string) => {
  const upper = line.toUpperCase();
  if (upper.includes('[ERROR') || upper.includes(' ERROR ') || upper.startsWith('ERROR')) {
    return { color: 'error.dark', bgcolor: 'rgba(211, 47, 47, 0.08)' };
  }
  if (upper.includes('[WARNING') || upper.includes(' WARNING ') || upper.startsWith('WARNING') || upper.includes('[WARN')) {
    return { color: 'warning.dark', bgcolor: 'rgba(237, 108, 2, 0.08)' };
  }
  if (upper.includes('[INFO') || upper.includes(' INFO ') || upper.startsWith('INFO')) {
    return { color: 'info.dark', bgcolor: 'rgba(2, 136, 209, 0.06)' };
  }
  if (upper.includes('[DEBUG') || upper.includes(' DEBUG ') || upper.startsWith('DEBUG')) {
    return { color: 'text.secondary', bgcolor: 'rgba(158, 158, 158, 0.05)' };
  }
  return { color: 'text.primary', bgcolor: 'transparent' };
};

const LogContent = ({ content }: { content: string }) => {
  const lines = String(content || '').replace(/\r/g, '').split('\n');
  return (
    <Box sx={{ fontFamily: 'monospace', fontSize: '0.78rem', lineHeight: 1.45 }}>
      {lines.map((line, index) => {
        const style = getLogLineStyle(line);
        return (
          <Box
            key={`${index}-${line.slice(0, 24)}`}
            sx={{
              px: 0.75,
              py: 0.125,
              borderRadius: 0.75,
              color: style.color,
              bgcolor: style.bgcolor,
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
            }}
          >
            {line || ' '}
          </Box>
        );
      })}
    </Box>
  );
};

const HostRow = ({ host, onPing, onLogs, pingState }: { host: NodeOverviewHost; onPing: (sshTarget: string) => Promise<void>; onLogs: (sshTarget: string) => Promise<void>; pingState?: string }) => {
  const runtime = host.runtime || {};
  const layerText = typeof runtime.start_layer === 'number' || typeof runtime.end_layer === 'number' || typeof runtime.total_layers === 'number'
    ? `[${typeof runtime.start_layer === 'number' ? runtime.start_layer : '?'}, ${typeof runtime.end_layer === 'number' ? runtime.end_layer : '?'})${typeof runtime.total_layers === 'number' ? ` of ${runtime.total_layers}` : ''}`
    : 'Not assigned';
  const details = [
    { label: 'SSH', value: host.ssh_target || 'Unavailable' },
    { label: 'Host', value: runtime.hostname || host.hostname_hint || 'Unknown' },
    { label: 'Layers', value: layerText },
    { label: 'GPU', value: `${runtime.gpu_num ? `${runtime.gpu_num}x ` : ''}${runtime.gpu_name || 'Unknown'}${runtime.gpu_memory ? ` ${runtime.gpu_memory}GB` : ''}` },
  ];
  return (
    <Paper
      variant='outlined'
      sx={{
        p: 1.5,
        borderRadius: 2,
        transition: 'background-color 140ms ease, border-color 140ms ease, box-shadow 140ms ease, transform 140ms ease',
        '&:hover': {
          bgcolor: 'rgba(255,255,255,0.72)',
          borderColor: 'primary.light',
          boxShadow: '0 6px 18px rgba(15, 23, 42, 0.08)',
          transform: 'translateY(-1px)',
        },
      }}
    >
      <Stack sx={{ gap: 1 }}>
        <Stack direction={{ xs: 'column', lg: 'row' }} sx={{ justifyContent: 'space-between', gap: 1.25, alignItems: { lg: 'flex-start' } }}>
          <Stack sx={{ gap: 0.75, minWidth: 0, flex: 1 }}>
            <Stack direction='row' sx={{ gap: 0.75, flexWrap: 'wrap', alignItems: 'center' }}>
              <Box
                sx={{
                  width: 24,
                  height: 24,
                  borderRadius: 1.25,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  bgcolor: 'rgba(255,255,255,0.6)',
                  color: 'text.secondary',
                  border: '1px solid',
                  borderColor: 'divider',
                  flex: 'none',
                }}
              >
                <HardwareIcon gpuName={runtime.gpu_name} />
              </Box>
              <Typography variant='body1' sx={{ fontWeight: 600 }}>{host.display_name}</Typography>
              <StatusChip label={host.joined ? 'Joined' : 'Not joined'} color={host.joined ? 'success' : 'warning'} />
              <StatusChip label={host.inventory_source === 'configured' ? 'Configured host' : 'Live node'} color={host.inventory_source === 'configured' ? 'info' : 'default'} />
              {runtime.status && <StatusChip label={`Runtime: ${runtime.status}`} color={runtime.status === 'available' ? 'success' : 'default'} />}
            </Stack>
            <Box
              sx={{
                display: 'grid',
                gridTemplateColumns: { xs: '1fr', md: 'repeat(2, minmax(0, 1fr))' },
                columnGap: 1.5,
                rowGap: 0.35,
                minWidth: 0,
              }}
            >
              {details.map((item) => (
                <Typography key={item.label} variant='caption' color='text.secondary' sx={{ minWidth: 0 }}>
                  <Box component='span' sx={{ color: 'text.disabled' }}>{item.label}: </Box>
                  <Box component='span' sx={{ color: 'text.primary' }}>{item.value}</Box>
                </Typography>
              ))}
            </Box>
          </Stack>
          <Stack direction='row' sx={{ gap: 0.25, flexWrap: 'wrap', alignItems: 'center', flex: 'none' }}>
            <Tooltip title={pingState === 'running' ? 'Pinging node' : 'Ping node over SSH'}>
              <span>
                <IconButton
                  size='small'
                  color='primary'
                  disabled={!host.actions.can_ping || pingState === 'running'}
                  onClick={() => onPing(host.ssh_target)}
                >
                  <IconPlugConnected size={16} />
                </IconButton>
              </span>
            </Tooltip>
            <Tooltip title='Start node (not wired yet)'>
              <span><IconButton size='small' disabled><IconPlayerPlay size={16} /></IconButton></span>
            </Tooltip>
            <Tooltip title='Stop node (not wired yet)'>
              <span><IconButton size='small' disabled><IconPlayerPause size={16} /></IconButton></span>
            </Tooltip>
            <Tooltip title='Restart node (not wired yet)'>
              <span><IconButton size='small' disabled><IconRotateClockwise2 size={16} /></IconButton></span>
            </Tooltip>
            <Tooltip title='Open remote logs'>
              <span><IconButton size='small' disabled={!host.ssh_target} onClick={() => onLogs(host.ssh_target)}><IconFileText size={16} /></IconButton></span>
            </Tooltip>
          </Stack>
        </Stack>
        {pingState && pingState !== 'running' && (
          <Alert severity={pingState.startsWith('ok:') ? 'success' : 'warning'} sx={{ '& .MuiAlert-message': { fontSize: '0.8125rem' }, py: 0 }}>
            {pingState.startsWith('ok:') ? pingState.slice(3) : pingState.startsWith('err:') ? pingState.slice(4) : pingState}
          </Alert>
        )}
      </Stack>
    </Paper>
  );
};

export default function PageNodes() {
  const [overview, setOverview] = useState<NodesOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [pingStates, setPingStates] = useState<Record<string, string>>({});
  const [logsDialog, setLogsDialog] = useState<{ open: boolean; host: string; source: string; content: string; loading: boolean; error: string }>({
    open: false,
    host: '',
    source: '',
    content: '',
    loading: false,
    error: '',
  });

  const loadOverview = async () => {
    try {
      setLoading(true);
      setError('');
      const next = await getNodesOverview();
      setOverview(next);
    } catch (err: any) {
      setError(err?.message || 'Failed to load node overview');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadOverview();
    const timer = setInterval(loadOverview, 10000);
    return () => clearInterval(timer);
  }, []);

  const onPing = async (sshTarget: string) => {
    if (!sshTarget) return;
    setPingStates((prev) => ({ ...prev, [sshTarget]: 'running' }));
    try {
      const result = await pingNodeHost(sshTarget);
      const message = result.ok
        ? `ok:${result.message}${typeof result.latency_ms === 'number' ? ` (${result.latency_ms} ms)` : ''}`
        : `err:${result.message}`;
      setPingStates((prev) => ({ ...prev, [sshTarget]: message }));
    } catch (err: any) {
      setPingStates((prev) => ({ ...prev, [sshTarget]: `err:${err?.message || 'Ping failed'}` }));
    }
  };

  const onLogs = async (sshTarget: string) => {
    if (!sshTarget) return;
    setLogsDialog({ open: true, host: sshTarget, source: '', content: '', loading: true, error: '' });
    try {
      const result = await getNodeLogs(sshTarget, 200);
      setLogsDialog({
        open: true,
        host: sshTarget,
        source: result.source || '',
        content: result.content || '',
        loading: false,
        error: result.ok ? '' : (result.message || 'Failed to fetch logs'),
      });
    } catch (err: any) {
      setLogsDialog({
        open: true,
        host: sshTarget,
        source: '',
        content: '',
        loading: false,
        error: err?.message || 'Failed to fetch logs',
      });
    }
  };

  const summary = overview?.summary || { configured_hosts: 0, joined_hosts: 0, unjoined_configured_hosts: 0, live_only_hosts: 0 };
  const hosts = overview?.hosts || [];

  return (
    <DrawerLayout contentWidth='wide'>
      <Stack sx={{ gap: 2, minHeight: 0, overflow: 'auto', pb: 2 }}>
        <Stack direction={{ xs: 'column', sm: 'row' }} sx={{ justifyContent: 'space-between', gap: 1, alignItems: { sm: 'center' } }}>
          <Stack sx={{ gap: 0.5 }}>
            <Typography variant='h5'>Node Management</Typography>
            <Typography variant='body2' color='text.secondary'>Configured hosts, live nodes, SSH ping, and logs.</Typography>
          </Stack>
          <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
            <Button component={RouterLink} to='/chat' variant='text' startIcon={<IconArrowLeft size={16} />}>Back to chat</Button>
            <Button onClick={loadOverview} variant='outlined' startIcon={<IconRefresh size={16} />}>Refresh</Button>
          </Stack>
        </Stack>

        <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1.5 }}>
          <SummaryCard label='Configured hosts' value={summary.configured_hosts} />
          <SummaryCard label='Joined hosts' value={summary.joined_hosts} />
          <SummaryCard label='Configured not joined' value={summary.unjoined_configured_hosts} />
          <SummaryCard label='Live-only hosts' value={summary.live_only_hosts} />
        </Stack>

        <Alert severity='info' sx={{ '& .MuiAlert-message': { fontSize: '0.8125rem' } }}>
          Start, stop, restart, and system metrics are placeholders for now.
        </Alert>

        {error && <Alert severity='warning'>{error}</Alert>}
        {loading && !overview && <Typography variant='body2' color='text.secondary'>Loading node overview...</Typography>}

        <Stack sx={{ gap: 1 }}>
          {hosts.map((host) => (
            <HostRow
              key={host.id}
              host={host}
              onPing={onPing}
              onLogs={onLogs}
              pingState={host.ssh_target ? pingStates[host.ssh_target] : ''}
            />
          ))}
          {!loading && hosts.length === 0 && (
            <Paper variant='outlined' sx={{ p: 2, borderRadius: 2 }}>
              <Typography variant='body2' color='text.secondary'>No configured hosts or live nodes found.</Typography>
            </Paper>
          )}
        </Stack>
      </Stack>
      <Dialog
        open={logsDialog.open}
        onClose={() => setLogsDialog((prev) => ({ ...prev, open: false }))}
        fullWidth
        maxWidth='md'
      >
        <DialogTitle>Node Logs{logsDialog.host ? `: ${logsDialog.host}` : ''}</DialogTitle>
        <DialogContent dividers>
          <Stack sx={{ gap: 1.5 }}>
            {logsDialog.source && (
              <Typography variant='caption' color='text.secondary'>Source: {logsDialog.source}</Typography>
            )}
            {logsDialog.loading && (
              <Typography variant='body2' color='text.secondary'>Loading logs...</Typography>
            )}
            {!logsDialog.loading && logsDialog.error && (
              <Alert severity='warning'>{logsDialog.error}</Alert>
            )}
            {!logsDialog.loading && !logsDialog.error && (
              <Paper variant='outlined' sx={{ p: 1.5, borderRadius: 2, bgcolor: 'grey.50', maxHeight: '26rem', overflow: 'auto' }}>
                <LogContent content={logsDialog.content || 'No log content returned.'} />
              </Paper>
            )}
          </Stack>
        </DialogContent>
      </Dialog>
    </DrawerLayout>
  );
}
