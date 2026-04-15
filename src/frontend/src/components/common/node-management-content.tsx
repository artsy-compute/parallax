import { useEffect, useState } from 'react';
import { Alert, Box, Button, Chip, Dialog, DialogContent, DialogTitle, IconButton, Paper, Stack, Tooltip, Typography } from '@mui/material';
import {
  IconBrandApple,
  IconCpu,
  IconFileText,
  IconSquareFilled,
  IconPlayerPlay,
  IconPlugConnected,
  IconRefresh,
  IconRotateClockwise2,
} from '@tabler/icons-react';
import { getNodeLogs, getNodesOverview, pingNodeHost, restartNodeHost, startNodeHost, stopNodeHost, type NodeOverviewHost, type NodesOverview } from '../../services/api';

const SummaryCard = ({ label, value }: { label: string; value: number }) => (
  <Paper variant='outlined' sx={{ p: 1.5, borderRadius: 2, minWidth: 0, flex: 1 }}>
    <Typography variant='caption' color='text.secondary' sx={{ fontSize: '0.72rem', letterSpacing: '0.02em' }}>{label}</Typography>
    <Typography variant='h6' sx={{ fontSize: '1.1rem', lineHeight: 1.15 }}>{value}</Typography>
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

const formatUsageStat = (used?: number | null, total?: number | null, percent?: number | null, unit = 'GB') => {
  if (typeof used === 'number' && typeof total === 'number' && typeof percent === 'number') {
    return `${used.toFixed(1)}/${total.toFixed(1)} ${unit} (${percent.toFixed(0)}%)`;
  }
  if (typeof percent === 'number') {
    return `${percent.toFixed(0)}%`;
  }
  return 'Unknown';
};

const HostRow = ({ host, onPing, onLogs, onStart, onStop, onRestart, pingState, actionState }: { host: NodeOverviewHost; onPing: (sshTarget: string) => Promise<void>; onLogs: (sshTarget: string) => Promise<void>; onStart: (sshTarget: string) => Promise<void>; onStop: (sshTarget: string) => Promise<void>; onRestart: (sshTarget: string) => Promise<void>; pingState?: string; actionState?: string }) => {
  const runtime = host.runtime || {};
  const lifecycle = host.lifecycle || {};
  const lifecycleProcess = lifecycle.process || {};
  const lifecycleScheduler = lifecycle.scheduler || {};
  const lifecycleServing = lifecycle.serving || {};
  const lifecycleManagement = lifecycle.management || {};
  const hostProcess = host.host_process || { running: host.joined, confirmed_running: host.joined, source: host.joined ? 'joined' : 'unknown', message: host.joined ? 'Node is joined to the scheduler' : 'Remote process status unavailable' };
  const layerStart = typeof lifecycleServing.start_layer === 'number' ? lifecycleServing.start_layer : runtime.start_layer;
  const layerEnd = typeof lifecycleServing.end_layer === 'number' ? lifecycleServing.end_layer : runtime.end_layer;
  const totalLayers = typeof lifecycleServing.total_layers === 'number' ? lifecycleServing.total_layers : runtime.total_layers;
  const layerText = typeof layerStart === 'number' || typeof layerEnd === 'number' || typeof totalLayers === 'number'
    ? `[${typeof layerStart === 'number' ? layerStart : '?'}, ${typeof layerEnd === 'number' ? layerEnd : '?'})${typeof totalLayers === 'number' ? ` of ${totalLayers}` : ''}`
    : 'Not assigned';
  const processState = String(lifecycleProcess.state || '');
  const schedulerMembership = String(lifecycleScheduler.membership || '');
  const servingState = String(lifecycleServing.state || '');
  const processChip = schedulerMembership === 'joined' && servingState === 'active'
    ? { label: 'Serving', color: 'success' as const }
    : schedulerMembership === 'joined'
      ? { label: 'Joined', color: 'success' as const }
      : processState === 'starting' || schedulerMembership === 'joining'
        ? { label: 'Joining', color: 'info' as const }
        : processState === 'running'
          ? { label: 'Process running', color: 'warning' as const }
          : processState === 'unknown'
            ? { label: 'Process unknown', color: 'warning' as const }
            : { label: 'Process stopped', color: 'default' as const };
  const schedulerChip = schedulerMembership === 'joined'
    ? { label: 'Scheduler joined', color: 'success' as const }
    : schedulerMembership === 'joining'
      ? { label: 'Scheduler joining', color: 'info' as const }
      : schedulerMembership === 'leaving'
        ? { label: 'Scheduler leaving', color: 'warning' as const }
        : { label: 'Scheduler not joined', color: 'warning' as const };
  const system = host.system || {};
  const isActionRunning = !!actionState?.startsWith('running:');
  const canPing = !!host.ssh_target && host.actions.can_ping;
  const lifecycleAllowsStart = processState === 'stopped' || processState === 'unknown';
  const lifecycleAllowsStop = processState === 'running' || processState === 'starting' || schedulerMembership === 'joined' || schedulerMembership === 'joining';
  const canStart = !!host.ssh_target && !isActionRunning && (host.actions.can_start || lifecycleAllowsStart);
  const canStop = !!host.ssh_target && !isActionRunning && (host.actions.can_stop || lifecycleAllowsStop);
  const canRestart = !!host.ssh_target && !isActionRunning && (host.actions.can_restart || lifecycleAllowsStop);
  const details = [
    { label: 'SSH', value: host.ssh_target || 'Unavailable' },
    { label: 'Host', value: runtime.hostname || host.hostname_hint || 'Unknown' },
    { label: 'Lifecycle', value: String(lifecycle.summary || 'Unknown') },
    { label: 'Management', value: String(lifecycleManagement.mode || 'unknown') },
    { label: 'Layers', value: layerText },
    { label: 'GPU', value: `${runtime.gpu_num ? `${runtime.gpu_num}x ` : ''}${runtime.gpu_name || 'Unknown'}${runtime.gpu_memory ? ` ${runtime.gpu_memory}GB` : ''}` },
    { label: 'CPU', value: typeof system.cpu_percent === 'number' ? `${system.cpu_percent.toFixed(0)}%` : 'Unknown' },
    { label: 'RAM', value: formatUsageStat(system.ram_used_gb, system.ram_total_gb, system.ram_used_percent) },
    { label: 'Disk', value: formatUsageStat(system.disk_used_gb, system.disk_total_gb, system.disk_used_percent) },
  ];
  return (
    <Paper variant='outlined' sx={{ p: 1.5, borderRadius: 2 }}>
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
              <StatusChip label={processChip.label} color={processChip.color} />
              <StatusChip label={schedulerChip.label} color={schedulerChip.color} />
              <StatusChip label={host.inventory_source === 'configured' ? 'Configured host' : 'Live node'} color={host.inventory_source === 'configured' ? 'info' : 'default'} />
              {runtime.status && <StatusChip label={`Runtime: ${runtime.status}`} color={runtime.status === 'available' ? 'success' : 'default'} />}
            </Stack>
            <Typography variant='caption' color='text.secondary'>
              {String(lifecycle.summary || lifecycleProcess.message || hostProcess.message || (hostProcess.running ? 'Node process detected on remote host' : 'No remote node process detected'))}
              {(lifecycleProcess.pid || hostProcess.pid) ? ` (pid ${String(lifecycleProcess.pid || hostProcess.pid)})` : ''}
            </Typography>
            <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: 'repeat(2, minmax(0, 1fr))' }, columnGap: 1.5, rowGap: 0.35, minWidth: 0 }}>
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
              <span><IconButton size='small' color='primary' disabled={!canPing || pingState === 'running' || isActionRunning} onClick={() => onPing(host.ssh_target)}><IconPlugConnected size={16} /></IconButton></span>
            </Tooltip>
            <Tooltip title={actionState === 'running:start' ? 'Starting node' : 'Start node'}>
              <span><IconButton size='small' color='success' disabled={!canStart} onClick={() => onStart(host.ssh_target)}><IconPlayerPlay size={16} /></IconButton></span>
            </Tooltip>
            <Tooltip title={actionState === 'running:stop' ? 'Stopping node' : 'Stop node'}>
              <span><IconButton size='small' color='warning' disabled={!canStop} onClick={() => onStop(host.ssh_target)}><IconSquareFilled size={14} /></IconButton></span>
            </Tooltip>
            <Tooltip title={actionState === 'running:restart' ? 'Restarting node' : 'Restart node'}>
              <span><IconButton size='small' color='secondary' disabled={!canRestart} onClick={() => onRestart(host.ssh_target)}><IconRotateClockwise2 size={16} /></IconButton></span>
            </Tooltip>
            <Tooltip title='Open remote logs'>
              <span><IconButton size='small' disabled={!host.ssh_target} onClick={() => onLogs(host.ssh_target)}><IconFileText size={16} /></IconButton></span>
            </Tooltip>
          </Stack>
        </Stack>
        {pingState && pingState !== 'running' && <Alert severity={pingState.startsWith('ok:') ? 'success' : 'warning'} sx={{ '& .MuiAlert-message': { fontSize: '0.8125rem' }, py: 0 }}>{pingState.startsWith('ok:') ? pingState.slice(3) : pingState.startsWith('err:') ? pingState.slice(4) : pingState}</Alert>}
        {actionState && !actionState.startsWith('running:') && <Alert severity={actionState.startsWith('ok:') ? 'success' : 'warning'} sx={{ '& .MuiAlert-message': { fontSize: '0.8125rem' }, py: 0 }}>{actionState.startsWith('ok:') ? actionState.slice(3) : actionState.startsWith('err:') ? actionState.slice(4) : actionState}</Alert>}
      </Stack>
    </Paper>
  );
};

export const NodeManagementContent = ({ embedded = false }: { embedded?: boolean }) => {
  const [overview, setOverview] = useState<NodesOverview | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [pingStates, setPingStates] = useState<Record<string, string>>({});
  const [actionStates, setActionStates] = useState<Record<string, string>>({});
  const [logsDialog, setLogsDialog] = useState<{ open: boolean; host: string; source: string; content: string; loading: boolean; error: string }>({
    open: false,
    host: '',
    source: '',
    content: '',
    loading: false,
    error: '',
  });

  const applyOptimisticHostAction = (sshTarget: string, action: 'start' | 'stop' | 'restart') => {
    setOverview((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        hosts: prev.hosts.map((host) => {
          if (host.ssh_target !== sshTarget) return host;
          if (action === 'stop') {
            return {
              ...host,
              joined: false,
              runtime: { ...host.runtime, status: 'waiting', start_layer: null, end_layer: null, approx_remaining_context: null },
              host_process: { ...host.host_process, running: false, confirmed_running: false, pid: '', source: 'action', message: 'Node stopped', checked_at: Date.now() / 1000 },
              actions: { ...host.actions, can_start: true, can_stop: false, can_restart: false },
              lifecycle: {
                ...host.lifecycle,
                summary: 'Stopped',
                management: { ...host.lifecycle?.management, last_action_state: 'action', last_action_message: 'Node stopped', checked_at: Date.now() / 1000 },
                process: { ...host.lifecycle?.process, state: 'stopped', pid: '', source: 'action', message: 'Node stopped', checked_at: Date.now() / 1000 },
                scheduler: { ...host.lifecycle?.scheduler, membership: 'leaving', joined: false, status: 'waiting' },
                serving: { ...host.lifecycle?.serving, state: 'unassigned', start_layer: null, end_layer: null },
              },
            };
          }
          return {
            ...host,
            host_process: { ...host.host_process, running: true, confirmed_running: false, source: 'action_pending', message: action === 'start' ? 'Node start requested' : 'Node restarted', checked_at: Date.now() / 1000 },
            actions: { ...host.actions, can_start: false, can_stop: false, can_restart: false },
            lifecycle: {
              ...host.lifecycle,
              summary: 'Joining scheduler',
              management: { ...host.lifecycle?.management, last_action_state: 'action_pending', last_action_message: action === 'start' ? 'Node start requested' : 'Node restarted', checked_at: Date.now() / 1000 },
              process: { ...host.lifecycle?.process, state: 'starting', source: 'action_pending', message: action === 'start' ? 'Node start requested' : 'Node restarted', checked_at: Date.now() / 1000 },
              scheduler: { ...host.lifecycle?.scheduler, membership: 'joining', joined: false },
              serving: { ...host.lifecycle?.serving, state: 'unassigned' },
            },
          };
        }),
      };
    });
  };

  const loadOverview = async () => {
    try {
      setLoading(true);
      setError('');
      setOverview(await getNodesOverview());
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
      const message = result.ok ? `ok:${result.message}${typeof result.latency_ms === 'number' ? ` (${result.latency_ms} ms)` : ''}` : `err:${result.message}`;
      setPingStates((prev) => ({ ...prev, [sshTarget]: message }));
    } catch (err: any) {
      setPingStates((prev) => ({ ...prev, [sshTarget]: `err:${err?.message || 'Ping failed'}` }));
    }
  };

  const runHostAction = async (sshTarget: string, action: 'start' | 'stop' | 'restart', runner: (sshTarget: string) => Promise<{ ok: boolean; message: string }>) => {
    if (!sshTarget) return;
    setActionStates((prev) => ({ ...prev, [sshTarget]: `running:${action}` }));
    try {
      const result = await runner(sshTarget);
      if (result.ok) applyOptimisticHostAction(sshTarget, action);
      setActionStates((prev) => ({ ...prev, [sshTarget]: `${result.ok ? 'ok' : 'err'}:${result.message}` }));
      window.setTimeout(() => {
        loadOverview();
      }, action === 'start' || action === 'restart' ? 1500 : 800);
    } catch (err: any) {
      setActionStates((prev) => ({ ...prev, [sshTarget]: `err:${err?.message || `${action} failed`}` }));
    }
  };

  const onLogs = async (sshTarget: string) => {
    if (!sshTarget) return;
    setLogsDialog({ open: true, host: sshTarget, source: '', content: '', loading: true, error: '' });
    try {
      const result = await getNodeLogs(sshTarget, 200);
      setLogsDialog({ open: true, host: sshTarget, source: result.source || '', content: result.content || '', loading: false, error: result.ok ? '' : (result.message || 'Failed to fetch logs') });
    } catch (err: any) {
      setLogsDialog({ open: true, host: sshTarget, source: '', content: '', loading: false, error: err?.message || 'Failed to fetch logs' });
    }
  };

  const summary = overview?.summary || { configured_hosts: 0, joined_hosts: 0, unjoined_configured_hosts: 0, live_only_hosts: 0 };
  const hosts = overview?.hosts || [];
  const configuredHostnames = new Set(hosts.filter((host) => host.inventory_source === 'configured').map((host) => (host.hostname_hint || host.runtime?.hostname || '').trim().toLowerCase()).filter(Boolean));
  const visibleHosts = hosts.filter((host) => {
    if (host.inventory_source !== 'live_only') return true;
    const hostname = (host.hostname_hint || host.runtime?.hostname || '').trim().toLowerCase();
    return !hostname || !configuredHostnames.has(hostname);
  });

  return (
    <>
      <Stack sx={{ gap: 1.25 }}>
        <Stack direction={{ xs: 'column', sm: 'row' }} sx={{ justifyContent: 'space-between', gap: 1, alignItems: { sm: 'center' } }}>
          {!embedded && (
            <Stack sx={{ gap: 0.25 }}>
              <Typography variant='body1'>Node Management</Typography>
              <Typography variant='body2' color='text.secondary'>Live start, stop, restart, health, SSH ping, and logs for inventory hosts and joined nodes.</Typography>
            </Stack>
          )}
          <Button onClick={loadOverview} variant='outlined' startIcon={<IconRefresh size={16} />}>Refresh</Button>
        </Stack>

        {!embedded && (
          <>
            <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1.5 }}>
              <SummaryCard label='Configured hosts' value={summary.configured_hosts} />
              <SummaryCard label='Joined hosts' value={summary.joined_hosts} />
              <SummaryCard label='Configured not joined' value={summary.unjoined_configured_hosts} />
              <SummaryCard label='Live-only hosts' value={summary.live_only_hosts} />
            </Stack>

            <Alert severity='info' sx={{ '& .MuiAlert-message': { fontSize: '0.8125rem' } }}>
              CPU, RAM, and disk usage come from native node heartbeat updates. Process status still uses the managed pid-file check so Start stays disabled when the managed node process is already running.
            </Alert>
          </>
        )}

        {error && <Alert severity='warning'>{error}</Alert>}
        {loading && !overview && <Typography variant='body2' color='text.secondary'>Loading node overview...</Typography>}

        <Stack sx={{ gap: 1 }}>
          {visibleHosts.map((host) => (
            <HostRow
              key={host.id}
              host={host}
              onPing={onPing}
              onLogs={onLogs}
              onStart={(sshTarget) => runHostAction(sshTarget, 'start', startNodeHost)}
              onStop={(sshTarget) => runHostAction(sshTarget, 'stop', stopNodeHost)}
              onRestart={(sshTarget) => runHostAction(sshTarget, 'restart', restartNodeHost)}
              pingState={host.ssh_target ? pingStates[host.ssh_target] : ''}
              actionState={host.ssh_target ? actionStates[host.ssh_target] : ''}
            />
          ))}
          {!loading && visibleHosts.length === 0 && (
            <Paper variant='outlined' sx={{ p: 2, borderRadius: 2 }}>
              <Typography variant='body2' color='text.secondary'>No configured hosts or live nodes found.</Typography>
            </Paper>
          )}
        </Stack>
      </Stack>
      <Dialog open={logsDialog.open} onClose={() => setLogsDialog((prev) => ({ ...prev, open: false }))} fullWidth maxWidth='md'>
        <DialogTitle>Node Logs{logsDialog.host ? `: ${logsDialog.host}` : ''}</DialogTitle>
        <DialogContent dividers>
          <Stack sx={{ gap: 1.5 }}>
            {logsDialog.source && <Typography variant='caption' color='text.secondary'>Source: {logsDialog.source}</Typography>}
            {logsDialog.loading && <Typography variant='body2' color='text.secondary'>Loading logs...</Typography>}
            {!logsDialog.loading && logsDialog.error && <Alert severity='warning'>{logsDialog.error}</Alert>}
            {!logsDialog.loading && !logsDialog.error && (
              <Paper variant='outlined' sx={{ p: 1.5, borderRadius: 2, bgcolor: 'grey.50', maxHeight: '26rem', overflow: 'auto' }}>
                <LogContent content={logsDialog.content || 'No log content returned.'} />
              </Paper>
            )}
          </Stack>
        </DialogContent>
      </Dialog>
    </>
  );
};
