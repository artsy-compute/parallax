import { useEffect, useState } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import { Alert, Box, Button, Chip, Dialog, DialogContent, DialogTitle, Paper, Stack, Typography } from '@mui/material';
import { IconArrowLeft, IconPlugConnected, IconRefresh } from '@tabler/icons-react';
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

const HostRow = ({ host, onPing, onLogs, pingState }: { host: NodeOverviewHost; onPing: (sshTarget: string) => Promise<void>; onLogs: (sshTarget: string) => Promise<void>; pingState?: string }) => {
  const runtime = host.runtime || {};
  const layerText = typeof runtime.start_layer === 'number' || typeof runtime.end_layer === 'number' || typeof runtime.total_layers === 'number'
    ? `[${typeof runtime.start_layer === 'number' ? runtime.start_layer : '?'}, ${typeof runtime.end_layer === 'number' ? runtime.end_layer : '?'})${typeof runtime.total_layers === 'number' ? ` of ${runtime.total_layers}` : ''}`
    : 'Not assigned';
  return (
    <Paper variant='outlined' sx={{ p: 2, borderRadius: 2 }}>
      <Stack direction={{ xs: 'column', md: 'row' }} sx={{ justifyContent: 'space-between', gap: 2 }}>
        <Stack sx={{ gap: 0.75, minWidth: 0 }}>
          <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap', alignItems: 'center' }}>
            <Typography variant='body1' sx={{ fontWeight: 600 }}>{host.display_name}</Typography>
            <StatusChip label={host.joined ? 'Joined' : 'Not joined'} color={host.joined ? 'success' : 'warning'} />
            <StatusChip label={host.inventory_source === 'configured' ? 'Configured host' : 'Live node'} color={host.inventory_source === 'configured' ? 'info' : 'default'} />
            {runtime.status && <StatusChip label={`Runtime: ${runtime.status}`} color={runtime.status === 'available' ? 'success' : 'default'} />}
          </Stack>
          {host.ssh_target && <Typography variant='caption' color='text.secondary'>SSH target: {host.ssh_target}</Typography>}
          {(runtime.hostname || host.hostname_hint) && <Typography variant='caption' color='text.secondary'>Hostname: {runtime.hostname || host.hostname_hint}</Typography>}
          <Typography variant='caption' color='text.secondary'>Layers: {layerText}</Typography>
          <Typography variant='caption' color='text.secondary'>GPU: {runtime.gpu_num ? `${runtime.gpu_num}x ` : ''}{runtime.gpu_name || 'Unknown'}{runtime.gpu_memory ? ` ${runtime.gpu_memory}GB` : ''}</Typography>
          <Typography variant='caption' color='text.disabled'>CPU / RAM / Disk metrics are not collected yet in this first node-management pass.</Typography>
        </Stack>
        <Stack direction='row' sx={{ gap: 1, alignItems: 'flex-start', flexWrap: 'wrap' }}>
          <Button
            size='small'
            variant='outlined'
            startIcon={<IconPlugConnected size={16} />}
            disabled={!host.actions.can_ping || pingState === 'running'}
            onClick={() => onPing(host.ssh_target)}
          >
            {pingState === 'running' ? 'Pinging...' : 'Ping'}
          </Button>
          <Button size='small' variant='outlined' disabled>Start</Button>
          <Button size='small' variant='outlined' disabled>Stop</Button>
          <Button size='small' variant='outlined' disabled>Restart</Button>
          <Button size='small' variant='outlined' disabled={!host.ssh_target} onClick={() => onLogs(host.ssh_target)}>Logs</Button>
        </Stack>
      </Stack>
      {pingState && pingState !== 'running' && (
        <Alert severity={pingState.startsWith('ok:') ? 'success' : 'warning'} sx={{ mt: 1.5, '& .MuiAlert-message': { fontSize: '0.8125rem' } }}>
          {pingState.startsWith('ok:') ? pingState.slice(3) : pingState.startsWith('err:') ? pingState.slice(4) : pingState}
        </Alert>
      )}
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
    <DrawerLayout>
      <Stack sx={{ gap: 2, minHeight: 0, overflow: 'auto', pb: 2 }}>
        <Stack direction={{ xs: 'column', sm: 'row' }} sx={{ justifyContent: 'space-between', gap: 1, alignItems: { sm: 'center' } }}>
          <Stack sx={{ gap: 0.5 }}>
            <Typography variant='h5'>Node Management</Typography>
            <Typography variant='body2' color='text.secondary'>Review configured hosts versus live joined nodes. Ping is available now; remote start/stop/restart and logs are the next step.</Typography>
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
          This first node-management page focuses on visibility and SSH ping. CPU/RAM/disk usage, logs, and remote process control are not wired yet.
        </Alert>

        {error && <Alert severity='warning'>{error}</Alert>}
        {loading && !overview && <Typography variant='body2' color='text.secondary'>Loading node overview...</Typography>}

        <Stack sx={{ gap: 1.5 }}>
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
                <Typography component='pre' sx={{ m: 0, fontFamily: 'monospace', fontSize: '0.78rem', whiteSpace: 'pre-wrap' }}>
                  {logsDialog.content || 'No log content returned.'}
                </Typography>
              </Paper>
            )}
          </Stack>
        </DialogContent>
      </Dialog>
    </DrawerLayout>
  );
}
