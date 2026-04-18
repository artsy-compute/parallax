import { useEffect, useMemo, useState } from 'react';
import { Link as RouterLink, useNavigate, useParams } from 'react-router-dom';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  Paper,
  Stack,
  Typography,
} from '@mui/material';
import { IconArrowLeft } from '@tabler/icons-react';
import { DrawerLayout } from '../components/common';
import { getAgentRunDetail, getAgentRunList, type AgentRunDetail, type AgentRunSummary } from '../services/api';

const formatDateTime = (value: number) =>
  new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  }).format(new Date(value * 1000));

const formatDuration = (durationMs: number) => {
  const totalSeconds = Math.max(0, Math.floor(durationMs / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes <= 0) {
    return `${seconds}s`;
  }
  if (minutes < 60) {
    return `${minutes}m ${seconds}s`;
  }
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
};

const getStatusColor = (status: string): 'warning' | 'success' | 'info' | 'error' | 'default' => {
  switch (status) {
    case 'running':
      return 'info';
    case 'waiting_for_approval':
      return 'warning';
    case 'completed':
      return 'success';
    case 'failed':
      return 'error';
    default:
      return 'default';
  }
};

const OverviewCard = ({
  label,
  value,
  hint,
}: {
  label: string;
  value: string | number;
  hint: string;
}) => (
  <Paper
    variant='outlined'
    sx={{
      p: 2.25,
      borderRadius: 3,
      minWidth: 0,
      bgcolor: 'background.paper',
      borderColor: 'divider',
    }}
  >
    <Stack sx={{ gap: 0.5 }}>
      <Typography variant='caption' color='text.secondary'>
        {label}
      </Typography>
      <Typography variant='h2'>{value}</Typography>
      <Typography variant='body2' color='text.secondary'>
        {hint}
      </Typography>
    </Stack>
  </Paper>
);

export default function PageRuns() {
  const navigate = useNavigate();
  const { runId } = useParams();

  const [counts, setCounts] = useState({
    total: 0,
    active: 0,
    waiting_for_approval: 0,
    completed: 0,
  });
  const [runs, setRuns] = useState<readonly AgentRunSummary[]>([]);
  const [selectedRun, setSelectedRun] = useState<AgentRunDetail | null>(null);
  const [listLoading, setListLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setListLoading(true);
    getAgentRunList()
      .then((data) => {
        if (cancelled) {
          return;
        }
        setCounts(data.counts);
        setRuns(data.items);
      })
      .catch((nextError) => {
        if (cancelled) {
          return;
        }
        setError(nextError instanceof Error ? nextError.message : String(nextError));
      })
      .finally(() => {
        if (!cancelled) {
          setListLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!runs.length || runId) {
      return;
    }
    navigate(`/runs/${runs[0].id}`, { replace: true });
  }, [navigate, runId, runs]);

  useEffect(() => {
    if (!runId) {
      return;
    }
    let cancelled = false;
    setDetailLoading(true);
    getAgentRunDetail(runId)
      .then((data) => {
        if (!cancelled) {
          setSelectedRun(data);
        }
      })
      .catch((nextError) => {
        if (cancelled) {
          return;
        }
        setError(nextError instanceof Error ? nextError.message : String(nextError));
      })
      .finally(() => {
        if (!cancelled) {
          setDetailLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [runId]);

  const selectedSummary = useMemo(
    () => runs.find((item) => item.id === runId) ?? runs[0] ?? null,
    [runId, runs],
  );

  return (
    <DrawerLayout contentWidth='wide'>
      <Stack sx={{ minHeight: 0, gap: 2.5 }}>
        <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'flex-start', gap: 2, flexWrap: 'wrap' }}>
          <Stack sx={{ gap: 1 }}>
            <Button component={RouterLink} to='/chat' variant='text' startIcon={<IconArrowLeft size={16} />} sx={{ alignSelf: 'flex-start' }}>
              Back to chat
            </Button>
            <Stack sx={{ gap: 0.75 }}>
              <Typography variant='h1'>Runs</Typography>
              <Typography variant='body1' color='text.secondary' sx={{ maxWidth: '48rem' }}>
                Mocked agent-runtime console for reviewing active work, approvals, artifacts, and policy boundaries in-place before the real execution layer is wired up.
              </Typography>
            </Stack>
          </Stack>
          <Paper
            variant='outlined'
            sx={{
              p: 2,
              borderRadius: 3,
              minWidth: '18rem',
              bgcolor: 'background.paper',
            }}
          >
            <Stack sx={{ gap: 0.75 }}>
              <Typography variant='caption' color='text.secondary'>
                What this mockup proves
              </Typography>
              <Typography variant='body2'>Runs become the operational view above chat: status, approvals, artifacts, and audit events live here instead of disappearing into a conversation bubble.</Typography>
            </Stack>
          </Paper>
        </Stack>

        {error && <Alert severity='error'>{error}</Alert>}

        <Box
          sx={{
            display: 'grid',
            gridTemplateColumns: {
              xs: '1fr',
              md: 'repeat(2, minmax(0, 1fr))',
              xl: 'repeat(4, minmax(0, 1fr))',
            },
            gap: 1.5,
          }}
        >
          <OverviewCard label='Runs tracked' value={counts.total} hint='All mocked agent executions in this workspace' />
          <OverviewCard label='Active now' value={counts.active} hint='Running, paused, or waiting on intervention' />
          <OverviewCard label='Needs approval' value={counts.waiting_for_approval} hint='Human decision required before side effects' />
          <OverviewCard label='Completed' value={counts.completed} hint='Finished runs with persisted artifacts and timeline' />
        </Box>

        <Box
          sx={{
            minHeight: 0,
            display: 'grid',
            gridTemplateColumns: { xs: '1fr', lg: 'minmax(18rem, 24rem) minmax(0, 1fr)' },
            gap: 2,
            overflow: 'hidden',
            flex: 1,
          }}
        >
          <Paper
            variant='outlined'
            sx={{
              minHeight: 0,
              overflow: 'auto',
              borderRadius: 3,
              p: 1.25,
              bgcolor: 'background.paper',
            }}
          >
            <Stack sx={{ gap: 1 }}>
              <Typography variant='h3' sx={{ px: 1, pt: 0.5 }}>
                Queue
              </Typography>
              {listLoading ? (
                <Stack sx={{ py: 6, alignItems: 'center' }}>
                  <CircularProgress size={28} />
                </Stack>
              ) : (
                runs.map((run) => {
                  const selected = run.id === selectedSummary?.id;
                  return (
                    <Paper
                      key={run.id}
                      component={RouterLink}
                      to={`/runs/${run.id}`}
                      variant='outlined'
                      sx={{
                        p: 1.5,
                        borderRadius: 3,
                        textDecoration: 'none',
                        color: 'inherit',
                        borderColor: selected ? 'brand.main' : 'divider',
                        bgcolor: selected ? 'brand.lighter' : 'background.paper',
                        transition: 'transform .12s ease, border-color .12s ease',
                        '&:hover': {
                          transform: 'translateY(-1px)',
                          borderColor: selected ? 'brand.main' : 'text.secondary',
                        },
                      }}
                    >
                      <Stack sx={{ gap: 1 }}>
                        <Stack direction='row' sx={{ justifyContent: 'space-between', gap: 1, alignItems: 'flex-start' }}>
                          <Typography variant='body1' sx={{ fontWeight: 700 }}>
                            {run.title}
                          </Typography>
                          <Chip size='small' color={getStatusColor(run.status)} label={run.status.replaceAll('_', ' ')} />
                        </Stack>
                        <Typography variant='body2' color='text.secondary'>
                          {run.summary}
                        </Typography>
                        <Stack direction='row' sx={{ gap: 0.75, flexWrap: 'wrap' }}>
                          <Chip size='small' variant='outlined' label={run.agent_name} />
                          <Chip size='small' variant='outlined' label={run.model} />
                          <Chip size='small' variant='outlined' label={`${run.tool_count} tools`} />
                          {run.approval_count > 0 && <Chip size='small' color='warning' variant='outlined' label={`${run.approval_count} approval`} />}
                        </Stack>
                        <Typography variant='caption' color='text.secondary'>
                          Updated {formatDateTime(run.updated_at)}
                        </Typography>
                      </Stack>
                    </Paper>
                  );
                })
              )}
            </Stack>
          </Paper>

          <Paper
            variant='outlined'
            sx={{
              minHeight: 0,
              overflow: 'auto',
              borderRadius: 3,
              p: 2.5,
              bgcolor: 'background.paper',
            }}
          >
            {detailLoading || (!selectedRun && selectedSummary) ? (
              <Stack sx={{ py: 8, alignItems: 'center', gap: 1.5 }}>
                <CircularProgress size={30} />
                <Typography variant='body2' color='text.secondary'>
                  Loading run detail…
                </Typography>
              </Stack>
            ) : !selectedRun ? (
              <Stack sx={{ py: 8, alignItems: 'center', gap: 1.5 }}>
                <Typography variant='h3'>No run selected</Typography>
                <Typography variant='body2' color='text.secondary'>
                  Pick a run from the left to inspect the mock operational layout.
                </Typography>
              </Stack>
            ) : (
              <Stack sx={{ gap: 3 }}>
                <Stack sx={{ gap: 1.5 }}>
                  <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'flex-start', gap: 2, flexWrap: 'wrap' }}>
                    <Stack sx={{ gap: 0.75 }}>
                      <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
                        <Chip color={getStatusColor(selectedRun.status)} label={selectedRun.status.replaceAll('_', ' ')} />
                        <Chip variant='outlined' label={selectedRun.agent_name} />
                        <Chip variant='outlined' label={selectedRun.priority} />
                        <Chip variant='outlined' label={`risk: ${selectedRun.risk_level}`} />
                      </Stack>
                      <Typography variant='h2'>{selectedRun.title}</Typography>
                      <Typography variant='body1' color='text.secondary'>
                        {selectedRun.summary}
                      </Typography>
                    </Stack>
                    <Paper
                      variant='outlined'
                      sx={{
                        p: 1.5,
                        borderRadius: 3,
                        minWidth: '15rem',
                        bgcolor: 'background.default',
                      }}
                    >
                      <Stack sx={{ gap: 0.4 }}>
                        <Typography variant='caption' color='text.secondary'>
                          Current step
                        </Typography>
                        <Typography variant='body1' sx={{ fontWeight: 700 }}>
                          {selectedRun.current_step}
                        </Typography>
                        <Typography variant='body2' color='text.secondary'>
                          Requested by {selectedRun.requested_by}
                        </Typography>
                        <Typography variant='body2' color='text.secondary'>
                          Running for {formatDuration(selectedRun.duration_ms)}
                        </Typography>
                      </Stack>
                    </Paper>
                  </Stack>

                  <Box
                    sx={{
                      display: 'grid',
                      gridTemplateColumns: { xs: '1fr', md: 'repeat(3, minmax(0, 1fr))' },
                      gap: 1.5,
                    }}
                  >
                    <OverviewCard label='Started' value={formatDateTime(selectedRun.started_at)} hint='Operator-visible lifecycle anchor' />
                    <OverviewCard label='Model' value={selectedRun.model} hint='Execution contract recorded for audit' />
                    <OverviewCard label='Conversation' value={selectedRun.conversation_id} hint='Thread link between chat and run state' />
                  </Box>
                </Stack>

                <Divider />

                <Box
                  sx={{
                    display: 'grid',
                    gridTemplateColumns: { xs: '1fr', xl: '1.4fr .9fr' },
                    gap: 2,
                  }}
                >
                  <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default' }}>
                    <Stack sx={{ gap: 2 }}>
                      <Typography variant='h3'>Execution timeline</Typography>
                      {selectedRun.events.map((event, index) => (
                        <Stack key={event.id} direction='row' sx={{ gap: 1.5, alignItems: 'stretch' }}>
                          <Stack sx={{ alignItems: 'center', width: '1.25rem', flex: 'none' }}>
                            <Box
                              sx={{
                                width: 10,
                                height: 10,
                                borderRadius: '50%',
                                bgcolor:
                                  event.status === 'pending'
                                    ? 'warning.main'
                                    : event.status === 'running'
                                      ? 'info.main'
                                      : event.status === 'completed'
                                        ? 'success.main'
                                        : 'grey.400',
                                mt: 0.75,
                              }}
                            />
                            {index < selectedRun.events.length - 1 && (
                              <Box sx={{ width: 2, flex: 1, bgcolor: 'divider', my: 0.5 }} />
                            )}
                          </Stack>
                          <Stack sx={{ gap: 0.35, pb: 1.25 }}>
                            <Stack direction='row' sx={{ gap: 1, alignItems: 'center', flexWrap: 'wrap' }}>
                              <Typography variant='body1' sx={{ fontWeight: 700 }}>
                                {event.title}
                              </Typography>
                              <Chip size='small' variant='outlined' label={event.kind} />
                            </Stack>
                            <Typography variant='caption' color='text.secondary'>
                              {formatDateTime(event.timestamp)}
                            </Typography>
                            <Typography variant='body2' color='text.secondary'>
                              {event.detail}
                            </Typography>
                          </Stack>
                        </Stack>
                      ))}
                    </Stack>
                  </Paper>

                  <Stack sx={{ gap: 2 }}>
                    <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default' }}>
                      <Stack sx={{ gap: 1.5 }}>
                        <Typography variant='h3'>Artifacts</Typography>
                        {selectedRun.artifacts.map((artifact) => (
                          <Stack
                            key={artifact.label}
                            sx={{
                              gap: 0.4,
                              p: 1.25,
                              borderRadius: 2,
                              bgcolor: 'background.paper',
                              border: '1px solid',
                              borderColor: 'divider',
                            }}
                          >
                            <Typography variant='caption' color='text.secondary'>
                              {artifact.kind}
                            </Typography>
                            <Typography variant='body1' sx={{ fontWeight: 700 }}>
                              {artifact.label}
                            </Typography>
                            <Typography variant='body2' color='text.secondary'>
                              {artifact.value}
                            </Typography>
                          </Stack>
                        ))}
                      </Stack>
                    </Paper>

                    <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default' }}>
                      <Stack sx={{ gap: 1.5 }}>
                        <Typography variant='h3'>Policy envelope</Typography>
                        <Stack sx={{ gap: 1 }}>
                          <Typography variant='body2' color='text.secondary'>
                            Routing mode
                          </Typography>
                          <Typography variant='body1' sx={{ fontWeight: 700 }}>
                            {selectedRun.policy.routing_mode}
                          </Typography>
                        </Stack>
                        <Stack sx={{ gap: 1 }}>
                          <Typography variant='body2' color='text.secondary'>
                            Filesystem access
                          </Typography>
                          <Typography variant='body1' sx={{ fontWeight: 700 }}>
                            {selectedRun.policy.filesystem_access}
                          </Typography>
                        </Stack>
                        <Stack sx={{ gap: 1 }}>
                          <Typography variant='body2' color='text.secondary'>
                            Network access
                          </Typography>
                          <Typography variant='body1' sx={{ fontWeight: 700 }}>
                            {selectedRun.policy.network_access}
                          </Typography>
                        </Stack>
                        <Stack sx={{ gap: 1 }}>
                          <Typography variant='body2' color='text.secondary'>
                            Remote provider used
                          </Typography>
                          <Typography variant='body1' sx={{ fontWeight: 700 }}>
                            {selectedRun.policy.remote_provider_used ? 'Yes' : 'No'}
                          </Typography>
                        </Stack>
                      </Stack>
                    </Paper>
                  </Stack>
                </Box>
              </Stack>
            )}
          </Paper>
        </Box>
      </Stack>
    </DrawerLayout>
  );
}
