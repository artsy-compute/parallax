import { memo, useEffect, useRef, useState, type FC, type UIEventHandler } from 'react';
import { useChat, type ChatMessage } from '../../services';
import { useCluster } from '../../services/cluster';
import { Box, Button, IconButton, Stack, Tooltip, Typography } from '@mui/material';
import { IconArrowDown, IconBolt, IconCopy, IconCopyCheck, IconRefresh, IconServer2, IconWorld, IconFolderSearch } from '@tabler/icons-react';
import { useRefCallback } from '../../hooks';
import ChatMarkdown from './chat-markdown';
import { DotPulse } from './dot-pulse';

export const ChatMessages: FC = () => {
  const [{ status, messages, runsByMessageId }] = useChat();

  const refContainer = useRef<HTMLDivElement>(null);
  // const refBottom = useRef<HTMLDivElement>(null);
  const [isBottom, setIsBottom] = useState(true);

  const userScrolledUpRef = useRef(false);
  const autoScrollingRef = useRef(false);
  const prevScrollTopRef = useRef(0);

  const scrollToBottom = useRefCallback(() => {
    const el = refContainer.current;
    if (!el) return;
    userScrolledUpRef.current = false;
    autoScrollingRef.current = true;
    requestAnimationFrame(() => {
      el.scrollTo({ top: el.scrollHeight, behavior: 'smooth' });
      // el.lastElementChild?.scrollIntoView({ behavior: 'smooth' });
    });
    setTimeout(() => {
      autoScrollingRef.current = false;
    }, 250);
  });

  useEffect(() => {
    if (userScrolledUpRef.current) return;
    autoScrollingRef.current = true;
    scrollToBottom();
    const t = setTimeout(() => {
      autoScrollingRef.current = false;
    }, 200);
    return () => clearTimeout(t);
  }, [messages]);

  const onScroll = useRefCallback<UIEventHandler<HTMLDivElement>>((event) => {
    event.stopPropagation();

    const container = refContainer.current;
    if (!container) return;

    const { scrollTop, scrollHeight, clientHeight } = container;
    const bottomGap = scrollHeight - scrollTop - clientHeight;

    setIsBottom(bottomGap < 10);

    if (!autoScrollingRef.current) {
      if (scrollTop < prevScrollTopRef.current - 2) {
        userScrolledUpRef.current = true;
      }
    }
    prevScrollTopRef.current = scrollTop;

    if (bottomGap < 10) {
      userScrolledUpRef.current = false;
    }
  });

  const nodeScrollToBottomButton = (
    <IconButton
      key='scroll-to-bottom'
      onClick={scrollToBottom}
      size='small'
      aria-label='Scroll to bottom'
      sx={{
        position: 'absolute',
        right: 12,
        bottom: 8,
        width: 28,
        height: 28,
        bgcolor: 'white',
        border: '1px solid',
        borderColor: 'grey.300',
        '&:hover': { bgcolor: 'grey.100' },
        opacity: isBottom ? 0 : 1,
        pointerEvents: isBottom ? 'none' : 'auto',
        transition: 'opacity .15s ease',
      }}
    >
      <IconArrowDown />
    </IconButton>
  );

  const nodeStream = (
    <Stack
      key='stream'
      ref={refContainer}
      sx={{
        width: '100%',
        height: '100%',

        overflowX: 'hidden',
        overflowY: 'scroll',
        '&::-webkit-scrollbar': { display: 'none' },
        scrollbarWidth: 'none',
        msOverflowStyle: 'none',

        display: 'flex',
        gap: 4,
      }}
      onScroll={onScroll}
      onWheel={(e) => {
        if (e.deltaY < 0) userScrolledUpRef.current = true;
      }}
      onTouchMove={() => {
        userScrolledUpRef.current = true;
      }}
    >
      {messages.map((message, idx) => (
        <ChatMessage key={message.id} message={message} run={runsByMessageId[message.id] || null} isLast={idx === messages.length - 1} />
      ))}

      {status === 'opened' && <DotPulse size='large' />}

      {/* Last child for scroll to bottom */}
      <Box sx={{ width: '100%', height: 0 }} />
    </Stack>
  );

  return (
    <Box
      sx={{
        position: 'relative',
        flex: 1,
        overflow: 'hidden',
      }}
    >
      {nodeStream}
      {nodeScrollToBottomButton}
    </Box>
  );
};

const runStatusSeverity = (status: string): 'success' | 'warning' | 'error' | 'info' | 'default' => {
  switch (status) {
    case 'completed':
      return 'success';
    case 'waiting_for_approval':
      return 'warning';
    case 'failed':
    case 'cancelled':
      return 'error';
    case 'queued':
    case 'running':
    case 'paused':
      return 'info';
    default:
      return 'default';
  }
};

const ChatMessage: FC<{ message: ChatMessage; run?: { id: string; status: string; current_step: string; title: string } | null; isLast?: boolean }> = memo(({ message, run, isLast }) => {
  const { role, status: messageStatus, thinking, content } = message;

  const [{ messages }, { generate, runTask, focusInput }] = useChat();
  const [{ config: { availableTools } }] = useCluster();

  const [copied, setCopied] = useState(false);
  useEffect(() => {
    const timeoutId = setTimeout(() => setCopied(false), 2000);
    return () => clearTimeout(timeoutId);
  }, [copied]);

  const onCopy = useRefCallback(() => {
    navigator.clipboard.writeText(content);
    setCopied(true);
  });

  const onRegenerate = useRefCallback(() => {
    generate(message);
  });

  const onRunTask = useRefCallback(() => {
    runTask(message, 'task');
  });

  const onFetchLiveData = useRefCallback(() => {
    runTask(message, 'live_data');
  });

  const onInspectWorkspace = useRefCallback(() => {
    runTask(message, 'workspace');
  });

  const onCheckCluster = useRefCallback(() => {
    runTask(message, 'cluster');
  });

  const onAskFollowUp = useRefCallback(() => {
    focusInput();
  });

  const justifyContent = role === 'user' ? 'flex-end' : 'flex-start';

  const nodeContent =
    role === 'user' ?
      <Typography
        key='user-message'
        variant='body1'
        sx={{
          px: 2,
          py: 1.5,
          borderRadius: '0.5rem',
          backgroundColor: 'background.default',
          fontSize: '0.875rem',
        }}
      >
        {content}
      </Typography>
    : <>
        {thinking && <ChatMarkdown key='assistant-thinking' isThinking content={thinking} />}
        {content && <ChatMarkdown key='assistant-message' content={content} />}
      </>;

  const assistantDone = messageStatus === 'done';
  const showCopy = role === 'user' || (role === 'assistant' && assistantDone);
  const showRegenerate = role === 'assistant' && assistantDone;
  const showActionSuggestions = role === 'assistant' && assistantDone && isLast;
  const messageIndex = messages.findIndex((item) => item.id === message.id);
  const precedingUserPrompt = messageIndex >= 0
    ? [...messages.slice(0, messageIndex)].reverse().find((item) => item.role === 'user')?.content || ''
    : '';
  const availableToolNames = new Set(
    availableTools
      .filter((tool) => tool.enabled_by_default)
      .map((tool) => String(tool.name || '').trim()),
  );
  const hasWebTools = availableToolNames.has('fetch_url') || availableToolNames.has('fetch_json');
  const hasWorkspaceTools =
    availableToolNames.has('read_file')
    || availableToolNames.has('list_files')
    || availableToolNames.has('search_files');
  const hasClusterTools =
    availableToolNames.has('get_cluster_status')
    || availableToolNames.has('list_nodes')
    || availableToolNames.has('list_models')
    || availableToolNames.has('get_join_command')
    || availableToolNames.has('get_nodes_overview')
    || availableToolNames.has('get_model_details');
  const suggestLiveData =
    hasWebTools
    && (
      /\b(latest|today|current|recent|price|weather|news|live|update|updated|market|stock|score)\b/i.test(precedingUserPrompt)
      || /\bhttps?:\/\/\S+/i.test(precedingUserPrompt)
      || /\b(?:[a-z0-9-]+\.)+[a-z]{2,}(?:\/\S*)?\b/i.test(precedingUserPrompt)
      || /\b(fetch|open|read|check|look up|visit)\b/i.test(precedingUserPrompt)
    );
  const suggestWorkspace =
    hasWorkspaceTools
    && (
      /\b(file|files|folder|directory|repo|repository|workspace|project|codebase|source|read|search|find)\b/i.test(precedingUserPrompt)
      || /(?:^|[\s"'`])(?:\.{0,2}\/|\/)[^\s"'`]+/.test(precedingUserPrompt)
      || /\b\w+\.(?:ts|tsx|js|jsx|py|rs|md|json|toml|yaml|yml|txt|sh)\b/i.test(precedingUserPrompt)
    );
  const suggestCluster =
    hasClusterTools
    && /\b(cluster|node|nodes|scheduler|model|models|join command|topology|gpu|vram|status)\b/i.test(precedingUserPrompt);

  const userHoverRevealSx =
    role === 'user' ?
      {
        '&:hover .actions-user': {
          opacity: 1,
          pointerEvents: 'auto',
        },
      }
    : {};

  return (
    <Stack direction='row' sx={{ width: '100%', justifyContent }}>
      <Stack
        sx={{
          maxWidth: role === 'user' ? { xs: '100%', md: '80%' } : '100%',
          alignSelf: role === 'user' ? 'flex-end' : 'flex-start',
          gap: 1,
          ...userHoverRevealSx,
        }}
      >
        {nodeContent}

        {(showCopy || showRegenerate) && (
          <Stack
            key='actions'
            direction='row'
            className={role === 'user' ? 'actions-user' : undefined}
            sx={{
              justifyContent,
              color: 'grey.600',
              gap: 0.5,
              ...(role === 'user' ?
                {
                  opacity: 0,
                  pointerEvents: 'none',
                  transition: 'opacity .15s ease',
                }
              : {}),
            }}
          >
            {showCopy && (
              <Tooltip
                key='copy'
                title={copied ? 'Copied!' : 'Copy'}
                slotProps={{
                  tooltip: { sx: { bgcolor: 'primary.main', borderRadius: 1 } },
                  popper: { modifiers: [{ name: 'offset', options: { offset: [0, -8] } }] },
                }}
              >
                <IconButton
                  onClick={onCopy}
                  size='small'
                  sx={{
                    width: 24,
                    height: 24,
                    borderRadius: '8px',
                    '&:hover': { bgcolor: 'action.hover' },
                  }}
                >
                  {copied ?
                    <IconCopyCheck />
                  : <IconCopy />}
                </IconButton>
              </Tooltip>
            )}

            {showRegenerate && (
              <Tooltip
                key='regenerate'
                title='Regenerate'
                slotProps={{
                  tooltip: { sx: { bgcolor: 'primary.main', borderRadius: 1 } },
                  popper: { modifiers: [{ name: 'offset', options: { offset: [0, -8] } }] },
                }}
              >
                <IconButton
                  onClick={onRegenerate}
                  size='small'
                  sx={{
                    width: 24,
                    height: 24,
                    borderRadius: '8px',
                    '&:hover': { bgcolor: 'action.hover' },
                  }}
                >
                  <IconRefresh />
                </IconButton>
              </Tooltip>
            )}
          </Stack>
        )}

        {showActionSuggestions && (
          <Stack
            sx={{
              gap: 0.75,
              mt: 0.5,
              maxWidth: '48rem',
              alignItems: 'flex-end',
            }}
          >
            <Typography variant='body2' color='text.secondary' sx={{ fontStyle: 'italic', textAlign: 'right' }}>
              Continue with:
            </Typography>
            <Stack direction='row' sx={{ gap: 0.75, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
              <Button
                size='small'
                variant='text'
                startIcon={<IconBolt size={16} />}
                onClick={onRunTask}
                sx={{ minWidth: 0, px: 1, color: 'text.secondary' }}
              >
                Run as task
              </Button>
              {suggestLiveData && (
                <Button
                  size='small'
                  variant='text'
                  startIcon={<IconWorld size={16} />}
                  onClick={onFetchLiveData}
                  sx={{ minWidth: 0, px: 1, color: 'text.secondary' }}
                >
                  Use web tools
                </Button>
              )}
              {suggestWorkspace && (
                <Button
                  size='small'
                  variant='text'
                  startIcon={<IconFolderSearch size={16} />}
                  onClick={onInspectWorkspace}
                  sx={{ minWidth: 0, px: 1, color: 'text.secondary' }}
                >
                  Inspect files
                </Button>
              )}
              {suggestCluster && (
                <Button
                  size='small'
                  variant='text'
                  startIcon={<IconServer2 size={16} />}
                  onClick={onCheckCluster}
                  sx={{ minWidth: 0, px: 1, color: 'text.secondary' }}
                >
                  Check cluster
                </Button>
              )}
              <Button
                size='small'
                variant='text'
                onClick={onAskFollowUp}
                sx={{ minWidth: 0, px: 1, color: 'text.secondary' }}
              >
                Ask follow-up
              </Button>
            </Stack>
            <Typography variant='caption' color='text.disabled' sx={{ textAlign: 'right' }}>
              Chat stays answer-first. These options only escalate when you choose them.
            </Typography>
          </Stack>
        )}

        {role === 'assistant' && run && (
          <Stack
            direction='row'
            sx={{
              justifyContent: 'flex-end',
              alignItems: 'center',
              gap: 0.75,
              flexWrap: 'wrap',
              mt: 0.25,
            }}
          >
            <Chip
              size='small'
              color={runStatusSeverity(run.status)}
              label={run.status.replaceAll('_', ' ')}
            />
            <Typography variant='caption' color='text.secondary'>
              {run.current_step}
            </Typography>
            <Button
              size='small'
              variant='text'
              component='a'
              href={`#/runs/${run.id}`}
              sx={{ minWidth: 0, px: 1, color: 'text.secondary' }}
            >
              Open run
            </Button>
          </Stack>
        )}
      </Stack>
    </Stack>
  );
});
