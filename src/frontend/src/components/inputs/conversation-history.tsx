import {
  Button,
  IconButton,
  List,
  ListItemButton,
  ListItemText,
  Skeleton,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material';
import { IconMessageCirclePlus, IconTrash } from '@tabler/icons-react';
import { useState, type FC } from 'react';
import { useChat } from '../../services';
import { useRefCallback } from '../../hooks';
import { AlertDialog } from '../mui';

const HISTORY_LABEL_MAX_CHARS = 36;

const cleanHistoryLabel = (value: string) =>
  (value || '')
    .replace(/<think>[\s\S]*?<\/think>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const truncateHistoryLabel = (value: string, maxChars = HISTORY_LABEL_MAX_CHARS) => {
  if (value.length <= maxChars) {
    return value;
  }
  return value.slice(0, Math.max(0, maxChars - 1)).trimEnd() + '…';
};

export const ConversationHistory: FC = () => {
  const [
    { conversationId, history, historyLoading },
    { loadConversation, deleteConversation, startNewConversation },
  ] = useChat();
  const [pendingDeleteConversation, setPendingDeleteConversation] = useState<{ id: string; label: string } | null>(null);

  const onNewConversation = useRefCallback(() => {
    startNewConversation();
  });

  const confirmDeleteConversation = useRefCallback(async () => {
    if (!pendingDeleteConversation) {
      return;
    }
    await deleteConversation(pendingDeleteConversation.id);
    setPendingDeleteConversation(null);
  });

  return (
    <Stack sx={{ minHeight: 0, height: '100%', flex: 1, gap: 1.5, overflow: 'hidden' }}>
      <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
        <Typography variant='body1' sx={{ color: '#A7A7A7FF', fontWeight: 600 }}>
          Conversations
        </Typography>
        <Button
          size='small'
          color='info'
          startIcon={<IconMessageCirclePlus size={16} />}
          onClick={onNewConversation}
        >
          New
        </Button>
      </Stack>

      <List
        dense
        disablePadding
        sx={{
          minHeight: 0,
          height: '100%',
          flex: 1,
          overflowX: 'hidden',
          overflowY: 'auto',
          p: 0,
          display: 'flex',
          flexDirection: 'column',
          flexWrap: 'nowrap',
          gap: 0.25,
          pr: 0.25,
          minWidth: 0,
        }}
      >
        {historyLoading && history.length === 0 &&
          Array.from({ length: 4 }).map((_, idx) => (
            <Skeleton key={idx} variant='rounded' height={34} sx={{ borderRadius: 1.5 }} />
          ))}

        {!historyLoading && history.length === 0 && (
          <Typography variant='body2' color='text.disabled' sx={{ px: 1, py: 1 }}>
            No saved conversations yet.
          </Typography>
        )}

        {history.map((item) => {
          const fullLabel = cleanHistoryLabel(item.title || item.last_message || '') || 'Untitled conversation';
          const label = truncateHistoryLabel(fullLabel);
          const summarySourceLabel = item.summary_source === 'model' ? 'Model summary' : item.summary_source === 'heuristic' ? 'Fallback summary' : 'No summary yet';
          const detailLines = [
            cleanHistoryLabel(item.summary),
            item.last_message && item.last_message !== item.summary ? cleanHistoryLabel(item.last_message) : '',
            `${item.message_count} messages`,
            summarySourceLabel,
          ].filter(Boolean);

          return (
            <Tooltip
              key={item.conversation_id}
              arrow
              placement='right'
              title={
                <Stack sx={{ gap: 0.5, maxWidth: 320 }}>
                  <Typography variant='body2' sx={{ fontWeight: 600 }}>
                    {fullLabel}
                  </Typography>
                  {detailLines.map((line, idx) => (
                    <Typography
                      key={idx}
                      variant={idx === detailLines.length - 1 ? 'caption' : 'body2'}
                      color={idx === detailLines.length - 1 ? 'grey.300' : 'inherit'}
                      sx={idx < detailLines.length - 1 ? {
                        display: '-webkit-box',
                        WebkitLineClamp: 3,
                        WebkitBoxOrient: 'vertical',
                        overflow: 'hidden',
                      } : undefined}
                    >
                      {line}
                    </Typography>
                  ))}
                </Stack>
              }
            >
              <ListItemButton
                selected={item.conversation_id === conversationId}
                onClick={() => loadConversation(item.conversation_id)}
                sx={{
                  boxSizing: 'border-box',
                  borderRadius: 1.5,
                  height: 36,
                  minHeight: 36,
                  maxHeight: 36,
                  flex: '0 0 36px',
                  alignItems: 'center',
                  overflow: 'hidden',
                  px: 1,
                  py: 0,
                  gap: 0.5,
                  color: 'text.primary',
                  '& .conversation-delete': {
                    opacity: 0,
                    pointerEvents: 'none',
                  },
                  '&:hover': {
                    bgcolor: 'rgba(255,255,255,0.55)',
                  },
                  '&:hover .conversation-delete, &.Mui-selected .conversation-delete': {
                    opacity: 0.65,
                    pointerEvents: 'auto',
                  },
                  '&.Mui-selected': {
                    bgcolor: 'rgba(255,255,255,0.75)',
                  },
                  '&.Mui-selected:hover': {
                    bgcolor: 'rgba(255,255,255,0.85)',
                  },
                }}
              >
                <ListItemText
                  primary={label}
                  primaryTypographyProps={{
                    variant: 'body2',
                    fontWeight: item.conversation_id === conversationId ? 600 : 500,
                    noWrap: true,
                    sx: {
                      lineHeight: '36px',
                      fontSize: '0.875rem',
                      display: 'block',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    },
                  }}
                  sx={{ my: 0, mx: 0, minWidth: 0, overflow: 'hidden' }}
                />
                <Tooltip title='Delete conversation' placement='right'>
                  <IconButton
                    className='conversation-delete'
                    size='small'
                    edge='end'
                    aria-label='Delete conversation'
                    onClick={(event) => {
                      event.preventDefault();
                      event.stopPropagation();
                      setPendingDeleteConversation({ id: item.conversation_id, label: fullLabel });
                    }}
                    sx={{
                      flex: 'none',
                      color: 'text.disabled',
                      transition: 'opacity 120ms ease',
                      '&:hover': { color: 'error.main', bgcolor: 'rgba(255,255,255,0.55)' },
                    }}
                  >
                    <IconTrash size={14} />
                  </IconButton>
                </Tooltip>
              </ListItemButton>
            </Tooltip>
          );
        })}
      </List>
      <AlertDialog
        open={!!pendingDeleteConversation}
        onClose={() => setPendingDeleteConversation(null)}
        color='warning'
        title='Delete conversation'
        content={
          <Typography variant='body2'>
            Delete {pendingDeleteConversation ? `"${truncateHistoryLabel(pendingDeleteConversation.label, 56)}"` : 'this conversation'}?
            This cannot be undone.
          </Typography>
        }
        cancelLabel='Cancel'
        confirmLabel='Delete'
        autoFocusAction='cancel'
        onConfirm={confirmDeleteConversation}
      />
    </Stack>
  );
};
