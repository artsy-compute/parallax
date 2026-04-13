import {
  Button,
  List,
  ListItemButton,
  ListItemText,
  Skeleton,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material';
import { IconMessageCirclePlus } from '@tabler/icons-react';
import type { FC } from 'react';
import { useChat } from '../../services';
import { useRefCallback } from '../../hooks';

const cleanHistoryLabel = (value: string) =>
  (value || '')
    .replace(/<think>[\s\S]*?<\/think>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

export const ConversationHistory: FC = () => {
  const [
    { conversationId, history, historyLoading },
    { loadConversation, startNewConversation },
  ] = useChat();

  const onNewConversation = useRefCallback(() => {
    startNewConversation();
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
          gap: 0.125,
          pr: 0.25,
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
          const label = cleanHistoryLabel(item.title || item.last_message || '') || 'Untitled conversation';
          const detailLines = [
            cleanHistoryLabel(item.summary),
            item.last_message && item.last_message !== item.summary ? cleanHistoryLabel(item.last_message) : '',
            `${item.message_count} messages`,
          ].filter(Boolean);

          return (
            <Tooltip
              key={item.conversation_id}
              arrow
              placement='right'
              title={
                <Stack sx={{ gap: 0.5, maxWidth: 320 }}>
                  <Typography variant='body2' sx={{ fontWeight: 600 }}>
                    {label}
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
                  height: 32,
                  minHeight: 32,
                  maxHeight: 32,
                  flex: '0 0 32px',
                  alignItems: 'center',
                  overflow: 'hidden',
                  px: 1,
                  py: 0,
                  color: 'text.primary',
                  '&:hover': {
                    bgcolor: 'rgba(255,255,255,0.55)',
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
                      lineHeight: '32px',
                      fontSize: '0.875rem',
                      display: 'block',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    },
                  }}
                  sx={{ my: 0, mx: 0, overflow: 'hidden' }}
                />
              </ListItemButton>
            </Tooltip>
          );
        })}
      </List>
    </Stack>
  );
};
