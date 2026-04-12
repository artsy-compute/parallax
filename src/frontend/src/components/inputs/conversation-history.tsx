import {
  Button,
  List,
  ListItemButton,
  ListItemText,
  Skeleton,
  Stack,
  Typography,
} from '@mui/material';
import { IconMessageCirclePlus } from '@tabler/icons-react';
import type { FC } from 'react';
import { useChat } from '../../services';
import { useRefCallback } from '../../hooks';

export const ConversationHistory: FC = () => {
  const [
    { conversationId, history, historyLoading },
    { loadConversation, startNewConversation },
  ] = useChat();

  const onNewConversation = useRefCallback(() => {
    startNewConversation();
  });

  return (
    <Stack sx={{ minHeight: 0, flex: 1, gap: 1.5, overflow: 'hidden' }}>
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
          flex: 1,
          overflowY: 'auto',
          bgcolor: 'rgba(255,255,255,0.45)',
          borderRadius: 2,
          p: 0.75,
          display: 'flex',
          flexDirection: 'column',
          gap: 0.5,
        }}
      >
        {historyLoading && history.length === 0 &&
          Array.from({ length: 4 }).map((_, idx) => (
            <Skeleton key={idx} variant='rounded' height={52} sx={{ borderRadius: 2 }} />
          ))}

        {!historyLoading && history.length === 0 && (
          <Typography variant='body2' color='text.disabled' sx={{ px: 1, py: 1.5 }}>
            No saved conversations yet.
          </Typography>
        )}

        {history.map((item) => (
          <ListItemButton
            key={item.conversation_id}
            selected={item.conversation_id === conversationId}
            onClick={() => loadConversation(item.conversation_id)}
            sx={{
              borderRadius: 2,
              alignItems: 'flex-start',
              px: 1.25,
              py: 1,
            }}
          >
            <ListItemText
              primary={item.title || 'Untitled conversation'}
              secondary={item.summary || item.last_message || `${item.message_count} messages`}
              primaryTypographyProps={{
                variant: 'body2',
                fontWeight: 600,
                noWrap: true,
              }}
              secondaryTypographyProps={{
                variant: 'caption',
                color: 'text.disabled',
                sx: {
                  display: '-webkit-box',
                  WebkitLineClamp: 2,
                  WebkitBoxOrient: 'vertical',
                  overflow: 'hidden',
                },
              }}
            />
          </ListItemButton>
        ))}
      </List>
    </Stack>
  );
};
