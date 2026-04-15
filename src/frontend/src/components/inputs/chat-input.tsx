import { Alert, Box, Button, Stack, TextField, Tooltip } from '@mui/material';
import { Link as RouterLink } from 'react-router-dom';
import {
  useEffect,
  useRef,
  type CompositionEventHandler,
  type FC,
  type KeyboardEventHandler,
  type MouseEventHandler,
} from 'react';
import { useRefCallback } from '../../hooks';
import { useChat, useCluster } from '../../services';
import { IconArrowBackUp, IconArrowUp, IconSquareFilled } from '@tabler/icons-react';
import { DotPulse } from './dot-pulse';

const BudgetTokenLabel: FC<{ label: string; title: string }> = ({ label, title }) => (
  <Tooltip title={title} arrow>
    <Box
      component='span'
      sx={{
        display: 'inline-block',
        textDecoration: 'underline dotted',
        textUnderlineOffset: '0.16em',
        cursor: 'help',
        whiteSpace: 'nowrap',
      }}
    >
      {label}
    </Box>
  </Tooltip>
);

export const ChatInput: FC = () => {
  const [
    {
      config: { activeClusterId, clusterProfiles },
      clusterInfo: { status: clusterStatus },
      nodeInfoList,
    },
  ] = useCluster();
  const [{ input, status, inputTruncationNotice, requestHealthNotice, promptBudgetNotice }, { setInput, generate, stop, clear, registerInputFocus, startNewConversation }] = useChat();

  const compositionRef = useRef(false);
  const inputRef = useRef<HTMLTextAreaElement | null>(null);

  useEffect(() => {
    registerInputFocus(() => {
      inputRef.current?.focus();
    });
    return () => {
      registerInputFocus(null);
    };
  }, [registerInputFocus]);

  const onCompositionStart = useRefCallback<CompositionEventHandler>((e) => {
    compositionRef.current = true;
  });

  const onCompositionEnd = useRefCallback<CompositionEventHandler>((e) => {
    compositionRef.current = false;
  });

  const onKeyDown = useRefCallback<KeyboardEventHandler>((e) => {
    if (e.key === 'Enter' && !e.shiftKey && !compositionRef.current) {
      e.preventDefault();
      generate();
    }
  });

  const onClickMainButton = useRefCallback<MouseEventHandler>((e) => {
    if (status === 'opened' || status === 'generating') {
      stop();
    } else if (status === 'closed' || status === 'error') {
      generate();
    }
  });

  const onClickClearButton = useRefCallback<MouseEventHandler>((e) => {
    clear();
  });

  const activeCluster = clusterProfiles.find((item) => item.id === activeClusterId) || clusterProfiles[0];
  const activeNodes = nodeInfoList.filter((node) => node.status === 'available').length;
  const inactiveNodes = nodeInfoList.length - activeNodes;
  const clusterUnavailableNotice = activeCluster && clusterStatus !== 'available'
    ? {
      severity: (clusterStatus === 'failed' || clusterStatus === 'offline') ? 'error' as const : 'warning' as const,
      message:
        activeNodes > 0
          ? `${activeCluster.name} is not ready yet. ${activeNodes} up, ${inactiveNodes} down.`
          : `${activeCluster.name} has no available nodes. Start or reconnect nodes, switch cluster, or adjust the cluster configuration before sending a message.`,
    }
    : null;

  return (
    <Stack data-status={status} sx={{ gap: 1 }}>
      {clusterUnavailableNotice && (
        <Alert
          severity={clusterUnavailableNotice.severity}
          action={(
            <Button component={RouterLink} to='/settings/cluster' color='inherit' size='small'>
              Open cluster
            </Button>
          )}
        >
          {clusterUnavailableNotice.message}
        </Alert>
      )}
      {inputTruncationNotice?.truncated && (
        <Alert
          severity='warning'
          action={
            <Button color='inherit' size='small' onClick={startNewConversation}>
              New Chat
            </Button>
          }
        >
          Prompt was truncated to fit context: kept {inputTruncationNotice.keptPromptTokens} of {inputTruncationNotice.originalPromptTokens} input tokens.
          Your next prompt will continue this conversation unless you start a new chat.
        </Alert>
      )}
      {promptBudgetNotice && (
        <Alert severity='info' sx={{ '& .MuiAlert-message': { fontSize: '0.8125rem', lineHeight: 1.45 } }}>
          <Box sx={{ display: 'flex', flexWrap: 'wrap', columnGap: 0.5, rowGap: 0.25, alignItems: 'baseline' }}>
            <Box component='span'>Context budget: using about</Box>
            <BudgetTokenLabel
              label={`${promptBudgetNotice.estimatedInputTokens} input tokens`}
              title='Estimated total input tokens used by the final prompt that was actually assembled for this request.'
            />
            <Box component='span'>out of</Box>
            <BudgetTokenLabel
              label={`${promptBudgetNotice.inputBudgetTokens}`}
              title='Estimated maximum input-token budget available after reserving room for model output and prompt overhead.'
            />
            <Box component='span'>, keeping</Box>
            <BudgetTokenLabel
              label={`${promptBudgetNotice.recentMessagesCount} recent turns`}
              title='How many recent chat messages were kept verbatim in the prompt after fitting the context budget.'
            />
            <Box component='span'>and</Box>
            <BudgetTokenLabel
              label={`${promptBudgetNotice.memorySectionsCount} memory section${promptBudgetNotice.memorySectionsCount === 1 ? '' : 's'}`}
              title='How many older-memory blocks were included, such as the conversation summary and retrieved long-term snippets.'
            />
            <Box component='span'>.</Box>
            <BudgetTokenLabel
              label={`Summary: ${promptBudgetNotice.summaryTokens} tok`}
              title='Estimated tokens used by the compact summary of older conversation history.'
            />
            <Box component='span'>,</Box>
            <BudgetTokenLabel
              label={`memory snippets: ${promptBudgetNotice.snippetTokens} tok`}
              title='Estimated tokens used by retrieved older snippets pulled in from long-term memory for this request.'
            />
            <Box component='span'>,</Box>
            <BudgetTokenLabel
              label={`recent turns: ${promptBudgetNotice.recentTurnTokens} tok`}
              title='Estimated tokens used by the raw recent chat messages kept verbatim in the prompt.'
            />
            <Box component='span'>.</Box>
            {promptBudgetNotice.adaptedOutputBudget && (
              <>
                <BudgetTokenLabel
                  label={`Response budget was reduced from ${promptBudgetNotice.requestedOutputTokens} to ${promptBudgetNotice.adjustedOutputTokens} tokens`}
                  title='The backend lowered the allowed output length for this request so more conversation context could fit into the model context window.'
                />
                <Box component='span'>to preserve more context.</Box>
              </>
            )}
          </Box>
        </Alert>
      )}
      {requestHealthNotice && (
        <Alert severity={requestHealthNotice.severity} sx={{ '& .MuiAlert-message': { fontSize: '0.8125rem', lineHeight: 1.45 } }}>
          {requestHealthNotice.message}
        </Alert>
      )}
      {/* <Stack direction='row' sx={{ gap: 1, p: 1 }}>
        {modelName}
      </Stack> */}
      <TextField
        inputRef={inputRef}
        value={input}
        onChange={(event) => setInput(event.target.value)}
        multiline
        maxRows={4}
        placeholder='Ask anything'
        fullWidth
        onCompositionStart={onCompositionStart}
        onCompositionEnd={onCompositionEnd}
        onKeyDown={onKeyDown}
        slotProps={{
          input: {
            sx: {
              border: '1px solid',
              borderColor: 'grey.300',
              borderRadius: 2,
              fontSize: '0.95rem',
              boxShadow: '2px 2px 4px rgba(0,0,0,0.05)',
              flexDirection: 'column',
              '& textarea': {
                fontSize: '0.875rem',
                scrollbarWidth: 'none', // Firefox
                msOverflowStyle: 'none', // IE, Edge
                '&::-webkit-scrollbar': {
                  display: 'none', // Chrome, Safari
                },
              },
            },
            endAdornment: (
              <Stack direction='row' sx={{ alignSelf: 'flex-end', alignItems: 'center', gap: 2 }}>
                <Button
                  variant='text'
                  sx={{ color: 'text.secondary' }}
                  startIcon={<IconArrowBackUp />}
                  disabled={status === 'opened' || status === 'generating'}
                  onClick={onClickClearButton}
                >
                  Clear
                </Button>
                <Button
                  size='small'
                  color='primary'
                  disabled={clusterStatus !== 'available'}
                  // loading={status === 'opened'}
                  onClick={onClickMainButton}
                >
                  {status === 'opened' ?
                    <DotPulse size='medium' />
                  : status === 'generating' ?
                    <IconSquareFilled size='1.25rem' />
                  : <IconArrowUp size='1.25rem' />}
                </Button>
              </Stack>
            ),
          },
        }}
      />
    </Stack>
  );
};
