import { Button, Stack, Typography } from '@mui/material';
import { Link as RouterLink, useParams } from 'react-router-dom';
import { IconArrowLeft } from '@tabler/icons-react';
import { DrawerLayout, SettingsContent } from '../components/common';

export default function PageSettings() {
  const { section = 'cluster' } = useParams();
  return (
    <DrawerLayout contentWidth='wide' hideConversationHistory>
      <Stack sx={{ gap: 3, minHeight: 0 }}>
        <Stack direction='row' sx={{ alignItems: 'center', justifyContent: 'space-between', gap: 2 }}>
          <Stack sx={{ gap: 0.5 }}>
            <Typography variant='h3'>Settings</Typography>
            <Typography variant='body2' color='text.secondary'>
              Define the available node pool, then configure the active cluster with its model and startup capacity.
            </Typography>
          </Stack>
          <Button component={RouterLink} to='/chat' variant='text' startIcon={<IconArrowLeft size={16} />}>
            Back to chat
          </Button>
        </Stack>
        <SettingsContent routeSection={section} />
      </Stack>
    </DrawerLayout>
  );
}
