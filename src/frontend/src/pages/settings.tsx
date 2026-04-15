import { Button, Stack, Typography } from '@mui/material';
import { Link as RouterLink, useParams } from 'react-router-dom';
import { IconArrowLeft } from '@tabler/icons-react';
import { DrawerLayout, SettingsContent } from '../components/common';

export default function PageSettings() {
  const { section = 'cluster' } = useParams();
  return (
    <DrawerLayout contentWidth='wide' hideConversationHistory>
      <Stack sx={{ gap: 3, minHeight: 0 }}>
        <Stack sx={{ gap: 1.5 }}>
          <Button component={RouterLink} to='/chat' variant='text' startIcon={<IconArrowLeft size={16} />} sx={{ alignSelf: 'flex-start' }}>
            Back to chat
          </Button>
          <Typography variant='h1'>Settings</Typography>
        </Stack>
        <SettingsContent routeSection={section} />
      </Stack>
    </DrawerLayout>
  );
}
