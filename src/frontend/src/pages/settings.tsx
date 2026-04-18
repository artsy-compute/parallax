import { Button, Stack, Typography } from '@mui/material';
import { useNavigate, useParams } from 'react-router-dom';
import { IconArrowLeft } from '@tabler/icons-react';
import { DrawerLayout, SettingsContent } from '../components/common';

export default function PageSettings() {
  const navigate = useNavigate();
  const { section = 'cluster' } = useParams();
  return (
    <DrawerLayout contentWidth='wide' hideConversationHistory>
      <Stack sx={{ gap: 3, minHeight: 0 }}>
        <Stack sx={{ gap: 1.5 }}>
          <Button
            variant='text'
            startIcon={<IconArrowLeft size={16} />}
            onClick={() => {
              if (globalThis.history.length > 1) {
                navigate(-1);
                return;
              }
              navigate('/chat');
            }}
            sx={{ alignSelf: 'flex-start' }}
          >
            Back
          </Button>
          <Typography variant='h1'>Settings</Typography>
        </Stack>
        <SettingsContent routeSection={section} />
      </Stack>
    </DrawerLayout>
  );
}
