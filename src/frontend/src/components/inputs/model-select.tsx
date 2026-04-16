import { useEffect, useState, type FC, type MouseEvent, type ReactNode } from 'react';
import * as motion from 'motion/react-client';
import {
  InputBase,
  MenuItem,
  OutlinedInput,
  Select,
  selectClasses,
  Stack,
  styled,
} from '@mui/material';
import { useCluster, useHost, type ModelInfo } from '../../services';
import { useRefCallback } from '../../hooks';
import { IconCheck, IconLoader } from '@tabler/icons-react';

const ModelSelectRoot = styled(Select)<{ ownerState: ModelSelectProps }>(({
  theme,
  ownerState,
}) => {
  const { spacing, typography, palette } = theme;
  const { variant = 'outlined' } = ownerState;

  return {
    height: variant === 'outlined' ? '4rem' : '1lh',
    paddingInline: spacing(0.5),
    borderRadius: 12,
    '&:hover': {
      backgroundColor: 'action.hover',
    },

    [`.${selectClasses.select}:hover`]: {
      backgroundColor: 'transparent',
    },

    ...(variant === 'text' && {
      ...typography.h3,
      fontWeight: typography.fontWeightMedium,
      [`.${selectClasses.select}`]: {
        fontSize: 'inherit',
        fontWeight: 'inherit',
        lineHeight: 'inherit',
        padding: 0,
      },
      '&:hover': { backgroundColor: 'transparent' },
    }),
  };
});

const ModelSelectOption = styled(MenuItem)(({ theme }) => ({
  height: '3.25rem',
  gap: theme.spacing(1),
  borderRadius: 10,
}));

const ValueRow = styled(Stack)(({ theme }) => ({
  flexDirection: 'row',
  alignItems: 'center',
  gap: theme.spacing(1),
  padding: theme.spacing(1),
  '&:hover': { backgroundColor: 'transparent' },
  pointerEvents: 'none',
}));

const ModelExtraStatus = styled(motion.div)(({ theme }) => ({
  width: '1rem',
  height: '1rem',
  '& > .tabler-icon': {
    width: '1rem',
    height: '1rem',
  },
}));

const ModelLogo = styled('img')(({ theme }) => ({
  width: '2.25rem',
  height: '2.25rem',
  borderRadius: '0.5rem',
  border: `1px solid ${theme.palette.divider}`,
  objectFit: 'cover',
}));

const ModelDisplayName = styled('span')(({ theme }) => ({
  ...theme.typography.subtitle2,
  fontSize: '0.875rem',
  lineHeight: '1.125rem',
  fontWeight: theme.typography.fontWeightLight,
  color: theme.palette.text.primary,
}));

const ModelName = styled('span')(({ theme }) => ({
  ...theme.typography.body2,
  fontSize: '0.75rem',
  lineHeight: '1rem',
  fontWeight: theme.typography.fontWeightLight,
  color: theme.palette.text.secondary,
}));

const ModelInfoColumn = styled(Stack)({
  minWidth: 0,
  flex: 1,
});

const ModelMemory = styled('span')(({ theme }) => ({
  ...theme.typography.caption,
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  minWidth: '3rem',
  padding: '0.125rem 0.45rem',
  borderRadius: 999,
  fontSize: '0.68rem',
  lineHeight: 1,
  fontWeight: theme.typography.fontWeightMedium,
  color: theme.palette.text.secondary,
  backgroundColor: theme.palette.grey[200],
  border: `1px solid ${theme.palette.divider}`,
  whiteSpace: 'nowrap',
}));

const ModelSourceBadge = styled('span')(({ theme }) => ({
  ...theme.typography.caption,
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  minWidth: '3.1rem',
  padding: '0.125rem 0.45rem',
  borderRadius: 999,
  fontSize: '0.68rem',
  lineHeight: 1,
  fontWeight: theme.typography.fontWeightMedium,
  color: theme.palette.info.dark,
  backgroundColor: 'rgba(25, 118, 210, 0.08)',
  border: `1px solid ${theme.palette.info.light}`,
  whiteSpace: 'nowrap',
}));

const NodeCounts = styled(Stack)(({ theme }) => ({
  flexDirection: 'row',
  alignItems: 'center',
  gap: theme.spacing(0.5),
  flexWrap: 'nowrap',
  cursor: 'pointer',
}));

const NodeCountBadge = styled('span')<{ ownerState: { tone: 'active' | 'inactive' } }>(
  ({ theme, ownerState }) => ({
    ...theme.typography.caption,
    display: 'inline-flex',
    alignItems: 'center',
    justifyContent: 'center',
    minWidth: '2.25rem',
    padding: '0.125rem 0.4rem',
    borderRadius: 999,
    fontSize: '0.68rem',
    lineHeight: 1,
    fontWeight: theme.typography.fontWeightMedium,
    whiteSpace: 'nowrap',
    color: ownerState.tone === 'active' ? theme.palette.success.dark : theme.palette.error.dark,
    backgroundColor: 'transparent',
    border: `1px solid ${
      ownerState.tone === 'active' ? theme.palette.success.main : theme.palette.error.main
    }`,
  }),
);

const formatRequiredMemory = (vram: number) => (vram > 0 ? `${vram} GB` : '');

const formatModelSource = ({ sourceType, custom }: Pick<ModelInfo, 'sourceType' | 'custom'>) => {
  if (sourceType === 'huggingface') return 'HF';
  if (sourceType === 'scheduler_root') return 'Local Root';
  if (sourceType === 'url') return 'URL';
  return custom ? 'Custom' : 'Built-in';
};

const renderOption = (
  { name, displayName, logoUrl, vram, sourceType, custom }: ModelInfo,
  {
    selected,
    loading,
    disabled,
    disabledReason,
  }: { selected?: boolean; loading?: boolean; disabled?: boolean; disabledReason?: string },
): ReactNode => (
  <ModelSelectOption key={name} value={name} disabled={disabled}>
    <ModelExtraStatus
      {...(loading && {
        animate: { rotate: 360 },
        transition: {
          repeat: Infinity,
          ease: 'linear',
          duration: 2,
        },
      })}
    >
      {(loading && <IconLoader />) || (selected && <IconCheck />)}
    </ModelExtraStatus>
    <ModelLogo src={logoUrl} />
    <ModelInfoColumn gap={0.125}>
      <ModelDisplayName>{displayName}</ModelDisplayName>
      <ModelName>
        {disabled && disabledReason ? disabledReason : name}
      </ModelName>
    </ModelInfoColumn>
    <ModelSourceBadge>{formatModelSource({ sourceType, custom })}</ModelSourceBadge>
    {vram > 0 && <ModelMemory>{formatRequiredMemory(vram)}</ModelMemory>}
  </ModelSelectOption>
);

export interface ModelSelectProps {
  variant?: 'outlined' | 'text';
  autoCommit?: boolean;
  showNodeCounts?: boolean;
  onNodeCountsClick?: () => void;
  capacityGb?: number;
}

export const ModelSelect: FC<ModelSelectProps> = ({
  variant = 'outlined',
  autoCommit = false,
  showNodeCounts = false,
  onNodeCountsClick,
  capacityGb = 0,
}) => {
  const [{ type: hostType }] = useHost();
  const [
    {
      config: { modelName: configModelName, modelInfoList },
      clusterInfo: { status: clusterStatus, modelName: clusterModelName },
      nodeInfoList,
    },
    {
      config: { setModelName },
      init,
    },
  ] = useCluster();

  // const [nodeDialog, { open: openDialog }] = useAlertDialog({
  //   titleIcon: <IconRestore />,
  //   title: 'Switch model',
  //   content: (
  //     <Typography variant='body2' color='text.secondary'>
  //       The current version of parallax only supports hosting one model at once. Switching the model
  //       will terminate your existing chat service. You can restart the current scheduler in your
  //       terminal. We will add node rebalancing and dynamic model allocation soon.
  //     </Typography>
  //   ),
  //   confirmLabel: 'Continue',
  // });

  const onChange = useRefCallback((e) => {
    // if (clusterStatus !== 'idle') {
    //   openDialog();
    //   return;
    // }
    if (autoCommit) {
      setCanAutoCommit(true);
    }
    setModelName(String(e.target.value));
  });

  const [canAutoCommit, setCanAutoCommit] = useState(false);
  const activeNodes = nodeInfoList.filter((node) => node.status === 'available').length;
  const inactiveNodes = nodeInfoList.length - activeNodes;
  const canEvaluateCapacity = capacityGb > 0;

  const handleNodeCountsClick = useRefCallback((event: MouseEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    onNodeCountsClick?.();
  });
  useEffect(() => {
    if (canAutoCommit && configModelName !== clusterModelName) {
      setCanAutoCommit(false);
      init();
    }
  }, [canAutoCommit, configModelName]);

  return (
    <>
      <Stack direction="row" alignItems="center" gap={1} sx={{ minWidth: 0 }}>
        <ModelSelectRoot
          ownerState={{ variant }}
          readOnly={hostType === 'node'}
          input={variant === 'outlined' ? <OutlinedInput /> : <InputBase />}
          value={configModelName}
          onChange={onChange}
          sx={{ minWidth: 0, flex: variant === 'outlined' ? 1 : 'none' }}
          renderValue={(value: unknown) => {
            const model = modelInfoList.find((m) => m.name === value);
            if (!model) return value as string;

            return variant === 'outlined' ?
                <ValueRow>
                  <ModelLogo src={model.logoUrl} />
                  <ModelInfoColumn gap={0.125}>
                    <ModelDisplayName>{model.displayName}</ModelDisplayName>
                    <Stack direction="row" alignItems="center" gap={0.75} sx={{ minWidth: 0 }}>
                      <ModelName>{model.name}</ModelName>
                      <ModelSourceBadge>{formatModelSource(model)}</ModelSourceBadge>
                    </Stack>
                  </ModelInfoColumn>
                  {model.vram > 0 && <ModelMemory>{formatRequiredMemory(model.vram)}</ModelMemory>}
                </ValueRow>
              : model.name;
          }}
          IconComponent={hostType === 'node' ? () => null : undefined}
        >
          {modelInfoList.map((model) => {
            const { name } = model;
            const selected = name === configModelName || name === clusterModelName;
            const loading =
              clusterStatus !== 'idle'
              && name === configModelName
              && configModelName !== clusterModelName;
            const disabledForCapacity = !selected && canEvaluateCapacity && model.vram > 0 && model.vram > capacityGb;
            const disabledReason = disabledForCapacity ? `Needs ${formatRequiredMemory(model.vram)}; assigned nodes provide ${formatRequiredMemory(capacityGb)}` : '';
            return renderOption(model, { selected, loading, disabled: disabledForCapacity, disabledReason });
          })}
        </ModelSelectRoot>
        {showNodeCounts && (
          <NodeCounts onClick={handleNodeCountsClick} role="button" aria-label="Open cluster settings">
            <NodeCountBadge ownerState={{ tone: 'active' }}>{activeNodes} up</NodeCountBadge>
            {inactiveNodes > 0 && (
              <NodeCountBadge ownerState={{ tone: 'inactive' }}>{inactiveNodes} down</NodeCountBadge>
            )}
          </NodeCounts>
        )}
      </Stack>

      {/* {nodeDialog} */}
    </>
  );
};
