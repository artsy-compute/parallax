import { useEffect, useMemo, useRef, useState, type SyntheticEvent } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
  MenuItem,
  Paper,
  Stack,
  Tab,
  Tabs,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
  Typography,
} from '@mui/material';
import {
  IconDatabaseSearch,
  IconEye,
  IconFilePlus,
  IconLink,
  IconX,
  IconSourceCode,
  IconExternalLink,
  IconRefresh,
  IconTrash,
  IconUpload,
} from '@tabler/icons-react';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { DrawerLayout } from '../components/common';
import ChatMarkdown from '../components/inputs/chat-markdown';
import { AlertDialog } from '../components/mui';
import { useCluster } from '../services';
import {
  createKnowledgeLocalSource,
  deleteKnowledgePages,
  createKnowledgeUploadedSource,
  createKnowledgeUrlSource,
  deleteKnowledgeSource,
  getAppSettings,
  getKnowledgeDocument,
  getKnowledgeHealth,
  getKnowledgeJobs,
  getKnowledgePage,
  getKnowledgePages,
  getKnowledgeSources,
  generateKnowledgePages,
  lintKnowledgeWiki,
  regenerateKnowledgePage,
  searchKnowledge,
  updateAppSettings,
  type KnowledgeDocumentDetail,
  type KnowledgeHealth,
  type KnowledgeJob,
  type KnowledgePageDetail,
  type KnowledgePageSummary,
  type KnowledgeSearchResponse,
  type KnowledgeSourceSummary,
} from '../services/api';
import { useRefCallback } from '../hooks';
import {
  enabledLlmProviders,
  maskSecret,
  mergeAdvancedLlmSettings,
  parseKnowledgeGenerationConfig,
  parseLlmProviderConfigs,
  type KnowledgeGenerationConfig,
  type LlmProviderConfig,
  type LlmProviderId,
} from '../services/llm-providers';

type KnowledgeSection = 'wiki' | 'overview' | 'ingest' | 'search' | 'sources' | 'jobs' | 'settings';
type WikiViewMode = 'rendered' | 'source';
type PendingUploadFile = {
  id: string;
  file: File;
};

const KNOWLEDGE_SECTIONS: readonly KnowledgeSection[] = [
  'wiki',
  'overview',
  'ingest',
  'search',
  'sources',
  'jobs',
  'settings',
];

const MANAGEMENT_SECTIONS: readonly Exclude<KnowledgeSection, 'wiki'>[] = [
  'overview',
  'ingest',
  'search',
  'sources',
  'jobs',
  'settings',
];

const formatDateTime = (value: number) =>
  new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  }).format(new Date(value * 1000));

const getStatusColor = (status: string): 'default' | 'success' | 'warning' | 'error' | 'info' => {
  switch (status) {
    case 'ready':
    case 'completed':
      return 'success';
    case 'queued':
    case 'running':
      return 'info';
    case 'failed':
      return 'error';
    default:
      return 'default';
  }
};

const normalizeSection = (search: string): KnowledgeSection => {
  const value = new URLSearchParams(search).get('section');
  return KNOWLEDGE_SECTIONS.includes(value as KnowledgeSection)
    ? (value as KnowledgeSection)
    : 'wiki';
};

const escapeRegExp = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const KNOWLEDGE_UPLOAD_ACCEPT = '.txt,.md,.rst,.json,.toml,.yaml,.yml,.py,.ts,.tsx,.js,.jsx,.html,.xml,.pdf,.docx,.odt,.ods,.odp';

const formatFileSize = (value: number) => {
  if (!Number.isFinite(value) || value <= 0) {
    return '0 B';
  }
  const units = ['B', 'KB', 'MB', 'GB'];
  let size = value;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024;
    index += 1;
  }
  return `${size >= 10 || index === 0 ? size.toFixed(0) : size.toFixed(1)} ${units[index]}`;
};

const normalizeWikiMarkdown = (content: string, title?: string, summary?: string) => {
  let normalized = String(content || '').replace(/\r\n/g, '\n');
  const normalizedTitle = String(title || '').trim();
  const normalizedSummary = String(summary || '').trim();

  if (normalizedTitle) {
    const titlePattern = new RegExp(
      `^\\s*#\\s*${escapeRegExp(normalizedTitle)}\\s*\\n+`,
      'i',
    );
    normalized = normalized.replace(titlePattern, '');
  }

  if (normalizedSummary) {
    const summaryPattern = new RegExp(
      `^\\s*${escapeRegExp(normalizedSummary)}\\s*(\\n\\s*){1,2}`,
      'i',
    );
    normalized = normalized.replace(summaryPattern, '');
  }

  if (!/(^|\n)##\s+Overview\b/i.test(normalized)) {
    normalized = normalized
      .replace(/(^|\n)(Overview)\s*\n/gi, '\n## $2\n')
      .replace(/(^|\n)(Key Details)\s*\n/gi, '\n## $2\n')
      .replace(/(^|\n)(Important Notes)\s*\n/gi, '\n## $2\n');
  }
  return normalized.trim();
};

export default function PageKnowledge() {
  const location = useLocation();
  const navigate = useNavigate();
  const { pageId: pageIdParam = '' } = useParams<{ pageId?: string }>();
  const activeSection = normalizeSection(location.search);
  const legacySelectedPageParam = new URLSearchParams(location.search).get('page') || '';
  const selectedPageParam = String(pageIdParam || legacySelectedPageParam || '').trim();
  const [{ config: { clusterProfiles } }] = useCluster();

  const [health, setHealth] = useState<KnowledgeHealth | null>(null);
  const [sources, setSources] = useState<readonly KnowledgeSourceSummary[]>([]);
  const [jobs, setJobs] = useState<readonly KnowledgeJob[]>([]);
  const [pages, setPages] = useState<readonly KnowledgePageSummary[]>([]);
  const [homePageId, setHomePageId] = useState('');
  const [selectedPage, setSelectedPage] = useState<KnowledgePageDetail | null>(null);
  const [searchResults, setSearchResults] = useState<KnowledgeSearchResponse | null>(null);
  const [selectedDocument, setSelectedDocument] = useState<KnowledgeDocumentDetail | null>(null);
  const [localPath, setLocalPath] = useState('');
  const [urlValue, setUrlValue] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [searching, setSearching] = useState(false);
  const [ingestingLocal, setIngestingLocal] = useState(false);
  const [ingestingUrl, setIngestingUrl] = useState(false);
  const [uploadingDocuments, setUploadingDocuments] = useState(false);
  const [pendingUploadFiles, setPendingUploadFiles] = useState<readonly PendingUploadFile[]>([]);
  const [uploadDragActive, setUploadDragActive] = useState(false);
  const [documentLoading, setDocumentLoading] = useState(false);
  const [pageLoading, setPageLoading] = useState(false);
  const [generatingWiki, setGeneratingWiki] = useState(false);
  const [lintingWiki, setLintingWiki] = useState(false);
  const [deletingWikiPages, setDeletingWikiPages] = useState(false);
  const [lintReportMarkdown, setLintReportMarkdown] = useState('');
  const [regeneratingPage, setRegeneratingPage] = useState(false);
  const [wikiViewMode, setWikiViewMode] = useState<WikiViewMode>('rendered');
  const [deletingSourceId, setDeletingSourceId] = useState('');
  const [pendingDeleteSource, setPendingDeleteSource] = useState<null | { id: string; label: string }>(null);
  const [confirmDeleteWikiPages, setConfirmDeleteWikiPages] = useState(false);
  const [llmProviders, setLlmProviders] = useState<Record<LlmProviderId, LlmProviderConfig>>(
    parseLlmProviderConfigs(undefined),
  );
  const [knowledgeGenerationConfig, setKnowledgeGenerationConfig] = useState<KnowledgeGenerationConfig>(
    parseKnowledgeGenerationConfig(undefined, parseLlmProviderConfigs(undefined)),
  );
  const [savingGenerationConfig, setSavingGenerationConfig] = useState(false);
  const [error, setError] = useState('');
  const uploadInputRef = useRef<HTMLInputElement | null>(null);

  const loadPageData = useRefCallback(async () => {
    setRefreshing(true);
    try {
      const [nextHealth, nextSources, nextJobs, nextSettings, nextPages] = await Promise.all([
        getKnowledgeHealth(),
        getKnowledgeSources(),
        getKnowledgeJobs(),
        getAppSettings(),
        getKnowledgePages(),
      ]);
      setHealth(nextHealth);
      setSources(nextSources);
      setJobs(nextJobs);
      setPages(nextPages.items || []);
      setHomePageId(String(nextPages.home_page_id || ''));
      const providers = parseLlmProviderConfigs(nextSettings.cluster_settings.advanced);
      setLlmProviders(providers);
      setKnowledgeGenerationConfig(parseKnowledgeGenerationConfig(nextSettings.cluster_settings.advanced, providers));
      setError('');
    } catch (nextError) {
      setHealth(null);
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  });

  useEffect(() => {
    void loadPageData();
  }, [loadPageData]);

  useEffect(() => {
    if (!pageIdParam && legacySelectedPageParam && activeSection === 'wiki') {
      navigate(`/knowledge/${encodeURIComponent(legacySelectedPageParam)}`, { replace: true });
    }
  }, [activeSection, legacySelectedPageParam, navigate, pageIdParam]);

  useEffect(() => {
    if (activeSection !== 'wiki') {
      return;
    }
    const validSelected = selectedPageParam && pages.some((page) => page.id === selectedPageParam);
    if (validSelected) {
      return;
    }
    const nextPageId = String(homePageId || pages[0]?.id || '');
    if (!nextPageId) {
      setSelectedPage(null);
      return;
    }
    navigate(`/knowledge/${encodeURIComponent(nextPageId)}`, { replace: true });
  }, [activeSection, homePageId, navigate, pages, selectedPageParam]);

  useEffect(() => {
    const normalizedPageId = String(selectedPageParam || '').trim();
    if (!normalizedPageId) {
      setSelectedPage(null);
      return;
    }
    setWikiViewMode('rendered');
    let cancelled = false;
    setPageLoading(true);
    void (async () => {
      try {
        const detail = await getKnowledgePage(normalizedPageId);
        if (!cancelled) {
          setSelectedPage(detail);
          setError('');
        }
      } catch (nextError) {
        if (!cancelled) {
          setSelectedPage(null);
          setError(nextError instanceof Error ? nextError.message : String(nextError));
        }
      } finally {
        if (!cancelled) {
          setPageLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [selectedPageParam]);

  const onSearch = useRefCallback(async () => {
    const normalizedQuery = searchQuery.trim();
    if (!normalizedQuery) {
      setSearchResults(null);
      return;
    }
    setSearching(true);
    try {
      const nextResults = await searchKnowledge(normalizedQuery, 12);
      setSearchResults(nextResults);
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setSearching(false);
    }
  });

  const onIngestLocal = useRefCallback(async () => {
    const normalizedPath = localPath.trim();
    if (!normalizedPath) {
      return;
    }
    setIngestingLocal(true);
    try {
      await createKnowledgeLocalSource(normalizedPath);
      setLocalPath('');
      await loadPageData();
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setIngestingLocal(false);
    }
  });

  const onIngestUrl = useRefCallback(async () => {
    const normalizedUrl = urlValue.trim();
    if (!normalizedUrl) {
      return;
    }
    setIngestingUrl(true);
    try {
      await createKnowledgeUrlSource(normalizedUrl);
      setUrlValue('');
      await loadPageData();
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setIngestingUrl(false);
    }
  });

  const onOpenDocument = useRefCallback(async (documentId: string) => {
    setDocumentLoading(true);
    try {
      const detail = await getKnowledgeDocument(documentId);
      setSelectedDocument(detail);
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setDocumentLoading(false);
    }
  });

  const onDeleteSource = useRefCallback(async (sourceId: string) => {
    const normalizedSourceId = String(sourceId || '').trim();
    if (!normalizedSourceId) {
      return;
    }
    setDeletingSourceId(normalizedSourceId);
    try {
      await deleteKnowledgeSource(normalizedSourceId);
      if (selectedDocument?.source_id === normalizedSourceId) {
        setSelectedDocument(null);
      }
      setPendingDeleteSource(null);
      await loadPageData();
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setDeletingSourceId('');
    }
  });

  const confirmDeleteSource = useRefCallback(async () => {
    if (!pendingDeleteSource) {
      return;
    }
    await onDeleteSource(pendingDeleteSource.id);
  });

  const addPendingUploadFiles = useRefCallback((files: FileList | File[] | null) => {
    const nextFiles = files ? Array.from(files) : [];
    if (nextFiles.length <= 0) {
      return;
    }
    setPendingUploadFiles((prev) => {
      const seen = new Set(prev.map((item) => `${item.file.name}:${item.file.size}:${item.file.lastModified}`));
      const appended = [...prev];
      for (const file of nextFiles) {
        const key = `${file.name}:${file.size}:${file.lastModified}`;
        if (seen.has(key)) {
          continue;
        }
        seen.add(key);
        appended.push({
          id: `${key}:${crypto.randomUUID()}`,
          file,
        });
      }
      return appended;
    });
  });

  const onUploadDocuments = useRefCallback(async () => {
    if (pendingUploadFiles.length <= 0) {
      return;
    }
    const fileList = pendingUploadFiles.map((item) => item.file);
    setUploadingDocuments(true);
    try {
      for (const file of fileList) {
        await createKnowledgeUploadedSource(file);
      }
      setPendingUploadFiles([]);
      await loadPageData();
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setUploadingDocuments(false);
      if (uploadInputRef.current) {
        uploadInputRef.current.value = '';
      }
    }
  });

  const onChooseUploadFiles = useRefCallback((files: FileList | null) => {
    const fileList = files ? Array.from(files) : [];
    if (fileList.length <= 0) {
      return;
    }
    addPendingUploadFiles(fileList);
    if (uploadInputRef.current) {
      uploadInputRef.current.value = '';
    }
  });

  const removePendingUploadFile = useRefCallback((id: string) => {
    setPendingUploadFiles((prev) => prev.filter((item) => item.id !== id));
  });

  const onGenerateWiki = useRefCallback(async () => {
    setGeneratingWiki(true);
    try {
      const result = await generateKnowledgePages();
      await loadPageData();
      const nextHomePageId = String(result.home_page_id || result.pages?.home_page_id || '');
      if (nextHomePageId) {
        navigate(`/knowledge/${encodeURIComponent(nextHomePageId)}`);
        return;
      }
      navigate('/knowledge');
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setGeneratingWiki(false);
    }
  });

  const onRegeneratePage = useRefCallback(async () => {
    const normalizedPageId = String(selectedPageParam || '').trim();
    if (!normalizedPageId) {
      return;
    }
    setRegeneratingPage(true);
    try {
      const result = await regenerateKnowledgePage(normalizedPageId);
      await loadPageData();
      if (result.page?.id) {
        navigate(`/knowledge/${encodeURIComponent(result.page.id)}`, { replace: true });
      }
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setRegeneratingPage(false);
    }
  });

  const onLintWiki = useRefCallback(async () => {
    setLintingWiki(true);
    try {
      const result = await lintKnowledgeWiki();
      setLintReportMarkdown(String(result.report_markdown || '').trim());
      await loadPageData();
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setLintingWiki(false);
    }
  });

  const onDeleteWikiPages = useRefCallback(async () => {
    setDeletingWikiPages(true);
    try {
      const result = await deleteKnowledgePages();
      setLintReportMarkdown('');
      setSelectedPage(null);
      setPages(result.pages?.items || []);
      setHomePageId(String(result.pages?.home_page_id || ''));
      void loadPageData();
      setError('');
      if (activeSection === 'wiki') {
        navigate('/knowledge', { replace: true });
      }
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
      return { closeDialog: false };
    } finally {
      setDeletingWikiPages(false);
    }
  });

  const onSectionChange = useRefCallback((_event: SyntheticEvent, value: string) => {
    if (!KNOWLEDGE_SECTIONS.includes(value as KnowledgeSection)) {
      return;
    }
    navigate(`/knowledge?section=${value}`);
  });

  const onSaveGenerationConfig = useRefCallback(async () => {
    try {
      setSavingGenerationConfig(true);
      const payload = await getAppSettings();
      const clusterSettings = payload.cluster_settings || { model_name: '', init_nodes_num: 1, is_local_network: true, network_type: 'local' as const };
      const advanced = mergeAdvancedLlmSettings(clusterSettings.advanced, llmProviders, knowledgeGenerationConfig);
      await updateAppSettings({
        cluster_settings: {
          advanced,
        },
      });
      await loadPageData();
      setError('');
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : String(nextError));
    } finally {
      setSavingGenerationConfig(false);
    }
  });

  const summaryCards = useMemo(
    () => [
      { label: 'Sources', value: health?.counts.sources ?? sources.length, hint: 'Tracked inputs across workspace paths, uploads, and URLs' },
      { label: 'Documents', value: health?.counts.documents ?? 0, hint: 'Normalized documents extracted into the KB' },
      { label: 'Chunks', value: health?.counts.chunks ?? 0, hint: 'Searchable retrieval units embedded for RAG' },
      { label: 'Jobs', value: health?.counts.jobs ?? jobs.length, hint: 'Recent ingest and indexing operations' },
    ],
    [health, jobs.length, sources.length],
  );
  const enabledProviders = useMemo(() => enabledLlmProviders(llmProviders), [llmProviders]);
  const selectedGenerationProvider = llmProviders[knowledgeGenerationConfig.provider];
  const renderIngestSection = () => (
    <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default' }}>
      <Stack sx={{ gap: 2 }}>
        <Stack sx={{ gap: 0.5 }}>
          <Typography variant='h3'>Ingest</Typography>
          <Typography variant='body2' color='text.secondary'>
            Ingest explicit workspace paths, uploaded documents, or URLs. PDF, DOCX, and OpenDocument text formats are supported. OCR is not enabled yet.
          </Typography>
        </Stack>
        <Stack sx={{ gap: 1.25 }}>
          <TextField
            label='Workspace path'
            value={localPath}
            onChange={(event) => setLocalPath(event.target.value)}
            placeholder='docs, README.md, src/backend'
            fullWidth
          />
          <Button
            variant='contained'
            startIcon={<IconFilePlus size={16} />}
            onClick={() => {
              void onIngestLocal();
            }}
            disabled={ingestingLocal || !localPath.trim()}
          >
            Ingest local content
          </Button>
        </Stack>
        <Stack sx={{ gap: 1.25 }}>
          <TextField
            label='URL'
            value={urlValue}
            onChange={(event) => setUrlValue(event.target.value)}
            placeholder='https://example.com/article'
            fullWidth
          />
          <Button
            variant='contained'
            color='secondary'
            startIcon={<IconLink size={16} />}
            onClick={() => {
              void onIngestUrl();
            }}
            disabled={ingestingUrl || !urlValue.trim()}
          >
            Ingest URL
          </Button>
        </Stack>
        <Paper
          variant='outlined'
          sx={{
            p: 1.5,
            borderRadius: 2.5,
            borderStyle: 'dashed',
            borderWidth: uploadDragActive ? 2 : 1,
            borderColor: uploadDragActive ? 'primary.main' : 'divider',
            bgcolor: 'background.paper',
            transition: 'border-color 120ms ease, background-color 120ms ease',
            ...(uploadDragActive ? { bgcolor: 'action.hover' } : null),
          }}
          onDragEnter={(event) => {
            event.preventDefault();
            event.stopPropagation();
            setUploadDragActive(true);
          }}
          onDragOver={(event) => {
            event.preventDefault();
            event.stopPropagation();
            if (!uploadDragActive) {
              setUploadDragActive(true);
            }
          }}
          onDragLeave={(event) => {
            event.preventDefault();
            event.stopPropagation();
            const relatedTarget = event.relatedTarget as Node | null;
            if (relatedTarget && event.currentTarget.contains(relatedTarget)) {
              return;
            }
            setUploadDragActive(false);
          }}
          onDrop={(event) => {
            event.preventDefault();
            event.stopPropagation();
            setUploadDragActive(false);
            addPendingUploadFiles(event.dataTransfer.files);
          }}
        >
          <input
            ref={uploadInputRef}
            type='file'
            multiple
            hidden
            accept={KNOWLEDGE_UPLOAD_ACCEPT}
            onChange={(event) => {
              onChooseUploadFiles(event.target.files);
            }}
          />
          <Stack sx={{ gap: 1.25 }}>
            <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
              <Stack sx={{ gap: 0.4 }}>
                <Typography variant='body1' sx={{ fontWeight: 700 }}>
                  Uploaded docs
                </Typography>
                <Typography variant='body2' color='text.secondary'>
                  Drag files here or choose them manually. Review the list before upload. PDF, DOCX, and OpenDocument files are supported. OCR is not enabled yet.
                </Typography>
              </Stack>
              <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
                <Button
                  variant='outlined'
                  startIcon={<IconFilePlus size={16} />}
                  disabled={uploadingDocuments}
                  onClick={() => {
                    uploadInputRef.current?.click();
                  }}
                >
                  Add files
                </Button>
                <Button
                  variant='contained'
                  startIcon={<IconUpload size={16} />}
                  disabled={uploadingDocuments || pendingUploadFiles.length === 0}
                  onClick={() => {
                    void onUploadDocuments();
                  }}
                >
                  {uploadingDocuments ? 'Uploading...' : `Upload ${pendingUploadFiles.length || ''}`.trim()}
                </Button>
              </Stack>
            </Stack>
            <Stack sx={{ gap: 0.4 }}>
              <Typography variant='body2' sx={{ fontWeight: 600 }}>
                Drop zone
              </Typography>
              <Typography variant='caption' color='text.secondary'>
                Supported: TXT, Markdown, JSON, HTML/XML, PDF, DOCX, ODT, ODS, ODP
              </Typography>
            </Stack>
            {pendingUploadFiles.length > 0 ? (
              <Stack sx={{ gap: 0.75 }}>
                <Typography variant='body2' sx={{ fontWeight: 600 }}>
                  Ready to upload
                </Typography>
                {pendingUploadFiles.map((item) => (
                  <Paper key={item.id} variant='outlined' sx={{ p: 1, borderRadius: 2, bgcolor: 'background.default' }}>
                    <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'center', gap: 1 }}>
                      <Stack sx={{ minWidth: 0, gap: 0.2 }}>
                        <Typography variant='body2' sx={{ fontWeight: 600 }} noWrap>
                          {item.file.name}
                        </Typography>
                        <Typography variant='caption' color='text.secondary'>
                          {formatFileSize(item.file.size)}
                        </Typography>
                      </Stack>
                      <Tooltip title='Remove from upload list'>
                        <span>
                          <IconButton
                            size='small'
                            onClick={() => removePendingUploadFile(item.id)}
                            disabled={uploadingDocuments}
                          >
                            <IconX size={15} />
                          </IconButton>
                        </span>
                      </Tooltip>
                    </Stack>
                  </Paper>
                ))}
              </Stack>
            ) : (
              <Typography variant='body2' color='text.secondary'>
                No files selected yet.
              </Typography>
            )}
          </Stack>
        </Paper>
      </Stack>
    </Paper>
  );

  const renderSearchSection = () => (
    <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default', minHeight: 0 }}>
      <Stack sx={{ gap: 1.5, minHeight: 0 }}>
        <Stack sx={{ gap: 0.5 }}>
          <Typography variant='h3'>Search</Typography>
          <Typography variant='body2' color='text.secondary'>
            Hybrid retrieval combines lexical recall with embeddings-backed semantic search. Results cite the document and source they came from.
          </Typography>
        </Stack>
        <Stack direction={{ xs: 'column', sm: 'row' }} sx={{ gap: 1 }}>
          <TextField
            fullWidth
            label='Search knowledge'
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                event.preventDefault();
                void onSearch();
              }
            }}
            placeholder='Search indexed docs and URLs'
          />
          <Button
            variant='contained'
            startIcon={<IconDatabaseSearch size={16} />}
            onClick={() => {
              void onSearch();
            }}
            disabled={searching || !searchQuery.trim()}
          >
            Search
          </Button>
        </Stack>
        <Stack sx={{ gap: 1, minHeight: 0, overflowY: 'auto', pr: 0.25 }}>
          {!searchResults && !searching && (
            <Typography variant='body2' color='text.secondary'>
              No search query yet.
            </Typography>
          )}
          {searchResults && searchResults.items.length === 0 && (
            <Typography variant='body2' color='text.secondary'>
              No knowledge results found for &ldquo;{searchResults.query}&rdquo;.
            </Typography>
          )}
          {searchResults?.items.map((item) => (
            <Paper key={item.chunk_id} variant='outlined' sx={{ p: 1.5, borderRadius: 2.5, bgcolor: 'background.paper' }}>
              <Stack sx={{ gap: 0.75 }}>
                <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'flex-start', gap: 1, flexWrap: 'wrap' }}>
                  <Stack sx={{ gap: 0.25 }}>
                    <Typography variant='body1' sx={{ fontWeight: 700 }}>
                      {item.document_title}
                    </Typography>
                    <Typography variant='caption' color='text.secondary'>
                      {item.source_title || item.canonical_uri}
                    </Typography>
                  </Stack>
                  <Stack direction='row' sx={{ gap: 0.75, alignItems: 'center', flexWrap: 'wrap' }}>
                    <Chip size='small' label={item.source_type === 'url' ? 'URL' : 'Workspace'} />
                    <Button
                      size='small'
                      variant='text'
                      onClick={() => {
                        void onOpenDocument(item.document_id);
                      }}
                      disabled={documentLoading}
                    >
                      Open document
                    </Button>
                  </Stack>
                </Stack>
                <Typography variant='body2' color='text.secondary'>
                  {item.snippet}
                </Typography>
                <Typography variant='caption' color='text.disabled'>
                  {item.document_uri}
                </Typography>
              </Stack>
            </Paper>
          ))}
        </Stack>
      </Stack>
    </Paper>
  );

  const renderSourcesSection = () => (
    <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default', minHeight: 0 }}>
      <Stack sx={{ gap: 1.5, minHeight: 0 }}>
        <Stack sx={{ gap: 0.5 }}>
          <Typography variant='h3'>Sources</Typography>
          <Typography variant='body2' color='text.secondary'>
            Explicitly ingested workspace paths, uploaded documents, and URLs tracked under the current workspace-scoped knowledge store.
          </Typography>
        </Stack>
        <Stack sx={{ gap: 1, minHeight: 0, overflowY: 'auto', pr: 0.25 }}>
          {!loading && sources.length === 0 && (
            <Typography variant='body2' color='text.secondary'>
              No sources ingested yet.
            </Typography>
          )}
          {sources.map((source) => (
            <Paper key={source.id} variant='outlined' sx={{ p: 1.5, borderRadius: 2.5, bgcolor: 'background.paper' }}>
              <Stack sx={{ gap: 0.75 }}>
                <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'flex-start', gap: 1, flexWrap: 'wrap' }}>
                  <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'flex-start', gap: 1, width: '100%' }}>
                    <Stack sx={{ gap: 0.25, minWidth: 0, flex: 1 }}>
                      <Typography variant='body1' sx={{ fontWeight: 700 }}>
                        {source.title}
                      </Typography>
                      <Stack direction='row' sx={{ alignItems: 'center', gap: 0.25, minWidth: 0 }}>
                        <Typography variant='caption' color='text.secondary' sx={{ minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                          {source.canonical_uri}
                        </Typography>
                        {source.source_type === 'url' && source.canonical_uri && (
                          <Tooltip title='Open source URL'>
                            <span>
                              <IconButton
                                size='small'
                                onClick={() => {
                                  globalThis.open(source.canonical_uri, '_blank', 'noopener,noreferrer');
                                }}
                                sx={{ p: 0.25 }}
                              >
                                <IconExternalLink size={14} />
                              </IconButton>
                            </span>
                          </Tooltip>
                        )}
                      </Stack>
                    </Stack>
                    <Stack direction='row' sx={{ gap: 0.25, alignItems: 'center', flex: 'none' }}>
                      <Chip size='small' color={getStatusColor(source.status)} label={source.status} />
                      <Tooltip title={deletingSourceId === source.id ? 'Deleting source' : 'Delete source'}>
                        <span>
                          <IconButton
                            size='small'
                            color='error'
                            disabled={deletingSourceId === source.id}
                            onClick={() => {
                              setPendingDeleteSource({
                                id: source.id,
                                label: source.title || source.canonical_uri || 'this source',
                              });
                            }}
                          >
                            <IconTrash size={16} />
                          </IconButton>
                        </span>
                      </Tooltip>
                    </Stack>
                  </Stack>
                </Stack>
                <Typography variant='body2' color='text.secondary'>
                  {source.document_count} document{source.document_count === 1 ? '' : 's'} indexed
                </Typography>
                {source.last_error && (
                  <Typography variant='caption' color='error.main'>
                    {source.last_error}
                  </Typography>
                )}
                <Typography variant='caption' color='text.disabled'>
                  Updated {formatDateTime(source.updated_at)}
                </Typography>
              </Stack>
            </Paper>
          ))}
        </Stack>
      </Stack>
    </Paper>
  );

  const renderJobsSection = () => (
    <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default', minHeight: 0 }}>
      <Stack sx={{ gap: 1.5, minHeight: 0 }}>
        <Stack sx={{ gap: 0.5 }}>
          <Typography variant='h3'>Jobs</Typography>
          <Typography variant='body2' color='text.secondary'>
            Recent ingest and indexing jobs persisted by the knowledge service.
          </Typography>
        </Stack>
        <Stack sx={{ gap: 1, minHeight: 0, overflowY: 'auto', pr: 0.25 }}>
          {!loading && jobs.length === 0 && (
            <Typography variant='body2' color='text.secondary'>
              No jobs recorded yet.
            </Typography>
          )}
          {jobs.map((job) => (
            <Paper key={job.id} variant='outlined' sx={{ p: 1.5, borderRadius: 2.5, bgcolor: 'background.paper' }}>
              <Stack sx={{ gap: 0.6 }}>
                <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'flex-start', gap: 1, flexWrap: 'wrap' }}>
                  <Typography variant='body1' sx={{ fontWeight: 700 }}>
                    {job.job_type}
                  </Typography>
                  <Chip size='small' color={getStatusColor(job.status)} label={job.status} />
                </Stack>
                <Typography variant='body2' color='text.secondary'>
                  {job.summary}
                </Typography>
                {job.error && (
                  <Typography variant='caption' color='error.main'>
                    {job.error}
                  </Typography>
                )}
                <Typography variant='caption' color='text.disabled'>
                  Updated {formatDateTime(job.updated_at)}
                </Typography>
              </Stack>
            </Paper>
          ))}
        </Stack>
      </Stack>
    </Paper>
  );

  const renderWikiSection = () => (
    <Paper
      variant='outlined'
      sx={{
        borderRadius: 3,
        bgcolor: 'background.default',
        minHeight: 0,
        height: '100%',
        flex: 1,
        display: 'flex',
        overflow: 'hidden',
      }}
    >
      <Stack sx={{ gap: 0, minHeight: 0, height: '100%' }}>
        {sources.length === 0 && (
          <Alert severity='info' sx={{ mx: 2, mb: 2 }}>
            Ingest one or more sources before generating wiki pages.
          </Alert>
        )}
        {sources.length > 0 && pages.length === 0 && !generatingWiki && (
          <Alert severity='info' sx={{ mx: 2, mb: 2 }}>
            No wiki pages generated yet. Use <strong>Generate wiki</strong> to create a homepage and child pages from the current sources.
          </Alert>
        )}
        {pages.length > 0 && (
          <Box
            sx={{
              p: { xs: 1.5, lg: 2.5 },
              bgcolor: 'background.paper',
              minHeight: 0,
              flex: 1,
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'flex-start',
              alignItems: 'stretch',
              borderTop: '1px solid',
              borderColor: 'divider',
              overflowY: 'auto',
            }}
          >
              {pageLoading && (
                <Typography variant='body2' color='text.secondary'>
                  Loading page…
                </Typography>
              )}
              {!pageLoading && selectedPage && (
                <Stack
                  sx={{
                    gap: 1.25,
                    flex: 1,
                    justifyContent: 'flex-start',
                    alignItems: 'stretch',
                    alignSelf: 'stretch',
                  }}
                >
                  <Stack
                    direction='row'
                    sx={{
                      position: 'sticky',
                      top: 0,
                      zIndex: 10,
                      width: '100%',
                      justifyContent: 'flex-end',
                      alignItems: 'flex-start',
                      pb: 1,
                      mb: 0.25,
                      bgcolor: 'transparent',
                      maxWidth: '100%',
                    }}
                  >
                    <Stack
                      direction='row'
                      sx={{
                        gap: 1,
                        alignItems: 'center',
                        flex: 'none',
                        flexWrap: 'wrap',
                        justifyContent: 'flex-end',
                        ml: 'auto',
                        px: 1,
                        py: 0.75,
                        borderRadius: 2,
                        border: '1px solid',
                        borderColor: 'divider',
                        bgcolor: 'background.default',
                        boxShadow: '0 2px 10px rgba(0,0,0,0.04)',
                        width: 'fit-content',
                        maxWidth: 'calc(100% - 0.5rem)',
                        alignSelf: 'flex-end',
                      }}
                    >
                      {selectedPage.source_id && (
                        <Tooltip title='Regenerate this page from its source'>
                          <IconButton
                            size='small'
                            onClick={() => {
                              void onRegeneratePage();
                            }}
                            disabled={regeneratingPage}
                            sx={{
                              p: 0.5,
                              borderRadius: 1.5,
                              color: 'text.secondary',
                              '&:hover': {
                                bgcolor: 'action.hover',
                              },
                            }}
                          >
                            <IconRefresh size={15} />
                          </IconButton>
                        </Tooltip>
                      )}
                      <ToggleButtonGroup
                        size='small'
                        exclusive
                        value={wikiViewMode}
                        onChange={(_event, value: WikiViewMode | null) => {
                          if (value) {
                            setWikiViewMode(value);
                          }
                        }}
                        sx={{
                          flex: 'none',
                          display: 'inline-flex',
                          maxWidth: '100%',
                          '& .MuiToggleButton-root': {
                            minWidth: '2.25rem',
                            px: 0.45,
                            py: 0.25,
                            fontSize: '0.68rem',
                            lineHeight: 1.1,
                            textTransform: 'none',
                          },
                        }}
                      >
                        <ToggleButton value='rendered' aria-label='Rendered markdown'>
                          <IconEye size={14} />
                        </ToggleButton>
                        <ToggleButton value='source' aria-label='Source view'>
                          <IconSourceCode size={14} />
                        </ToggleButton>
                      </ToggleButtonGroup>
                    </Stack>
                  </Stack>
                  <Stack
                    direction={{ xs: 'column', md: 'row' }}
                    sx={{
                      gap: 1,
                      justifyContent: 'space-between',
                      alignItems: { md: 'flex-start' },
                    }}
                  >
                    <Stack sx={{ gap: 0.5, minWidth: 0 }}>
                      <Typography variant='h2'>{selectedPage.title}</Typography>
                      <Typography variant='caption' color='text.secondary'>
                        {Math.abs((selectedPage.updated_at || 0) - (selectedPage.created_at || 0)) < 1
                          ? 'Generated'
                          : 'Regenerated'} {formatDateTime(selectedPage.updated_at || selectedPage.created_at || 0)}
                      </Typography>
                    </Stack>
                  </Stack>
                  {wikiViewMode === 'rendered' ? (
                    <Box sx={{ minWidth: 0 }}>
                      <ChatMarkdown
                        content={normalizeWikiMarkdown(
                          selectedPage.content,
                          selectedPage.title,
                          selectedPage.summary,
                        )}
                      />
                    </Box>
                  ) : (
                    <Box
                      component='pre'
                      sx={{
                        m: 0,
                        p: 0,
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-word',
                        fontFamily: 'monospace',
                        fontSize: '0.92rem',
                        lineHeight: 1.7,
                        maxWidth: '100%',
                      }}
                    >
                      {selectedPage.content}
                    </Box>
                  )}
                </Stack>
              )}
              {!pageLoading && !selectedPage && (
                <Typography variant='body2' color='text.secondary'>
                  Select a page from the sidebar tree.
                </Typography>
              )}
          </Box>
        )}
      </Stack>
    </Paper>
  );

  const renderSettingsSection = () => (
    <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default' }}>
      <Stack sx={{ gap: 1.5 }}>
        <Stack sx={{ gap: 0.5 }}>
          <Typography variant='h3'>Settings</Typography>
          <Typography variant='body2' color='text.secondary'>
            Choose which configured provider Knowledge should use for future wiki-page generation. Provider credentials stay in global Settings.
          </Typography>
        </Stack>
        {enabledProviders.length === 0 ? (
          <Alert severity='warning'>
            No LLM providers are enabled. Configure one in Settings before adding wiki-page generation.
          </Alert>
        ) : (
          <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1, alignItems: { md: 'flex-start' } }}>
            <TextField
              select
              label='Page generation provider'
              value={knowledgeGenerationConfig.provider}
              onChange={(event) => {
                const nextProvider = event.target.value as LlmProviderId;
                setKnowledgeGenerationConfig({
                  provider: nextProvider,
                  model: llmProviders[nextProvider]?.defaultModel || '',
                });
              }}
              sx={{ width: { xs: '100%', md: '50%' } }}
            >
              {enabledProviders.map((provider) => (
                <MenuItem key={provider.id} value={provider.id}>
                  {provider.displayName}
                </MenuItem>
              ))}
            </TextField>
          </Stack>
        )}
        {selectedGenerationProvider && selectedGenerationProvider.id !== 'local_cluster' && (
          <>
            <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
              <TextField
                size='small'
                label='Default model'
                value={selectedGenerationProvider.defaultModel || 'Not configured'}
                fullWidth
                InputProps={{ readOnly: true }}
              />
              <TextField
                size='small'
                label='API key'
                value={maskSecret(selectedGenerationProvider.apiKey)}
                fullWidth
                InputProps={{ readOnly: true }}
              />
            </Stack>
            <Stack direction='row' sx={{ justifyContent: 'flex-start', gap: 1 }}>
              <Button
                variant='contained'
                onClick={() => {
                  void onSaveGenerationConfig();
                }}
                disabled={savingGenerationConfig}
              >
                {savingGenerationConfig ? 'Saving...' : 'Save'}
              </Button>
              <Button variant='outlined' onClick={() => navigate('/settings/llm-providers')}>
                Manage providers
              </Button>
            </Stack>
          </>
        )}
        {knowledgeGenerationConfig.provider === 'local_cluster' && (
          <Stack sx={{ gap: 1 }}>
            <Typography variant='body2' sx={{ fontWeight: 600 }}>
              Local cluster options
            </Typography>
            <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1 }}>
              <TextField
                select
                size='small'
                label='Routing'
                value={llmProviders.local_cluster.useActiveCluster ? 'active' : 'specific'}
                onChange={(event) => setLlmProviders((prev) => ({
                  ...prev,
                  local_cluster: {
                    ...prev.local_cluster,
                    useActiveCluster: event.target.value !== 'specific',
                  },
                }))}
                fullWidth
              >
                <MenuItem value='active'>Use active cluster</MenuItem>
                <MenuItem value='specific'>Pin a specific cluster</MenuItem>
              </TextField>
              <TextField
                select
                size='small'
                label='Cluster'
                value={llmProviders.local_cluster.clusterId}
                onChange={(event) => setLlmProviders((prev) => ({
                  ...prev,
                  local_cluster: {
                    ...prev.local_cluster,
                    clusterId: String(event.target.value || ''),
                  },
                }))}
                fullWidth
                disabled={llmProviders.local_cluster.useActiveCluster}
              >
                {clusterProfiles.map((profile) => (
                  <MenuItem key={profile.id} value={profile.id}>
                    {profile.name}
                  </MenuItem>
                ))}
              </TextField>
              <TextField
                size='small'
                label='Model override'
                value={llmProviders.local_cluster.defaultModel}
                onChange={(event) => setLlmProviders((prev) => ({
                  ...prev,
                  local_cluster: { ...prev.local_cluster, defaultModel: event.target.value },
                }))}
                placeholder='Optional'
                fullWidth
              />
            </Stack>
            <Stack direction='row' sx={{ justifyContent: 'flex-start' }}>
              <Button
                variant='contained'
                onClick={() => {
                  void onSaveGenerationConfig();
                }}
                disabled={savingGenerationConfig}
              >
                {savingGenerationConfig ? 'Saving...' : 'Save'}
              </Button>
            </Stack>
          </Stack>
        )}
        {selectedGenerationProvider && (
          <Typography variant='caption' color='text.secondary'>
            {selectedGenerationProvider.id === 'local_cluster'
              ? (
                selectedGenerationProvider.useActiveCluster
                  ? 'Local generation follows the currently active cluster.'
                  : `Local generation is pinned to cluster ${selectedGenerationProvider.clusterId || 'not set'}.`
              )
              : (
                selectedGenerationProvider.baseUrl
                  ? `Requests will be routed through ${selectedGenerationProvider.baseUrl}.`
                  : 'Provider routing will use the configured default endpoint.'
              )}{selectedGenerationProvider.defaultModel ? ` Default model: ${selectedGenerationProvider.defaultModel}.` : ''}
          </Typography>
        )}
      </Stack>
    </Paper>
  );

  return (
    <DrawerLayout contentWidth={activeSection === 'wiki' ? 'full' : 'wide'}>
      <Stack sx={{ minHeight: 0, height: '100%', flex: 1, gap: 2.5 }}>
        {activeSection !== 'wiki' && (
          <Stack direction='row' sx={{ justifyContent: 'flex-end', alignItems: 'flex-start', gap: 2, flexWrap: 'wrap' }}>
            <Button
              variant='outlined'
              onClick={() => {
                void loadPageData();
              }}
              disabled={refreshing}
            >
              Refresh
            </Button>
          </Stack>
        )}

        {activeSection === 'wiki' ? (
          renderWikiSection()
        ) : (
          <Paper variant='outlined' sx={{ borderRadius: 3, bgcolor: 'background.default', overflow: 'hidden' }}>
            <Tabs
              value={activeSection}
              onChange={onSectionChange}
              variant='scrollable'
              allowScrollButtonsMobile
              sx={{ px: 1.5, pt: 1 }}
            >
              {MANAGEMENT_SECTIONS.map((section) => (
                <Tab
                  key={section}
                  value={section}
                  label={section.charAt(0).toUpperCase() + section.slice(1)}
                />
              ))}
            </Tabs>
            <Box sx={{ p: 2 }}>
              {activeSection === 'overview' && (
              <Stack sx={{ gap: 2 }}>
                {error && (
                  <Alert severity='warning'>
                    {error}
                  </Alert>
                )}

                <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.paper' }}>
                  <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1.5, alignItems: { md: 'center' }, justifyContent: 'space-between' }}>
                    <Stack sx={{ gap: 0.35 }}>
                      <Typography variant='body1' sx={{ fontWeight: 700 }}>
                        Wiki maintenance
                      </Typography>
                      <Typography variant='body2' color='text.secondary'>
                        Run whole-wiki maintenance here. Use the reader for single-page regeneration.
                      </Typography>
                    </Stack>
                    <Stack direction='row' sx={{ gap: 1, flexWrap: 'wrap' }}>
                      <Button
                        variant='outlined'
                        onClick={() => {
                          void onLintWiki();
                        }}
                        disabled={lintingWiki}
                      >
                        {lintingWiki ? 'Running lint...' : 'Run wiki lint'}
                      </Button>
                      <Button
                        variant='contained'
                        onClick={() => {
                          void onGenerateWiki();
                        }}
                        disabled={generatingWiki || sources.length === 0}
                      >
                        {generatingWiki ? 'Generating...' : (pages.length > 0 ? 'Regenerate all pages' : 'Generate wiki')}
                      </Button>
                    </Stack>
                  </Stack>
                </Paper>

                {lintReportMarkdown && (
                  <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.paper' }}>
                    <Stack sx={{ gap: 1 }}>
                      <Typography variant='body1' sx={{ fontWeight: 700 }}>
                        Latest lint report
                      </Typography>
                      <Box sx={{ minWidth: 0 }}>
                        <ChatMarkdown content={lintReportMarkdown} />
                      </Box>
                    </Stack>
                  </Paper>
                )}

                {!error && health && (
                  <Alert severity='info'>
                    KB service reachable. Workspace <strong>{health.workspace_id}</strong> is using <strong>{health.embeddings.active_provider || health.embeddings.configured_provider}</strong> with <strong>{health.vector_backend || 'none'}</strong> vector search.
                  </Alert>
                )}

                <Box
                  sx={{
                    display: 'grid',
                    gridTemplateColumns: { xs: '1fr', md: 'repeat(4, minmax(0, 1fr))' },
                    gap: 1.5,
                  }}
                >
                  {summaryCards.map((card) => (
                    <Paper key={card.label} variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.paper' }}>
                      <Stack sx={{ gap: 0.5 }}>
                        <Typography variant='caption' color='text.secondary'>
                          {card.label}
                        </Typography>
                        <Typography variant='h2'>{card.value}</Typography>
                        <Typography variant='body2' color='text.secondary'>
                          {card.hint}
                        </Typography>
                      </Stack>
                    </Paper>
                  ))}
                </Box>

                <Paper
                  variant='outlined'
                  sx={{
                    p: 2,
                    borderRadius: 3,
                    borderColor: 'divider',
                    bgcolor: 'rgba(120, 53, 15, 0.04)',
                  }}
                >
                  <Stack direction={{ xs: 'column', md: 'row' }} sx={{ gap: 1.5, alignItems: { md: 'center' }, justifyContent: 'space-between' }}>
                    <Stack sx={{ gap: 0.35 }}>
                      <Typography variant='body1' sx={{ fontWeight: 700, color: 'text.primary' }}>
                        Delete wiki
                      </Typography>
                      <Typography variant='body2' color='text.secondary'>
                        Remove generated wiki pages and wiki log entries. Sources and ingested knowledge documents stay intact.
                      </Typography>
                    </Stack>
                    <Button
                      color='error'
                      variant='contained'
                      startIcon={<IconTrash size={16} />}
                      disabled={deletingWikiPages || pages.length === 0}
                      sx={{
                        bgcolor: 'transparent',
                        color: 'error.main',
                        boxShadow: 'none',
                        '&:hover': {
                          bgcolor: 'rgba(211, 47, 47, 0.06)',
                          boxShadow: 'none',
                        },
                      }}
                      onClick={() => {
                        setConfirmDeleteWikiPages(true);
                      }}
                    >
                      {deletingWikiPages ? 'Deleting...' : 'Delete wiki'}
                    </Button>
                  </Stack>
                </Paper>
              </Stack>
              )}
              {activeSection === 'ingest' && renderIngestSection()}
              {activeSection === 'search' && renderSearchSection()}
              {activeSection === 'sources' && renderSourcesSection()}
              {activeSection === 'jobs' && renderJobsSection()}
              {activeSection === 'settings' && renderSettingsSection()}
            </Box>
          </Paper>
        )}

        <Dialog
          open={!!selectedDocument}
          onClose={() => setSelectedDocument(null)}
          fullWidth
          maxWidth='md'
        >
          <DialogTitle>{selectedDocument?.title || 'Document'}</DialogTitle>
          <DialogContent dividers>
            {selectedDocument && (
              <Stack sx={{ gap: 1.5 }}>
                <Typography variant='caption' color='text.secondary'>
                  {selectedDocument.document_uri}
                </Typography>
                <Typography variant='body2' color='text.secondary'>
                  Source: {selectedDocument.source_title || selectedDocument.canonical_uri}
                </Typography>
                <Box
                  component='pre'
                  sx={{
                    m: 0,
                    p: 1.5,
                    borderRadius: 2,
                    bgcolor: 'background.default',
                    overflowX: 'auto',
                    whiteSpace: 'pre-wrap',
                    wordBreak: 'break-word',
                    fontFamily: 'monospace',
                    fontSize: '0.82rem',
                  }}
                >
                  {selectedDocument.content}
                </Box>
              </Stack>
            )}
          </DialogContent>
        </Dialog>
        <AlertDialog
          open={!!pendingDeleteSource}
          onClose={() => setPendingDeleteSource(null)}
          color='warning'
          title='Delete source'
          content={(
            <Typography variant='body2'>
              Delete {pendingDeleteSource ? `"${pendingDeleteSource.label}"` : 'this source'}? This removes its documents, chunks, and search index entries.
            </Typography>
          )}
          cancelLabel='Cancel'
          confirmLabel='Delete'
          autoFocusAction='cancel'
          onConfirm={confirmDeleteSource}
        />
        <AlertDialog
          open={confirmDeleteWikiPages}
          onClose={() => setConfirmDeleteWikiPages(false)}
          color='error'
          title='Delete wiki'
          content={(
            <Typography variant='body2'>
              Delete generated wiki pages and wiki log entries? Sources and ingested knowledge documents will remain.
            </Typography>
          )}
          cancelLabel='Cancel'
          confirmLabel='Delete wiki'
          autoFocusAction='cancel'
          onConfirm={onDeleteWikiPages}
        />
      </Stack>
    </DrawerLayout>
  );
}
