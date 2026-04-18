import { useEffect, useMemo, useState, type SyntheticEvent } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  Dialog,
  DialogContent,
  DialogTitle,
  Paper,
  Stack,
  Tab,
  Tabs,
  TextField,
  Typography,
} from '@mui/material';
import {
  IconDatabaseSearch,
  IconFilePlus,
  IconLink,
  IconRefresh,
  IconUpload,
} from '@tabler/icons-react';
import { useLocation, useNavigate } from 'react-router-dom';
import { DrawerLayout } from '../components/common';
import {
  createKnowledgeLocalSource,
  createKnowledgeUrlSource,
  getKnowledgeDocument,
  getKnowledgeHealth,
  getKnowledgeJobs,
  getKnowledgeSources,
  searchKnowledge,
  type KnowledgeDocumentDetail,
  type KnowledgeHealth,
  type KnowledgeJob,
  type KnowledgeSearchResponse,
  type KnowledgeSourceSummary,
} from '../services/api';
import { useRefCallback } from '../hooks';

type KnowledgeSection = 'overview' | 'ingest' | 'search' | 'sources' | 'jobs';

const KNOWLEDGE_SECTIONS: readonly KnowledgeSection[] = [
  'overview',
  'ingest',
  'search',
  'sources',
  'jobs',
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
    : 'overview';
};

export default function PageKnowledge() {
  const location = useLocation();
  const navigate = useNavigate();
  const activeSection = normalizeSection(location.search);

  const [health, setHealth] = useState<KnowledgeHealth | null>(null);
  const [sources, setSources] = useState<readonly KnowledgeSourceSummary[]>([]);
  const [jobs, setJobs] = useState<readonly KnowledgeJob[]>([]);
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
  const [documentLoading, setDocumentLoading] = useState(false);
  const [error, setError] = useState('');

  const loadPageData = useRefCallback(async () => {
    setRefreshing(true);
    try {
      const [nextHealth, nextSources, nextJobs] = await Promise.all([
        getKnowledgeHealth(),
        getKnowledgeSources(),
        getKnowledgeJobs(),
      ]);
      setHealth(nextHealth);
      setSources(nextSources);
      setJobs(nextJobs);
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

  const onSectionChange = useRefCallback((_event: SyntheticEvent, value: string) => {
    if (!KNOWLEDGE_SECTIONS.includes(value as KnowledgeSection)) {
      return;
    }
    navigate(`/knowledge?section=${value}`);
  });

  const summaryCards = useMemo(
    () => [
      { label: 'Sources', value: health?.counts.sources ?? sources.length, hint: 'Tracked inputs across workspace paths and URLs' },
      { label: 'Documents', value: health?.counts.documents ?? 0, hint: 'Normalized documents extracted into the KB' },
      { label: 'Chunks', value: health?.counts.chunks ?? 0, hint: 'Searchable retrieval units embedded for RAG' },
      { label: 'Jobs', value: health?.counts.jobs ?? jobs.length, hint: 'Recent ingest and indexing operations' },
    ],
    [health, jobs.length, sources.length],
  );

  const renderIngestSection = () => (
    <Paper variant='outlined' sx={{ p: 2, borderRadius: 3, bgcolor: 'background.default' }}>
      <Stack sx={{ gap: 2 }}>
        <Stack sx={{ gap: 0.5 }}>
          <Typography variant='h3'>Ingest</Typography>
          <Typography variant='body2' color='text.secondary'>
            Start with explicit local paths or URLs. Uploaded documents stay mocked for V1 until the core retrieval pipeline is stable.
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
            bgcolor: 'background.paper',
          }}
        >
          <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
            <Stack sx={{ gap: 0.4 }}>
              <Typography variant='body1' sx={{ fontWeight: 700 }}>
                Uploaded docs
              </Typography>
              <Typography variant='body2' color='text.secondary'>
                Mockup only for V1. Real upload parsing will land after the local-path and URL pipeline is stable.
              </Typography>
            </Stack>
            <Button variant='outlined' startIcon={<IconUpload size={16} />} disabled>
              Coming soon
            </Button>
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
            Explicitly ingested workspace paths and URLs tracked under the current workspace-scoped knowledge store.
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
                  <Stack sx={{ gap: 0.25 }}>
                    <Typography variant='body1' sx={{ fontWeight: 700 }}>
                      {source.title}
                    </Typography>
                    <Typography variant='caption' color='text.secondary'>
                      {source.canonical_uri}
                    </Typography>
                  </Stack>
                  <Chip size='small' color={getStatusColor(source.status)} label={source.status} />
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

  return (
    <DrawerLayout contentWidth='wide'>
      <Stack sx={{ minHeight: 0, gap: 2.5 }}>
        <Stack direction='row' sx={{ justifyContent: 'space-between', alignItems: 'flex-start', gap: 2, flexWrap: 'wrap' }}>
          <Stack sx={{ gap: 0.75 }}>
            <Typography variant='h1'>Knowledge</Typography>
            <Typography variant='body1' color='text.secondary' sx={{ maxWidth: '52rem' }}>
              Explicit ingest and hybrid retrieval over workspace files and URLs. This is the first RAG-focused slice of the llm-wiki direction, without automatic chat routing or editable pages yet.
            </Typography>
          </Stack>
          <Button
            variant='outlined'
            startIcon={<IconRefresh size={16} />}
            onClick={() => {
              void loadPageData();
            }}
            disabled={refreshing}
          >
            Refresh
          </Button>
        </Stack>

        <Paper variant='outlined' sx={{ borderRadius: 3, bgcolor: 'background.default', overflow: 'hidden' }}>
          <Tabs
            value={activeSection}
            onChange={onSectionChange}
            variant='scrollable'
            allowScrollButtonsMobile
            sx={{ px: 1.5, pt: 1 }}
          >
            <Tab value='overview' label='Overview' />
            <Tab value='ingest' label='Ingest' />
            <Tab value='search' label='Search' />
            <Tab value='sources' label='Sources' />
            <Tab value='jobs' label='Jobs' />
          </Tabs>
          <Box sx={{ p: 2 }}>
            {activeSection === 'overview' && (
              <Stack sx={{ gap: 2 }}>
                {error && (
                  <Alert severity='warning'>
                    {error}
                  </Alert>
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
              </Stack>
            )}
            {activeSection === 'ingest' && renderIngestSection()}
            {activeSection === 'search' && renderSearchSection()}
            {activeSection === 'sources' && renderSourcesSection()}
            {activeSection === 'jobs' && renderJobsSection()}
          </Box>
        </Paper>

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
      </Stack>
    </DrawerLayout>
  );
}
