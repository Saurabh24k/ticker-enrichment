import React, { useMemo, useState, useEffect, useRef, useCallback } from "react";
import {
  Box, Container, Heading, Text, HStack, VStack, Button, IconButton, useToast,
  Table, Thead, Tbody, Tr, Th, Td, Tag, Select, Code, Input, Divider, Spinner,
  SimpleGrid, InputGroup, InputLeftElement, Tooltip, useBreakpointValue, Badge,
  Switch, FormControl, FormLabel, Progress, useClipboard, Kbd, Modal, ModalOverlay,
  ModalContent, ModalHeader, ModalBody, ModalCloseButton, ModalFooter, Drawer, DrawerOverlay,
  DrawerContent, DrawerHeader, DrawerBody, DrawerFooter, CloseButton, RadioGroup,
  Radio, Stack, Slider, SliderTrack, SliderFilledTrack, SliderThumb, useDisclosure
} from "@chakra-ui/react";
import {
  DownloadIcon, RepeatIcon, SearchIcon, AttachmentIcon,
  InfoOutlineIcon, CheckCircleIcon, WarningTwoIcon, NotAllowedIcon,
  CopyIcon
} from "@chakra-ui/icons";

/* ---------- Types ---------- */
type Candidate = { symbol: string; name: string; type?: string; score?: number; source?: string };
type PreviewStatus = "FILLED" | "AMBIGUOUS" | "NOT_FOUND" | "UNCHANGED" | "ENRICHED";
type PreviewRow = {
  index: number;
  status: PreviewStatus | string; // tolerate unexpected server values
  candidates?: Candidate[];
  notes?: string;
  input?: { Name?: string | null; Symbol?: string | null };
  Name?: string | null;
  Symbol?: string | null;
  proposed?: Record<string, any>; // backend surface for ENRICHED preview
};
type Counts = Record<PreviewStatus, number>;
type LogKind = "step" | "info" | "success" | "warn" | "error";
type LogEntry = { id: string; ts: string; kind: LogKind; text: string };
type LastRun = {
  filename?: string;
  sizeBytes?: number;
  startedAt?: number;
  finishedAt?: number;
  durationMs?: number;
  rows?: number;
  mode?: "API-FIRST" | "API+LOCAL";
  commitDurationMs?: number;
};

/* ---------- Config ---------- */
const API_BASE = (import.meta as any)?.env?.VITE_API_BASE ?? "http://localhost:8000";

/* ---------- Utils ---------- */
const makeId = () =>
  (window.crypto && "randomUUID" in window.crypto)
    ? (window.crypto as any).randomUUID()
    : Math.random().toString(36).slice(2);

const ms = (n?: number) => (typeof n === "number" ? `${n.toFixed(0)} ms` : "—");
const pct = (n: number) => `${(n * 100).toFixed(0)}%`;
const s1 = (n: number) => n.toLocaleString();

const useDebounced = (value: string, delay = 200) => {
  const [v, setV] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setV(value), delay);
    return () => clearTimeout(t);
  }, [value, delay]);
  return v;
};

const useLocalStorage = <T,>(key: string, initial: T) => {
  const [val, setVal] = useState<T>(() => {
    try {
      const raw = localStorage.getItem(key);
      return raw ? (JSON.parse(raw) as T) : initial;
    } catch {
      return initial;
    }
  });
  useEffect(() => {
    try { localStorage.setItem(key, JSON.stringify(val)); } catch {}
  }, [key, val]);
  return [val, setVal] as const;
};

function computeCounts(rows: PreviewRow[]): Counts {
  const base: Counts = { FILLED: 0, AMBIGUOUS: 0, NOT_FOUND: 0, UNCHANGED: 0, ENRICHED: 0 };
  for (const r of rows) {
    const k = (r.status as PreviewStatus);
    if (k in base) base[k] += 1;
  }
  return base;
}
function breakdownText(c: Counts) {
  return `FILLED ${s1(c.FILLED)} • AMBIGUOUS ${s1(c.AMBIGUOUS)} • NOT_FOUND ${s1(c.NOT_FOUND)} • UNCHANGED ${s1(c.UNCHANGED)}${c.ENRICHED ? ` • ENRICHED ${s1(c.ENRICHED)}` : ""}`;
}
function computeInsights(rows: PreviewRow[]) {
  const counts = computeCounts(rows);
  let rowsWithCandidates = 0;
  let totalCandidates = 0;
  const sourceTop: Record<string, number> = {};
  for (const r of rows) {
    const n = r.candidates?.length ?? 0;
    if (n > 0) rowsWithCandidates++;
    totalCandidates += n;
    const top = r.candidates?.[0];
    if (top?.source) sourceTop[top.source] = (sourceTop[top.source] || 0) + 1;
  }
  const avgCandidates = rows.length ? totalCandidates / rows.length : 0;
  const topSources = Object.entries(sourceTop).sort((a, b) => b[1] - a[1]).slice(0, 3).map(([src, n]) => `${src} ${s1(n)}`);
  const coverage = rows.length ? rowsWithCandidates / rows.length : 0;
  return { counts, rowsWithCandidates, avgCandidates, topSources, coverage };
}

function StatusTag({ s }: { s?: PreviewRow["status"] }) {
  const map: Record<PreviewStatus, { color: string; label: string; icon: React.ReactNode }> = {
    FILLED:     { color: "green",  label: "FILLED",     icon: <CheckCircleIcon mr={1} /> },
    AMBIGUOUS:  { color: "yellow", label: "AMBIGUOUS",  icon: <InfoOutlineIcon mr={1} /> },
    NOT_FOUND:  { color: "red",    label: "NOT_FOUND",  icon: <NotAllowedIcon mr={1} /> },
    UNCHANGED:  { color: "gray",   label: "UNCHANGED",  icon: <WarningTwoIcon mr={1} /> },
    ENRICHED:   { color: "blue",   label: "ENRICHED",   icon: <CheckCircleIcon mr={1} /> },
  };
  const fallback = { color: "gray", label: String(s ?? "UNKNOWN").toUpperCase(), icon: <InfoOutlineIcon mr={1} /> };
  const m = (s && map[s as PreviewStatus]) || fallback;

  return (
    <Tag size="sm" variant="subtle" colorScheme={m.color} rounded="full" display="inline-flex" alignItems="center">
      {m.icon}{m.label}
    </Tag>
  );
}

function formatCandidate(c?: Candidate | null) {
  if (!c) return "—";
  const parts: string[] = [];
  if (c.symbol) parts.push(c.symbol);
  if (c.name) parts.push(c.name);
  if (c.type) parts.push(c.type);
  if (typeof c.score === "number") parts.push(`score ${c.score.toFixed(2)}`);
  if (c.source) parts.push(`[${c.source}]`);
  return parts.join(" · ");
}

/* ---------- Activity Log ---------- */
function LogIcon({ kind }: { kind: LogKind }) {
  if (kind === "success") return <CheckCircleIcon color="green.300" boxSize={3.5} />;
  if (kind === "warn") return <WarningTwoIcon color="yellow.300" boxSize={3.5} />;
  if (kind === "error") return <NotAllowedIcon color="red.300" boxSize={3.5} />;
  if (kind === "info") return <InfoOutlineIcon color="cyan.300" boxSize={3.5} />;
  return <InfoOutlineIcon color="gray.300" boxSize={3.5} />;
}
function ActivityLog({
  logs, loading, onClear, onCopy, onDownload, clipboardValue,
}: {
  logs: LogEntry[]; loading: boolean; onClear: () => void; onCopy: () => void; onDownload: () => void; clipboardValue: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const { hasCopied } = useClipboard(clipboardValue);
  useEffect(() => {
    ref.current?.scrollTo({ top: ref.current.scrollHeight, behavior: "smooth" });
  }, [logs.length]);

  return (
    <Box rounded="2xl" borderWidth="1px" borderColor="whiteAlpha.200" bg="blackAlpha.400" backdropFilter="blur(8px)" overflow="hidden">
      <HStack px={4} py={2} justify="space-between" bg="whiteAlpha.100">
        <HStack spacing={2}>
          <Text fontSize="sm" color="gray.300" fontWeight="semibold">Activity</Text>
          <Badge colorScheme="blackAlpha" variant="subtle">{logs.length}</Badge>
        </HStack>
        <HStack>
          {loading && (
            <HStack spacing={2}>
              <Spinner size="xs" />
              <Text fontSize="xs" color="gray.300">Working…</Text>
            </HStack>
          )}
          <Tooltip label={hasCopied ? "Copied" : "Copy log"}>
            <IconButton aria-label="Copy" icon={<CopyIcon />} size="xs" variant="ghost" onClick={onCopy} />
          </Tooltip>
          <Tooltip label="Download .log">
            <IconButton aria-label="Download log" icon={<DownloadIcon />} size="xs" variant="ghost" onClick={onDownload} />
          </Tooltip>
          <Tooltip label="Clear log">
            <IconButton aria-label="Clear" icon={<RepeatIcon />} size="xs" variant="ghost" onClick={onClear} />
          </Tooltip>
        </HStack>
      </HStack>
      <Box ref={ref} maxH="300px" overflow="auto" px={3} py={2}>
        {logs.length === 0 ? (
          <Text fontSize="sm" color="gray.400" px={1} py={1}>Nothing yet. Actions will show up here.</Text>
        ) : (
          <VStack align="stretch" spacing={1}>
            {logs.map((l) => (
              <HStack key={l.id} spacing={2} align="start">
                <Box pt="3px"><LogIcon kind={l.kind} /></Box>
                <Text fontSize="xs" color="gray.500" minW="64px">{l.ts}</Text>
                <Text fontSize="sm" color="gray.200" flex="1" whiteSpace="pre-wrap">{l.text}</Text>
              </HStack>
            ))}
          </VStack>
        )}
      </Box>
    </Box>
  );
}

/* ---------- Commit Preview Modal ---------- */
type RiskLevel = "HIGH" | "MEDIUM" | "LOW";
type PendingChange = {
  index: number;
  name: string;
  fromSymbol: string;
  toSymbol: string;
  score?: number;
  source?: string;
  risk: RiskLevel;
};
function CommitPreview({
  isOpen, onClose, changes, onConfirm
}: {
  isOpen: boolean;
  onClose: () => void;
  changes: PendingChange[];
  onConfirm: () => void;
}) {
  const hi = changes.filter(c => c.risk === "HIGH").length;
  const me = changes.filter(c => c.risk === "MEDIUM").length;
  const lo = changes.filter(c => c.risk === "LOW").length;
  return (
    <Modal isOpen={isOpen} onClose={onClose} size="3xl" isCentered>
      <ModalOverlay />
      <ModalContent bg="blackAlpha.700" backdropFilter="blur(10px)" borderColor="whiteAlpha.200" borderWidth="1px">
        <ModalHeader color="gray.100">Commit Preview</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <VStack align="stretch" spacing={3}>
            <HStack spacing={3}>
              <Badge colorScheme="red">HIGH {hi}</Badge>
              <Badge colorScheme="yellow">MED {me}</Badge>
              <Badge colorScheme="green">LOW {lo}</Badge>
              <Text fontSize="sm" color="gray.400">Sorted by risk • review before writing CSV</Text>
            </HStack>
            <Box borderWidth="1px" borderColor="whiteAlpha.200" rounded="xl" overflow="hidden">
              <Table size="sm">
                <Thead bg="whiteAlpha.100">
                  <Tr>
                    <Th>#</Th>
                    <Th>Name</Th>
                    <Th>From</Th>
                    <Th>To</Th>
                    <Th>Score</Th>
                    <Th>Source</Th>
                    <Th>Risk</Th>
                  </Tr>
                </Thead>
                <Tbody>
                  {changes.map((c, i) => (
                    <Tr key={`${c.index}-${i}`}>
                      <Td>{c.index}</Td>
                      <Td maxW="360px"><Text noOfLines={1}>{c.name}</Text></Td>
                      <Td>{c.fromSymbol || "—"}</Td>
                      <Td>{c.toSymbol}</Td>
                      <Td>{typeof c.score === "number" ? c.score.toFixed(2) : "—"}</Td>
                      <Td>{c.source || "—"}</Td>
                      <Td>
                        <Badge colorScheme={c.risk === "HIGH" ? "red" : c.risk === "MEDIUM" ? "yellow" : "green"}>
                          {c.risk}
                        </Badge>
                      </Td>
                    </Tr>
                  ))}
                </Tbody>
              </Table>
            </Box>
          </VStack>
        </ModalBody>
        <ModalFooter>
          <HStack spacing={3}>
            <Button variant="ghost" onClick={onClose}>Cancel</Button>
            <Button colorScheme="blue" onClick={onConfirm} leftIcon={<DownloadIcon />}>Confirm & Download</Button>
          </HStack>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
}

/* ---------- Row Drawer ---------- */
function RowDrawer({
  row, isOpen, onClose, currentOverride, onSetOverride,
}: {
  row: PreviewRow | null;
  isOpen: boolean;
  onClose: () => void;
  currentOverride?: string;
  onSetOverride: (symbol: string | "") => void;
}) {
  const [value, setValue] = useState(currentOverride ?? "");
  useEffect(() => setValue(currentOverride ?? ""), [currentOverride, row?.index]);
  const top = row?.candidates?.[0];
  return (
    <Drawer isOpen={isOpen} placement="right" onClose={onClose} size="md">
      <DrawerOverlay />
      <DrawerContent bg="blackAlpha.600" borderLeftWidth="1px" borderColor="whiteAlpha.200" backdropFilter="blur(10px)">
        <DrawerHeader display="flex" alignItems="center" justifyContent="space-between">
          <HStack spacing={3}>
            <Heading size="sm" color="gray.100">Row {row?.index ?? "—"}</Heading>
            {row && <StatusTag s={row.status} />}
            {top?.source && <Badge variant="outline">{top.source}</Badge>}
            {typeof top?.score === "number" && (
              <Badge colorScheme={top.score >= 0.85 ? "green" : top.score >= 0.6 ? "yellow" : "red"} variant="solid">
                score {top.score.toFixed(2)}
              </Badge>
            )}
          </HStack>
          <CloseButton />
        </DrawerHeader>
        <DrawerBody>
          {row ? (
            <VStack align="stretch" spacing={4} color="gray.200">
              <Box>
                <Text fontSize="xs" color="gray.400" mb={1}>Name</Text>
                <Text>{row.Name ?? row.input?.Name ?? "(blank)"}{row.proposed?.Name ? ` → ${row.proposed.Name}` : ""}</Text>
              </Box>
              <Box>
                <Text fontSize="xs" color="gray.400" mb={1}>Current Symbol</Text>
                <Text>{row.Symbol ?? row.input?.Symbol ?? "—"}</Text>
              </Box>
              <Box>
                <Text fontSize="xs" color="gray.400" mb={2}>Candidates</Text>
                <RadioGroup value={value} onChange={(v) => setValue(v)}>
                  <Stack spacing={3}>
                    <Radio value="">{`(no override)`}</Radio>
                    {(row.candidates ?? []).map((c, i) => (
                      <Radio key={i} value={c.symbol}>
                        <HStack spacing={2}>
                          <Text fontWeight="semibold">{c.symbol}</Text>
                          <Text color="gray.300">— {c.name}</Text>
                          {typeof c.score === "number" && (
                            <Badge colorScheme={c.score >= 0.85 ? "green" : c.score >= 0.6 ? "yellow" : "gray"} variant="subtle">
                              {c.score.toFixed(2)}
                            </Badge>
                          )}
                          {c.source && <Badge variant="outline">{c.source}</Badge>}
                        </HStack>
                      </Radio>
                    ))}
                  </Stack>
                </RadioGroup>
              </Box>
            </VStack>
          ) : (
            <Text color="gray.400">Select a row to view details.</Text>
          )}
        </DrawerBody>
        <DrawerFooter>
          <HStack spacing={3}>
            {value && (
              <Tooltip label="Copy selected symbol">
                <IconButton
                  aria-label="Copy symbol"
                  icon={<CopyIcon />}
                  variant="ghost"
                  onClick={() => navigator.clipboard.writeText(value)}
                />
              </Tooltip>
            )}
            <Button onClick={() => { onSetOverride(value); onClose(); }} colorScheme="blue">Apply</Button>
          </HStack>
        </DrawerFooter>
      </DrawerContent>
    </Drawer>
  );
}

/* ---------- App ---------- */
export default function App() {
  const toast = useToast();

  // persisted prefs
  const [useLocalMaps, setUseLocalMaps] = useLocalStorage<boolean>("prefs.useLocalMaps", false);
  const [statusFilter, setStatusFilter] = useLocalStorage<PreviewRow["status"] | "ALL">("prefs.statusFilter", "ALL");
  const [query, setQuery] = useLocalStorage<string>("prefs.query", "");
  const qDebounced = useDebounced(query, 180);

  // learned aliases: normalized name -> symbol
  const [aliases, setAliases] = useLocalStorage<Record<string, string>>("learned.aliases", {});
  const [appliedAliasCount, setAppliedAliasCount] = useState(0);

  // core state
  const [file, setFile] = useState<File | null>(null);
  const [rows, setRows] = useState<PreviewRow[]>([]);
  const [overrides, setOverrides] = useState<Record<number, string>>({});
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [lastRun, setLastRun] = useState<LastRun>({});
  const [drawerRow, setDrawerRow] = useState<PreviewRow | null>(null);

  // commit preview
  const commitPreview = useDisclosure();
  const [pendingChanges, setPendingChanges] = useState<PendingChange[]>([]);

  const counts = useMemo(() => computeCounts(rows), [rows]);
  const insights = useMemo(() => computeInsights(rows), [rows]);
  const isNarrow = useBreakpointValue({ base: true, md: false }) ?? true;

  const pushLog = useCallback((kind: LogKind, text: string) => {
    setLogs((prev) => [...prev, { id: makeId(), ts: new Date().toLocaleTimeString(), kind, text }]);
  }, []);

  const buildLogText = useCallback(
    () => logs.map(l => `[${l.ts}] ${l.kind.toUpperCase()}: ${l.text}`).join("\n"),
    [logs]
  );
  const { onCopy } = useClipboard(buildLogText());
  const downloadLog = () => {
    const blob = new Blob([buildLogText()], { type: "text/plain;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = "activity.log"; a.click();
    URL.revokeObjectURL(url);
  };

  // normalize helper
  const norm = (s: string | null | undefined) => (s ?? "").toString().trim().toLowerCase();

  // file -> preview
  async function onChooseFile(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0] || null;
    setFile(f);
    setRows([]); setOverrides({}); setError(null);
    if (!f) return;

    const startedAt = performance.now();
    setLastRun({ startedAt, filename: f.name, sizeBytes: f.size, mode: useLocalMaps ? "API+LOCAL" : "API-FIRST" });

    try {
      setLoading(true);
      pushLog("step", `Selected file: ${f.name} (${s1(f.size)} bytes)`);
      const fd = new FormData();
      fd.append("file", f);
      fd.append("use_local_maps", String(useLocalMaps));
      pushLog("info", `POST /files/preview-file (local maps: ${useLocalMaps ? "ON" : "OFF"})`);

      const reqStart = performance.now();
      const resp = await fetch(`${API_BASE}/files/preview-file`, { method: "POST", body: fd });
      const reqDur = performance.now() - reqStart;
      pushLog(resp.ok ? "success" : "error", `Preview response: ${resp.status} • ${ms(reqDur)}`);
      if (!resp.ok) {
        const msg = `Preview failed: ${resp.status}`;
        setError(msg); toast({ title: msg, status: "error" }); return;
      }

      const parseStart = performance.now();
      const data = (await resp.json()) as PreviewRow[];
      const parseDur = performance.now() - parseStart;
      pushLog("info", `Parsing JSON… ${ms(parseDur)} • rows: ${s1(data.length)}`);

      // Apply learned aliases as boosts to the FIRST candidate if symbol matches alias
      let applied = 0;
      const normalized = data.map((r, i) => {
        const out: PreviewRow = {
          index: r.index ?? i,
          status: r.status,
          candidates: r.candidates || [],
          notes: r.notes,
          input: r.input,
          Name: (r as any).Name ?? r.input?.Name ?? null,
          Symbol: (r as any).Symbol ?? r.input?.Symbol ?? null,
          proposed: (r as any).proposed,
        };

        const nm = norm(out.Name ?? out.input?.Name);
        const learned = aliases[nm];
        if (learned && out.candidates && out.candidates.length > 0) {
          // if our learned symbol appears in candidates, move it to the top & add a tiny score bump
          const idx = out.candidates.findIndex(c => c.symbol?.toUpperCase() === learned.toUpperCase());
          if (idx >= 0) {
            const [hit] = out.candidates.splice(idx, 1);
            const bumped = { ...hit, score: typeof hit.score === "number" ? Math.min(hit.score + 0.05, 0.999) : hit.score };
            out.candidates.unshift(bumped);
            applied++;
          }
        }
        return out;
      });
      setAppliedAliasCount(applied);

      setRows(normalized);

      const finishedAt = performance.now();
      const durationMs = finishedAt - startedAt;
      setLastRun(prev => ({ ...prev, finishedAt, durationMs, rows: normalized.length }));

      pushLog("info", `Normalize… ${ms(performance.now() - finishedAt + durationMs)}`);
      const cov = computeInsights(normalized);
      pushLog("info", `Status breakdown → ${breakdownText(cov.counts)}`);
      pushLog("info", `Candidates → rows with candidates ${s1(cov.rowsWithCandidates)} / ${s1(normalized.length)} (${(cov.coverage * 100).toFixed(1)}%), avg candidates/row ${cov.avgCandidates.toFixed(2)}`);
      if (applied > 0) pushLog("success", `Learned aliases applied to ${applied} row(s).`);
      if (cov.topSources.length) pushLog("info", `Top candidate sources → ${cov.topSources.join(" • ")}`);

      pushLog("success", `Preview ready • total ${ms(durationMs)}`);
      toast({ title: "Preview ready", status: "success" });
    } catch (err: any) {
      const msg = err?.message || "Preview failed";
      setError(msg); pushLog("error", msg); toast({ title: msg, status: "error" });
    } finally {
      setLoading(false);
    }
  }

  // overrides
  function setOverrideForRow(rowIndex: number, value: string) {
    setOverrides((prev) => {
      const next = { ...prev };
      if (!value) { delete next[rowIndex]; pushLog("info", `Cleared override for row ${rowIndex}`); }
      else {
        next[rowIndex] = value;
        // learn alias immediately
        const r = rows.find(rr => rr.index === rowIndex);
        const nm = norm(r?.Name ?? r?.input?.Name);
        if (nm) setAliases(a => ({ ...a, [nm]: value }));
        pushLog("info", `Override set for row ${rowIndex}: ${value} (learned)`);
      }
      return next;
    });
  }

  // bulk top-candidate apply for filtered rows (with optional min score)
  const [bulkMinScore, setBulkMinScore] = useState(0.0);
  function bulkApplyTopCandidates() {
    let changed = 0;
    setOverrides((prev) => {
      const next = { ...prev };
      for (const r of filtered) {
        if ((r.status === "FILLED" || r.status === "AMBIGUOUS") && (r.candidates?.length ?? 0) > 0) {
          const top = r.candidates![0];
          if (typeof top.score === "number" && top.score < bulkMinScore) continue;
          next[r.index] = top.symbol; changed++;
          const nm = norm(r.Name ?? r.input?.Name);
          if (nm) setAliases(a => ({ ...a, [nm]: top.symbol })); // learn while bulk applying
        }
      }
      return next;
    });
    pushLog("step", `Bulk applied top candidates to ${changed} row(s) (min score ≥ ${bulkMinScore.toFixed(2)})`);
    toast({ title: `Bulk applied ${changed}`, status: "success" });
  }

  // risk scoring for commit preview
  function riskOf(score?: number, source?: string): RiskLevel {
    if (typeof score !== "number") return "HIGH";
    if (score >= 0.85) return "LOW";
    if (score >= 0.6) return "MEDIUM";
    return "HIGH";
  }

  /* ---------- Shortcuts Modal ---------- */
function ShortcutsModal({ isOpen, onClose }: { isOpen: boolean; onClose: () => void }) {
  const Row = ({ keys, label }: { keys: React.ReactNode; label: string }) => (
    <HStack justify="space-between" py={1.5}>
      <HStack spacing={2}>{keys}</HStack>
      <Text color="gray.300">{label}</Text>
    </HStack>
  );

  return (
    <Modal isOpen={isOpen} onClose={onClose} size="sm" isCentered>
      <ModalOverlay />
      <ModalContent
        bg="blackAlpha.700"
        borderWidth="1px"
        borderColor="whiteAlpha.200"
        backdropFilter="blur(10px)"
      >
        <ModalHeader color="gray.100">Keyboard Shortcuts</ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          <VStack align="stretch" spacing={2}>
            <Row keys={<><Kbd>⌘/Ctrl</Kbd><Text> + </Text><Kbd>K</Kbd></>} label="Focus search" />
            <Row keys={<><Kbd>⌘/Ctrl</Kbd><Text> + </Text><Kbd>Enter</Kbd></>} label="Open commit preview" />
            <Row keys={<><Kbd>⌘/Ctrl</Kbd><Text> + </Text><Kbd>L</Kbd></>} label="Toggle Local Maps" />
            <Row keys={<Kbd>A</Kbd>} label="Bulk-apply top candidates" />
            <Row keys={<Kbd>R</Kbd>} label="Reset session" />
            <Row keys={<Kbd>?</Kbd>} label="Open this help" />
          </VStack>
        </ModalBody>
      </ModalContent>
    </Modal>
  );
}


  // build pending changes and open modal
  function openCommitPreview() {
    if (!rows.length) { toast({ title: "Select a file first", status: "warning" }); return; }
    const changes: PendingChange[] = [];
    for (const r of rows) {
      const to = overrides[r.index];
      if (!to && r.status !== "FILLED") continue; // only FILLED rows or ones with overrides result in a symbol write
      const top = r.candidates?.[0];
      const chosen = to || (r.status === "FILLED" ? top?.symbol : "");
      if (!chosen) continue;
      const sc = (to ? r.candidates?.find(c => c.symbol === to)?.score : top?.score);
      const src = (to ? r.candidates?.find(c => c.symbol === to)?.source : top?.source);
      changes.push({
        index: r.index,
        name: (r.Name ?? r.input?.Name ?? "").toString(),
        fromSymbol: (r.Symbol ?? r.input?.Symbol ?? "")?.toString(),
        toSymbol: chosen,
        score: sc,
        source: src,
        risk: riskOf(sc, src),
      });
    }
    changes.sort((a, b) => {
      const rk = { HIGH: 2, MEDIUM: 1, LOW: 0 } as const;
      const d = rk[a.risk] - rk[b.risk];
      if (d !== 0) return d > 0 ? -1 : 1;
      return (b.score ?? -1) - (a.score ?? -1);
    });
    setPendingChanges(changes);
    commitPreview.onOpen();
  }

  // commit
  const commitAndDownload = useCallback(async () => {
    if (!file) { toast({ title: "Select a file first", status: "warning" }); pushLog("warn", "Commit aborted: no file selected."); return; }
    try {
      setLoading(true);
      const overrideCount = Object.keys(overrides).length;
      pushLog("step", `Committing ${overrideCount} overrides…`);
      const fd = new FormData();
      fd.append("file", file);
      fd.append("use_local_maps", String(useLocalMaps));
      fd.append("overrides_json", new Blob([JSON.stringify(overrides)], { type: "application/json" }));

      const reqStart = performance.now();
      const resp = await fetch(`${API_BASE}/files/commit-file`, { method: "POST", body: fd });
      const reqDur = performance.now() - reqStart;
      setLastRun((prev) => ({ ...prev, commitDurationMs: reqDur }));
      pushLog(resp.ok ? "success" : "error", `Commit response: ${resp.status} • ${ms(reqDur)}`);
      if (!resp.ok) { const msg = `Commit failed: ${resp.status}`; setError(msg); toast({ title: msg, status: "error" }); return; }

      pushLog("step", "Downloading enriched_holdings.csv…");
      const blob = await resp.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a"); a.href = url; a.download = "enriched_holdings.csv"; a.click(); URL.revokeObjectURL(url);
      pushLog("success", "CSV downloaded."); toast({ title: "CSV downloaded", status: "success" });
    } catch (err: any) {
      const msg = err?.message || "Commit failed"; setError(msg); pushLog("error", msg); toast({ title: msg, status: "error" });
    } finally { setLoading(false); commitPreview.onClose(); }
  }, [file, overrides, useLocalMaps]); // eslint-disable-line

  // reset
  const resetAll = useCallback(() => {
    setFile(null); setRows([]); setOverrides({}); setError(null);
    setQuery(""); setStatusFilter("ALL"); setLogs([]); setLastRun({});
    toast({ title: "Reset", status: "info" });
  }, [toast]);

  // shortcuts
  const shortcuts = useDisclosure();
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const cmd = e.metaKey || e.ctrlKey;
      if (cmd && e.key.toLowerCase() === "k") { e.preventDefault(); const el = document.getElementById("search-box"); (el as HTMLInputElement)?.focus(); }
      if (cmd && e.key === "Enter") { e.preventDefault(); openCommitPreview(); }
      if (cmd && e.key.toLowerCase() === "l") { e.preventDefault(); setUseLocalMaps(v => !v); }
      if (!cmd && e.key === "?") { e.preventDefault(); shortcuts.onOpen(); }
      if (!cmd && e.key.toLowerCase() === "r") { e.preventDefault(); resetAll(); }
      if (!cmd && e.key.toLowerCase() === "a") { e.preventDefault(); bulkApplyTopCandidates(); }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [bulkApplyTopCandidates, resetAll, setUseLocalMaps]); // eslint-disable-line

  type StatCardProps = {
    label: string;
    value: number | string;
    color: string;
    icon?: React.ReactNode;
  };

  function StatCard({ label, value, color, icon }: StatCardProps) {
    return (
      <Box
        p={4}
        rounded="2xl"
        borderWidth="1px"
        borderColor="whiteAlpha.200"
        bg="whiteAlpha.100"
        backdropFilter="blur(6px)"
        _hover={{ borderColor: "whiteAlpha.300", bg: "whiteAlpha.200", transition: "all 0.2s" }}
      >
        <HStack spacing={3}>
          {icon && (
            <Box p={2} rounded="lg" bg="whiteAlpha.200" border="1px solid" borderColor="whiteAlpha.300">
              {icon}
            </Box>
          )}
          <VStack align="start" spacing={0}>
            <Text fontSize="xs" letterSpacing="wide" color="gray.400" textTransform="uppercase">
              {label}
            </Text>
            <Heading size="md" mt={1} color={color}>
              {value}
            </Heading>
          </VStack>
        </HStack>
      </Box>
    );
  }

  // filtering
  const filtered = useMemo(() => {
    const q = qDebounced.trim().toLowerCase();
    return rows.filter((r) => {
      if (statusFilter !== "ALL" && r.status !== statusFilter) return false;
      if (!q) return true;
      const name = (r.Name ?? r.input?.Name ?? "")?.toString().toLowerCase();
      const sym = (r.Symbol ?? r.input?.Symbol ?? "")?.toString().toLowerCase();
      const top = r.candidates?.[0]?.symbol?.toLowerCase() ?? "";
      return name.includes(q) || sym.includes(q) || String(r.status).toLowerCase().includes(q) || top.includes(q);
    });
  }, [rows, qDebounced, statusFilter]);

  // confidence heat map bg (very subtle)
  const rowBgForScore = (score?: number) => {
    if (typeof score !== "number") return undefined;
    const a = Math.min(Math.max((score - 0.5) * 0.8, 0), 0.35); // alpha 0..0.35 for 0.5..1.0
    const bad = score < 0.6;
    const [r,g,b] = bad ? [255, 80, 80] : score >= 0.85 ? [80, 200, 120] : [220, 180, 60];
    return `rgba(${r},${g},${b},${a.toFixed(2)})`;
  };

  return (
    <Box minH="100vh" bgGradient="linear(to-b, gray.900, rgba(10,10,12,0.95))" color="gray.100" py={8}>
      <Container maxW="7xl">
        {/* Sticky toolbar header */}
        <Box position="sticky" top={4} zIndex={2} p={{ base: 4, md: 5 }} rounded="2xl" borderWidth="1px" borderColor="whiteAlpha.200" bg="blackAlpha.500" backdropFilter="blur(8px)">
          <HStack justify="space-between" align={{ base: "start", md: "center" }} spacing={4}>
            <VStack align="start" spacing={1}>
              <Heading size={isNarrow ? "md" : "lg"} bgClip="text" bgGradient="linear(to-r, teal.300, blue.300)">Ticker Enrichment</Heading>
              <Text opacity={0.85} fontSize="sm">Upload, enrich, review, override, export — with full auditability.</Text>
            </VStack>
            <HStack spacing={2} wrap="wrap">
              <Tooltip label="Shortcuts (?)"><Button size="sm" variant="ghost" onClick={shortcuts.onOpen}>?</Button></Tooltip>
              <Tooltip label="Reset (R)"><IconButton aria-label="Reset" icon={<RepeatIcon />} onClick={resetAll} size="sm" variant="ghost" /></Tooltip>
              <Tooltip label="Preview commit (⌘/Ctrl+Enter)">
                <Button leftIcon={<DownloadIcon />} colorScheme="blue" onClick={openCommitPreview} isDisabled={!rows.length} size="sm">Commit & Download</Button>
              </Tooltip>
            </HStack>
          </HStack>

          <VStack align="stretch" spacing={3} mt={4}>
            <HStack spacing={3} align="stretch" flexWrap="wrap">
              {/* File */}
              <HStack spacing={3}>
                <Button as="label" leftIcon={<AttachmentIcon />} variant="outline" size="sm" cursor="pointer">
                  Choose File
                  <input type="file" hidden accept=".csv,.xlsx,.xls" onChange={onChooseFile} />
                </Button>
                <Text noOfLines={1} fontSize="sm" opacity={file ? 0.9 : 0.5}>
                  {file ? <Code colorScheme="blackAlpha">{file.name}</Code> : "No file selected"}
                </Text>
              </HStack>

              {/* Search */}
              <InputGroup minW={{ base: "100%", md: "360px" }} maxW="480px" flex="1">
                <InputLeftElement pointerEvents="none"><SearchIcon color="gray.400" /></InputLeftElement>
                <Input id="search-box" placeholder="Search name / symbol / status…" value={query} onChange={(e) => setQuery(e.target.value)} variant="filled" />
              </InputGroup>

              {/* Status chips with counts */}
              <HStack spacing={2}>
                {(["ALL","FILLED","AMBIGUOUS","NOT_FOUND","UNCHANGED","ENRICHED"] as const).map((s) => {
                  const count = s === "ALL" ? rows.length : counts[s as PreviewStatus];
                  return (
                    <Badge
                      key={s} px={3} py={1} rounded="full"
                      variant={statusFilter === s ? "solid" : "subtle"}
                      colorScheme={statusFilter === s ? "blue" : "gray"}
                      cursor="pointer" onClick={() => setStatusFilter(s as any)}
                    >
                      {s} {count > 0 ? count : ""}
                    </Badge>
                  );
                })}
              </HStack>

              {/* Local maps */}
              <FormControl display="flex" alignItems="center" w="auto">
                <FormLabel mb="0" fontWeight="semibold">Use Local Maps</FormLabel>
                <Tooltip label="Use ETF/alias canon as a safety net. APIs stay primary. (⌘/Ctrl+L)" openDelay={300}>
                  <Switch isChecked={useLocalMaps} onChange={(e) => setUseLocalMaps(e.target.checked)} />
                </Tooltip>
              </FormControl>
            </HStack>

            {loading && <Progress size="xs" isIndeterminate colorScheme="blue" rounded="full" />}
            {!!rows.length && (
              <HStack spacing={3} wrap="wrap">
                <Badge colorScheme="purple" variant="subtle">{lastRun.mode ?? "API-FIRST"}</Badge>
                <Badge colorScheme="blackAlpha" variant="subtle">Rows: {s1(rows.length)}</Badge>
                {typeof lastRun.durationMs === "number" && <Badge colorScheme="blue" variant="subtle">Preview: {ms(lastRun.durationMs)}</Badge>}
                {typeof lastRun.commitDurationMs === "number" && <Badge colorScheme="green" variant="subtle">Commit: {ms(lastRun.commitDurationMs)}</Badge>}
                <Badge colorScheme="teal" variant="subtle">Coverage: {pct(insights.coverage)}</Badge>
                {appliedAliasCount > 0 && <Badge colorScheme="pink" variant="subtle">Learned aliases applied: {appliedAliasCount}</Badge>}
              </HStack>
            )}
          </VStack>
        </Box>

        {/* Main grid */}
        <Box mt={6}>
          <SimpleGrid columns={{ base: 1, lg: 12 }} spacing={6}>
            {/* Data left */}
            <Box gridColumn={{ base: "1 / -1", lg: "1 / span 8" }}>
              {!!rows.length && (
                <Box>
                  <SimpleGrid columns={{ base: 2, md: 5 }} spacing={4}>
                    <StatCard label="FILLED" value={counts.FILLED} color="green.300" icon={<CheckCircleIcon />} />
                    <StatCard label="AMBIGUOUS" value={counts.AMBIGUOUS} color="yellow.300" icon={<InfoOutlineIcon />} />
                    <StatCard label="NOT_FOUND" value={counts.NOT_FOUND} color="red.300" icon={<NotAllowedIcon />} />
                    <StatCard label="UNCHANGED" value={counts.UNCHANGED} color="gray.300" icon={<WarningTwoIcon />} />
                    <StatCard label="ENRICHED" value={counts.ENRICHED} color="blue.300" icon={<CheckCircleIcon />} />
                  </SimpleGrid>
                  <Divider mt={4} borderColor="whiteAlpha.200" />
                </Box>
              )}

              {/* Bulk apply bar */}
              {!!filtered.length && (
                <HStack mt={4} spacing={4} align="center">
                  <Text fontSize="sm" color="gray.400">Bulk apply top candidate to filtered rows (FILLED/AMBIGUOUS)</Text>
                  <Button size="sm" onClick={bulkApplyTopCandidates} variant="outline">Apply</Button>
                  <HStack>
                    <Text fontSize="xs" color="gray.400">Min score</Text>
                    <Slider aria-label="score" min={0} max={1} step={0.05} value={bulkMinScore} onChange={setBulkMinScore} w="160px">
                      <SliderTrack><SliderFilledTrack /></SliderTrack>
                      <SliderThumb />
                    </Slider>
                    <Code fontSize="xs">{bulkMinScore.toFixed(2)}</Code>
                  </HStack>
                </HStack>
              )}

              {/* Table */}
              {rows.length > 0 && (
                <Box mt={4} overflow="auto" borderWidth="1px" borderColor="whiteAlpha.200" rounded="2xl" bg="blackAlpha.400" backdropFilter="blur(8px)"
                  sx={{ "th, td": { whiteSpace: "nowrap" } }}
                >
                  <Table size="sm" variant="simple">
                    <Thead bg="whiteAlpha.100" position="sticky" top={0} zIndex={1}>
                      <Tr>
                        <Th>#</Th>
                        <Th>Name</Th>
                        <Th display={{ base: "none", md: "table-cell" }}>Current Symbol</Th>
                        <Th>Status</Th>
                        <Th display={{ base: "none", md: "table-cell" }}>Top Candidate</Th>
                        <Th>Override</Th>
                      </Tr>
                    </Thead>
                    <Tbody>
                      {filtered.map((r, i) => {
                        const name = r.Name ?? r.input?.Name ?? "(blank)";
                        const currentSymbol = r.Symbol ?? r.input?.Symbol ?? "—";
                        const top = (r.candidates && r.candidates.length > 0) ? r.candidates[0] : null;
                        const canOverride = r.status !== "UNCHANGED" && (r.candidates?.length || 0) > 0;
                        const rowKey = r.index ?? i;
                        const bg = rowBgForScore(top?.score);

                        return (
                          <Tr
                            key={`${rowKey}-${i}`}
                            borderTop="1px solid"
                            borderColor="whiteAlpha.200"
                            _hover={{ bg: "whiteAlpha.100", cursor: "pointer" }}
                            onClick={() => setDrawerRow(r)}
                            style={bg ? { background: bg } : undefined}
                          >
                            <Td>{rowKey}</Td>
                            <Td maxW="360px"><Text noOfLines={1}>{name || "(blank)"}</Text></Td>
                            <Td display={{ base: "none", md: "table-cell" }}>{currentSymbol || "—"}</Td>
                            <Td><StatusTag s={r.status} /></Td>
                            <Td display={{ base: "none", md: "table-cell" }} maxW="520px">
                              <HStack spacing={2}>
                                <Text noOfLines={1}>{formatCandidate(top)}</Text>
                                {top?.source && <Badge variant="outline">{top.source}</Badge>}
                              </HStack>
                            </Td>
                            <Td minW={{ base: "160px", md: "240px" }} onClick={(e) => e.stopPropagation()}>
                              <Select
                                value={overrides[rowKey] ?? ""}
                                onChange={(e) => setOverrideForRow(rowKey, e.target.value)}
                                isDisabled={!canOverride}
                                size="sm"
                                variant="filled"
                              >
                                <option value="">(no override)</option>
                                {(r.candidates || []).map((c, j) => (
                                  <option key={j} value={c.symbol}>
                                    {c.symbol} — {c.name}
                                  </option>
                                ))}
                              </Select>
                            </Td>
                          </Tr>
                        );
                      })}
                    </Tbody>
                  </Table>
                </Box>
              )}

              {/* Empty state */}
              {!rows.length && !loading && (
                <Box mt={4} borderWidth="1px" borderColor="whiteAlpha.200" rounded="2xl" p={10} textAlign="center" color="gray.300" bg="blackAlpha.300" backdropFilter="blur(6px)">
                  <Text fontSize="lg" mb={2}>Start by choosing a CSV/XLSX file.</Text>
                  <Text fontSize="sm" opacity={0.8}>
                    Tip: Use <Kbd>⌘/Ctrl</Kbd> + <Kbd>K</Kbd> to focus search. Press <Kbd>A</Kbd> to bulk-apply top candidates.
                  </Text>
                </Box>
              )}
            </Box>

            {/* Right: summary + activity */}
            <VStack gridColumn={{ base: "1 / -1", lg: "span 4" }} spacing={4} align="stretch">
              <Box rounded="2xl" borderWidth="1px" borderColor="whiteAlpha.200" bg="blackAlpha.400" backdropFilter="blur(8px)" p={4}>
                <Heading size="sm" color="gray.200" mb={2}>Run Summary</Heading>
                <VStack align="stretch" spacing={2} fontSize="sm" color="gray.300">
                  <HStack justify="space-between"><Text opacity={0.8}>File</Text><Text>{lastRun.filename ?? "—"}</Text></HStack>
                  <HStack justify="space-between"><Text opacity={0.8}>Size</Text><Text>{lastRun.sizeBytes ? `${s1(lastRun.sizeBytes)} bytes` : "—"}</Text></HStack>
                  <HStack justify="space-between"><Text opacity={0.8}>Mode</Text><Text>{lastRun.mode ?? "API-FIRST"}</Text></HStack>
                  <HStack justify="space-between"><Text opacity={0.8}>Rows</Text><Text>{rows.length ? s1(rows.length) : "—"}</Text></HStack>
                  <HStack justify="space-between"><Text opacity={0.8}>Preview Time</Text><Text>{ms(lastRun.durationMs)}</Text></HStack>
                  {typeof lastRun.commitDurationMs === "number" && (
                    <HStack justify="space-between"><Text opacity={0.8}>Commit Time</Text><Text>{ms(lastRun.commitDurationMs)}</Text></HStack>
                  )}
                </VStack>
                {rows.length > 0 && (
                  <>
                    <Divider my={3} borderColor="whiteAlpha.200" />
                    <Text fontSize="xs" color="gray.400" mb={1}>Quality snapshot</Text>
                    <VStack align="stretch" spacing={1} fontSize="sm" color="gray.300">
                      <Text>{breakdownText(insights.counts)}</Text>
                      <Text>Coverage {(insights.coverage * 100).toFixed(1)}% • Avg candidates/row {insights.avgCandidates.toFixed(2)}</Text>
                      {insights.topSources.length > 0 && <Text>Top sources: {insights.topSources.join(" • ")}</Text>}
                    </VStack>
                  </>
                )}
              </Box>

              <ActivityLog
                logs={logs}
                loading={loading}
                onClear={() => setLogs([])}
                onCopy={onCopy}
                onDownload={downloadLog}
                clipboardValue={buildLogText()}
              />
            </VStack>
          </SimpleGrid>
        </Box>

        {/* Errors */}
        {error && <Box mt={4} color="red.300" fontWeight="medium">{error}</Box>}
      </Container>

      {/* Overlays */}
      <ShortcutsModal isOpen={shortcuts.isOpen} onClose={shortcuts.onClose} />
      <RowDrawer
        row={drawerRow}
        isOpen={!!drawerRow}
        onClose={() => setDrawerRow(null)}
        currentOverride={drawerRow ? overrides[drawerRow.index] ?? "" : ""}
        onSetOverride={(val) => drawerRow && setOverrideForRow(drawerRow.index, val)}
      />
      <CommitPreview
        isOpen={commitPreview.isOpen}
        onClose={commitPreview.onClose}
        changes={pendingChanges}
        onConfirm={commitAndDownload}
      />
    </Box>
  );
}
