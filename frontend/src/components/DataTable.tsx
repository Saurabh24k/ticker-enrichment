import {
    Box, Table, Thead, Tbody, Tr, Th, Td,
    Select, Tag, Text
  } from "@chakra-ui/react";
  import type { Candidate, PreviewRow, StatusKey } from "../lib/types";
  
  function formatCandidate(c?: Candidate | null) {
    if (!c) return "—";
    const bits: string[] = [];
    if (c.symbol) bits.push(c.symbol);
    if (c.name) bits.push(c.name);
    if (c.type) bits.push(c.type);
    if (typeof c.score === "number") bits.push(`score ${c.score.toFixed(2)}`);
    return bits.join(" · ");
  }
  
  function StatusPill({ s }: { s: StatusKey }) {
    const color: Record<StatusKey, string> = {
      FILLED: "green",
      AMBIGUOUS: "orange",
      NOT_FOUND: "red",
      UNCHANGED: "gray",
    };
    return <Tag colorScheme={color[s]}>{s}</Tag>;
  }
  
  export default function DataTable({
    rows,
    overrides,
    onOverride,
  }: {
    rows: PreviewRow[];
    overrides: Record<number, string>;
    onOverride: (rowIndex: number, symbol: string) => void;
  }) {
    return (
      <Box borderWidth="1px" borderRadius="lg" overflow="auto">
        <Table size="sm" variant="simple">
          <Thead position="sticky" top={0} bg="gray.800" zIndex={1}>
            <Tr>
              <Th>#</Th>
              <Th>Name</Th>
              <Th>Current Symbol</Th>
              <Th>Status</Th>
              <Th>Top Candidate</Th>
              <Th>Override</Th>
            </Tr>
          </Thead>
          <Tbody>
            {rows.map((r, i) => {
              const idx = r.index ?? i;
              const name = r.Name ?? r.input?.Name ?? "(blank)";
              const cur = r.Symbol ?? r.input?.Symbol ?? "—";
              const top = r.candidates?.[0] ?? null;
              const canOverride = r.status !== "UNCHANGED" && (r.candidates?.length || 0) > 0;
  
              return (
                <Tr key={idx}>
                  <Td>{idx}</Td>
                  <Td><Text noOfLines={1}>{name || "(blank)"}</Text></Td>
                  <Td>{cur || "—"}</Td>
                  <Td><StatusPill s={r.status} /></Td>
                  <Td><Text noOfLines={1}>{formatCandidate(top)}</Text></Td>
                  <Td>
                    <Select
                      size="sm"
                      value={overrides[idx] ?? ""}
                      onChange={(e) => onOverride(idx, e.target.value)}
                      isDisabled={!canOverride}
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
    );
  }
  