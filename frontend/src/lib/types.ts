export type StatusKey = "FILLED" | "AMBIGUOUS" | "NOT_FOUND" | "UNCHANGED";

export type Candidate = {
  symbol: string;
  name: string;
  type?: string;
  score?: number;
};

export type PreviewRow = {
  index: number;
  status: StatusKey;
  candidates?: Candidate[];
  notes?: string;
  input?: { Name?: string | null; Symbol?: string | null };
  Name?: string | null;
  Symbol?: string | null;
};

export type Counts = Record<StatusKey, number>;
