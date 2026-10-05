export interface Limits {
  initial_collateral_usd: string;
  max_imbalance_shares: string;
  lot_shares: string;
  half_width: string;
  max_feed_age_ms: number;
}
export interface Order {
  id: number | string;
  quote: { slot: number | string; outcome: 'up' | 'down'; price_micros: number; quantity_micros: number };
  phase: string;
  cumulative_filled_micros: number;
}
export interface Simulation {
  simulated: true;
  now_ms: number;
  account: {
    initial_cash_micros: number; spent_micros_ceil: number; reserved_micros_ceil: number;
    available_micros_floor: number; up_inventory_micros: number; down_inventory_micros: number;
  };
  orders: Order[];
  recent_audit: { sequence: number; at_ms: number; kind: string; order_id: number | null; quantity_micros: number }[];
  audit_evicted: number;
  fault: string | null;
}
export interface EngineState {
  boot_id: string; published_at_ms: number; command_version: number;
  mode: 'paused' | 'shadow' | 'killed'; execution_mode: 'emulator' | 'simulation' | 'shadow' | 'trading' | 'both';
  feed_source: string; api_ready: boolean; feed_ready: boolean; shadow_ready: boolean;
  live_eligible: boolean; exact_twap_available: false;
  live?: {
    simulated: false; phase: 'paused' | 'running' | 'killed' | 'faulted'; can_resume: boolean;
    account_reconciled: boolean; user_stream_connected: boolean;
    last_reconciliation_ms: number | null; last_heartbeat_ms: number | null;
    collateral_micros: number; available_micros: number; reserved_pico: string;
    orders: { order_id: string; quote: Order['quote']; phase: string; cumulative_filled_micros: number }[];
    positions: { condition_id: string; token_id: string; outcome: 'up' | 'down'; quantity_micros: number }[];
    fault: string | null; unsupported_operations: string[];
  } | null;
  paper_portfolio?: { cash_pico: string; available_pico: string; spent_pico: string;
    position_count?:number;positions_truncated?:number;
    available_cash_micros: number; positions: { market: { id: string }; up: { quantity_micros: number }; down: { quantity_micros: number } }[] } | null;
  parallel_simulation?: { mode: string; fault: string | null; published_at_ms: number; command_version: number } | null;
  journal?: { total_bytes: number; total_max_bytes: number; archive_segments: number } | null;
  simulation: Simulation | null; simulation_error?: string | null;
  market: { slug: string; window_start_ms: number; window_end_ms: number } | null;
  limits: Limits;
  source_status: { phase: string; message?: string | null; retry_in_ms?: number | null; at_ms?: number };
  books: { up: { exchange_age_ms: number | null; receive_age_ms: number | null; initialized: boolean }; down: { exchange_age_ms: number | null; receive_age_ms: number | null; initialized: boolean } };
  fault: string | null; last_input_error: string | null; stop_reasons: string[];
  metrics: { telemetry_coalesced?: number; telemetry_max_hz?: number; decision_count?: number };
}
export interface Envelope {
  state: EngineState | null; observed_at_ms: number | null; connected: boolean; error: string | null;
  telemetry: { sampling_interval_ms: number; sample_count: number; history_dropped: number; logs_dropped: number;
    overruns: number; last_sampling_ms: number | null; storage_error: string | null; publication_lag_ms: number | null };
}
export interface Sample { observed_at_ms: number; state: EngineState }
export interface Log { id: number; observed_at_ms: number; level: string; message: string }
export type Control = 'pause' | 'resume' | 'kill' | 'reset';
export type Command = { id: string; expected_version: number; action: Control } | { id: string; expected_version: number; limits: Limits };
export interface CommandReply {
  receipt: { command_id: string; applied_version: number; mode_at_ack: string };
  replayed: boolean; state?: EngineState;
}
