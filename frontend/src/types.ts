export type User = { id: string; email: string; created_at: string };
export type TelegramAccount = {
  id: string; telegram_user_id: number; username: string | null; first_name: string | null;
  status: 'ACTIVE' | 'NEEDS_REAUTH'; created_at: string; last_used_at: string | null; flood_wait_until: string | null;
};
export type Channel = {
  id: string; telegram_id: number; kind: 'CHANNEL' | 'SUPERGROUP' | 'GROUP'; title: string;
  username: string | null; participants_count: number | null; is_creator: boolean; is_admin: boolean;
  can_invite_users: boolean; refreshed_at: string;
};
export type Job = {
  id: string; status: string; pause_reason: string | null; pause_requested: boolean; cancellation_requested: boolean;
  retry_of_id: string | null; source: { id: string | null; title: string }; destination: { id: string | null; title: string };
  created_at: string; started_at: string | null; completed_at: string | null; total_members: number; processed_members: number;
  successful_count: number; skipped_count: number; failed_count: number; rate_limited_count: number; remaining_members: number;
  percent_complete: number; current_member: { id: string; label: string | null } | null; error_message: string | null;
  error_code: string | null; rate_limited_until: string | null; rate_limit_remaining_seconds: number; worker_active: boolean;
  allowed_actions: string[]; outcome_counts: Record<string, number>; analysis_state: string; analysis_note: string | null;
  member_limit: number; source_reported_total: number | null; source_listing_complete: boolean; destination_listing_complete: boolean;
  destination_banned_known: boolean; analysis_finished_at: string | null; summary: { total_analyzed: number; by_eligibility: Record<string, number> };
  notes: string[]; recent_events: MigrationEvent[];
};
export type JobItem = {
  id: string; status: string; pause_reason: string | null; analysis_state: string; source_title: string;
  destination_title: string; created_at: string; started_at: string | null; completed_at: string | null;
  total_members: number; processed_members: number; successful_count: number; skipped_count: number; failed_count: number;
  percent_complete: number; rate_limited_until: string | null;
};
export type MigrationEvent = {
  id: string; seq: number; event_type: string; message: string; status: string | null; reason: string | null;
  error_code: string | null; metadata: Record<string, unknown> | null; member_id: string | null; created_at: string;
};
export type Member = {
  id: string; telegram_user_id: number; username: string | null; display_name: string; role: string; selected: boolean;
  eligibility: string; eligibility_status: string; state: string; processing_status: string; reason: string | null;
  outcome: string | null; attempt_count: number; last_attempt_at: string | null; processed_at: string | null;
  failure_reason: string | null; telegram_error_code: string | null;
};
export type Page<T> = { items: T[]; total: number; limit: number; offset: number };
