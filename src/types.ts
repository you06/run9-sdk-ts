export interface Credentials {
  ak: string;
  sk: string;
}

export type MembershipRole = string;
export type OrgKind = "personal" | "shared" | string;
export type ProjectRole = string;
export type ProjectSecretScope = "project" | "box" | string;
export type InvitationState = string;
export type BoxState = string;
export type BoxNetworkMode = "normal" | "managed" | string;
export type BoxSecurityMode = "restricted" | "unsafe" | string;
export type SnapState = string;

export interface CurrentSubscriptionView {
  tier: string;
  start_date?: string;
  end_date?: string;
}

export interface MeView {
  user_id: string;
  primary_email: string;
  display_name?: string;
  created_at: string;
  is_system_manager: boolean;
}

export interface OrgView {
  org_id: string;
  org_cid: string;
  display_name: string;
  kind: OrgKind;
  role: MembershipRole;
  created_by: string;
  created_at: string;
  current_subscription: CurrentSubscriptionView;
}

export interface CurrentOrgIdentityView {
  user: MeView;
  org: OrgView;
  auth_kind: string;
}

export interface DeleteOrgResult {
  org_id: string;
  status: string;
}

export interface MembershipView {
  org_id: string;
  user_id: string;
  primary_email: string;
  display_name?: string;
  role: MembershipRole;
  created_at: string;
  updated_at: string;
  last_login_at?: string;
}

export interface InvitationView {
  invitation_id: string;
  org_id: string;
  invitee_email: string;
  role: MembershipRole;
  invited_by: string;
  state: InvitationState;
  expires_at: string;
  accepted_by?: string;
  created_at: string;
  updated_at: string;
}

export interface DeleteInvitationResult {
  invitation_id: string;
  status: string;
}

export interface APIKeyView {
  api_key_id: string;
  org_id: string;
  user_id: string;
  owner_primary_email: string;
  owner_display_name?: string;
  description?: string;
  ak: string;
  display_prefix: string;
  display_suffix: string;
  created_at: string;
  expires_at?: string;
  no_expire: boolean;
}

export interface CreatedAPIKeyView extends APIKeyView {
  sk: string;
}

export interface SSHKeyView {
  ssh_key_id: string;
  label: string;
  fingerprint: string;
  created_at: string;
  last_used_at?: string | null;
}

export interface ProjectView {
  project_id: string;
  org_id: string;
  project_cid: string;
  display_name: string;
  description?: string;
  role: ProjectRole;
  created_by: string;
  created_at: string;
  updated_at: string;
}

export interface DeleteProjectResult {
  project_id: string;
  status: string;
}

export interface ProjectMembershipView {
  project_id: string;
  user_id: string;
  primary_email: string;
  display_name?: string;
  role: ProjectRole;
  created_at: string;
  updated_at: string;
}

export interface ProjectSecretView {
  secret_id: string;
  org_id: string;
  project_id: string;
  scope: ProjectSecretScope;
  box_id?: string;
  name?: string;
  placeholder: string;
  allowed_hosts: string[];
  inject_header_name: string;
  created_by: string;
  updated_by: string;
  created_at: string;
  updated_at: string;
}

export interface CreateProjectRequest {
  display_name: string;
  description?: string;
}

export interface CreateProjectSecretRequest {
  name?: string;
  value: string;
  placeholder: string;
  allowed_hosts: string[];
  inject_header_name: string;
}

export interface UpdateProjectSecretRequest {
  name?: string;
  value?: string;
  allowed_hosts?: string[];
  inject_header_name?: string;
}

export interface ListBoxesRequest {
  creator?: string;
  label?: string;
  state?: BoxState;
}

export interface BoxView {
  box_id: string;
  org_id: string;
  project_id: string;
  creator: string;
  created_at: string;
  last_used_at: string;
  description?: string;
  labels?: Record<string, string>;
  state: BoxState;
  reason?: string;
  box_snap_id: string;
  desired_shape: string;
  network_mode: BoxNetworkMode;
  security_mode: BoxSecurityMode;
  current_host_id?: string;
  current_runtime_shape?: string;
  current_runtime_network_mode?: BoxNetworkMode;
  current_runtime_security_mode?: BoxSecurityMode;
  pending_shape_change: boolean;
  pending_network_mode_change: boolean;
  pending_security_mode_change: boolean;
}

export interface SnapView {
  snap_id: string;
  org_id: string;
  project_id: string;
  creator: string;
  created_at: string;
  last_used_at: string;
  state: SnapState;
  inuse_reason?: string;
  reason?: string;
  parent_chain?: string[];
  source_image_ref?: string;
  source_image_digest?: string;
  source_image_platform?: string;
  attached: boolean;
  attached_box_id?: string;
  size?: SnapSize;
  owned_storage?: SnapOwnedStorage;
}

export interface SnapTreeView {
  supported: boolean;
  root_snap_id?: string;
  nodes?: SnapTreeNodeView[];
}

export interface SnapTreeNodeView {
  snap_id: string;
  parent_snap_id?: string;
  creator: string;
  creator_display_name?: string;
  creator_primary_email?: string;
  created_at: string;
  state: SnapState;
  reason?: string;
  source_image_ref?: string;
  source_image_digest?: string;
  source_image_platform?: string;
  attached_box?: SnapTreeAttachedBoxView;
}

export interface SnapTreeAttachedBoxView {
  box_id: string;
  desired_shape: string;
  state: BoxState;
}

export interface SnapSize {
  used_bytes: number;
  used_inodes: number;
  measured_at: string;
}

export interface SnapOwnedStorage {
  bytes: number;
  objects: number;
  measured_at: string;
}

export interface RuntimeRequestView {
  runtime_request_id: string;
  state: string;
  session_id?: string;
  host_id?: string;
}

export interface ExecView {
  exec_id: string;
  box_id: string;
  org_id: string;
  project_id: string;
  creator: string;
  accepted_at: string;
  source: string;
  command_summary: string;
  shape_snapshot: string;
  mode?: string;
  state: string;
  exit_code?: number | null;
  output_summary?: string;
  reason?: string;
  hard_deadline_at?: string | null;
  idle_deadline_at?: string | null;
  stdin_enabled?: boolean;
  attach_url?: string;
  diagnostics?: Record<string, unknown>;
}

export type ExecTerminalStatus = "exited" | "cancelled" | "error";

export interface ExecTerminalResult {
  status: ExecTerminalStatus;
  exitCode?: number;
  reason?: string;
}

export type ExecOutputWriter =
  | { write(chunk: Uint8Array): void | Promise<void> | boolean }
  | WritableStream<Uint8Array>
  | ((chunk: Uint8Array) => void | Promise<void>);

export interface ExecOutputWriters {
  stdout?: ExecOutputWriter;
  stderr?: ExecOutputWriter;
}

export interface SharedSnapLineView {
  org_id: string;
  name: string;
  latest_version: number;
  publisher: string;
  published_by: string;
  published_at: string;
  description?: string;
  source_project_id: string;
  source_project_cid: string;
  source_project_display_name: string;
}

export interface SharedSnapVersionView {
  org_id: string;
  name: string;
  version: number;
  description?: string;
  published_by: string;
  published_at: string;
  source_project_id: string;
  source_project_cid: string;
  source_project_display_name: string;
}

export interface SharedSnapDetailView {
  org_id: string;
  name: string;
  latest_version: number;
  publisher: string;
  versions: SharedSnapVersionView[];
}

export interface HostIssue {
  code: string;
  message: string;
}

export interface HostView {
  host_id: string;
  host_class?: string;
  lifecycle_state?: string;
  instance_id?: string;
  owner_org_id?: string;
  machine_id?: string;
  boot_id?: string;
  hostname?: string;
  vma_version?: string;
  ch_version?: string;
  rt_version?: string;
  foreground_relay_configured?: boolean;
  connected: boolean;
  ready: boolean;
  last_heartbeat_at?: string;
  active_boxes: number;
  active_execs: number;
  active_transfers: number;
  active_background_owner_execs: number;
  vma_service_restart_preserves_background_owners: boolean;
  planning_reserved_cpu_millis: number;
  planning_reserved_memory_bytes: number;
  cpu_total_cores: number;
  memory_total_bytes: number;
  cpu_used_cores: number;
  memory_used_bytes: number;
  runtime_reserved_cpu_millis: number;
  runtime_reserved_memory_bytes: number;
  last_issue_summary?: string;
  issues?: HostIssue[];
}

export interface OrgHostsView {
  org_id: string;
  assigned_hosts: number;
  hosts: HostView[];
}

export interface TTYSize {
  rows?: number;
  cols?: number;
}

export interface CreateBoxRequest {
  box_id?: string;
  desired_shape?: string;
  network_mode?: BoxNetworkMode;
  security_mode?: BoxSecurityMode;
  description?: string;
  labels?: Record<string, string>;
  source_snap_id?: string;
  source_image_ref?: string;
}

export interface CreateBoxFromSharedSnapRequest {
  version?: number;
  box_id?: string;
  desired_shape?: string;
  network_mode?: BoxNetworkMode;
  security_mode?: BoxSecurityMode;
  description?: string;
  labels?: Record<string, string>;
}

export interface ImportSnapRequest {
  image_ref: string;
}

export interface ListSnapsRequest {
  attached?: boolean;
}

export interface UpdateMeRequest {
  display_name?: string;
}

export interface CreateSSHKeyRequest {
  label: string;
  public_key: string;
}

export interface UpdateOrgRequest {
  display_name?: string;
  org_cid?: string;
}

export interface UpdateMembershipRequest {
  role: MembershipRole;
}

export interface CreateInvitationRequest {
  invitee_email: string;
  role: MembershipRole;
}

export interface CreateAPIKeyRequest {
  description?: string;
  expires_at?: string;
  no_expire: boolean;
}

export interface UpdateProjectRequest {
  display_name?: string;
  description?: string;
}

export interface UpdateProjectMembershipRequest {
  role: ProjectRole;
}

export interface UpdateBoxRequest {
  description?: string;
  labels?: Record<string, string>;
  desired_shape?: string;
  network_mode?: BoxNetworkMode;
  security_mode?: BoxSecurityMode;
}

export interface PublishSharedSnapRequest {
  name: string;
  description?: string;
  source_snap_id: string;
}

export interface CreateSnapFromSharedSnapRequest {
  version?: number;
}

export interface ExecRequest {
  deadline_at?: string;
  command: string[];
  env_overrides?: Record<string, string>;
  user?: string;
  workdir?: string;
  stdin_enabled?: boolean;
  tty?: boolean;
  tty_size?: TTYSize;
}

export interface ExecStreamEvent {
  type: string;
  exec_id?: string;
  data?: Uint8Array;
  exit_code?: number;
  failure_reason?: string;
  cancel_reason?: string;
}

export type BackgroundExecOutputEventType =
  | "started"
  | "stdout"
  | "stderr"
  | "gap"
  | "truncated"
  | "exit"
  | "cancelled"
  | "error";

export interface BackgroundExecOutputEvent {
  seq: number;
  type: BackgroundExecOutputEventType;
  data?: Uint8Array;
  gapBytes?: number;
  exitCode?: number;
  reason?: string;
}

export interface ExecAttachInput {
  type: string;
  data?: string | Uint8Array;
  rows?: number;
  cols?: number;
}

export interface ListExecsRequest {
  boxID?: string;
  state?: string;
  creator?: string;
  acceptedAfter?: string | Date;
  acceptedBefore?: string | Date;
  order?: string;
  paged?: boolean;
  limit?: number;
  cursor?: string;
}

export interface ListExecsResult {
  execs: ExecView[];
  nextCursor: string;
}

export interface PullBackgroundExecOutputRequest {
  cursor?: string;
  wait?: number;
}

export interface WriteBackgroundExecStdinRequest {
  data: BodyInit;
  closeStdin?: boolean;
}

export interface ExecCapture {
  execID: string;
  terminal: ExecTerminalResult;
  transcript?: Uint8Array;
  transcriptUnavailableReason?: string;
}
