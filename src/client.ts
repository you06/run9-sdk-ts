import WebSocket from "ws";
import type { IncomingMessage } from "node:http";
import type {
  APIKeyView,
  BackgroundExecPullOutput,
  BoxView,
  CreateAPIKeyRequest,
  CreateBoxFromSharedSnapRequest,
  CreateBoxRequest,
  CreateInvitationRequest,
  CreateProjectRequest,
  CreateProjectSecretRequest,
  CreateSSHKeyRequest,
  CreateSnapFromSharedSnapRequest,
  CreatedAPIKeyView,
  Credentials,
  CurrentOrgIdentityView,
  DeleteInvitationResult,
  DeleteOrgResult,
  DeleteProjectResult,
  ExecAttachInput,
  ExecBoxRequest,
  ExecListRequest,
  ExecListResult,
  ExecStreamEvent,
  ExecStreamResult,
  ExecView,
  InvitationView,
  MeView,
  MembershipView,
  OrgHostsView,
  OrgView,
  ProjectMembershipView,
  ProjectSecretView,
  ProjectView,
  PublishSharedSnapRequest,
  RuntimeRequestView,
  SharedSnapDetailView,
  SharedSnapLineView,
  SharedSnapVersionView,
  SnapTreeView,
  SnapView,
  SSHKeyView,
  UpdateBoxRequest,
  UpdateMeRequest,
  UpdateMembershipRequest,
  UpdateOrgRequest,
  UpdateProjectMembershipRequest,
  UpdateProjectRequest,
  UpdateProjectSecretRequest
} from "./types.js";

export interface Run9ClientOptions {
  fetch?: typeof fetch;
}

export interface BoxesFilters {
  creator?: string;
  label?: string;
  state?: string;
}

export interface SnapsFilters {
  attached?: string;
}

interface RequestOptions {
  query?: Record<string, string | number | boolean | undefined>;
  headers?: Record<string, string>;
  body?: BodyInit | object | null;
  signal?: AbortSignal;
}

interface ErrorPayload {
  error?: string;
}

export class Run9Error extends Error {
  readonly statusCode: number;

  constructor(statusCode: number, message?: string) {
    super(message?.trim() || `portal api request failed with status ${statusCode}`);
    this.name = "Run9Error";
    this.statusCode = statusCode;
  }
}

export class ExecAttachSocket {
  constructor(private readonly socket: WebSocket) {}

  readEvent(): Promise<ExecStreamEvent> {
    return new Promise((resolve, reject) => {
      const onMessage = (data: WebSocket.RawData) => {
        cleanup();
        try {
          const raw = typeof data === "string" ? data : Buffer.concat(asBuffers(data)).toString("utf8");
          resolve(JSON.parse(raw) as ExecStreamEvent);
        } catch (error) {
          reject(error);
        }
      };
      const onError = (error: Error) => {
        cleanup();
        reject(error);
      };
      const onClose = () => {
        cleanup();
        reject(new Error("exec attach websocket closed"));
      };
      const cleanup = () => {
        this.socket.off("message", onMessage);
        this.socket.off("error", onError);
        this.socket.off("close", onClose);
      };
      this.socket.once("message", onMessage);
      this.socket.once("error", onError);
      this.socket.once("close", onClose);
    });
  }

  writeInput(input: ExecAttachInput): Promise<void> {
    const payload = {
      ...input,
      data: input.data instanceof Uint8Array ? Buffer.from(input.data).toString("base64") : input.data
    };
    return new Promise((resolve, reject) => {
      this.socket.send(JSON.stringify(payload), (error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  }

  close(): void {
    this.socket.close();
  }
}

export class Run9Client {
  private readonly baseURL: string;
  private readonly projectCID: string;
  private readonly fetchImpl: typeof fetch;

  constructor(endpoint: string, options: Run9ClientOptions = {}) {
    this.baseURL = endpoint.trim().replace(/\/+$/, "");
    this.projectCID = "";
    this.fetchImpl = options.fetch ?? fetch;
  }

  private clone(projectCID: string): Run9Client {
    const clone = Object.create(Run9Client.prototype) as Run9Client;
    Object.assign(clone, this, { projectCID: projectCID.trim() });
    return clone;
  }

  withProject(projectCID: string): Run9Client {
    return this.clone(projectCID);
  }

  whoAmI(creds: Credentials, options: { signal?: AbortSignal } = {}): Promise<CurrentOrgIdentityView> {
    return this.request("GET", "/whoami", creds, { signal: options.signal });
  }

  updateAccount(creds: Credentials, req: UpdateMeRequest, options: { signal?: AbortSignal } = {}): Promise<MeView> {
    return this.request("PATCH", "/account", creds, { body: req, signal: options.signal });
  }

  sshKeys(creds: Credentials, options: { signal?: AbortSignal } = {}): Promise<SSHKeyView[]> {
    return this.request("GET", "/account/ssh-keys", creds, { signal: options.signal });
  }

  createSSHKey(creds: Credentials, req: CreateSSHKeyRequest, options: { signal?: AbortSignal } = {}): Promise<SSHKeyView> {
    return this.request("POST", "/account/ssh-keys", creds, { body: req, signal: options.signal });
  }

  deleteSSHKey(creds: Credentials, sshKeyID: string, options: { signal?: AbortSignal } = {}): Promise<SSHKeyView> {
    return this.request("DELETE", `/account/ssh-keys/${encodePath(sshKeyID)}`, creds, { signal: options.signal });
  }

  projects(creds: Credentials, options: { signal?: AbortSignal } = {}): Promise<ProjectView[]> {
    return this.request("GET", "/projects", creds, { signal: options.signal });
  }

  createProject(creds: Credentials, req: CreateProjectRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectView> {
    return this.request("POST", "/projects", creds, { body: req, signal: options.signal });
  }

  projectByCID(creds: Credentials, projectCID: string, options: { signal?: AbortSignal } = {}): Promise<ProjectView> {
    return this.request("GET", `/projects/${encodePath(projectCID)}`, creds, { signal: options.signal });
  }

  updateProject(creds: Credentials, projectCID: string, req: UpdateProjectRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectView> {
    return this.request("PATCH", projectPath(projectCID), creds, { body: req, signal: options.signal });
  }

  deleteProject(creds: Credentials, projectCID: string, options: { signal?: AbortSignal } = {}): Promise<DeleteProjectResult> {
    return this.request("DELETE", projectPath(projectCID), creds, { signal: options.signal });
  }

  projectMembers(creds: Credentials, projectCID: string, options: { signal?: AbortSignal } = {}): Promise<ProjectMembershipView[]> {
    return this.request("GET", projectPath(projectCID, "/members"), creds, { signal: options.signal });
  }

  updateProjectMember(
    creds: Credentials,
    projectCID: string,
    userID: string,
    req: UpdateProjectMembershipRequest,
    options: { signal?: AbortSignal } = {}
  ): Promise<ProjectMembershipView> {
    return this.request("PATCH", projectPath(projectCID, `/members/${encodePath(userID)}`), creds, { body: req, signal: options.signal });
  }

  removeProjectMember(creds: Credentials, projectCID: string, userID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", projectPath(projectCID, `/members/${encodePath(userID)}`), creds, { signal: options.signal });
  }

  updateOrg(creds: Credentials, orgID: string, req: UpdateOrgRequest, options: { signal?: AbortSignal } = {}): Promise<OrgView> {
    return this.request("PATCH", orgPath(orgID), creds, { body: req, signal: options.signal });
  }

  deleteOrg(creds: Credentials, orgID: string, options: { signal?: AbortSignal } = {}): Promise<DeleteOrgResult> {
    return this.request("DELETE", orgPath(orgID), creds, { signal: options.signal });
  }

  orgMembers(creds: Credentials, orgID: string, options: { signal?: AbortSignal } = {}): Promise<MembershipView[]> {
    return this.request("GET", orgPath(orgID, "/members"), creds, { signal: options.signal });
  }

  updateOrgMember(
    creds: Credentials,
    orgID: string,
    userID: string,
    req: UpdateMembershipRequest,
    options: { signal?: AbortSignal } = {}
  ): Promise<MembershipView> {
    return this.request("PATCH", orgPath(orgID, `/members/${encodePath(userID)}`), creds, { body: req, signal: options.signal });
  }

  removeOrgMember(creds: Credentials, orgID: string, userID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", orgPath(orgID, `/members/${encodePath(userID)}`), creds, { signal: options.signal });
  }

  invitations(creds: Credentials, orgID: string, options: { signal?: AbortSignal } = {}): Promise<InvitationView[]> {
    return this.request("GET", orgPath(orgID, "/invitations"), creds, { signal: options.signal });
  }

  createInvitation(creds: Credentials, orgID: string, req: CreateInvitationRequest, options: { signal?: AbortSignal } = {}): Promise<InvitationView> {
    return this.request("POST", orgPath(orgID, "/invitations"), creds, { body: req, signal: options.signal });
  }

  revokeInvitation(creds: Credentials, orgID: string, invitationID: string, options: { signal?: AbortSignal } = {}): Promise<DeleteInvitationResult> {
    return this.request("DELETE", orgPath(orgID, `/invitations/${encodePath(invitationID)}`), creds, { signal: options.signal });
  }

  apiKeys(creds: Credentials, options: { signal?: AbortSignal } = {}): Promise<APIKeyView[]> {
    return this.request("GET", "/api-keys", creds, { signal: options.signal });
  }

  createAPIKey(creds: Credentials, req: CreateAPIKeyRequest, options: { signal?: AbortSignal } = {}): Promise<CreatedAPIKeyView> {
    return this.request("POST", "/api-keys", creds, { body: req, signal: options.signal });
  }

  revokeAPIKey(creds: Credentials, apiKeyID: string, options: { signal?: AbortSignal } = {}): Promise<APIKeyView> {
    return this.request("DELETE", `/api-keys/${encodePath(apiKeyID)}`, creds, { signal: options.signal });
  }

  orgHosts(creds: Credentials, options: { signal?: AbortSignal } = {}): Promise<OrgHostsView> {
    return this.request("GET", "/org-runtime/hosts", creds, { signal: options.signal });
  }

  createBox(creds: Credentials, req: CreateBoxRequest, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("POST", this.workspacePath("/boxes"), creds, { body: req, signal: options.signal });
  }

  boxes(creds: Credentials, filters: BoxesFilters = {}, options: { signal?: AbortSignal } = {}): Promise<BoxView[]> {
    return this.request("GET", this.workspacePath("/boxes"), creds, {
      query: {
        creator: trimmed(filters.creator),
        label: trimmed(filters.label),
        state: trimmed(filters.state)
      },
      signal: options.signal
    });
  }

  box(creds: Credentials, boxID: string, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("GET", this.workspacePath(`/boxes/${encodePath(boxID)}`), creds, { signal: options.signal });
  }

  updateBox(creds: Credentials, boxID: string, req: UpdateBoxRequest, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("PATCH", this.workspacePath(`/boxes/${encodePath(boxID)}`), creds, { body: req, signal: options.signal });
  }

  stopBox(creds: Credentials, boxID: string, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/stop`), creds, { signal: options.signal });
  }

  removeBox(creds: Credentials, boxID: string, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("DELETE", this.workspacePath(`/boxes/${encodePath(boxID)}`), creds, { signal: options.signal });
  }

  importSnap(creds: Credentials, imageRef: string, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("POST", this.workspacePath("/snaps/import"), creds, {
      body: { image_ref: imageRef.trim() },
      signal: options.signal
    });
  }

  snaps(creds: Credentials, filters: SnapsFilters = {}, options: { signal?: AbortSignal } = {}): Promise<SnapView[]> {
    return this.request("GET", this.workspacePath("/snaps"), creds, {
      query: { attached: trimmed(filters.attached) },
      signal: options.signal
    });
  }

  snap(creds: Credentials, snapID: string, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("GET", this.workspacePath(`/snaps/${encodePath(snapID)}`), creds, { signal: options.signal });
  }

  forkSnap(creds: Credentials, snapID: string, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("POST", this.workspacePath(`/snaps/${encodePath(snapID)}/fork`), creds, { signal: options.signal });
  }

  removeSnap(creds: Credentials, snapID: string, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("DELETE", this.workspacePath(`/snaps/${encodePath(snapID)}`), creds, { signal: options.signal });
  }

  snapTree(creds: Credentials, snapID: string, options: { signal?: AbortSignal } = {}): Promise<SnapTreeView> {
    return this.request("GET", this.workspacePath(`/snaps/${encodePath(snapID)}/tree`), creds, { signal: options.signal });
  }

  exec(creds: Credentials, boxID: string, req: ExecBoxRequest, options: { signal?: AbortSignal } = {}): Promise<ExecView> {
    return this.request("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/execs`), creds, { body: req, signal: options.signal });
  }

  backgroundExec(creds: Credentials, boxID: string, req: ExecBoxRequest, options: { signal?: AbortSignal } = {}): Promise<ExecView> {
    return this.request("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/background-execs`), creds, { body: req, signal: options.signal });
  }

  async execStream(creds: Credentials, boxID: string, req: ExecBoxRequest, options: { signal?: AbortSignal } = {}): Promise<ExecStreamResult> {
    const response = await this.rawRequest("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/execs/stream`), creds, {
      body: req,
      headers: {
        Accept: "application/x-ndjson",
        "X-Run9-Exec-Stream-Mode": "inline"
      },
      signal: options.signal
    });
    if (!response.body) {
      throw new Error("portal api returned empty response body");
    }
    return {
      execID: response.headers.get("X-Run9-Exec-ID")?.trim() ?? "",
      body: response.body
    };
  }

  async execs(creds: Credentials, req: ExecListRequest = {}, options: { signal?: AbortSignal } = {}): Promise<ExecListResult> {
    const response = await this.rawRequest("GET", this.workspacePath("/execs"), creds, {
      query: {
        box_id: trimmed(req.boxID),
        state: trimmed(req.state),
        creator: trimmed(req.creator),
        accepted_after: dateParam(req.acceptedAfter),
        accepted_before: dateParam(req.acceptedBefore),
        order: trimmed(req.order),
        paged: req.paged ? "true" : undefined,
        limit: req.limit,
        cursor: trimmed(req.cursor)
      },
      signal: options.signal
    });
    const execs = await decodeJSON<ExecView[]>(response);
    return {
      execs,
      nextCursor: response.headers.get("X-Run9-Next-Cursor")?.trim() ?? ""
    };
  }

  execByID(creds: Credentials, execID: string, options: { signal?: AbortSignal } = {}): Promise<ExecView> {
    return this.request("GET", this.workspacePath(`/execs/${encodePath(execID)}`), creds, { signal: options.signal });
  }

  downloadExecLog(creds: Credentials, execID: string, options: { signal?: AbortSignal } = {}): Promise<ReadableStream<Uint8Array>> {
    return this.download("GET", this.workspacePath(`/execs/${encodePath(execID)}/log-download`), creds, { signal: options.signal });
  }

  async pullBackgroundExecOutput(
    creds: Credentials,
    execID: string,
    cursor = "",
    waitMs = 0,
    options: { signal?: AbortSignal } = {}
  ): Promise<BackgroundExecPullOutput> {
    const response = await this.rawRequest("POST", this.workspacePath(`/execs/${encodePath(execID)}/pull-output`), creds, {
      body: {
        cursor: cursor.trim() || undefined,
        wait_ms: waitMs > 0 ? waitMs : undefined
      },
      signal: options.signal
    });
    const body = new Uint8Array(await response.arrayBuffer());
    const rawExitCode = response.headers.get("X-Run9-Exit-Code")?.trim();
    return {
      body,
      nextCursor: response.headers.get("X-Run9-Next-Cursor")?.trim() ?? "",
      state: response.headers.get("X-Run9-Exec-State")?.trim() ?? "",
      exitCode: rawExitCode ? Number.parseInt(rawExitCode, 10) : undefined,
      reason: response.headers.get("X-Run9-Reason")?.trim() ?? "",
      idleDeadlineAt: response.headers.get("X-Run9-Idle-Deadline-At")?.trim() || undefined
    };
  }

  async writeBackgroundExecStdin(
    creds: Credentials,
    execID: string,
    data: BodyInit,
    closeStdin = false,
    options: { signal?: AbortSignal } = {}
  ): Promise<string | undefined> {
    const response = await this.rawRequest("POST", this.workspacePath(`/execs/${encodePath(execID)}/write-stdin`), creds, {
      body: data,
      headers: {
        "Content-Type": "application/octet-stream",
        "X-Run9-Close-Stdin": String(closeStdin)
      },
      signal: options.signal
    });
    await drain(response);
    return response.headers.get("X-Run9-Idle-Deadline-At")?.trim() || undefined;
  }

  killBackgroundExec(creds: Credentials, execID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("POST", this.workspacePath(`/execs/${encodePath(execID)}/kill`), creds, { signal: options.signal });
  }

  uploadArchive(
    creds: Credentials,
    boxID: string,
    boxAbsPath: string,
    source: BodyInit,
    options: { signal?: AbortSignal } = {}
  ): Promise<RuntimeRequestView> {
    return this.request("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/files/upload`), creds, {
      query: { box_abs_path: boxAbsPath.trim() },
      headers: { "Content-Type": "application/x-tar" },
      body: source,
      signal: options.signal
    });
  }

  downloadArchive(creds: Credentials, boxID: string, boxAbsPath: string, options: { signal?: AbortSignal } = {}): Promise<ReadableStream<Uint8Array>> {
    return this.download("GET", this.workspacePath(`/boxes/${encodePath(boxID)}/files/download`), creds, {
      query: {
        archive: "tar",
        box_abs_path: boxAbsPath.trim()
      },
      signal: options.signal
    });
  }

  projectSecrets(creds: Credentials, projectCID: string, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView[]> {
    return this.request("GET", projectPath(projectCID, "/secrets"), creds, { signal: options.signal });
  }

  createProjectSecret(creds: Credentials, projectCID: string, req: CreateProjectSecretRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView> {
    return this.request("POST", projectPath(projectCID, "/secrets"), creds, { body: req, signal: options.signal });
  }

  updateProjectSecret(
    creds: Credentials,
    projectCID: string,
    secretID: string,
    req: UpdateProjectSecretRequest,
    options: { signal?: AbortSignal } = {}
  ): Promise<ProjectSecretView> {
    return this.request("PATCH", projectPath(projectCID, `/secrets/${encodePath(secretID)}`), creds, { body: req, signal: options.signal });
  }

  deleteProjectSecret(creds: Credentials, projectCID: string, secretID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", projectPath(projectCID, `/secrets/${encodePath(secretID)}`), creds, { signal: options.signal });
  }

  boxSecrets(creds: Credentials, boxID: string, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView[]> {
    return this.request("GET", this.workspacePath(boxSecretPath(boxID)), creds, { signal: options.signal });
  }

  createBoxSecret(creds: Credentials, boxID: string, req: CreateProjectSecretRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView> {
    return this.request("POST", this.workspacePath(boxSecretPath(boxID)), creds, { body: req, signal: options.signal });
  }

  updateBoxSecret(
    creds: Credentials,
    boxID: string,
    secretID: string,
    req: UpdateProjectSecretRequest,
    options: { signal?: AbortSignal } = {}
  ): Promise<ProjectSecretView> {
    return this.request("PATCH", this.workspacePath(boxSecretPath(boxID, secretID)), creds, { body: req, signal: options.signal });
  }

  deleteBoxSecret(creds: Credentials, boxID: string, secretID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", this.workspacePath(boxSecretPath(boxID, secretID)), creds, { signal: options.signal });
  }

  sharedSnaps(creds: Credentials, options: { signal?: AbortSignal } = {}): Promise<SharedSnapLineView[]> {
    return this.request("GET", "/shared-snaps", creds, { signal: options.signal });
  }

  sharedSnapDetail(creds: Credentials, name: string, options: { signal?: AbortSignal } = {}): Promise<SharedSnapDetailView> {
    return this.request("GET", `/shared-snaps/${encodePath(name)}`, creds, { signal: options.signal });
  }

  publishSharedSnap(creds: Credentials, req: PublishSharedSnapRequest, options: { signal?: AbortSignal } = {}): Promise<SharedSnapVersionView> {
    return this.request("POST", "/shared-snaps", creds, { body: req, signal: options.signal });
  }

  deleteSharedSnap(creds: Credentials, name: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", `/shared-snaps/${encodePath(name)}`, creds, { signal: options.signal });
  }

  deleteSharedSnapVersion(creds: Credentials, name: string, version: number, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", `/shared-snaps/${encodePath(name)}/versions/${version}`, creds, { signal: options.signal });
  }

  createBoxFromSharedSnap(
    creds: Credentials,
    name: string,
    req: CreateBoxFromSharedSnapRequest,
    options: { signal?: AbortSignal } = {}
  ): Promise<BoxView> {
    return this.request("POST", this.workspacePath(`/shared-snaps/${encodePath(name)}/boxes`), creds, { body: req, signal: options.signal });
  }

  createSnapFromSharedSnap(
    creds: Credentials,
    name: string,
    req: CreateSnapFromSharedSnapRequest,
    options: { signal?: AbortSignal } = {}
  ): Promise<SnapView> {
    return this.request("POST", this.workspacePath(`/shared-snaps/${encodePath(name)}/snaps`), creds, { body: req, signal: options.signal });
  }

  async execAttachURL(attachURL: string): Promise<ExecAttachSocket> {
    const httpURL = resolveHTTPURL(this.baseURL, attachURL);
    const wsURL = websocketURL(httpURL);
    const socket = new WebSocket(wsURL, { handshakeTimeout: 15_000 });
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const done = (callback: () => void) => {
        if (settled) {
          return;
        }
        settled = true;
        cleanup();
        callback();
      };
      const onOpen = () => {
        done(resolve);
      };
      const onError = (error: Error) => {
        done(() => reject(error));
      };
      const onUnexpectedResponse = (_: unknown, response: IncomingMessage) => {
        void readIncomingMessageText(response)
          .then((body) => {
            done(() => reject(run9ErrorFromBody(response.statusCode ?? 0, response.statusMessage ?? "", body)));
          })
          .catch((error: unknown) => {
            done(() => reject(error));
          });
      };
      const cleanup = () => {
        socket.off("open", onOpen);
        socket.off("error", onError);
        socket.off("unexpected-response", onUnexpectedResponse);
      };
      socket.once("open", onOpen);
      socket.once("error", onError);
      socket.once("unexpected-response", onUnexpectedResponse);
    });
    return new ExecAttachSocket(socket);
  }

  private async request<T>(method: string, path: string, creds: Credentials, options: RequestOptions = {}): Promise<T> {
    const response = await this.rawRequest(method, path, creds, options);
    return decodeJSON<T>(response);
  }

  private async requestVoid(method: string, path: string, creds: Credentials, options: RequestOptions = {}): Promise<void> {
    const response = await this.rawRequest(method, path, creds, options);
    await drain(response);
  }

  private async download(method: string, path: string, creds: Credentials, options: RequestOptions = {}): Promise<ReadableStream<Uint8Array>> {
    const response = await this.rawRequest(method, path, creds, options);
    if (!response.body) {
      throw new Error("portal api returned empty response body");
    }
    return response.body;
  }

  private async rawRequest(method: string, path: string, creds: Credentials, options: RequestOptions = {}): Promise<Response> {
    const response = await this.fetchImpl(this.requestURL(path, options.query), {
      method,
      headers: this.headers(creds, options),
      body: requestBody(options.body),
      signal: options.signal,
      ...duplexOption(options.body)
    });
    if (response.status >= 400) {
      throw await responseError(response);
    }
    return response;
  }

  private headers(creds: Credentials, options: RequestOptions): Headers {
    const headers = new Headers(options.headers);
    headers.set("Authorization", `Basic ${Buffer.from(`${creds.ak}:${creds.sk}`).toString("base64")}`);
    if (!headers.has("Accept")) {
      headers.set("Accept", "application/json");
    }
    if (options.body != null && !headers.has("Content-Type") && !isBodyInit(options.body)) {
      headers.set("Content-Type", "application/json");
    }
    return headers;
  }

  private requestURL(path: string, query?: RequestOptions["query"]): string {
    const parsed = new URL(this.baseURL);
    parsed.pathname = `${parsed.pathname.replace(/\/+$/, "")}/${path.trim().replace(/^\/+/, "")}`;
    for (const [key, value] of Object.entries(query ?? {})) {
      if (value === undefined || value === "") {
        continue;
      }
      parsed.searchParams.set(key, String(value));
    }
    parsed.hash = "";
    return parsed.toString();
  }

  private workspacePath(path: string): string {
    const cleanPath = `/${path.trim().replace(/^\/+/, "")}`;
    if (!this.projectCID.trim()) {
      return cleanPath;
    }
    return `/projects/${encodePath(this.projectCID)}/workspace${cleanPath}`;
  }
}

function requestBody(body: RequestOptions["body"]): BodyInit | null | undefined {
  if (body == null || isBodyInit(body)) {
    return body;
  }
  return JSON.stringify(body);
}

function isBodyInit(value: unknown): value is BodyInit {
  return (
    typeof value === "string" ||
    value instanceof Blob ||
    value instanceof ArrayBuffer ||
    value instanceof Uint8Array ||
    value instanceof URLSearchParams ||
    value instanceof FormData ||
    value instanceof ReadableStream
  );
}

function duplexOption(body: RequestOptions["body"]): { duplex?: "half" } {
  return body instanceof ReadableStream ? { duplex: "half" } : {};
}

async function decodeJSON<T>(response: Response): Promise<T> {
  const text = await response.text();
  if (!text.trim()) {
    throw new Error("portal api returned empty response body");
  }
  return JSON.parse(text) as T;
}

async function drain(response: Response): Promise<void> {
  await response.arrayBuffer();
}

async function responseError(response: Response): Promise<Run9Error> {
  const data = await response.text();
  return run9ErrorFromBody(response.status, response.statusText, data);
}

function run9ErrorFromBody(statusCode: number, statusText: string, data: string): Run9Error {
  if (!data.trim()) {
    return new Run9Error(statusCode, statusText);
  }
  try {
    const payload = JSON.parse(data) as ErrorPayload;
    if (payload.error?.trim()) {
      return new Run9Error(statusCode, payload.error);
    }
  } catch {
    return new Run9Error(statusCode, data.trim());
  }
  return new Run9Error(statusCode, data.trim());
}

async function readIncomingMessageText(response: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of response) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

function encodePath(value: string): string {
  return encodeURIComponent(value.trim());
}

function trimmed(value: string | undefined): string | undefined {
  const clean = value?.trim();
  return clean ? clean : undefined;
}

function dateParam(value: string | Date | undefined): string | undefined {
  if (value instanceof Date) {
    return value.toISOString();
  }
  return trimmed(value);
}

function projectPath(projectCID: string, suffix = ""): string {
  return joinPath(`/projects/${encodePath(projectCID)}`, suffix);
}

function orgPath(orgID: string, suffix = ""): string {
  return joinPath(`/orgs/${encodePath(orgID)}`, suffix);
}

function boxSecretPath(boxID: string, secretID = ""): string {
  return joinPath(`/boxes/${encodePath(boxID)}/secrets`, secretID ? `/${encodePath(secretID)}` : "");
}

function joinPath(base: string, suffix: string): string {
  if (!suffix.trim()) {
    return base;
  }
  return `${base}/${suffix.trim().replace(/^\/+/, "")}`;
}

function resolveHTTPURL(baseURL: string, target: string): string {
  const clean = target.trim();
  if (!clean) {
    throw new Error("missing attach url");
  }
  return new URL(clean, baseURL).toString();
}

function websocketURL(httpURL: string): string {
  const parsed = new URL(httpURL);
  switch (parsed.protocol) {
    case "http:":
      parsed.protocol = "ws:";
      break;
    case "https:":
      parsed.protocol = "wss:";
      break;
    default:
      throw new Error("expected http or https endpoint");
  }
  parsed.hash = "";
  return parsed.toString();
}

function asBuffers(data: WebSocket.RawData): Buffer[] {
  if (Array.isArray(data)) {
    return data.map((part) => rawPartToBuffer(part));
  }
  return [rawPartToBuffer(data)];
}

function rawPartToBuffer(data: Buffer | ArrayBuffer | Buffer[]): Buffer {
  if (Buffer.isBuffer(data)) {
    return data;
  }
  if (Array.isArray(data)) {
    return Buffer.concat(data);
  }
  return Buffer.from(new Uint8Array(data));
}
