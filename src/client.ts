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
  ImportSnapRequest,
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
  ExecRequest,
  ExecStreamEvent,
  ExecView,
  InvitationView,
  ListBoxesRequest,
  ListExecsRequest,
  ListExecsResult,
  ListSnapsRequest,
  MeView,
  MembershipView,
  OrgHostsView,
  OrgView,
  ProjectMembershipView,
  ProjectSecretView,
  ProjectView,
  PullBackgroundExecOutputRequest,
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
  UpdateProjectSecretRequest,
  WriteBackgroundExecStdinRequest
} from "./types.js";

export interface Run9ClientOptions {
  fetch?: typeof fetch;
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
  private readonly queue: Array<{ event?: ExecStreamEvent; error?: Error }> = [];
  private readonly readers: Array<{
    resolve: (event: ExecStreamEvent) => void;
    reject: (error: Error) => void;
  }> = [];
  private terminalError: Error | undefined;

  constructor(private readonly socket: WebSocket) {
    this.socket.on("message", (data) => {
      try {
        const raw = typeof data === "string" ? data : Buffer.concat(asBuffers(data)).toString("utf8");
        this.push({ event: decodeExecStreamEvent(raw) });
      } catch (error) {
        this.push({ error: asError(error) });
      }
    });
    this.socket.on("error", (error) => {
      this.fail(error);
    });
    this.socket.on("close", () => {
      this.fail(new Error("exec attach websocket closed"));
    });
  }

  readEvent(): Promise<ExecStreamEvent> {
    const item = this.queue.shift();
    if (item?.event) {
      return Promise.resolve(item.event);
    }
    if (item?.error) {
      return Promise.reject(item.error);
    }
    if (this.terminalError) {
      return Promise.reject(this.terminalError);
    }
    return new Promise((resolve, reject) => {
      this.readers.push({ resolve, reject });
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

  private push(item: { event?: ExecStreamEvent; error?: Error }): void {
    const reader = this.readers.shift();
    if (reader) {
      if (item.event) {
        reader.resolve(item.event);
      } else {
        reader.reject(item.error ?? new Error("exec attach websocket read failed"));
      }
      return;
    }
    this.queue.push(item);
  }

  private fail(error: Error): void {
    if (!this.terminalError) {
      this.terminalError = error;
    }
    while (this.readers.length > 0) {
      this.readers.shift()?.reject(error);
    }
  }
}

export class ExecStream {
  readonly execID: string;
  private readonly reader: ReadableStreamDefaultReader<Uint8Array>;
  private readonly decoder = new TextDecoder();
  private buffered = "";
  private closed = false;

  constructor(execID: string, body: ReadableStream<Uint8Array>) {
    this.execID = execID;
    this.reader = body.getReader();
  }

  async readEvent(): Promise<ExecStreamEvent> {
    while (true) {
      const line = this.shiftLine();
      if (line !== undefined) {
        return decodeExecStreamEvent(line);
      }
      const { done, value } = await this.reader.read();
      if (done) {
        const tail = this.buffered.trim();
        this.buffered = "";
        if (tail) {
          return decodeExecStreamEvent(tail);
        }
        throw new Error("exec stream closed");
      }
      this.buffered += this.decoder.decode(value, { stream: true });
    }
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await this.reader.cancel();
  }

  private shiftLine(): string | undefined {
    const index = this.buffered.indexOf("\n");
    if (index < 0) {
      return undefined;
    }
    const line = this.buffered.slice(0, index).trim();
    this.buffered = this.buffered.slice(index + 1);
    return line || this.shiftLine();
  }
}

export class Run9Client {
  private readonly baseURL: string;
  private readonly creds: Credentials;
  private readonly projectCID: string;
  private readonly fetchImpl: typeof fetch;

  constructor(endpoint: string, creds: Credentials, options: Run9ClientOptions = {}) {
    this.baseURL = normalizeEndpoint(endpoint);
    this.creds = normalizeCredentials(creds);
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

  whoAmI(options: { signal?: AbortSignal } = {}): Promise<CurrentOrgIdentityView> {
    return this.request("GET", "/whoami", { signal: options.signal });
  }

  updateAccount(req: UpdateMeRequest, options: { signal?: AbortSignal } = {}): Promise<MeView> {
    return this.request("PATCH", "/account", { body: req, signal: options.signal });
  }

  listSSHKeys(options: { signal?: AbortSignal } = {}): Promise<SSHKeyView[]> {
    return this.request("GET", "/account/ssh-keys", { signal: options.signal });
  }

  createSSHKey(req: CreateSSHKeyRequest, options: { signal?: AbortSignal } = {}): Promise<SSHKeyView> {
    return this.request("POST", "/account/ssh-keys", { body: req, signal: options.signal });
  }

  deleteSSHKey(sshKeyID: string, options: { signal?: AbortSignal } = {}): Promise<SSHKeyView> {
    return this.request("DELETE", `/account/ssh-keys/${encodePath(sshKeyID)}`, { signal: options.signal });
  }

  listProjects(options: { signal?: AbortSignal } = {}): Promise<ProjectView[]> {
    return this.request("GET", "/projects", { signal: options.signal });
  }

  createProject(req: CreateProjectRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectView> {
    return this.request("POST", "/projects", { body: req, signal: options.signal });
  }

  getProject(projectCID: string, options: { signal?: AbortSignal } = {}): Promise<ProjectView> {
    return this.request("GET", `/projects/${encodePath(projectCID)}`, { signal: options.signal });
  }

  updateProject(req: UpdateProjectRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectView> {
    return this.request("PATCH", projectPath(this.projectCID), { body: req, signal: options.signal });
  }

  deleteProject(options: { signal?: AbortSignal } = {}): Promise<DeleteProjectResult> {
    return this.request("DELETE", projectPath(this.projectCID), { signal: options.signal });
  }

  listProjectMembers(options: { signal?: AbortSignal } = {}): Promise<ProjectMembershipView[]> {
    return this.request("GET", projectPath(this.projectCID, "/members"), { signal: options.signal });
  }

  updateProjectMember(userID: string, req: UpdateProjectMembershipRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectMembershipView> {
    return this.request("PATCH", projectPath(this.projectCID, `/members/${encodePath(userID)}`), { body: req, signal: options.signal });
  }

  deleteProjectMember(userID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", projectPath(this.projectCID, `/members/${encodePath(userID)}`), { signal: options.signal });
  }

  updateOrg(orgID: string, req: UpdateOrgRequest, options: { signal?: AbortSignal } = {}): Promise<OrgView> {
    return this.request("PATCH", orgPath(orgID), { body: req, signal: options.signal });
  }

  deleteOrg(orgID: string, options: { signal?: AbortSignal } = {}): Promise<DeleteOrgResult> {
    return this.request("DELETE", orgPath(orgID), { signal: options.signal });
  }

  listOrgMembers(orgID: string, options: { signal?: AbortSignal } = {}): Promise<MembershipView[]> {
    return this.request("GET", orgPath(orgID, "/members"), { signal: options.signal });
  }

  updateOrgMember(orgID: string, userID: string, req: UpdateMembershipRequest, options: { signal?: AbortSignal } = {}): Promise<MembershipView> {
    return this.request("PATCH", orgPath(orgID, `/members/${encodePath(userID)}`), { body: req, signal: options.signal });
  }

  deleteOrgMember(orgID: string, userID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", orgPath(orgID, `/members/${encodePath(userID)}`), { signal: options.signal });
  }

  listInvitations(orgID: string, options: { signal?: AbortSignal } = {}): Promise<InvitationView[]> {
    return this.request("GET", orgPath(orgID, "/invitations"), { signal: options.signal });
  }

  createInvitation(orgID: string, req: CreateInvitationRequest, options: { signal?: AbortSignal } = {}): Promise<InvitationView> {
    return this.request("POST", orgPath(orgID, "/invitations"), { body: req, signal: options.signal });
  }

  revokeInvitation(orgID: string, invitationID: string, options: { signal?: AbortSignal } = {}): Promise<DeleteInvitationResult> {
    return this.request("DELETE", orgPath(orgID, `/invitations/${encodePath(invitationID)}`), { signal: options.signal });
  }

  listAPIKeys(options: { signal?: AbortSignal } = {}): Promise<APIKeyView[]> {
    return this.request("GET", "/api-keys", { signal: options.signal });
  }

  createAPIKey(req: CreateAPIKeyRequest, options: { signal?: AbortSignal } = {}): Promise<CreatedAPIKeyView> {
    return this.request("POST", "/api-keys", { body: req, signal: options.signal });
  }

  revokeAPIKey(apiKeyID: string, options: { signal?: AbortSignal } = {}): Promise<APIKeyView> {
    return this.request("DELETE", `/api-keys/${encodePath(apiKeyID)}`, { signal: options.signal });
  }

  getOrgHosts(options: { signal?: AbortSignal } = {}): Promise<OrgHostsView> {
    return this.request("GET", "/org-runtime/hosts", { signal: options.signal });
  }

  createBox(req: CreateBoxRequest, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("POST", this.workspacePath("/boxes"), { body: req, signal: options.signal });
  }

  listBoxes(req: ListBoxesRequest = {}, options: { signal?: AbortSignal } = {}): Promise<BoxView[]> {
    return this.request("GET", this.workspacePath("/boxes"), {
      query: {
        creator: trimmed(req.creator),
        label: trimmed(req.label),
        state: trimmed(req.state)
      },
      signal: options.signal
    });
  }

  getBox(boxID: string, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("GET", this.workspacePath(`/boxes/${encodePath(boxID)}`), { signal: options.signal });
  }

  updateBox(boxID: string, req: UpdateBoxRequest, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("PATCH", this.workspacePath(`/boxes/${encodePath(boxID)}`), { body: req, signal: options.signal });
  }

  stopBox(boxID: string, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/stop`), { signal: options.signal });
  }

  deleteBox(boxID: string, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("DELETE", this.workspacePath(`/boxes/${encodePath(boxID)}`), { signal: options.signal });
  }

  importSnap(req: ImportSnapRequest, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("POST", this.workspacePath("/snaps/import"), {
      body: { image_ref: req.image_ref.trim() },
      signal: options.signal
    });
  }

  listSnaps(req: ListSnapsRequest = {}, options: { signal?: AbortSignal } = {}): Promise<SnapView[]> {
    return this.request("GET", this.workspacePath("/snaps"), {
      query: { attached: req.attached === undefined ? undefined : String(req.attached) },
      signal: options.signal
    });
  }

  getSnap(snapID: string, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("GET", this.workspacePath(`/snaps/${encodePath(snapID)}`), { signal: options.signal });
  }

  forkSnap(snapID: string, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("POST", this.workspacePath(`/snaps/${encodePath(snapID)}/fork`), { signal: options.signal });
  }

  deleteSnap(snapID: string, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("DELETE", this.workspacePath(`/snaps/${encodePath(snapID)}`), { signal: options.signal });
  }

  getSnapTree(snapID: string, options: { signal?: AbortSignal } = {}): Promise<SnapTreeView> {
    return this.request("GET", this.workspacePath(`/snaps/${encodePath(snapID)}/tree`), { signal: options.signal });
  }

  startExec(boxID: string, req: ExecRequest, options: { signal?: AbortSignal } = {}): Promise<ExecView> {
    return this.request("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/execs`), { body: req, signal: options.signal });
  }

  startBackgroundExec(boxID: string, req: ExecRequest, options: { signal?: AbortSignal } = {}): Promise<ExecView> {
    return this.request("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/background-execs`), { body: req, signal: options.signal });
  }

  async startExecStream(boxID: string, req: ExecRequest, options: { signal?: AbortSignal } = {}): Promise<ExecStream> {
    const response = await this.rawRequest("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/execs/stream`), {
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
    return new ExecStream(response.headers.get("X-Run9-Exec-ID")?.trim() ?? "", response.body);
  }

  async listExecs(req: ListExecsRequest = {}, options: { signal?: AbortSignal } = {}): Promise<ListExecsResult> {
    const response = await this.rawRequest("GET", this.workspacePath("/execs"), {
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

  getExec(execID: string, options: { signal?: AbortSignal } = {}): Promise<ExecView> {
    return this.request("GET", this.workspacePath(`/execs/${encodePath(execID)}`), { signal: options.signal });
  }

  downloadExecLog(execID: string, options: { signal?: AbortSignal } = {}): Promise<ReadableStream<Uint8Array>> {
    return this.download("GET", this.workspacePath(`/execs/${encodePath(execID)}/log-download`), { signal: options.signal });
  }

  async pullBackgroundExecOutput(
    execID: string,
    req: PullBackgroundExecOutputRequest = {},
    options: { signal?: AbortSignal } = {}
  ): Promise<BackgroundExecPullOutput> {
    const response = await this.rawRequest("POST", this.workspacePath(`/execs/${encodePath(execID)}/pull-output`), {
      body: {
        cursor: trimmed(req.cursor),
        wait_ms: req.wait && req.wait > 0 ? req.wait : undefined
      },
      signal: options.signal
    });
    const body = new Uint8Array(await response.arrayBuffer());
    const rawExitCode = response.headers.get("X-Run9-Exit-Code")?.trim();
    return {
      body,
      nextCursor: response.headers.get("X-Run9-Next-Cursor")?.trim() ?? "",
      state: response.headers.get("X-Run9-Exec-State")?.trim() ?? "",
      exitCode: rawExitCode ? parseStrictInteger(rawExitCode, "X-Run9-Exit-Code") : undefined,
      reason: response.headers.get("X-Run9-Reason")?.trim() ?? "",
      idleDeadlineAt: parseOptionalHeaderDate(response.headers, "X-Run9-Idle-Deadline-At")
    };
  }

  async writeBackgroundExecStdin(
    execID: string,
    req: WriteBackgroundExecStdinRequest,
    options: { signal?: AbortSignal } = {}
  ): Promise<string | undefined> {
    const response = await this.rawRequest("POST", this.workspacePath(`/execs/${encodePath(execID)}/write-stdin`), {
      body: req.data,
      headers: {
        "Content-Type": "application/octet-stream",
        "X-Run9-Close-Stdin": String(req.closeStdin ?? false)
      },
      signal: options.signal
    });
    await drain(response);
    return parseOptionalHeaderDate(response.headers, "X-Run9-Idle-Deadline-At");
  }

  killBackgroundExec(execID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("POST", this.workspacePath(`/execs/${encodePath(execID)}/kill`), { signal: options.signal });
  }

  uploadArchive(boxID: string, boxAbsPath: string, source: BodyInit, options: { signal?: AbortSignal } = {}): Promise<RuntimeRequestView> {
    return this.request("POST", this.workspacePath(`/boxes/${encodePath(boxID)}/files/upload`), {
      query: { box_abs_path: boxAbsPath.trim() },
      headers: { "Content-Type": "application/x-tar" },
      body: source,
      signal: options.signal
    });
  }

  downloadArchive(boxID: string, boxAbsPath: string, options: { signal?: AbortSignal } = {}): Promise<ReadableStream<Uint8Array>> {
    return this.download("GET", this.workspacePath(`/boxes/${encodePath(boxID)}/files/download`), {
      query: {
        archive: "tar",
        box_abs_path: boxAbsPath.trim()
      },
      signal: options.signal
    });
  }

  listProjectSecrets(options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView[]> {
    return this.request("GET", projectPath(this.projectCID, "/secrets"), { signal: options.signal });
  }

  createProjectSecret(req: CreateProjectSecretRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView> {
    return this.request("POST", projectPath(this.projectCID, "/secrets"), { body: req, signal: options.signal });
  }

  updateProjectSecret(secretID: string, req: UpdateProjectSecretRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView> {
    return this.request("PATCH", projectPath(this.projectCID, `/secrets/${encodePath(secretID)}`), { body: req, signal: options.signal });
  }

  deleteProjectSecret(secretID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", projectPath(this.projectCID, `/secrets/${encodePath(secretID)}`), { signal: options.signal });
  }

  listBoxSecrets(boxID: string, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView[]> {
    return this.request("GET", this.workspacePath(boxSecretPath(boxID)), { signal: options.signal });
  }

  createBoxSecret(boxID: string, req: CreateProjectSecretRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView> {
    return this.request("POST", this.workspacePath(boxSecretPath(boxID)), { body: req, signal: options.signal });
  }

  updateBoxSecret(boxID: string, secretID: string, req: UpdateProjectSecretRequest, options: { signal?: AbortSignal } = {}): Promise<ProjectSecretView> {
    return this.request("PATCH", this.workspacePath(boxSecretPath(boxID, secretID)), { body: req, signal: options.signal });
  }

  deleteBoxSecret(boxID: string, secretID: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", this.workspacePath(boxSecretPath(boxID, secretID)), { signal: options.signal });
  }

  listSharedSnaps(options: { signal?: AbortSignal } = {}): Promise<SharedSnapLineView[]> {
    return this.request("GET", "/shared-snaps", { signal: options.signal });
  }

  getSharedSnap(name: string, options: { signal?: AbortSignal } = {}): Promise<SharedSnapDetailView> {
    return this.request("GET", `/shared-snaps/${encodePath(name)}`, { signal: options.signal });
  }

  publishSharedSnap(req: PublishSharedSnapRequest, options: { signal?: AbortSignal } = {}): Promise<SharedSnapVersionView> {
    return this.request("POST", "/shared-snaps", { body: req, signal: options.signal });
  }

  deleteSharedSnap(name: string, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", `/shared-snaps/${encodePath(name)}`, { signal: options.signal });
  }

  deleteSharedSnapVersion(name: string, version: number, options: { signal?: AbortSignal } = {}): Promise<void> {
    return this.requestVoid("DELETE", `/shared-snaps/${encodePath(name)}/versions/${version}`, { signal: options.signal });
  }

  createBoxFromSharedSnap(name: string, req: CreateBoxFromSharedSnapRequest, options: { signal?: AbortSignal } = {}): Promise<BoxView> {
    return this.request("POST", this.workspacePath(`/shared-snaps/${encodePath(name)}/boxes`), { body: req, signal: options.signal });
  }

  createSnapFromSharedSnap(name: string, req: CreateSnapFromSharedSnapRequest, options: { signal?: AbortSignal } = {}): Promise<SnapView> {
    return this.request("POST", this.workspacePath(`/shared-snaps/${encodePath(name)}/snaps`), { body: req, signal: options.signal });
  }

  async openExecAttach(attachURL: string): Promise<ExecAttachSocket> {
    const httpURL = resolveHTTPURL(this.baseURL, attachURL);
    const wsURL = websocketURL(httpURL);
    const socket = new WebSocket(wsURL, { handshakeTimeout: 15_000 });
    const attachSocket = new ExecAttachSocket(socket);
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
    return attachSocket;
  }

  private async request<T>(method: string, path: string, options: RequestOptions = {}): Promise<T> {
    const response = await this.rawRequest(method, path, options);
    return decodeJSON<T>(response);
  }

  private async requestVoid(method: string, path: string, options: RequestOptions = {}): Promise<void> {
    const response = await this.rawRequest(method, path, options);
    await drain(response);
  }

  private async download(method: string, path: string, options: RequestOptions = {}): Promise<ReadableStream<Uint8Array>> {
    const response = await this.rawRequest(method, path, options);
    if (!response.body) {
      throw new Error("portal api returned empty response body");
    }
    return response.body;
  }

  private async rawRequest(method: string, path: string, options: RequestOptions = {}): Promise<Response> {
    const response = await this.fetchImpl(this.requestURL(path, options.query), {
      method,
      headers: this.headers(options),
      body: requestBody(options.body),
      signal: options.signal,
      ...duplexOption(options.body)
    });
    if (response.status >= 400) {
      throw await responseError(response);
    }
    return response;
  }

  private headers(options: RequestOptions): Headers {
    const headers = new Headers(options.headers);
    headers.set("Authorization", `Basic ${Buffer.from(`${this.creds.ak}:${this.creds.sk}`).toString("base64")}`);
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
    return `${projectPath(this.projectCID, "/workspace")}${cleanPath}`;
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

function decodeExecStreamEvent(raw: string): ExecStreamEvent {
  const event = JSON.parse(raw) as ExecStreamEvent;
  if (typeof event.data === "string") {
    event.data = new Uint8Array(Buffer.from(event.data, "base64"));
  }
  return event;
}

function parseStrictInteger(value: string, headerName: string): number {
  if (!/^-?\d+$/.test(value)) {
    throw new Error(`invalid ${headerName} header: ${value}`);
  }
  return Number.parseInt(value, 10);
}

function parseOptionalHeaderDate(headers: Headers, headerName: string): string | undefined {
  const value = headers.get(headerName)?.trim();
  if (!value) {
    return undefined;
  }
  const timestamp = Date.parse(value);
  if (Number.isNaN(timestamp)) {
    throw new Error(`invalid ${headerName} header: ${value}`);
  }
  return value;
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
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
  const cleanProjectCID = projectCID.trim();
  if (!cleanProjectCID) {
    throw new Error("missing project cid: use client.withProject(...) for project-scoped APIs");
  }
  return joinPath(`/projects/${encodePath(cleanProjectCID)}`, suffix);
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

function normalizeEndpoint(endpoint: string): string {
  const trimmedEndpoint = endpoint.trim();
  if (!trimmedEndpoint) {
    throw new Error("missing run9 endpoint");
  }
  let parsed: URL;
  try {
    parsed = new URL(trimmedEndpoint);
  } catch (error) {
    throw new Error(`parse endpoint: ${asError(error).message}`);
  }
  if (!parsed.protocol || !parsed.host) {
    throw new Error(`invalid endpoint: ${endpoint}`);
  }
  if (parsed.search || parsed.hash) {
    throw new Error(`invalid endpoint: must not contain query or fragment: ${endpoint}`);
  }
  parsed.pathname = parsed.pathname.replace(/\/+$/, "");
  return parsed.toString();
}

function normalizeCredentials(creds: Credentials): Credentials {
  const ak = creds.ak.trim();
  const sk = creds.sk.trim();
  if (!ak) {
    throw new Error("missing run9 access key");
  }
  if (!sk) {
    throw new Error("missing run9 secret key");
  }
  return { ak, sk };
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
