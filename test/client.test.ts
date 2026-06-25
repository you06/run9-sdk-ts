import { once } from "node:events";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { describe, expect, it } from "vitest";
import { WebSocketServer } from "ws";
import {
  BackgroundExecPullOutput,
  Run9Client,
  Run9Error,
  type BackgroundExecOutputEvent,
  type CreateProjectRequest,
  type ListExecsRequest,
  type UpdateBoxRequest
} from "../src/index.js";

const creds = { ak: "ak-1", sk: "sk-1" };

describe("Run9Client", () => {
  it("normalizes base URL, preserves path prefixes, and binds credentials", async () => {
    const server = await testServer((req, res) => {
      expect(req.url).toBe("/api/whoami");
      expect(req.method).toBe("GET");
      expect(req.headers.authorization).toBe(`Basic ${Buffer.from("ak-1:sk-1").toString("base64")}`);
      respondJSON(res, { user: { user_id: "u-1" }, org: { org_id: "org-1" }, auth_kind: "api_key" });
    });

    try {
      const identity = await new Run9Client(`${server.url}/api/`, creds).whoAmI();
      expect(identity.auth_kind).toBe("api_key");
    } finally {
      await server.close();
    }
  });

  it("rejects missing credentials and project-scoped calls without a project", () => {
    expect(() => new Run9Client("http://127.0.0.1", { ak: "", sk: "sk" })).toThrow("missing run9 access key");
    expect(() => new Run9Client("http://127.0.0.1?x=1", creds)).toThrow("invalid endpoint: must not contain query or fragment");
    expect(() => new Run9Client("http://127.0.0.1", creds).listBoxes()).toThrow(
      "missing project cid: use client.withProject(...) for project-scoped APIs"
    );
  });

  it("uses project workspace paths", async () => {
    const server = await testServer((req, res) => {
      expect(req.url).toBe("/api/projects/default/workspace/boxes?state=running");
      expect(req.method).toBe("GET");
      respondJSON(res, []);
    });

    try {
      const boxes = await new Run9Client(`${server.url}/api`, creds).withProject("default").listBoxes({
        state: "running"
      });
      expect(boxes).toEqual([]);
    } finally {
      await server.close();
    }
  });

  it("sends JSON payloads", async () => {
    const server = await testServer(async (req, res) => {
      expect(req.url).toBe("/projects");
      expect(req.method).toBe("POST");
      expect(req.headers.accept).toBe("application/json");
      expect(req.headers["content-type"]).toContain("application/json");
      const body = (await readBody(req)) as CreateProjectRequest;
      expect(body).toEqual({ display_name: "Sandbox", description: "Isolated experiments" });
      respondJSON(res, { project_id: "proj-1", org_id: "org-1", project_cid: "sandbox", display_name: "Sandbox", role: "admin" });
    });

    try {
      const project = await new Run9Client(server.url, creds).createProject({
        display_name: "Sandbox",
        description: "Isolated experiments"
      });
      expect(project.project_cid).toBe("sandbox");
    } finally {
      await server.close();
    }
  });

  it("can clear labels by sending an empty object", async () => {
    const server = await testServer(async (req, res) => {
      expect(req.url).toBe("/projects/default/workspace/boxes/box-1");
      expect(req.method).toBe("PATCH");
      const body = (await readBody(req)) as UpdateBoxRequest;
      expect(body).toEqual({
        labels: {},
        desired_shape: "2c4g",
        network_mode: "managed",
        security_mode: "restricted"
      });
      respondJSON(res, { box_id: "box-1", labels: {} });
    });

    try {
      const box = await new Run9Client(server.url, creds).withProject("default").updateBox(" box-1 ", {
        labels: {},
        desired_shape: "2c4g",
        network_mode: "managed",
        security_mode: "restricted"
      });
      expect(box.box_id).toBe("box-1");
    } finally {
      await server.close();
    }
  });

  it("includes exec filters and exposes next cursor", async () => {
    const acceptedAfter = new Date("2026-03-28T12:00:00.123Z");
    const server = await testServer((req, res) => {
      const url = new URL(req.url ?? "/", "http://local");
      expect(url.pathname).toBe("/projects/default/workspace/execs");
      expect(url.searchParams.get("box_id")).toBe("box-1");
      expect(url.searchParams.get("accepted_after")).toBe(acceptedAfter.toISOString());
      expect(url.searchParams.get("paged")).toBe("true");
      expect(url.searchParams.get("limit")).toBe("10");
      res.setHeader("X-Run9-Next-Cursor", "cursor-2");
      respondJSON(res, []);
    });

    try {
      const req: ListExecsRequest = {
        boxID: "box-1",
        acceptedAfter,
        paged: true,
        limit: 10
      };
      const result = await new Run9Client(server.url, creds).withProject("default").listExecs(req);
      expect(result.execs).toEqual([]);
      expect(result.nextCursor).toBe("cursor-2");
    } finally {
      await server.close();
    }
  });

  it("returns JSON error messages as Run9Error", async () => {
    const server = await testServer((_req, res) => {
      respondJSON(res, { error: "invalid state filter" }, 400);
    });

    try {
      await expect(new Run9Client(server.url, creds).withProject("default").listBoxes({ state: "broken" })).rejects.toMatchObject({
        statusCode: 400,
        message: "invalid state filter"
      } satisfies Partial<Run9Error>);
    } finally {
      await server.close();
    }
  });

  it("preserves explicit archive upload content type", async () => {
    const server = await testServer(async (req, res) => {
      const url = new URL(req.url ?? "/", "http://local");
      expect(url.pathname).toBe("/projects/default/workspace/boxes/box-1/files/upload");
      expect(url.searchParams.get("box_abs_path")).toBe("/work/result.txt");
      expect(req.headers["content-type"]).toBe("application/x-tar");
      expect(await readText(req)).toBe("tar-body");
      respondJSON(res, { runtime_request_id: "runtime-upload-1", state: "prepared" });
    });

    try {
      const view = await new Run9Client(server.url, creds).withProject("default").uploadArchive("box-1", "/work/result.txt", "tar-body");
      expect(view.runtime_request_id).toBe("runtime-upload-1");
    } finally {
      await server.close();
    }
  });

  it("reads foreground exec stream events", async () => {
    const server = await testServer((_req, res) => {
      expect(_req.url).toBe("/projects/default/workspace/boxes/box-1/execs/stream");
      res.setHeader("X-Run9-Exec-ID", "exec-stream-1");
      res.end(`${JSON.stringify({ type: "stdout", data: Buffer.from("hi").toString("base64") })}\n`);
    });

    try {
      const stream = await new Run9Client(server.url, creds).withProject("default").startExecStream("box-1", { command: ["echo", "hi"] });
      expect(stream.execID).toBe("exec-stream-1");
      const event = await stream.readEvent();
      expect(event.type).toBe("stdout");
      expect(Buffer.from(event.data ?? new Uint8Array()).toString("utf8")).toBe("hi");
      await stream.close();
    } finally {
      await server.close();
    }
  });

  it("reads foreground exec stream events after redirects", async () => {
    const seen: string[] = [];
    const server = await testServer((req, res) => {
      seen.push(`${req.method ?? ""} ${req.url ?? ""} ${req.headers.accept ?? ""}`);
      if (req.url === "/projects/default/workspace/boxes/box-1/execs/stream") {
        res.statusCode = 303;
        res.setHeader("Location", "/redirected-exec-stream");
        res.end();
        return;
      }
      expect(req.url).toBe("/redirected-exec-stream");
      expect(req.headers.accept).toBe("application/x-ndjson");
      res.setHeader("X-Run9-Exec-ID", "exec-redirected");
      res.end(
        `${JSON.stringify({ type: "stdout", data: Buffer.from("hi").toString("base64") })}\n${JSON.stringify({ type: "exit", exit_code: 0 })}\n`
      );
    });

    try {
      const stream = await new Run9Client(server.url, creds).withProject("default").startExecStream("box-1", { command: ["echo", "hi"] });
      expect(stream.execID).toBe("exec-redirected");
      const stdout = await stream.readEvent();
      expect(stdout.type).toBe("stdout");
      expect(Buffer.from(stdout.data ?? new Uint8Array()).toString("utf8")).toBe("hi");
      const exit = await stream.readEvent();
      expect(exit).toMatchObject({ type: "exit", exit_code: 0 });
      expect(seen).toEqual([
        "POST /projects/default/workspace/boxes/box-1/execs/stream application/x-ndjson",
        "GET /redirected-exec-stream application/x-ndjson"
      ]);
      await stream.close();
    } finally {
      await server.close();
    }
  });

  it("decodes background exec output frames and headers", async () => {
    const server = await testServer(async (req, res) => {
      expect(req.url).toBe("/projects/default/workspace/execs/exec-1/pull-output");
      expect(req.method).toBe("POST");
      expect(await readBody(req)).toEqual({ cursor: "cursor-1", wait_ms: 2000 });
      res.setHeader("X-Run9-Next-Cursor", "cursor-2");
      res.setHeader("X-Run9-Exec-State", "running");
      res.setHeader("X-Run9-Exit-Code", "0");
      res.setHeader("X-Run9-Reason", "done");
      res.setHeader("X-Run9-Idle-Deadline-At", "2026-03-28T12:00:00.000Z");
      res.end(
        encodeBackgroundExecOutputEvents([
          { seq: 1, type: "started" },
          { seq: 2, type: "stdout", data: Buffer.from("hello") },
          { seq: 3, type: "exit", exitCode: 0 }
        ])
      );
    });

    try {
      const result = await new Run9Client(server.url, creds).withProject("default").pullBackgroundExecOutput("exec-1", {
        cursor: "cursor-1",
        wait: 2000
      });
      expect(result.events).toHaveLength(3);
      expect(result.events[0]).toMatchObject({ seq: 1, type: "started" });
      expect(result.events[1].type).toBe("stdout");
      expect(Buffer.from(result.events[1].data ?? new Uint8Array()).toString("utf8")).toBe("hello");
      expect(result.events[2]).toMatchObject({ seq: 3, type: "exit", exitCode: 0 });
      expect(result.nextCursor).toBe("cursor-2");
      expect(result.state).toBe("running");
      expect(result.exitCode).toBe(0);
      expect(result.reason).toBe("done");
      expect(result.idleDeadlineAt).toBe("2026-03-28T12:00:00.000Z");
    } finally {
      await server.close();
    }
  });

  it("writes background output and reports terminal results", async () => {
    const result = new BackgroundExecPullOutput({
      state: "failed",
      events: [
        { seq: 1, type: "started" },
        { seq: 2, type: "stdout", data: Buffer.from("hello\n") },
        { seq: 3, type: "stderr", data: Buffer.from("warn\n") },
        { seq: 4, type: "gap", gapBytes: 12 },
        { seq: 5, type: "truncated" },
        { seq: 6, type: "exit", exitCode: 23 }
      ]
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];

    await result.writeOutput({
      stdout: (chunk) => stdout.push(Buffer.from(chunk)),
      stderr: (chunk) => stderr.push(Buffer.from(chunk))
    });

    expect(Buffer.concat(stdout).toString("utf8")).toBe("hello\n");
    expect(Buffer.concat(stderr).toString("utf8")).toContain("warn\n");
    expect(Buffer.concat(stderr).toString("utf8")).toContain("background exec omitted 12 bytes");
    expect(Buffer.concat(stderr).toString("utf8")).toContain("background exec output was truncated");
    expect(result.terminalResult()).toEqual({ status: "exited", exitCode: 23 });
  });

  it("runs foreground exec streams into output writers", async () => {
    const server = await testServer((req, res) => {
      expect(req.url).toBe("/projects/default/workspace/boxes/box-1/execs/stream");
      res.setHeader("X-Run9-Exec-ID", "exec-stream-1");
      res.end(
        `${JSON.stringify({ type: "started" })}\n${JSON.stringify({ type: "stdout", data: Buffer.from("hi\n").toString("base64") })}\n${JSON.stringify({ type: "stderr", data: Buffer.from("warn\n").toString("base64") })}\n${JSON.stringify({ type: "exit", exit_code: 0 })}\n`
      );
    });

    try {
      const stdout: Buffer[] = [];
      const stderr: Buffer[] = [];
      const terminal = await new Run9Client(server.url, creds).withProject("default").runExec(
        "box-1",
        { command: ["echo", "hi"] },
        {
          stdout: (chunk) => stdout.push(Buffer.from(chunk)),
          stderr: (chunk) => stderr.push(Buffer.from(chunk))
        }
      );
      expect(Buffer.concat(stdout).toString("utf8")).toBe("hi\n");
      expect(Buffer.concat(stderr).toString("utf8")).toBe("warn\n");
      expect(terminal).toEqual({ status: "exited", exitCode: 0 });
    } finally {
      await server.close();
    }
  });

  it("captures foreground exec terminal result and merged transcript", async () => {
    const server = await testServer((req, res) => {
      if (req.url === "/projects/default/workspace/boxes/box-1/execs/stream") {
        res.setHeader("X-Run9-Exec-ID", "exec-stream-1");
        res.end(`${JSON.stringify({ type: "stdout", data: Buffer.from("live\n").toString("base64") })}\n${JSON.stringify({ type: "exit", exit_code: 0 })}\n`);
        return;
      }
      expect(req.url).toBe("/projects/default/workspace/execs/exec-stream-1/log-download");
      res.end("merged transcript\n");
    });

    try {
      const capture = await new Run9Client(server.url, creds).withProject("default").runExecCapture("box-1", { command: ["echo", "hi"] });
      expect(capture.execID).toBe("exec-stream-1");
      expect(capture.terminal).toEqual({ status: "exited", exitCode: 0 });
      expect(Buffer.from(capture.transcript ?? new Uint8Array()).toString("utf8")).toBe("merged transcript\n");
    } finally {
      await server.close();
    }
  });

  it("recovers foreground exec capture after stream disconnect", async () => {
    const fetchImpl: typeof fetch = async (input) => {
      const url = new URL(String(input));
      if (url.pathname === "/projects/default/workspace/boxes/box-1/execs/stream") {
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(Buffer.from(`${JSON.stringify({ type: "stdout", data: Buffer.from("live\n").toString("base64") })}\n`));
            controller.error(new Error("stream transport lost"));
          }
        });
        return new Response(body, {
          headers: { "X-Run9-Exec-ID": "exec-stream-1" }
        });
      }
      if (url.pathname === "/projects/default/workspace/execs/exec-stream-1") {
        return new Response(JSON.stringify({ exec_id: "exec-stream-1", state: "succeeded", exit_code: 0 }), {
          headers: { "Content-Type": "application/json" }
        });
      }
      expect(url.pathname).toBe("/projects/default/workspace/execs/exec-stream-1/log-download");
      return new Response("recovered transcript\n");
    };

    const capture = await new Run9Client("http://run9.local", creds, { fetch: fetchImpl }).withProject("default").runExecCapture("box-1", {
      command: ["echo", "hi"]
    });
    expect(capture.terminal).toEqual({ status: "exited", exitCode: 0 });
    expect(Buffer.from(capture.transcript ?? new Uint8Array()).toString("utf8")).toBe("recovered transcript\n");
  });

  it("follows background exec output and skips started-only polls", async () => {
    let calls = 0;
    const server = await testServer((req, res) => {
      expect(req.url).toBe("/projects/default/workspace/execs/exec-1/pull-output");
      calls += 1;
      if (calls === 1) {
        res.setHeader("X-Run9-Next-Cursor", "cursor-1");
        res.setHeader("X-Run9-Exec-State", "running");
        res.end(encodeBackgroundExecOutputEvents([{ seq: 1, type: "started" }]));
        return;
      }
      res.setHeader("X-Run9-Next-Cursor", "cursor-2");
      res.setHeader("X-Run9-Exec-State", "failed");
      res.setHeader("X-Run9-Exit-Code", "0");
      res.end(
        encodeBackgroundExecOutputEvents([
          { seq: 2, type: "stdout", data: Buffer.from("done\n") },
          { seq: 3, type: "exit", exitCode: 0 }
        ])
      );
    });

    try {
      const follower = new Run9Client(server.url, creds).withProject("default").followBackgroundExec("exec-1");
      const result = await follower.read(2000);
      expect(follower.cursor()).toBe("cursor-2");
      expect(calls).toBe(2);
      expect(result.events[0].type).toBe("stdout");
      expect(Buffer.from(result.events[0].data ?? new Uint8Array()).toString("utf8")).toBe("done\n");
      expect(result.terminalResult()).toEqual({ status: "exited", exitCode: 0 });
    } finally {
      await server.close();
    }
  });

  it("rejects invalid background exec exit-code headers", async () => {
    const server = await testServer((_req, res) => {
      res.setHeader("X-Run9-Exit-Code", "12abc");
      res.end();
    });

    try {
      await expect(new Run9Client(server.url, creds).withProject("default").pullBackgroundExecOutput("exec-1")).rejects.toThrow(
        "invalid X-Run9-Exit-Code header: 12abc"
      );
    } finally {
      await server.close();
    }
  });

  it("buffers exec attach frames sent before readEvent and decodes data bytes", async () => {
    const server = await testServer((_req, res) => {
      res.statusCode = 404;
      res.end();
    });
    const wss = new WebSocketServer({ server: server.raw, path: "/attach" });
    wss.on("connection", (ws) => {
      ws.send(JSON.stringify({ type: "stdout", data: Buffer.from("hi").toString("base64") }));
    });

    try {
      const socket = await new Run9Client(server.url, creds).openExecAttach("/attach");
      await new Promise((resolve) => setTimeout(resolve, 25));
      const event = await socket.readEvent();
      expect(event.type).toBe("stdout");
      expect(Buffer.from(event.data ?? new Uint8Array()).toString("utf8")).toBe("hi");
      socket.close();
    } finally {
      for (const client of wss.clients) {
        client.terminate();
      }
      wss.close();
      await server.close();
    }
  });

  it("returns websocket handshake JSON error messages as Run9Error", async () => {
    const server = await testServer((_req, res) => {
      res.statusCode = 404;
      res.end();
    });
    server.raw.on("upgrade", (_req, socket) => {
      const body = JSON.stringify({ error: "exec attach denied" });
      socket.write(
        [
          "HTTP/1.1 403 Forbidden",
          "Content-Type: application/json",
          `Content-Length: ${Buffer.byteLength(body)}`,
          "Connection: close",
          "",
          body
        ].join("\r\n")
      );
      socket.destroy();
    });

    try {
      await expect(new Run9Client(server.url, creds).openExecAttach("/attach")).rejects.toMatchObject({
        statusCode: 403,
        message: "exec attach denied"
      } satisfies Partial<Run9Error>);
    } finally {
      await server.close();
    }
  });
});

async function testServer(handler: (req: IncomingMessage, res: ServerResponse) => void | Promise<void>) {
  const server = createServer((req, res) => {
    void Promise.resolve(handler(req, res)).catch((error: unknown) => {
      res.statusCode = 500;
      res.end(error instanceof Error ? error.message : String(error));
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (typeof address !== "object" || address === null) {
    throw new Error("server did not bind to a TCP port");
  }
  return {
    raw: server,
    url: `http://127.0.0.1:${address.port}`,
    close: async () => {
      server.close();
      await once(server, "close");
    }
  };
}

function respondJSON(res: ServerResponse, value: unknown, status = 200): void {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(value));
}

async function readBody(req: IncomingMessage): Promise<unknown> {
  return JSON.parse(await readText(req)) as unknown;
}

async function readText(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

function encodeBackgroundExecOutputEvents(events: BackgroundExecOutputEvent[]): Buffer {
  return Buffer.concat(events.map((event) => encodeBackgroundExecOutputEvent(event)));
}

function encodeBackgroundExecOutputEvent(event: BackgroundExecOutputEvent): Buffer {
  const payload = backgroundExecPayload(event);
  const header = Buffer.alloc(13);
  header.writeUInt8(backgroundExecFrameType(event.type), 0);
  header.writeBigUInt64BE(BigInt(event.seq), 1);
  header.writeUInt32BE(payload.byteLength, 9);
  return Buffer.concat([header, payload]);
}

function backgroundExecPayload(event: BackgroundExecOutputEvent): Buffer {
  switch (event.type) {
    case "started":
    case "truncated":
      return Buffer.alloc(0);
    case "stdout":
    case "stderr":
      return Buffer.from(event.data ?? new Uint8Array());
    case "gap": {
      const payload = Buffer.alloc(8);
      payload.writeBigUInt64BE(BigInt(event.gapBytes ?? 0), 0);
      return payload;
    }
    case "exit": {
      const payload = Buffer.alloc(4);
      payload.writeInt32BE(event.exitCode ?? 0, 0);
      return payload;
    }
    case "cancelled":
    case "error":
      return Buffer.from(event.reason ?? "");
  }
}

function backgroundExecFrameType(type: BackgroundExecOutputEvent["type"]): number {
  switch (type) {
    case "started":
      return 1;
    case "stdout":
      return 2;
    case "stderr":
      return 3;
    case "gap":
      return 4;
    case "truncated":
      return 5;
    case "exit":
      return 6;
    case "cancelled":
      return 7;
    case "error":
      return 8;
  }
}
