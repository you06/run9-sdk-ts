import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { once } from "node:events";
import { describe, expect, it } from "vitest";
import { Run9Client, Run9Error, type CreateProjectRequest, type ExecListRequest, type UpdateBoxRequest } from "../src/index.js";

const creds = { ak: "ak-1", sk: "sk-1" };

describe("Run9Client", () => {
  it("normalizes base URL and preserves path prefixes", async () => {
    const server = await testServer((req, res) => {
      expect(req.url).toBe("/api/boxes");
      expect(req.method).toBe("GET");
      respondJSON(res, []);
    });

    try {
      const boxes = await new Run9Client(`${server.url}/api/`).boxes(creds);
      expect(boxes).toEqual([]);
    } finally {
      await server.close();
    }
  });

  it("uses project workspace paths", async () => {
    const server = await testServer((req, res) => {
      expect(req.url).toBe("/api/projects/default/workspace/boxes?state=running");
      expect(req.method).toBe("GET");
      respondJSON(res, []);
    });

    try {
      const boxes = await new Run9Client(`${server.url}/api`).withProject("default").boxes(creds, {
        state: "running"
      });
      expect(boxes).toEqual([]);
    } finally {
      await server.close();
    }
  });

  it("sends basic auth and JSON payloads", async () => {
    const server = await testServer(async (req, res) => {
      expect(req.url).toBe("/projects");
      expect(req.method).toBe("POST");
      expect(req.headers.authorization).toBe(`Basic ${Buffer.from("ak-1:sk-1").toString("base64")}`);
      expect(req.headers.accept).toBe("application/json");
      expect(req.headers["content-type"]).toContain("application/json");
      const body = (await readBody(req)) as CreateProjectRequest;
      expect(body).toEqual({ display_name: "Sandbox", description: "Isolated experiments" });
      respondJSON(res, { project_id: "proj-1", org_id: "org-1", project_cid: "sandbox", display_name: "Sandbox", role: "admin" });
    });

    try {
      const project = await new Run9Client(server.url).createProject(creds, {
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
      const box = await new Run9Client(server.url).withProject("default").updateBox(creds, " box-1 ", {
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
      const req: ExecListRequest = {
        boxID: "box-1",
        acceptedAfter,
        paged: true,
        limit: 10
      };
      const result = await new Run9Client(server.url).withProject("default").execs(creds, req);
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
      await expect(new Run9Client(server.url).boxes(creds, { state: "broken" })).rejects.toMatchObject({
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
      expect(url.pathname).toBe("/boxes/box-1/files/upload");
      expect(url.searchParams.get("box_abs_path")).toBe("/work/result.txt");
      expect(req.headers["content-type"]).toBe("application/x-tar");
      expect(await readText(req)).toBe("tar-body");
      respondJSON(res, { runtime_request_id: "runtime-upload-1", state: "prepared" });
    });

    try {
      const view = await new Run9Client(server.url).uploadArchive(creds, "box-1", "/work/result.txt", "tar-body");
      expect(view.runtime_request_id).toBe("runtime-upload-1");
    } finally {
      await server.close();
    }
  });

  it("reads background exec output body and headers", async () => {
    const server = await testServer(async (req, res) => {
      expect(req.url).toBe("/execs/exec-1/pull-output");
      expect(req.method).toBe("POST");
      expect(await readBody(req)).toEqual({ cursor: "cursor-1", wait_ms: 2000 });
      res.setHeader("X-Run9-Next-Cursor", "cursor-2");
      res.setHeader("X-Run9-Exec-State", "running");
      res.setHeader("X-Run9-Exit-Code", "0");
      res.setHeader("X-Run9-Reason", "done");
      res.setHeader("X-Run9-Idle-Deadline-At", "2026-03-28T12:00:00.000Z");
      res.end("binary-body");
    });

    try {
      const result = await new Run9Client(server.url).pullBackgroundExecOutput(creds, "exec-1", "cursor-1", 2000);
      expect(Buffer.from(result.body).toString("utf8")).toBe("binary-body");
      expect(result.nextCursor).toBe("cursor-2");
      expect(result.state).toBe("running");
      expect(result.exitCode).toBe(0);
      expect(result.reason).toBe("done");
      expect(result.idleDeadlineAt).toBe("2026-03-28T12:00:00.000Z");
    } finally {
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
      await expect(new Run9Client(server.url).execAttachURL("/attach")).rejects.toMatchObject({
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
