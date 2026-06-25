# run9-sdk-ts

TypeScript SDK for the run9 control-plane API.

This repository mirrors the public control-plane surface exposed by
[`run9-sdk-go`](https://github.com/sys9-ai/run9-sdk-go): API-key authenticated
HTTP requests, project-scoped workspace paths, box/snap/org/project/shared-snap
views and mutations, foreground/background exec helpers, and archive
upload/download helpers. The current API follows `run9-sdk-go` main at
`efa6004` / `v0.3.0`.

It intentionally does not include CLI-only concerns such as config persistence,
text formatting, shell completion, or command-line UX.

## Install

```bash
pnpm add run9-sdk-ts
```

## Example

```ts
import { Run9Client } from "run9-sdk-ts";

const client = new Run9Client("https://api.run.sys9.ai", {
  ak: process.env.RUN9_AK ?? "",
  sk: process.env.RUN9_SK ?? ""
}).withProject("default");

const boxes = await client.listBoxes({ state: "running" });

console.log(`loaded ${boxes.length} boxes`);
```

Run one foreground exec and capture its terminal result plus the merged log
snapshot:

```ts
const capture = await client.runExecCapture("box-1", {
  command: ["bash", "-lc", "echo hello"]
});

console.log(capture.execID, capture.terminal.status, capture.terminal.exitCode);
console.log(new TextDecoder().decode(capture.transcript ?? new Uint8Array()));
```

Follow one background exec incrementally:

```ts
const exec = await client.startBackgroundExec("box-1", {
  command: ["bash", "-lc", "echo hello && sleep 1 && echo done"]
});
const follower = client.followBackgroundExec(exec.exec_id);

while (true) {
  const window = await follower.pump(2_000, {
    stdout: (chunk) => process.stdout.write(chunk),
    stderr: (chunk) => process.stderr.write(chunk)
  });
  const terminal = window.terminalResult();
  if (terminal) {
    console.log(`terminal: ${terminal.status}`);
    break;
  }
}
```

## Development

```bash
pnpm install
pnpm typecheck
pnpm test
pnpm build
```

## Notes

- Uses global `fetch` and `AbortSignal`, available in Node 20+.
- Credentials are bound to the client at construction time.
- Project-scoped APIs require `client.withProject("project-cid")`.
- Raw download APIs return Web `ReadableStream<Uint8Array>`.
- Foreground exec streaming uses `ExecStream.readEvent()` or the higher-level
  `runExec` / `runExecCapture` helpers.
- Background exec output is decoded from run9 binary frames into typed events.
- Exec attach uses the `ws` package for Node WebSocket support and supports
  `ExecAttachSocket.pump(...)`.
