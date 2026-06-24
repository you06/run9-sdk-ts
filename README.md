# run9-sdk-ts

TypeScript SDK for the run9 control-plane API.

This repository mirrors the public control-plane surface exposed by
[`run9-sdk-go`](https://github.com/sys9-ai/run9-sdk-go): API-key authenticated
HTTP requests, project-scoped workspace paths, box/snap/org/project/shared-snap
views and mutations, foreground/background exec helpers, and archive
upload/download helpers. The current API follows `run9-sdk-go` main at
`53cea25`.

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
- Foreground exec streaming uses `ExecStream.readEvent()`.
- Exec attach uses the `ws` package for Node WebSocket support.
