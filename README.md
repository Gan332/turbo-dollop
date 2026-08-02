# OpenCode Free API

Small Rust reverse proxy for OpenAI-compatible upstreams. It exposes health,
model, and chat-completion endpoints, filters models to `*-free`, and fails
over sequentially across configured nodes.

## Download

Prebuilt single-file binaries for each platform are attached to every
[GitHub Release](../../releases). No dependencies required — copy the right
file to your machine and run it.

| Platform | File | Notes |
| --- | --- | --- |
| Linux x86_64 | `opencode-free-api-linux-x86_64` | statically linked (musl) |
| Linux arm64 | `opencode-free-api-linux-aarch64` | statically linked (musl), e.g. Raspberry Pi |
| Windows x86_64 | `opencode-free-api-windows-x86_64.exe` | only uses system DLLs |
| macOS x86_64 | `opencode-free-api-macos-x86_64` | Intel |
| macOS arm64 | `opencode-free-api-macos-arm64` | Apple Silicon |

On Linux/macOS make it executable first: `chmod +x opencode-free-api-*`.

## Runtime configuration

| Variable | Default | Description |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8788` | Bind port |
| `NODES` | `https://opencode.ai/zen/v1` | Comma-separated upstream base URLs; defaults to the official OpenCode Zen gateway |
| `API_TOKEN` | unset | Optional bearer token sent to every upstream (set to your Zen key) |
| `AUTH_TOKEN` | unset | When set, requests must send `Authorization: Bearer <value>` (except `/health` and preflight) |
| `STRIP_FREE` | off | Strip the `-free` suffix from model ids in `/v1/models` and map calls back to the real id |
| `UPSTREAM_TIMEOUT` | `90` | Seconds for non-streaming upstream requests |
| `STREAM_TIMEOUT` | `1800` | Seconds for streaming upstream requests |
| `CONNECT_TIMEOUT` | `10` | Seconds for upstream TCP/TLS connect |
| `WORKERS` | CPU count | Number of concurrent request worker threads |

```sh
HOST=0.0.0.0 PORT=8788 \
NODES=https://api-one.example,https://api-two.example \
AUTH_TOKEN=my-secret \
./opencode-free-api
```

## API

- `GET /health`
- `GET /v1/models`
- `POST /v1/chat/completions`

The project has no embedded node address, credential, or recovered-binary
content. It is released under the MIT license.

`/v1/models` pulls the live upstream list (defaults to the official OpenCode
Zen catalog, e.g. `https://opencode.ai/zen/v1/models`) and keeps only free
models in post-processing. Requests carrying `"stream": true` are relayed
chunk-by-chunk as Server-Sent Events; failures are logged to stderr and the
proxy fails over to the next node. `/health` reports request and
upstream-failure counters.

## Build

```sh
cargo test
cargo build --release
```

The release profile already applies `opt-level = "z"`, `lto`, `strip`, and
`panic = "abort"`. For an even smaller binary, compress with UPX (~50%):

```sh
upx --best target/release/opencode-free-api
```

### Cross-compile every platform

Requires [rustup](https://rustup.rs), [zig](https://ziglang.org), and
[cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild):

```sh
rustup target add \
  x86_64-pc-windows-gnu x86_64-unknown-linux-musl aarch64-unknown-linux-musl \
  x86_64-apple-darwin aarch64-apple-darwin
cargo install cargo-zigbuild --locked
cargo zigbuild --release --target x86_64-pc-windows-gnu
cargo zigbuild --release --target x86_64-unknown-linux-musl
cargo zigbuild --release --target aarch64-unknown-linux-musl
cargo zigbuild --release --target x86_64-apple-darwin
cargo zigbuild --release --target aarch64-apple-darwin
```

The same build runs automatically on every `v*` tag via the
[`release` workflow](../../.github/workflows/release.yml) and uploads the
binaries to the GitHub Release.
