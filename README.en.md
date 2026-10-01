<div align="center">

![:name](https://count.getloli.com/@opencode-free-api?name=opencode-free-api&theme=minecraft&padding=6&offset=0&align=top&scale=1&pixelated=1&darkmode=auto)

# OpenCode Free API

_✨ OpenAI-compatible reverse proxy · single-binary deploy ✨_

[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-stable-orange.svg)](https://www.rust-lang.org/)
[![Version](https://img.shields.io/badge/version-0.1.8-blue.svg)](../../releases)
[![GitHub](https://img.shields.io/badge/author-ERX399-blue)](https://github.com/ERX399)

[English](./README.en.md) | [简体中文](./README.md)

</div>

Lightweight Rust reverse proxy for OpenAI-compatible upstreams. Filters models to `*-free` variants, failovers across multiple nodes, single-binary zero-dependency deployment.

## Download

Each [GitHub Release](../../releases) ships no-install archives per platform — a single executable, no dependencies, no scripts. Unzip and run.

The console prints three addresses on startup (loopback / LAN / public). Point any OpenAI-compatible client at one of them; the API key can be left blank. Set `API_TOKEN`, `AUTH_TOKEN` or a custom `NODES` list either via environment variables before launch (see table below) or later from the **Service settings** panel in the web UI — changes apply immediately, are written to the config file and are reloaded on restart.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8788` | Bind port |
| `NODES` | `https://opencode.ai/zen/v1` | Comma-separated upstream base URLs; defaults to the official OpenCode Zen gateway |
| `API_TOKEN` | unset | Optional Bearer token sent to each upstream; unset means no Authorization header is sent (fill in your Zen key) |
| `AUTH_TOKEN` | unset | If set, requests must carry `Authorization: Bearer <value>` (`/health`, the index page and preflight are exempt) |
| `STRIP_FREE` | off | Strip the `-free` suffix from model IDs in `/v1/models` and map calls back to the real ID |
| `CONFIG_PATH` | `opencode-free-api-config.json` | Config file written by the web **Service settings** panel; when present its fields override the four variables above, and a malformed file falls back to the environment |
| `UPSTREAM_TIMEOUT` | `90` | Non-streaming upstream request timeout (seconds) |
| `STREAM_TIMEOUT` | `1800` | Streaming upstream request timeout (seconds) |
| `CONNECT_TIMEOUT` | `10` | Upstream TCP/TLS connect timeout (seconds) |
| `WORKERS` | CPU cores | Concurrent request worker threads |

```sh
HOST=0.0.0.0 PORT=8788 \
NODES=https://api-one.example,https://api-two.example \
AUTH_TOKEN=my-secret \
./opencode-free-api
```

## API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/health` `/healthz` `/ready` `/api/health` `/api/status` | Health check & stats (request count, listen addresses, start time) |
| `GET` | `/v1/models` | Free model list — only `*-free` models from the upstream |
| `GET` | `/claude/v1/models` `/anthropic/v1/models` | Claude-compatible model list |
| `POST` | `/v1/chat/completions` | Chat completions, streaming SSE supported |
| `POST` | `/v1/responses` | Responses API (OpenAI Responses compatible), streaming SSE supported |
| `GET` `POST` | `/api/config` | Read / save service settings (upstream nodes, tokens, `STRIP_FREE`); saves take effect immediately and are written to the config file |
| `*` | anything else | Transparently forwarded upstream; failure triggers failover to the next node |

`/v1`-prefixed endpoints also accept `/models`, `/chat/completions`, `/responses` and common aliases like `/api/v1`, `/api/v3`, `/api/paas/v4`, `/v1beta`. `/v1/models` fetches the upstream list and keeps only free models; upstream requests use `User-Agent: opencode/1.0`, streaming requests are forwarded chunk-by-chunk as SSE and fail over to the next node on errors. `/health` reports request count and listen address information.

`/api/config` access rules: when `AUTH_TOKEN` is set the matching Bearer token is required; otherwise only requests from `127.0.0.1` may write, every other origin is read-only (token fields come back masked). `GET /api/config` returns `nodes`, `strip_free`, `api_token`/`auth_token` (empty for non-admins), `*_set`, `admin`, `auth_required` and `config_path`; `POST` accepts `nodes` (array), `strip_free` (boolean), `api_token`/`auth_token` (string, empty means unchanged) and `clear_api_token`/`clear_auth_token` (`true` to clear). Fields that are absent keep their current value.

## Thinking mode

A request carrying `thinking.enabled`, `reasoning_effort`, or `reasoning.effort` is treated as thinking mode:

- **Request side**: an empty `reasoning_content` is injected at the top level and into each assistant message, so upstream (OpenCode Console) validation passes even when the client does not echo reasoning back
- **Response side**: when upstream reports only `reasoning_tokens` without `reasoning_content`, both non-streaming responses and streaming SSE emit an empty-string fallback, keeping clients like ChatGPT-Next-Web from complaining about missing reasoning
- Real `reasoning_content` from upstream passes through untouched

## Web UI

Visiting the root path (e.g. `http://localhost:8788/`) returns an embedded status page:

- **Status & stats**: polls `/health` for uptime, request count and free-model count, and checks GitHub releases to show an update banner
- **Available models**: an expanded-by-default model panel with search filtering, manual refresh and click-to-copy model IDs; when `AUTH_TOKEN` is enabled the admin token is attached automatically, otherwise the panel reports that an admin token is needed
- **Service settings**: edit `NODES` (one per line), `STRIP_FREE`, `API_TOKEN` and `AUTH_TOKEN` right in the page — saves hot-apply to the running service and are written to the file at `CONFIG_PATH`, then reloaded on restart; the admin token can be remembered in the local browser
- **Access**: the index page and `/health` stay open without credentials; with `AUTH_TOKEN` set the read/write endpoints require a Bearer token, without it only `127.0.0.1` may change settings while every other origin stays read-only

## Android

Download `OC-Free-API-arm64.apk` from [Releases](../../releases) and install. It wraps the binary as a foreground service with a persistent notification and boot-start support.

**Keep-alive note**: Vendor ROMs (vivo / OPPO / Xiaomi / Huawei, etc.) aggressively freeze background processes, which makes the proxy unresponsive after backgrounding. After install you **must** manually allow "OC Free API" to run in the background / high-power-usage / autostart in the system battery settings, otherwise background mode won't work.

## Build

```sh
cargo test
cargo build --release
```

The release profile enables `opt-level = "z"`, `lto`, `strip` and `panic = "abort"`. Use UPX for a smaller binary (~ -50%).

### Cross-compile all platforms

Requires [rustup](https://rustup.rs), [zig](https://ziglang.org) and [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild):

```sh
rustup target add \
  x86_64-pc-windows-gnu x86_64-unknown-linux-musl aarch64-unknown-linux-musl \
  x86_64-apple-darwin aarch64-apple-darwin \
  aarch64-linux-android x86_64-linux-android
cargo install cargo-zigbuild --locked
for t in x86_64-pc-windows-gnu x86_64-unknown-linux-musl \
         aarch64-unknown-linux-musl x86_64-apple-darwin \
         aarch64-apple-darwin; do
  cargo zigbuild --release --target "$t"
done
# Android targets use the NDK toolchain, not zigbuild
cargo build --release --target aarch64-linux-android
cargo build --release --target x86_64-linux-android
```

Stage the artifacts into `dist/`, then run `scripts/make_packages.sh` to produce per-platform no-install archives.

Android requires the [Android NDK](https://developer.android.com/ndk) with `ANDROID_NDK_HOME` set. Point the linker in `~/.cargo/config.toml`:

```toml
[target.aarch64-linux-android]
linker = "<NDK>/toolchains/llvm/prebuilt/<host>/bin/aarch64-linux-android24-clang"
[target.x86_64-linux-android]
linker = "<NDK>/toolchains/llvm/prebuilt/<host>/bin/x86_64-linux-android24-clang"
```

The same flow runs automatically on every `v*` tag push via the [`release` workflow](../../.github/workflows/release.yml) and uploads the archives to a GitHub Release.
