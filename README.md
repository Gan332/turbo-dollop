<div align="center">

![:name](https://count.getloli.com/@opencode-free-api?name=opencode-free-api&theme=minecraft&padding=6&offset=0&align=top&scale=1&pixelated=1&darkmode=auto)

# OpenCode Free API

_✨ OpenAI 兼容反代 · 单文件部署 ✨_

[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-stable-orange.svg)](https://www.rust-lang.org/)
[![Version](https://img.shields.io/badge/version-0.1.8-blue.svg)](../../releases)
[![GitHub](https://img.shields.io/badge/作者-ERX399-blue)](https://github.com/ERX399)

[简体中文](./README.md) | [English](./README.en.md)

</div>

面向 OpenAI 兼容上游的轻量 Rust 反向代理。只暴露 `*-free` 模型，多节点故障转移，单文件二进制零依赖部署。

## 下载

每个 [GitHub Release](../../releases) 提供各平台的免安装压缩包——单文件可执行，无依赖、无脚本，解压即用。

启动后控制台会打印三个访问地址（本机 / 局域网 / 公网）。将任意 OpenAI 兼容客户端指向其中之一即可，API Key 留空。如需设置 `API_TOKEN`、`AUTH_TOKEN` 或自定义 `NODES`，既可以在运行前通过环境变量配置（见下表），也可以启动后在网页「服务设置」里直接修改——保存后立即生效并写入配置文件，重启自动加载。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `HOST` | `0.0.0.0` | 绑定地址 |
| `PORT` | `8788` | 绑定端口 |
| `NODES` | `https://opencode.ai/zen/v1` | 逗号分隔的上游基础 URL，默认官方 OpenCode Zen 网关 |
| `API_TOKEN` | 未设置 | 发往上游的可选 Bearer 令牌，未设置时不发送 Authorization 请求头（填你的 Zen Key） |
| `AUTH_TOKEN` | 未设置 | 设置后请求须携带 `Authorization: Bearer <value>`（`/health`、首页与预检除外） |
| `STRIP_FREE` | off | 去掉 `/v1/models` 返回 ID 的 `-free` 后缀，调用时映射回真实 ID |
| `UPSTREAM_PROTOCOL` | `auto` | 上游补全协议：`auto` 按客户端协议直通；`chat` 上游固定 `/chat/completions`，Responses 客户端自动转成 Chat；`responses` 上游固定 `/responses`，Chat 客户端自动转成 Responses。也可在网页「服务设置」里修改，热更新 |
| `CONFIG_PATH` | `opencode-free-api-config.json` | 网页「服务设置」保存的配置文件路径；文件存在时按字段覆盖上面几个环境变量，解析失败自动回退环境变量 |
| `UPSTREAM_TIMEOUT` | `90` | 非流式上游请求超时（秒） |
| `STREAM_TIMEOUT` | `1800` | 流式上游请求超时（秒） |
| `CONNECT_TIMEOUT` | `10` | 上游 TCP/TLS 连接超时（秒） |
| `WORKERS` | CPU 核心数 | 并发工作线程数 |

```sh
HOST=0.0.0.0 PORT=8788 \
NODES=https://api-one.example,https://api-two.example \
AUTH_TOKEN=my-secret \
./opencode-free-api
```

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/health` `/healthz` `/ready` `/api/health` `/api/status` | 健康检查与统计（请求数、监听地址、启动时间） |
| `GET` | `/v1/models` | 免费模型列表，仅暴露上游 `*-free` 模型 |
| `GET` | `/claude/v1/models` `/anthropic/v1/models` | Claude 兼容模型列表 |
| `POST` | `/v1/chat/completions` | 对话补全，支持流式 SSE |
| `POST` | `/v1/responses` | Responses 补全（OpenAI Responses 兼容），支持流式 SSE |
| `GET` `POST` | `/api/config` | 读取 / 保存服务设置（上游节点、令牌、`STRIP_FREE`、`UPSTREAM_PROTOCOL`），保存后立即生效并写入配置文件 |
| `GET` | `/api/connectivity` | 逐节点连通性测试：并发请求各上游 `/models`，返回状态码、延迟毫秒、模型数量与错误信息 |
| `*` | 其他 | 透传上游，失败则切换下一节点 |

`/v1` 前缀的端点同时兼容 `/models`、`/chat/completions`、`/responses` 及 `/api/v1`、`/api/v3`、`/api/paas/v4`、`/v1beta` 等常见别名。`/v1/models` 拉取上游列表并只保留免费模型；上游请求统一使用 `User-Agent: opencode/1.0`，流式请求以 SSE 逐块转发并在失败时切换多节点。`/health` 报告请求数和监听地址信息。

`/api/config` 的读写权限：已设置 `AUTH_TOKEN` 时必须携带对应 Bearer 令牌；未设置时只有来自 `127.0.0.1` 的请求可以写入，其他来源只读（令牌字段以掩码返回）。`GET /api/config` 返回 `nodes`、`strip_free`、`api_token`/`auth_token`（非管理员为空串）、`*_set`、`admin`、`auth_required` 与 `config_path`；`POST` 接受 `nodes`（数组）、`strip_free`（布尔）、`api_token`/`auth_token`（字符串，空值不变）与 `clear_api_token`/`clear_auth_token`（`true` 清除），未出现的字段保持原值。

## 协议转换（Chat ⇄ Responses）

`UPSTREAM_PROTOCOL`（网页「服务设置 → 上游协议」）决定补全请求用哪种协议打上游：

| 取值 | 行为 |
| --- | --- |
| `auto` | 保持现状：客户端 `/v1/chat/completions` 走上游 `/chat/completions`，`/v1/responses` 走上游 `/responses`，不做转换 |
| `chat` | 上游固定 `/chat/completions`：Chat 客户端直通，Responses 客户端的请求先转成 Chat、响应回转成 Responses |
| `responses` | 上游固定 `/responses`：Responses 客户端直通，Chat 客户端的请求先转成 Responses、响应回转成 Chat |

转换覆盖：

- **请求**：system / instructions 互转、多模态 content part 互转、`tools` 与 `tool_choice` 扁平化和嵌套互转、`tool_calls` ↔ `function_call` + `function_call_output`、`max_tokens` ↔ `max_output_tokens`、`reasoning_effort` ↔ `reasoning.effort`，采样参数透传
- **响应**：文本、`tool_calls`（`finish_reason` 换算 `tool_calls` / `length` / `stop`）、`reasoning_content` → `reasoning` 输出项、usage 字段（`prompt/completion_tokens` ↔ `input/output_tokens`）双向换算，`max_output_tokens` 截断映射为 `incomplete`
- **流式**：`response.output_text.delta`、`response.function_call_arguments.delta`、`response.completed` 等事件与 `chat.completion.chunk` 互相换算（含 `finish_reason` 与 usage）；`auto` 模式下思考模式的 `reasoning_content` 兜底逻辑保持不变

## 思考模式（thinking）

请求里带 `thinking.enabled`、`reasoning_effort` 或 `reasoning.effort` 即视为思考模式：

- **请求侧**：自动在顶层与各 assistant 消息注入空的 `reasoning_content`，客户端不回传推理内容时也能通过上游（OpenCode Console）的校验
- **响应侧**：上游只返回 `reasoning_tokens` 却不带 `reasoning_content` 时，非流式响应与流式 SSE 都会补发空串，避免 ChatGPT-Next-Web 等客户端报"未找到推理内容"
- 上游返回了真实的 `reasoning_content` 时原样透传，不做任何改写

## 前端页面

访问根路径（如 `http://localhost:8788/`）返回内嵌状态页：

- **状态与统计**：轮询 `/health` 展示运行时长、请求数与免费模型数量，并检测 GitHub Release 显示更新横幅
- **可用模型**：默认展开的模型面板，支持搜索过滤、手动刷新与点击复制模型 ID；启用 `AUTH_TOKEN` 后自动携带管理令牌，未提供时显示「需要管理令牌」
- **服务设置**：在页面里直接修改 `NODES`（每行一个）、`STRIP_FREE`、`UPSTREAM_PROTOCOL`、`API_TOKEN`、`AUTH_TOKEN`，保存后立即热更新并写入 `CONFIG_PATH` 指向的配置文件，重启自动加载；管理令牌可勾选「记住」存放在本机浏览器
- **连通性测试**：一键并发探测每个上游 `/models`，逐节点显示 ✓/✗、HTTP 状态码、延迟与模型数量，失败时展示错误摘要
- **权限**：首页与 `/health` 无需凭证即可打开；启用 `AUTH_TOKEN` 后读写接口需 Bearer 令牌，未启用时仅 `127.0.0.1` 可修改配置，其他来源只读（连通性测试与模型列表跟随读取权限）

## Android

从 [Releases](../../releases) 下载 `OC-Free-API-arm64.apk` 安装即可，内嵌前台服务与常驻通知、开机自启。

**保活提示**：国产 ROM（vivo / OPPO / 小米 / 华为 等）会激进冻结后台进程，导致切后台后代理无响应。安装后**必须**在系统电池设置里手动允许「OC Free API」后台运行 / 高耗电 / 自启动，否则后台不可用。

## 构建

```sh
cargo test
cargo build --release
```

release 配置已启用 `opt-level = "z"`、`lto`、`strip`、`panic = "abort"`。可用 UPX 进一步压缩（约 -50%）。

### 交叉编译全部平台

需要 [rustup](https://rustup.rs)、[zig](https://ziglang.org) 和 [cargo-zigbuild](https://github.com/rust-cross/cargo-zigbuild)：

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
# Android 使用 NDK 工具链，不走 zigbuild
cargo build --release --target aarch64-linux-android
cargo build --release --target x86_64-linux-android
```

将产物整理进 `dist/`，运行 `scripts/make_packages.sh` 生成各平台免安装压缩包。

Android 需安装 [Android NDK](https://developer.android.com/ndk) 并设置 `ANDROID_NDK_HOME`，在 `~/.cargo/config.toml` 指定链接器：

```toml
[target.aarch64-linux-android]
linker = "<NDK>/toolchains/llvm/prebuilt/<host>/bin/aarch64-linux-android24-clang"
[target.x86_64-linux-android]
linker = "<NDK>/toolchains/llvm/prebuilt/<host>/bin/x86_64-linux-android24-clang"
```

同样的流程在每次推送 `v*` 标签时由 [`release` 工作流](../../.github/workflows/release.yml) 自动执行，并将压缩包上传到 GitHub Release。
