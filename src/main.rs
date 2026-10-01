use std::{
    collections::HashMap,
    env, fs,
    io::{Cursor, Read},
    net::{IpAddr, UdpSocket},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::{Map, Value, json};
use tiny_http::{Header, Method, Request as HttpRequest, Response, Server, StatusCode};
use ureq::Agent;

type ResponseBox = tiny_http::ResponseBox;
type UpstreamResponse = http::Response<Vec<u8>>;

#[derive(Debug)]
struct UpstreamError {
    status: u16,
    message: String,
    body: Vec<u8>,
}

const MAX_BODY_BYTES: u64 = 64 << 20;
const MAX_CONFIG_BYTES: u64 = 64 << 10;
const MAX_CONFIG_NODES: usize = 64;
const DEFAULT_NODE: &str = "https://opencode.ai/zen/v1";
const DEFAULT_CONFIG_PATH: &str = "opencode-free-api-config.json";
const PROTOCOL_AUTO: &str = "auto";
const PROTOCOL_CHAT: &str = "chat";
const PROTOCOL_RESPONSES: &str = "responses";
const FREE_MAP_TTL: u64 = 60;
const UPSTREAM_USER_AGENT: &str = "opencode/1.0";
const VERSION: &str = env!("CARGO_PKG_VERSION");
const INDEX_HTML: &str = include_str!("../static/index.html");

#[derive(Clone, Debug)]
struct RuntimeConfig {
    nodes: Vec<String>,
    api_token: Option<String>,
    auth_token: Option<String>,
    strip_free: bool,
    protocol: String,
}

#[derive(Debug, Default)]
struct StoredConfig {
    nodes: Option<Value>,
    api_token: Option<Option<String>>,
    auth_token: Option<Option<String>>,
    strip_free: Option<bool>,
    protocol: Option<String>,
}

#[derive(Clone)]
struct App {
    config: Arc<Mutex<RuntimeConfig>>,
    config_path: PathBuf,
    free_map: Arc<Mutex<FreeMap>>,
    client: Agent,
    stream_client: Agent,
    requests: Arc<AtomicU64>,
    port: String,
    local_ip: String,
    public_ip: Option<String>,
    started: u64,
}

struct FreeMap {
    entries: HashMap<String, String>,
    refreshed_at: u64,
}

impl FreeMap {
    fn new() -> Self {
        Self {
            entries: HashMap::new(),
            refreshed_at: 0,
        }
    }
}

impl App {
    fn from_env() -> Self {
        let connect_timeout = env_seconds("CONNECT_TIMEOUT", 10);
        let nodes = split_nodes(&env::var("NODES").unwrap_or_default());
        let nodes = if nodes.is_empty() {
            vec![DEFAULT_NODE.to_owned()]
        } else {
            nodes
        };
        let port = env_or("PORT", "8788");
        let mut runtime = RuntimeConfig {
            nodes,
            api_token: env::var("API_TOKEN").ok().filter(|value| !value.is_empty()),
            auth_token: env::var("AUTH_TOKEN")
                .ok()
                .filter(|value| !value.is_empty()),
            strip_free: env_flag("STRIP_FREE"),
            protocol: normalize_protocol(&env::var("UPSTREAM_PROTOCOL").unwrap_or_default())
                .unwrap_or(PROTOCOL_AUTO)
                .to_owned(),
        };
        let path = config_path();
        if let Some(stored) = load_stored(&path) {
            stored.apply(&mut runtime);
            eprintln!("已从配置文件加载 {}", path.display());
        }
        App {
            config: Arc::new(Mutex::new(runtime)),
            config_path: path,
            free_map: Arc::new(Mutex::new(FreeMap::new())),
            client: client(env_seconds("UPSTREAM_TIMEOUT", 90), connect_timeout),
            stream_client: client(env_seconds("STREAM_TIMEOUT", 1800), connect_timeout),
            requests: Arc::new(AtomicU64::new(0)),
            port,
            local_ip: primary_ip().unwrap_or_else(|| "127.0.0.1".to_owned()),
            public_ip: public_ip(),
            started: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
        }
    }

    fn cfg(&self) -> RuntimeConfig {
        self.config.lock().expect("config lock").clone()
    }

    fn auth_required(&self) -> bool {
        self.config
            .lock()
            .expect("config lock")
            .auth_token
            .is_some()
    }

    fn protocol(&self) -> String {
        self.config.lock().expect("config lock").protocol.clone()
    }

    fn admin_authorized(&self, request: &HttpRequest) -> bool {
        let auth_required = self.auth_required();
        let authorized = !auth_required || self.authorized(request);
        let remote = request.remote_addr().map(|addr| addr.ip());
        admin_allowed(auth_required, authorized, remote)
    }
}

fn main() {
    setup_console_utf8();
    let address = format!("{}:{}", env_or("HOST", "0.0.0.0"), env_or("PORT", "8788"));
    let app = App::from_env();
    let workers = env::var("WORKERS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| {
            thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
        .max(1);

    eprintln!(
        "正在监听 {address}，上游节点 {} 个，工作线程 {workers} 个",
        app.cfg().nodes.len()
    );
    eprintln!("  控制台: http://127.0.0.1:{}/", app.port);
    eprintln!("  本机:   http://127.0.0.1:{}/v1", app.port);
    eprintln!("  局域网: http://{}:{}/v1", app.local_ip, app.port);
    if let Some(ip) = &app.public_ip {
        eprintln!("  公网:   http://{ip}:{}/v1", app.port);
        eprintln!("  （中国大陆家庭网络可能无法直接使用公网连接，建议走局域网）");
    } else {
        eprintln!("  公网:   无法获取（探测失败）");
    }
    if app.auth_required() {
        eprintln!("已启用 AUTH_TOKEN 鉴权");
    } else {
        eprintln!("未启用 AUTH_TOKEN 鉴权：服务处于开放模式，服务设置仅本机可改");
    }

    let server = match Server::http(&address) {
        Ok(server) => server,
        Err(_) => {
            eprintln!("[Error] 端口已被占用，无法在 {address} 启动 HTTP 服务");
            eprintln!("请设置环境变量 PORT 改用其他端口后重新启动");
            eprintln!("5 秒后自动退出 ...");
            thread::sleep(Duration::from_secs(5));
            std::process::exit(1);
        }
    };
    let (tx, rx) = mpsc::channel::<HttpRequest>();
    let rx = Arc::new(Mutex::new(rx));
    for _ in 0..workers {
        let rx = Arc::clone(&rx);
        let app = app.clone();
        thread::spawn(move || {
            while let Ok(mut request) = rx.lock().expect("request queue lock").recv() {
                let response = app.handle(&mut request);
                let _ = request.respond(response);
            }
        });
    }
    for request in server.incoming_requests() {
        let _ = tx.send(request);
    }
}

fn client(timeout: Duration, connect_timeout: Duration) -> Agent {
    Agent::config_builder()
        .timeout_global(Some(timeout))
        .timeout_connect(Some(connect_timeout))
        .http_status_as_error(false)
        .build()
        .into()
}

fn add_headers<B>(
    mut builder: ureq::RequestBuilder<B>,
    accept: Option<&str>,
    token: &Option<String>,
) -> ureq::RequestBuilder<B> {
    builder = add_token(builder, token);
    if let Some(accept) = accept {
        builder = builder.header("Accept", accept);
    }
    builder
}

fn add_token<B>(
    mut builder: ureq::RequestBuilder<B>,
    token: &Option<String>,
) -> ureq::RequestBuilder<B> {
    builder = builder.header("User-Agent", UPSTREAM_USER_AGENT);
    if let Some(token) = token {
        builder = builder.header("Authorization", &format!("Bearer {token}"));
    }
    builder
}

fn read_body(body: ureq::Body) -> Vec<u8> {
    use std::io::Read as _;
    let mut bytes = Vec::new();
    let mut reader = body.into_reader();
    let _ = reader.read_to_end(&mut bytes);
    bytes
}

fn read_body_response(response: http::Response<ureq::Body>) -> UpstreamResponse {
    let (parts, body) = response.into_parts();
    let body = read_body(body);
    http::Response::from_parts(parts, body)
}

#[cfg(windows)]
fn setup_console_utf8() {
    use std::ffi::c_int;
    const CP_UTF8: u32 = 65001;
    unsafe extern "C" {
        fn SetConsoleOutputCP(cp: u32) -> c_int;
        fn SetConsoleCP(cp: u32) -> c_int;
    }
    unsafe {
        let _ = SetConsoleOutputCP(CP_UTF8);
        let _ = SetConsoleCP(CP_UTF8);
    }
}

#[cfg(not(windows))]
fn setup_console_utf8() {}

fn primary_ip() -> Option<String> {
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect("8.8.8.8:80").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    if ip.is_loopback() {
        None
    } else {
        Some(ip.to_string())
    }
}

fn public_ip() -> Option<String> {
    let agent = client(Duration::from_secs(5), Duration::from_secs(5));
    let text = agent.get("https://api.ipify.org").call().ok()?.into_body();
    let ip = String::from_utf8(read_body(text)).ok()?;
    let ip = ip.trim().to_owned();
    if ip.is_empty() { None } else { Some(ip) }
}

impl App {
    fn handle(&self, request: &mut HttpRequest) -> ResponseBox {
        let method = request.method().clone();
        let full_path = request.url().to_owned();
        let raw_path = full_path.split('?').next().unwrap_or(&full_path);
        let path = normalize_path(raw_path);

        if method == Method::Options {
            return cors_response(204, Vec::new());
        }
        if method == Method::Get && is_health_path(path) {
            return json_response(
                200,
                json!({
                    "status": "ok",
                    "nodes": self.cfg().nodes,
                    "requests": self.requests.load(Ordering::Relaxed),
                    "port": self.port,
                    "local_ip": self.local_ip,
                    "public_ip": self.public_ip,
                    "started": self.started,
                }),
            );
        }
        if method == Method::Get && path == "/" {
            return self.index_response();
        }
        if self.auth_required() && !self.authorized(request) {
            return error_response(401, "invalid_api_key", "missing or invalid bearer token");
        }
        self.requests.fetch_add(1, Ordering::Relaxed);
        if method == Method::Get && is_config_path(path) {
            return self.config_view_response(request);
        }
        if method == Method::Post && is_config_path(path) {
            return self.config_update(request);
        }
        if method == Method::Get && is_connectivity_path(path) {
            return self.connectivity();
        }
        if method == Method::Get && is_models_path(path) {
            return self.models(request);
        }
        if method == Method::Get && is_claude_models_path(path) {
            return self.claude_models();
        }
        if method == Method::Post && is_chat_path(path) {
            return self.chat_completions(request);
        }
        if method == Method::Post && is_responses_path(path) {
            return self.responses_completions(request);
        }
        self.forward(request, &method, &full_path)
    }

    fn authorized(&self, request: &HttpRequest) -> bool {
        let expected = self.cfg().auth_token.unwrap_or_default();
        request_header(request, "Authorization")
            .and_then(|value| {
                value
                    .strip_prefix("Bearer ")
                    .map(str::trim)
                    .map(str::to_owned)
            })
            .is_some_and(|token| token == expected)
    }

    fn config_view_response(&self, request: &HttpRequest) -> ResponseBox {
        let admin = self.admin_authorized(request);
        json_response(200, self.config_view(admin))
    }

    fn config_view(&self, admin: bool) -> Value {
        let cfg = self.cfg();
        let api_token_set = cfg.api_token.is_some();
        let auth_token_set = cfg.auth_token.is_some();
        let auth_required = auth_token_set;
        let api_token = match cfg.api_token {
            Some(token) if admin => token,
            _ => String::new(),
        };
        let auth_token = match cfg.auth_token {
            Some(token) if admin => token,
            _ => String::new(),
        };
        json!({
            "nodes": cfg.nodes,
            "strip_free": cfg.strip_free,
            "protocol": cfg.protocol,
            "api_token": api_token,
            "auth_token": auth_token,
            "api_token_set": api_token_set,
            "auth_token_set": auth_token_set,
            "admin": admin,
            "auth_required": auth_required,
            "config_path": self.config_path.display().to_string(),
        })
    }

    fn config_update(&self, request: &mut HttpRequest) -> ResponseBox {
        if !self.admin_authorized(request) {
            return error_response(
                403,
                "forbidden",
                "config changes require the AUTH_TOKEN bearer or loopback access",
            );
        }
        let mut body = Vec::new();
        if request
            .as_reader()
            .take(MAX_CONFIG_BYTES + 1)
            .read_to_end(&mut body)
            .is_err()
            || body.len() as u64 > MAX_CONFIG_BYTES
        {
            return error_response(
                400,
                "invalid_request_error",
                "config body must be a small JSON object",
            );
        }
        let payload: Value = match serde_json::from_slice(&body) {
            Ok(payload) => payload,
            Err(_) => {
                return error_response(
                    400,
                    "invalid_request_error",
                    "config body must be valid JSON",
                );
            }
        };
        let object = match payload.as_object() {
            Some(object) => object.clone(),
            None => {
                return error_response(
                    400,
                    "invalid_request_error",
                    "config body must be a JSON object",
                );
            }
        };
        let strip_free = match object.get("strip_free") {
            Some(Value::Bool(value)) => Some(*value),
            Some(Value::Null) | None => None,
            Some(_) => {
                return error_response(
                    400,
                    "invalid_request_error",
                    "strip_free must be a boolean",
                );
            }
        };
        let protocol = match object.get("protocol") {
            Some(Value::String(value)) => match normalize_protocol(value) {
                Some(value) => Some(value.to_owned()),
                None => {
                    return error_response(
                        400,
                        "invalid_request_error",
                        "protocol must be auto, chat or responses",
                    );
                }
            },
            Some(Value::Null) | None => None,
            Some(_) => {
                return error_response(
                    400,
                    "invalid_request_error",
                    "protocol must be a string",
                );
            }
        };
        let api_token = match token_patch(&object, "api_token", "clear_api_token") {
            Ok(value) => value,
            Err(message) => return error_response(400, "invalid_request_error", &message),
        };
        let auth_token = match token_patch(&object, "auth_token", "clear_auth_token") {
            Ok(value) => value,
            Err(message) => return error_response(400, "invalid_request_error", &message),
        };
        let nodes = match object.get("nodes") {
            Some(raw) => match parse_nodes(raw) {
                Ok(nodes) => Some(nodes),
                Err(message) => return error_response(400, "invalid_request_error", &message),
            },
            None => None,
        };
        let path = &self.config_path;
        let persisted;
        {
            let mut config = self.config.lock().expect("config lock");
            if let Some(nodes) = nodes {
                config.nodes = nodes;
            }
            if let Some(strip_free) = strip_free {
                config.strip_free = strip_free;
            }
            if let Some(protocol) = protocol {
                config.protocol = protocol;
            }
            if let Some(api_token) = api_token {
                config.api_token = api_token;
            }
            if let Some(auth_token) = auth_token {
                config.auth_token = auth_token;
            }
            persisted = persist_config(path, &config).is_ok();
            if !persisted {
                eprintln!("配置已生效，但写入 {} 失败", path.display());
            }
        }
        {
            let mut map = self.free_map.lock().expect("model map lock");
            map.entries.clear();
            map.refreshed_at = 0;
        }
        let mut payload = self.config_view(true);
        payload["saved"] = json!(true);
        payload["persisted"] = json!(persisted);
        if !persisted {
            payload["warning"] = json!("配置已生效，但写入配置文件失败");
        }
        json_response(200, payload)
    }

    fn connectivity(&self) -> ResponseBox {
        let config = self.cfg();
        let mut handles = Vec::with_capacity(config.nodes.len());
        for node in config.nodes.clone() {
            let token = config.api_token.clone();
            handles.push(thread::spawn(move || probe_node(node, token)));
        }
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.join() {
                Ok(result) => results.push(result),
                Err(_) => results.push(json!({
                    "node": "unknown",
                    "ok": false,
                    "status": 0,
                    "latency_ms": 0,
                    "models": 0,
                    "error": "探测线程异常退出",
                })),
            }
        }
        json_response(
            200,
            json!({
                "protocol": config.protocol,
                "tested": results.len(),
                "results": results,
            }),
        )
    }

    fn models(&self, request: &HttpRequest) -> ResponseBox {
        let accept = request_header(request, "Accept");
        let response =
            match self.upstream("/models", Method::Get, Vec::new(), accept.as_deref(), false) {
                Ok(response) => response,
                Err(error) => {
                    return error_response(error.status, "upstream_error", &error.message);
                }
            };
        let payload: Value = match serde_json::from_slice(&response.into_body()).ok() {
            Some(payload) => payload,
            None => return error_response(502, "upstream_error", "invalid models response"),
        };
        let models = self.process_models(payload);
        json_response(200, json!({"data": models}))
    }

    fn chat_completions(&self, request: &mut HttpRequest) -> ResponseBox {
        let mut body = Vec::new();
        if request
            .as_reader()
            .take(MAX_BODY_BYTES + 1)
            .read_to_end(&mut body)
            .is_err()
            || body.len() as u64 > MAX_BODY_BYTES
        {
            return bad_request();
        }
        let payload: Value = match serde_json::from_slice(&body) {
            Ok(payload) => payload,
            Err(_) => return bad_request(),
        };
        let stream = is_stream_request(&payload);
        let thinking = thinking_mode(&payload);
        let accept = request_header(request, "Accept");
        if self.protocol() == PROTOCOL_RESPONSES {
            return self.chat_via_responses(payload, stream, thinking, accept.as_deref());
        }
        let body = self.remap_chat_body(payload);
        if stream {
            return self.chat_stream(body, accept.as_deref(), thinking);
        }
        match self.upstream(
            "/chat/completions",
            Method::Post,
            body,
            accept.as_deref(),
            false,
        ) {
            Ok(response) => {
                if thinking {
                    let (parts, mut body) = response.into_parts();
                    ensure_completion_reasoning(&mut body);
                    return proxy_response(http::Response::from_parts(parts, body));
                }
                proxy_response(response)
            }
            Err(error) => upstream_error_response(error, "application/json; charset=utf-8"),
        }
    }

    fn responses_completions(&self, request: &mut HttpRequest) -> ResponseBox {
        let mut body = Vec::new();
        if request
            .as_reader()
            .take(MAX_BODY_BYTES + 1)
            .read_to_end(&mut body)
            .is_err()
            || body.len() as u64 > MAX_BODY_BYTES
        {
            return bad_request();
        }
        let payload: Value = match serde_json::from_slice(&body) {
            Ok(payload) => payload,
            Err(_) => return bad_request(),
        };
        let stream = is_stream_request(&payload);
        let accept = request_header(request, "Accept");
        if self.protocol() == PROTOCOL_CHAT {
            return self.responses_via_chat(payload, stream, accept.as_deref());
        }
        let payload = self.remap_model(payload);
        let body = serde_json::to_vec(&payload).unwrap_or_default();
        if stream {
            return self.stream_upstream("/responses", body, accept.as_deref(), false);
        }
        match self.upstream("/responses", Method::Post, body, accept.as_deref(), false) {
            Ok(response) => proxy_response(response),
            Err(error) => upstream_error_response(error, "application/json; charset=utf-8"),
        }
    }

    fn remap_model(&self, mut payload: Value) -> Value {
        if self.cfg().strip_free {
            self.refresh_free_map();
            if let Some(original) = payload
                .get("model")
                .and_then(Value::as_str)
                .and_then(|model| {
                    self.free_map
                        .lock()
                        .expect("model map lock")
                        .entries
                        .get(model)
                        .cloned()
                })
            {
                payload["model"] = Value::String(original);
            }
        }
        payload
    }

    fn remap_chat_body(&self, payload: Value) -> Vec<u8> {
        let mut payload = self.remap_model(payload);
        // reasoning.effort 写法归一到 reasoning_effort，两种写法都能触发思考模式；
        // 已有显式 reasoning_effort 时优先保留原值
        if payload.get("reasoning_effort").is_none()
            && let Some(effort) = payload
                .get("reasoning")
                .and_then(|reasoning| reasoning.get("effort"))
                .and_then(Value::as_str)
                .filter(|effort| !effort.is_empty())
        {
            payload["reasoning_effort"] = Value::String(effort.to_owned());
        }
        // Codex/thinking 协议要求把推理内容回传给上游，否则上游报 400。
        // OpenCode Console (zen/v1) 期望在请求顶层带上 reasoning_content；
        // 客户端通常不回传，这里伪装一个空串让上游通过校验。
        if thinking_mode(&payload) {
            inject_thinking_fields(&mut payload);
        }
        serde_json::to_vec(&payload).unwrap_or_default()
    }

    fn chat_via_responses(
        &self,
        payload: Value,
        stream: bool,
        thinking: bool,
        accept: Option<&str>,
    ) -> ResponseBox {
        let payload = self.remap_model(payload);
        let upstream = chat_to_responses_request(&payload);
        let body = serde_json::to_vec(&upstream).unwrap_or_default();
        if stream {
            return match self.upstream("/responses", Method::Post, body, accept, true) {
                Ok(response) => {
                    let text = String::from_utf8_lossy(&response.into_body()).replace("\r\n", "\n");
                    let mut converted = responses_stream_to_chat_stream(&text).into_bytes();
                    if thinking {
                        transform_stream_body(&mut converted);
                    }
                    sse_response(200, converted)
                }
                Err(error) => {
                    upstream_error_response(error, "text/event-stream; charset=utf-8")
                }
            };
        }
        match self.upstream("/responses", Method::Post, body, accept, false) {
            Ok(upstream) => {
                let payload: Value = match serde_json::from_slice(&upstream.into_body()) {
                    Ok(payload) => payload,
                    Err(_) => {
                        return error_response(502, "upstream_error", "invalid responses payload");
                    }
                };
                let mut converted =
                    serde_json::to_vec(&responses_to_chat_response(&payload)).unwrap_or_default();
                if thinking {
                    ensure_completion_reasoning(&mut converted);
                }
                response(
                    200,
                    converted,
                    Some("application/json; charset=utf-8"),
                    None,
                )
            }
            Err(error) => upstream_error_response(error, "application/json; charset=utf-8"),
        }
    }

    fn responses_via_chat(
        &self,
        payload: Value,
        stream: bool,
        accept: Option<&str>,
    ) -> ResponseBox {
        let payload = self.remap_model(payload);
        let mut upstream = responses_request_to_chat(&payload);
        if thinking_mode(&upstream) {
            inject_thinking_fields(&mut upstream);
        }
        let body = serde_json::to_vec(&upstream).unwrap_or_default();
        if stream {
            return match self.upstream("/chat/completions", Method::Post, body, accept, true) {
                Ok(response) => {
                    let text = String::from_utf8_lossy(&response.into_body()).replace("\r\n", "\n");
                    sse_response(200, chat_stream_to_responses_stream(&text).into_bytes())
                }
                Err(error) => {
                    upstream_error_response(error, "text/event-stream; charset=utf-8")
                }
            };
        }
        match self.upstream("/chat/completions", Method::Post, body, accept, false) {
            Ok(response) => {
                let payload: Value = match serde_json::from_slice(&response.into_body()) {
                    Ok(payload) => payload,
                    Err(_) => {
                        return error_response(502, "upstream_error", "invalid chat payload");
                    }
                };
                json_response(200, chat_to_responses_response(&payload))
            }
            Err(error) => upstream_error_response(error, "application/json; charset=utf-8"),
        }
    }

    fn chat_stream(&self, body: Vec<u8>, accept: Option<&str>, thinking: bool) -> ResponseBox {
        self.stream_upstream("/chat/completions", body, accept, thinking)
    }

    fn stream_upstream(
        &self,
        path: &str,
        body: Vec<u8>,
        accept: Option<&str>,
        thinking: bool,
    ) -> ResponseBox {
        match self.upstream(path, Method::Post, body, accept, true) {
            Ok(response) => {
                let status = response.status().as_u16();
                let mut body = response.into_body();
                if thinking {
                    transform_stream_body(&mut body);
                }
                sse_response(status, body)
            }
            Err(error) => upstream_error_response(error, "text/event-stream; charset=utf-8"),
        }
    }

    fn index_response(&self) -> ResponseBox {
        let public = self.public_ip.as_deref().unwrap_or("无法获取公网 IP");
        let html = INDEX_HTML
            .replace("__VERSION__", VERSION)
            .replace("__PORT__", &self.port)
            .replace("__LOCAL_IP__", &self.local_ip)
            .replace("__PUBLIC_IP__", public);
        let body = html.into_bytes();
        let len = body.len();
        let mut headers = vec![
            header("Content-Type", "text/html; charset=utf-8"),
            header("Cache-Control", "no-cache, no-store, must-revalidate"),
        ];
        append_cors(&mut headers);
        Response::new(StatusCode(200), headers, Cursor::new(body), Some(len), None).boxed()
    }
}

impl App {
    fn process_models(&self, payload: Value) -> Vec<Value> {
        let strip_free = self.cfg().strip_free;
        let mut map = self.free_map.lock().expect("model map lock");
        map.entries.clear();
        payload
            .get("data")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|mut item| {
                let id = item.get("id").and_then(Value::as_str)?;
                if !self.is_free_model(id) {
                    return None;
                }
                if !strip_free {
                    map.entries.insert(id.to_owned(), id.to_owned());
                    return Some(item);
                }
                let stripped = strip_free_suffix(id);
                if map
                    .entries
                    .get(&stripped)
                    .is_some_and(|existing| existing != id)
                {
                    return None;
                }
                map.entries.insert(stripped.clone(), id.to_owned());
                item.as_object_mut()
                    .map(|object| object.insert("id".to_owned(), Value::String(stripped)));
                Some(item)
            })
            .collect()
    }

    fn is_free_model(&self, id: &str) -> bool {
        id.to_lowercase().contains("free")
    }

    fn refresh_free_map(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let stale = {
            let map = self.free_map.lock().expect("model map lock");
            now.saturating_sub(map.refreshed_at) >= FREE_MAP_TTL
        };
        if !stale {
            return;
        }
        let refreshed = self
            .upstream("/models", Method::Get, Vec::new(), None, false)
            .ok()
            .and_then(|response| {
                serde_json::from_slice(&response.into_body())
                    .ok()
                    .filter(|value: &Value| value.get("data").is_some())
            });
        if let Some(payload) = refreshed {
            self.process_models(payload);
            self.free_map.lock().expect("model map lock").refreshed_at = now;
        }
    }

    fn claude_models(&self) -> ResponseBox {
        let ids = self.free_model_ids();
        let data: Vec<Value> = ids
            .into_iter()
            .map(|id| {
                json!({
                    "id": id,
                    "display_name": id,
                    "created_at": 0,
                    "type": "model"
                })
            })
            .collect();
        json_response(200, json!({ "data": data }))
    }

    fn free_model_ids(&self) -> Vec<String> {
        self.refresh_free_map();
        let map = self.free_map.lock().expect("model map lock");
        let mut ids: Vec<String> = map.entries.keys().cloned().collect();
        ids.sort();
        ids
    }
}

impl App {
    fn upstream(
        &self,
        path: &str,
        method: Method,
        body: Vec<u8>,
        accept: Option<&str>,
        stream: bool,
    ) -> Result<UpstreamResponse, UpstreamError> {
        let config = self.cfg();
        if config.nodes.is_empty() {
            return Err(UpstreamError {
                status: 502,
                message: "no upstream nodes configured".to_string(),
                body: Vec::new(),
            });
        }
        let agent = if stream {
            &self.stream_client
        } else {
            &self.client
        };
        let mut failures = Vec::with_capacity(config.nodes.len());
        for node in &config.nodes {
            let url = format!("{node}{path}");
            let outcome = match method {
                Method::Post => {
                    let builder = add_headers(agent.post(&url), accept, &config.api_token);
                    builder
                        .header("Content-Type", "application/json")
                        .send(&body)
                        .map(read_body_response)
                }
                Method::Get => {
                    let builder = add_headers(agent.get(&url), accept, &config.api_token);
                    builder.call().map(read_body_response)
                }
                _ => continue,
            };
            match outcome {
                Ok(response) => {
                    if !response.status().is_success() {
                        let status = response.status();
                        let body_bytes = response.into_body();
                        let body_str = String::from_utf8_lossy(&body_bytes).trim().to_owned();
                        let message = if body_str.is_empty() {
                            format!("{node}: HTTP {status}")
                        } else {
                            format!("{node}: HTTP {status} - {body_str}")
                        };
                        eprintln!("上游请求失败 {message}");
                        if status.is_client_error() {
                            return Err(UpstreamError {
                                status: status.as_u16(),
                                message,
                                body: body_bytes,
                            });
                        }
                        failures.push(message);
                        continue;
                    }
                    if stream {
                        eprintln!("正在通过 {node} 流式转发 {path}");
                    }
                    return Ok(response);
                }
                Err(error) => {
                    eprintln!("上游请求失败 {node}: {error}");
                    failures.push(format!("{node}: {error}"));
                }
            }
        }
        Err(UpstreamError {
            status: 502,
            message: failures.join("; "),
            body: Vec::new(),
        })
    }

    fn forward(&self, request: &mut HttpRequest, method: &Method, full_path: &str) -> ResponseBox {
        let config = self.cfg();
        if config.nodes.is_empty() {
            return error_response(502, "upstream_error", "no upstream nodes configured");
        }
        let mut body = Vec::new();
        if matches!(method, &Method::Post | &Method::Put | &Method::Patch)
            && (request
                .as_reader()
                .take(MAX_BODY_BYTES + 1)
                .read_to_end(&mut body)
                .is_err()
                || body.len() as u64 > MAX_BODY_BYTES)
        {
            return bad_request();
        }
        let target = route_upstream_path(full_path);
        for node in &config.nodes {
            let url = format!("{node}{target}");
            let result = self.call_with_method(method, &url, &body, &config.api_token);
            match result {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let content_type = response
                        .headers()
                        .get("Content-Type")
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned)
                        .unwrap_or_else(|| "application/json; charset=utf-8".to_owned());
                    let body = response.into_body();
                    let mut headers = vec![header("Cache-Control", "no-cache")];
                    headers.push(header("Content-Type", &content_type));
                    append_cors(&mut headers);
                    return Response::new(
                        StatusCode(status),
                        headers,
                        Cursor::new(body),
                        None,
                        None,
                    )
                    .boxed();
                }
                Err(error) => {
                    eprintln!("上游请求失败 {node}: {error}");
                }
            }
        }
        error_response(502, "upstream_error", "all upstream nodes failed")
    }

    fn call_with_method(
        &self,
        method: &Method,
        url: &str,
        body: &[u8],
        token: &Option<String>,
    ) -> Result<UpstreamResponse, ureq::Error> {
        let agent = &self.stream_client;
        let response = match method {
            Method::Post | Method::Put | Method::Patch => {
                let builder = match method {
                    Method::Post => add_token(agent.post(url), token),
                    Method::Put => add_token(agent.put(url), token),
                    _ => add_token(agent.patch(url), token),
                };
                builder
                    .header("Content-Type", "application/json")
                    .send(body)?
            }
            Method::Delete => add_token(agent.delete(url), token).call()?,
            _ => add_token(agent.get(url), token).call()?,
        };
        Ok(read_body_response(response))
    }
}

fn probe_node(node: String, token: Option<String>) -> Value {
    let agent = client(Duration::from_secs(10), Duration::from_secs(5));
    let url = format!("{node}/models");
    let started = Instant::now();
    let outcome = add_headers(agent.get(&url), None, &token).call();
    let latency_ms = started.elapsed().as_millis() as u64;
    let mut result = json!({
        "node": node,
        "ok": false,
        "status": 0,
        "latency_ms": latency_ms,
        "models": 0,
        "error": Value::Null,
    });
    match outcome {
        Ok(response) => {
            let status = response.status().as_u16();
            result["status"] = json!(status);
            let body = read_body(response.into_body());
            if (200..300).contains(&status) {
                let payload: Option<Value> = serde_json::from_slice(&body).ok();
                match payload.and_then(|value| value.get("data").cloned()) {
                    Some(Value::Array(models)) => {
                        result["ok"] = json!(true);
                        result["models"] = json!(models.len());
                    }
                    Some(_) => result["error"] = json!("上游返回的 data 字段不是数组"),
                    None => result["error"] = json!("上游返回不是合法的模型列表 JSON"),
                }
            } else {
                result["error"] = json!(format!("HTTP {status}{}", error_snippet(&body)));
            }
        }
        Err(error) => {
            result["error"] = json!(format!("{error}"));
        }
    }
    result
}

fn error_snippet(body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let text = text.trim();
    if text.is_empty() {
        return String::new();
    }
    let clipped: String = text.chars().take(160).collect();
    if text.chars().count() > 160 {
        format!(" - {clipped}…")
    } else {
        format!(" - {clipped}")
    }
}

fn upstream_error_response(error: UpstreamError, content_type: &str) -> ResponseBox {
    if (400..500).contains(&error.status) && !error.body.is_empty() {
        response(error.status, error.body, Some(content_type), None)
    } else {
        error_response(error.status, "upstream_error", &error.message)
    }
}

fn proxy_response(upstream: UpstreamResponse) -> ResponseBox {
    let status = upstream.status().as_u16();
    let content_type = upstream
        .headers()
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let cache_control = upstream
        .headers()
        .get("Cache-Control")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = upstream.into_body();
    response(
        status,
        body,
        content_type.as_deref(),
        cache_control.as_deref(),
    )
}

fn sse_response(status: u16, body: Vec<u8>) -> ResponseBox {
    let length = body.len();
    let mut headers = vec![
        header("Content-Type", "text/event-stream; charset=utf-8"),
        header("Cache-Control", "no-cache"),
    ];
    append_cors(&mut headers);
    Response::new(
        StatusCode(status),
        headers,
        Cursor::new(body),
        Some(length),
        None,
    )
    .boxed()
}

fn cors_response(status: u16, body: Vec<u8>) -> ResponseBox {
    response(status, body, None, None)
}

fn json_response(status: u16, payload: Value) -> ResponseBox {
    let body = serde_json::to_vec(&payload).expect("serialize JSON response");
    response(status, body, Some("application/json; charset=utf-8"), None)
}

fn error_response(status: u16, kind: &str, message: &str) -> ResponseBox {
    json_response(status, json!({"error": {"type": kind, "message": message}}))
}

fn bad_request() -> ResponseBox {
    error_response(
        400,
        "invalid_request_error",
        "request body must be valid JSON up to 64 MiB",
    )
}

fn response(
    status: u16,
    body: Vec<u8>,
    content_type: Option<&str>,
    cache_control: Option<&str>,
) -> ResponseBox {
    let mut headers = Vec::with_capacity(4);
    if let Some(value) = content_type {
        headers.push(header("Content-Type", value));
    }
    if let Some(value) = cache_control {
        headers.push(header("Cache-Control", value));
    }
    append_cors(&mut headers);
    let data_length = Some(body.len());
    Response::new(
        StatusCode(status),
        headers,
        Cursor::new(body),
        data_length,
        None,
    )
    .boxed()
}

fn append_cors(headers: &mut Vec<Header>) {
    headers.push(header("Access-Control-Allow-Origin", "*"));
    headers.push(header("Access-Control-Allow-Methods", "GET, POST, OPTIONS"));
    headers.push(header(
        "Access-Control-Allow-Headers",
        "Content-Type, Authorization",
    ));
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name, value).expect("valid static header")
}

fn request_header(request: &HttpRequest, name: &str) -> Option<String> {
    request
        .headers()
        .iter()
        .find(|header| header.field.as_str().as_str().eq_ignore_ascii_case(name))
        .map(|header| header.value.as_str().to_owned())
}

fn is_stream_request(payload: &Value) -> bool {
    payload
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn thinking_mode(payload: &Value) -> bool {
    payload
        .get("thinking")
        .and_then(|value| value.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || payload
            .get("reasoning_effort")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
        || payload
            .get("reasoning")
            .and_then(|reasoning| reasoning.get("effort"))
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
}

// 思考模式下 Codex/OpenCode Console 要求请求里回传 reasoning_content，
// 客户端通常不带，这里给顶层与各 assistant 消息补一个空串
fn inject_thinking_fields(payload: &mut Value) {
    if !payload
        .get("reasoning_content")
        .is_some_and(Value::is_string)
    {
        payload["reasoning_content"] = Value::String(String::new());
    }
    if let Some(messages) = payload.get_mut("messages").and_then(Value::as_array_mut) {
        for message in messages {
            if message.get("role").and_then(Value::as_str) == Some("assistant")
                && !message
                    .get("reasoning_content")
                    .is_some_and(Value::is_string)
            {
                message["reasoning_content"] = Value::String(String::new());
            }
        }
    }
}

// 思考模式下客户端期望响应必带 reasoning_content；
// 实测上游 reasoning_tokens 记账不可靠（思考了也常报 0），
// 因此只按"字段缺失"补空串，不看 token 数
fn ensure_completion_reasoning(body: &mut Vec<u8>) {
    let mut payload: Value = match serde_json::from_slice(body) {
        Ok(payload) => payload,
        Err(_) => return,
    };
    if payload.get("choices").is_none() {
        return;
    }
    if !payload
        .get("reasoning_content")
        .is_some_and(Value::is_string)
    {
        payload["reasoning_content"] = Value::String(String::new());
    }
    if let Some(choices) = payload.get_mut("choices").and_then(Value::as_array_mut) {
        for choice in choices {
            if choice.get("reasoning_content").is_some() {
                continue;
            }
            if let Some(message) = choice.get_mut("message")
                && !message
                    .get("reasoning_content")
                    .is_some_and(Value::is_string)
            {
                message["reasoning_content"] = Value::String(String::new());
            }
        }
    }
    if let Ok(updated) = serde_json::to_vec(&payload) {
        *body = updated;
    }
}

// 从流里找一个带 id/model 的 chunk，借它的身份字段给兜底 chunk，
// 否则严格的 OpenAI SDK 会因缺字段解析失败
fn stream_chunk_identity(text: &str) -> Option<Value> {
    for block in text.split("\n\n") {
        let Some(data_line) = block.lines().find_map(|line| {
            line.strip_prefix("data:")
                .map(str::trim_start)
                .filter(|value| !value.is_empty() && *value != "[DONE]")
        }) else {
            continue;
        };
        let Ok(payload) = serde_json::from_str::<Value>(data_line) else {
            continue;
        };
        if payload.get("id").is_some() || payload.get("model").is_some() {
            return Some(payload);
        }
    }
    None
}

fn fallback_reasoning_chunk(text: &str) -> String {
    let mut chunk = json!({ "choices": [{ "index": 0, "delta": { "reasoning_content": "" } }] });
    if let Some(object) = chunk.as_object_mut()
        && let Some(identity) = stream_chunk_identity(text)
    {
        for key in ["id", "object", "created", "model"] {
            if let Some(value) = identity.get(key) {
                object.insert(key.to_owned(), value.clone());
            }
        }
    }
    format!("data: {chunk}\n\n")
}

// 思考模式下上游已经发过 reasoning_content delta 就不再动手；
// reasoning_tokens 实测记账不可靠，不作为补偿依据
fn stream_has_reasoning(text: &str) -> bool {
    text.contains("\"reasoning_content\"")
}

fn inject_reasoning_into_finish_block(block: &str) -> Option<String> {
    let data_line = block.lines().find_map(|line| {
        line.strip_prefix("data:")
            .map(str::trim_start)
            .filter(|value| !value.is_empty() && *value != "[DONE]")
    })?;
    let mut payload: Value = serde_json::from_str(data_line).ok()?;
    let mut modified = false;
    if let Some(choices) = payload.get_mut("choices").and_then(Value::as_array_mut) {
        for choice in choices {
            if choice.get("finish_reason").is_none() {
                continue;
            }
            if let Some(delta) = choice.get_mut("delta")
                && delta.get("content").is_none()
                && delta.get("reasoning_content").is_none()
            {
                delta["reasoning_content"] = Value::String(String::new());
                modified = true;
            }
        }
    }
    if !modified {
        return None;
    }
    let rewritten = serde_json::to_string(&payload).ok()?;
    let mut rebuilt = Vec::new();
    let mut replaced = false;
    for line in block.lines() {
        if !replaced
            && line
                .strip_prefix("data:")
                .is_some_and(|value| !value.trim().is_empty())
        {
            rebuilt.push(format!("data: {rewritten}"));
            replaced = true;
        } else {
            rebuilt.push(line.to_owned());
        }
    }
    Some(rebuilt.join("\n") + "\n\n")
}

// 思考模式下上游从头到尾没发过 reasoning_content delta 时，
// 在 finish chunk 里补一个空串，客户端才不会抱怨"推理内容缺失"
fn transform_stream_body(body: &mut Vec<u8>) {
    let text = String::from_utf8_lossy(body).replace("\r\n", "\n");
    if stream_has_reasoning(&text) {
        return;
    }
    let fallback = fallback_reasoning_chunk(&text);
    let mut transformed = String::with_capacity(text.len() + fallback.len());
    let mut injected = false;
    for block in text.split_inclusive("\n\n") {
        if !injected {
            if let Some(rewritten) = inject_reasoning_into_finish_block(block) {
                transformed.push_str(&rewritten);
                injected = true;
                continue;
            }
            if block.lines().any(|line| line.trim() == "data: [DONE]") {
                transformed.push_str(&fallback);
                injected = true;
            }
        }
        transformed.push_str(block);
    }
    if !injected {
        transformed.push_str(&fallback);
    }
    *body = transformed.into_bytes();
}

// ---------- Chat ⇄ Responses 协议转换 ----------

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn chat_message_text(message: &Value) -> String {
    match message.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.get("text").and_then(Value::as_str))
            .collect::<Vec<&str>>()
            .join("\n"),
        _ => String::new(),
    }
}

fn chat_content_parts(content: Option<&Value>, assistant: bool) -> Vec<Value> {
    let text_type = if assistant { "output_text" } else { "input_text" };
    match content {
        Some(Value::String(text)) => vec![json!({"type": text_type, "text": text})],
        Some(Value::Array(items)) => {
            let mut parts = Vec::new();
            for item in items {
                match item.get("type").and_then(Value::as_str) {
                    Some("text") | Some("input_text") | Some("output_text") => {
                        let text = item.get("text").and_then(Value::as_str).unwrap_or_default();
                        parts.push(json!({"type": text_type, "text": text}));
                    }
                    Some("image_url") => {
                        let url = match item.get("image_url") {
                            Some(Value::String(url)) => url.clone(),
                            Some(other) => other
                                .get("url")
                                .and_then(Value::as_str)
                                .unwrap_or_default()
                                .to_owned(),
                            None => String::new(),
                        };
                        parts.push(json!({"type": "input_image", "image_url": url}));
                    }
                    Some(_) => parts.push(item.clone()),
                    None => {}
                }
            }
            parts
        }
        _ => Vec::new(),
    }
}

fn responses_text(parts: Option<&Value>) -> String {
    match parts {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| match item.get("type").and_then(Value::as_str) {
                Some("output_text") | Some("input_text") | Some("text") | Some("refusal") => {
                    item.get("text").and_then(Value::as_str).unwrap_or_default()
                }
                _ => "",
            })
            .collect::<Vec<&str>>()
            .join(""),
        _ => String::new(),
    }
}

fn responses_message_content(parts: Option<&Value>) -> Value {
    match parts {
        Some(Value::String(text)) => json!(text),
        Some(Value::Array(items)) => {
            let mut converted = Vec::new();
            for item in items {
                match item.get("type").and_then(Value::as_str) {
                    Some("output_text") | Some("input_text") | Some("text") => converted.push(
                        json!({"type": "text", "text": item.get("text").and_then(Value::as_str).unwrap_or_default()}),
                    ),
                    Some("input_image") => {
                        let url = item
                            .get("image_url")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        converted.push(json!({"type": "image_url", "image_url": {"url": url}}));
                    }
                    Some("image_url") => converted.push(item.clone()),
                    Some(_) | None => {}
                }
            }
            if converted.is_empty() {
                return json!("");
            }
            if converted.len() == 1
                && converted
                    .first()
                    .and_then(|part| part.get("type"))
                    .and_then(Value::as_str)
                    == Some("text")
            {
                return converted.remove(0)
                    .get("text")
                    .cloned()
                    .unwrap_or_else(|| json!(""));
            }
            Value::Array(converted)
        }
        None => json!(""),
        Some(other) => other.clone(),
    }
}

fn convert_chat_tool(tool: &Value) -> Option<Value> {
    let function = tool.get("function").unwrap_or(tool);
    let name = function.get("name").and_then(Value::as_str)?;
    let mut converted = json!({"type": "function", "name": name});
    if let Some(description) = function.get("description") {
        converted["description"] = description.clone();
    }
    if let Some(parameters) = function.get("parameters") {
        converted["parameters"] = parameters.clone();
    }
    if let Some(strict) = function.get("strict") {
        converted["strict"] = strict.clone();
    }
    Some(converted)
}

fn convert_responses_tool(tool: &Value) -> Option<Value> {
    if let Some(function) = tool.get("function") {
        return convert_chat_tool(function);
    }
    let name = tool.get("name").and_then(Value::as_str)?;
    let mut converted = json!({
        "type": "function",
        "function": {"name": name},
    });
    if let Some(description) = tool.get("description") {
        converted["function"]["description"] = description.clone();
    }
    if let Some(parameters) = tool.get("parameters") {
        converted["function"]["parameters"] = parameters.clone();
    }
    if let Some(strict) = tool.get("strict") {
        converted["function"]["strict"] = strict.clone();
    }
    Some(converted)
}

fn convert_tool_choice(choice: Option<&Value>, to_responses: bool) -> Option<Value> {
    let choice = choice?;
    if let Some(name) = choice
        .get("function")
        .and_then(|value| value.get("name"))
        .and_then(Value::as_str)
        .or_else(|| choice.get("name").and_then(Value::as_str))
    {
        return Some(if to_responses {
            json!({"type": "function", "name": name})
        } else {
            json!({"type": "function", "function": {"name": name}})
        });
    }
    choice.as_str().map(|text| json!(text))
}

fn chat_to_responses_request(payload: &Value) -> Value {
    let mut request = Map::new();
    if let Some(model) = payload.get("model") {
        request.insert("model".to_owned(), model.clone());
    }
    let mut instructions: Vec<String> = Vec::new();
    let mut input: Vec<Value> = Vec::new();
    if let Some(messages) = payload.get("messages").and_then(Value::as_array) {
        for message in messages {
            let role = message.get("role").and_then(Value::as_str).unwrap_or("user");
            if role == "system" || role == "developer" {
                let text = chat_message_text(message);
                if !text.is_empty() {
                    instructions.push(text);
                }
                continue;
            }
            if role == "tool" {
                input.push(json!({
                    "type": "function_call_output",
                    "call_id": message.get("tool_call_id").and_then(Value::as_str).unwrap_or_default(),
                    "output": chat_message_text(message),
                }));
                continue;
            }
            let assistant = role == "assistant";
            if assistant
                && let Some(calls) = message.get("tool_calls").and_then(Value::as_array)
            {
                for call in calls {
                    let function = call.get("function");
                    let call_id = call
                        .get("id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .unwrap_or_else(|| format!("call_{}", input.len()));
                    input.push(json!({
                        "type": "function_call",
                        "call_id": call_id,
                        "name": function.and_then(|value| value.get("name")).and_then(Value::as_str).unwrap_or_default(),
                        "arguments": function.and_then(|value| value.get("arguments")).and_then(Value::as_str).unwrap_or("{}"),
                    }));
                }
            }
            let parts = chat_content_parts(message.get("content"), assistant);
            if parts.is_empty() {
                continue;
            }
            input.push(json!({
                "type": "message",
                "role": if assistant { "assistant" } else { "user" },
                "content": parts,
            }));
        }
    }
    if !instructions.is_empty() {
        request.insert("instructions".to_owned(), json!(instructions.join("\n\n")));
    }
    request.insert("input".to_owned(), Value::Array(input));
    if let Some(limit) = payload
        .get("max_tokens")
        .or_else(|| payload.get("max_completion_tokens"))
    {
        request.insert("max_output_tokens".to_owned(), limit.clone());
    }
    for key in [
        "temperature",
        "top_p",
        "stop",
        "parallel_tool_calls",
        "user",
        "metadata",
        "service_tier",
    ] {
        if let Some(value) = payload.get(key) {
            request.insert(key.to_owned(), value.clone());
        }
    }
    request.insert("stream".to_owned(), json!(is_stream_request(payload)));
    if let Some(tools) = payload.get("tools").and_then(Value::as_array) {
        let converted: Vec<Value> = tools.iter().filter_map(convert_chat_tool).collect();
        if !converted.is_empty() {
            request.insert("tools".to_owned(), Value::Array(converted));
        }
    }
    if let Some(choice) = convert_tool_choice(payload.get("tool_choice"), true) {
        request.insert("tool_choice".to_owned(), choice);
    }
    if let Some(reasoning) = payload.get("reasoning").filter(|value| value.is_object()) {
        request.insert("reasoning".to_owned(), reasoning.clone());
    } else if let Some(effort) = payload
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        request.insert("reasoning".to_owned(), json!({"effort": effort}));
    }
    Value::Object(request)
}

fn responses_request_to_chat(payload: &Value) -> Value {
    let mut request = Map::new();
    if let Some(model) = payload.get("model") {
        request.insert("model".to_owned(), model.clone());
    }
    let mut messages: Vec<Value> = Vec::new();
    if let Some(instructions) = payload
        .get("instructions")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        messages.push(json!({"role": "system", "content": instructions}));
    }
    match payload.get("input") {
        Some(Value::String(text)) => messages.push(json!({"role": "user", "content": text})),
        Some(Value::Array(items)) => {
            for item in items {
                let kind = item.get("type").and_then(Value::as_str).unwrap_or("message");
                match kind {
                    "function_call" => messages.push(json!({
                        "role": "assistant",
                        "content": Value::Null,
                        "tool_calls": [{
                            "id": item.get("call_id").and_then(Value::as_str).unwrap_or_default(),
                            "type": "function",
                            "function": {
                                "name": item.get("name").and_then(Value::as_str).unwrap_or_default(),
                                "arguments": item.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
                            },
                        }],
                    })),
                    "function_call_output" => messages.push(json!({
                        "role": "tool",
                        "tool_call_id": item.get("call_id").and_then(Value::as_str).unwrap_or_default(),
                        "content": responses_text(Some(item.get("output").unwrap_or(&Value::Null))),
                    })),
                    "reasoning" => {}
                    _ => {
                        let role = item.get("role").and_then(Value::as_str).unwrap_or("user");
                        let content = responses_message_content(item.get("content"));
                        messages.push(json!({"role": role, "content": content}));
                    }
                }
            }
        }
        _ => {}
    }
    request.insert("messages".to_owned(), Value::Array(messages));
    if let Some(limit) = payload.get("max_output_tokens") {
        request.insert("max_tokens".to_owned(), limit.clone());
    }
    for key in [
        "temperature",
        "top_p",
        "stop",
        "parallel_tool_calls",
        "user",
        "metadata",
    ] {
        if let Some(value) = payload.get(key) {
            request.insert(key.to_owned(), value.clone());
        }
    }
    request.insert("stream".to_owned(), json!(is_stream_request(payload)));
    if let Some(effort) = payload
        .get("reasoning")
        .and_then(|reasoning| reasoning.get("effort"))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        request.insert("reasoning_effort".to_owned(), json!(effort));
    }
    if let Some(tools) = payload.get("tools").and_then(Value::as_array) {
        let converted: Vec<Value> = tools.iter().filter_map(convert_responses_tool).collect();
        if !converted.is_empty() {
            request.insert("tools".to_owned(), Value::Array(converted));
        }
    }
    if let Some(choice) = convert_tool_choice(payload.get("tool_choice"), false) {
        request.insert("tool_choice".to_owned(), choice);
    }
    Value::Object(request)
}

fn responses_usage_to_chat(usage: &Value) -> Value {
    let input = usage.get("input_tokens").and_then(Value::as_u64).unwrap_or(0);
    let output = usage
        .get("output_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(input + output);
    let mut converted = Map::new();
    converted.insert("prompt_tokens".to_owned(), json!(input));
    converted.insert("completion_tokens".to_owned(), json!(output));
    converted.insert("total_tokens".to_owned(), json!(total));
    if let Some(cached) = usage
        .get("input_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
    {
        converted.insert(
            "prompt_tokens_details".to_owned(),
            json!({"cached_tokens": cached}),
        );
    }
    if let Some(tokens) = usage
        .get("output_tokens_details")
        .and_then(|details| details.get("reasoning_tokens"))
    {
        converted.insert(
            "completion_tokens_details".to_owned(),
            json!({"reasoning_tokens": tokens}),
        );
    }
    Value::Object(converted)
}

fn chat_usage_from_response(response: &Value) -> Option<Value> {
    response.get("usage").map(responses_usage_to_chat)
}

fn chat_usage_to_responses(usage: &Value) -> Value {
    let prompt = usage
        .get("prompt_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let completion = usage
        .get("completion_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total = usage
        .get("total_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(prompt + completion);
    let mut converted = Map::new();
    converted.insert("input_tokens".to_owned(), json!(prompt));
    converted.insert("output_tokens".to_owned(), json!(completion));
    converted.insert("total_tokens".to_owned(), json!(total));
    if let Some(cached) = usage
        .get("prompt_tokens_details")
        .and_then(|details| details.get("cached_tokens"))
    {
        converted.insert(
            "input_tokens_details".to_owned(),
            json!({"cached_tokens": cached}),
        );
    }
    if let Some(tokens) = usage
        .get("completion_tokens_details")
        .and_then(|details| details.get("reasoning_tokens"))
    {
        converted.insert(
            "output_tokens_details".to_owned(),
            json!({"reasoning_tokens": tokens}),
        );
    }
    Value::Object(converted)
}

fn responses_usage_from_chat(payload: &Value) -> Option<Value> {
    payload.get("usage").map(chat_usage_to_responses)
}

fn responses_to_chat_response(payload: &Value) -> Value {
    let response = match payload.get("response") {
        Some(value) if value.is_object() => value,
        _ => payload,
    };
    let id = response
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("chatcmpl-converted");
    let model = response.get("model").and_then(Value::as_str).unwrap_or("unknown");
    let created = response
        .get("created_at")
        .and_then(Value::as_u64)
        .unwrap_or_else(now_secs);
    let mut content = String::new();
    let mut reasoning = String::new();
    let mut tool_calls: Vec<Value> = Vec::new();
    if let Some(output) = response.get("output").and_then(Value::as_array) {
        for item in output {
            match item.get("type").and_then(Value::as_str) {
                Some("message") => content.push_str(&responses_text(item.get("content"))),
                Some("function_call") => tool_calls.push(json!({
                    "id": item.get("call_id").and_then(Value::as_str).unwrap_or_default(),
                    "type": "function",
                    "function": {
                        "name": item.get("name").and_then(Value::as_str).unwrap_or_default(),
                        "arguments": item.get("arguments").and_then(Value::as_str).unwrap_or("{}"),
                    },
                })),
                Some("reasoning") => {
                    if let Some(summary) = item.get("summary").and_then(Value::as_array) {
                        for part in summary {
                            if let Some(text) = part.get("text").and_then(Value::as_str) {
                                reasoning.push_str(text);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    let status = response.get("status").and_then(Value::as_str).unwrap_or("");
    let incomplete = status == "incomplete"
        && response
            .get("incomplete_details")
            .and_then(|details| details.get("reason"))
            .and_then(Value::as_str)
            == Some("max_output_tokens");
    let finish_reason = if !tool_calls.is_empty() {
        "tool_calls"
    } else if incomplete {
        "length"
    } else {
        "stop"
    };
    let mut message = Map::new();
    message.insert("role".to_owned(), json!("assistant"));
    if tool_calls.is_empty() {
        message.insert("content".to_owned(), json!(content));
    } else {
        message.insert(
            "content".to_owned(),
            if content.is_empty() {
                Value::Null
            } else {
                json!(content)
            },
        );
        message.insert("tool_calls".to_owned(), Value::Array(tool_calls));
    }
    if !reasoning.is_empty() {
        message.insert("reasoning_content".to_owned(), json!(reasoning));
    }
    let mut result = Map::new();
    result.insert("id".to_owned(), json!(id));
    result.insert("object".to_owned(), json!("chat.completion"));
    result.insert("created".to_owned(), json!(created));
    result.insert("model".to_owned(), json!(model));
    result.insert(
        "choices".to_owned(),
        json!([{
            "index": 0,
            "message": Value::Object(message),
            "finish_reason": finish_reason,
        }]),
    );
    if let Some(usage) = chat_usage_from_response(response) {
        result.insert("usage".to_owned(), usage);
    }
    Value::Object(result)
}

fn chat_to_responses_response(payload: &Value) -> Value {
    let choice = payload
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first());
    let message = choice.and_then(|choice| choice.get("message"));
    let finish_reason = choice
        .and_then(|choice| choice.get("finish_reason"))
        .and_then(Value::as_str)
        .unwrap_or("stop");
    let text = message
        .and_then(|message| message.get("content"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let reasoning = message
        .and_then(|message| message.get("reasoning_content"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    let tool_calls = message
        .and_then(|message| message.get("tool_calls"))
        .and_then(Value::as_array);
    let id = payload
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("resp_converted");
    let model = payload.get("model").and_then(Value::as_str).unwrap_or("unknown");
    let created = payload
        .get("created")
        .and_then(Value::as_u64)
        .unwrap_or_else(now_secs);
    let mut output: Vec<Value> = Vec::new();
    if !reasoning.is_empty() {
        output.push(json!({
            "type": "reasoning",
            "id": format!("rs_{id}"),
            "summary": [{"type": "summary_text", "text": reasoning}],
        }));
    }
    let has_calls = tool_calls.is_some_and(|calls| !calls.is_empty());
    if !text.is_empty() || !has_calls {
        output.push(json!({
            "type": "message",
            "id": format!("msg_{id}"),
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": text, "annotations": []}],
        }));
    }
    if let Some(calls) = tool_calls {
        for (index, call) in calls.iter().enumerate() {
            let function = call.get("function");
            output.push(json!({
                "type": "function_call",
                "id": format!("fc_{id}_{index}"),
                "call_id": call.get("id").and_then(Value::as_str).unwrap_or_default(),
                "name": function.and_then(|value| value.get("name")).and_then(Value::as_str).unwrap_or_default(),
                "arguments": function
                    .and_then(|value| value.get("arguments"))
                    .and_then(Value::as_str)
                    .unwrap_or("{}"),
                "status": "completed",
            }));
        }
    }
    let incomplete = finish_reason == "length";
    let mut result = Map::new();
    result.insert("id".to_owned(), json!(id));
    result.insert("object".to_owned(), json!("response"));
    result.insert("created_at".to_owned(), json!(created));
    result.insert("model".to_owned(), json!(model));
    result.insert(
        "status".to_owned(),
        json!(if incomplete { "incomplete" } else { "completed" }),
    );
    result.insert("output".to_owned(), Value::Array(output));
    result.insert("output_text".to_owned(), json!(text));
    result.insert("error".to_owned(), Value::Null);
    result.insert(
        "incomplete_details".to_owned(),
        if incomplete {
            json!({"reason": "max_output_tokens"})
        } else {
            Value::Null
        },
    );
    if let Some(usage) = responses_usage_from_chat(payload) {
        result.insert("usage".to_owned(), usage);
    }
    Value::Object(result)
}

fn sse_event_payload(block: &str) -> Option<(String, Value)> {
    let mut event = String::new();
    let mut data: Option<&str> = None;
    for line in block.lines() {
        if let Some(value) = line.strip_prefix("data:") {
            let value = value.trim_start();
            if !value.is_empty() && value != "[DONE]" {
                data = Some(value);
            }
        } else if let Some(value) = line.strip_prefix("event:") {
            event = value.trim().to_owned();
        }
    }
    let payload: Value = serde_json::from_str(data?).ok()?;
    if event.is_empty() {
        event = payload
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
    }
    Some((event, payload))
}

fn chat_chunk(
    id: &str,
    model: &str,
    created: u64,
    delta: Value,
    finish_reason: Option<&str>,
    usage: Option<&Value>,
) -> String {
    let mut choice = json!({"index": 0, "delta": delta, "finish_reason": Value::Null});
    if let Some(reason) = finish_reason {
        choice["finish_reason"] = json!(reason);
    }
    let mut chunk = json!({
        "id": id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [choice],
    });
    if let Some(usage) = usage {
        chunk["usage"] = usage.clone();
    }
    format!("data: {chunk}\n\n")
}

fn emit_response_event(out: &mut String, sequence: &mut u64, kind: &str, data: Value) {
    *sequence += 1;
    let mut payload = data;
    if let Some(object) = payload.as_object_mut() {
        object.insert("type".to_owned(), json!(kind));
        object.insert("sequence_number".to_owned(), json!(*sequence));
    }
    out.push_str(&format!("event: {kind}\ndata: {payload}\n\n"));
}

fn responses_stream_to_chat_stream(text: &str) -> String {
    let mut id = String::new();
    let mut model = String::new();
    let mut created = 0u64;
    let mut started = false;
    let mut has_tools = false;
    let mut finished = false;
    let mut out = String::new();

    for block in text.split("\n\n") {
        let Some((event, payload)) = sse_event_payload(block) else {
            continue;
        };
        if let Some(response) = payload.get("response") {
            if id.is_empty()
                && let Some(value) = response.get("id").and_then(Value::as_str)
            {
                id = value.to_owned();
            }
            if model.is_empty()
                && let Some(value) = response.get("model").and_then(Value::as_str)
            {
                model = value.to_owned();
            }
            if created == 0
                && let Some(value) = response.get("created_at").and_then(Value::as_u64)
            {
                created = value;
            }
        }
        if !started {
            started = true;
            if id.is_empty() {
                id = "chatcmpl-converted".to_owned();
            }
            if model.is_empty() {
                model = "unknown".to_owned();
            }
            if created == 0 {
                created = now_secs();
            }
            out.push_str(&chat_chunk(
                &id,
                &model,
                created,
                json!({"role": "assistant", "content": ""}),
                None,
                None,
            ));
        }
        match event.as_str() {
            "response.output_text.delta" => {
                if let Some(delta) = payload.get("delta").and_then(Value::as_str)
                    && !delta.is_empty()
                {
                    out.push_str(&chat_chunk(
                        &id,
                        &model,
                        created,
                        json!({"content": delta}),
                        None,
                        None,
                    ));
                }
            }
            "response.output_item.added" => {
                let item = payload.get("item");
                let kind = item
                    .and_then(|value| value.get("type"))
                    .and_then(Value::as_str);
                if kind == Some("function_call") {
                    has_tools = true;
                    let index = payload
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    out.push_str(&chat_chunk(
                        &id,
                        &model,
                        created,
                        json!({"tool_calls": [{
                            "index": index,
                            "id": item.and_then(|value| value.get("call_id")).and_then(Value::as_str).unwrap_or_default(),
                            "type": "function",
                            "function": {
                                "name": item.and_then(|value| value.get("name")).and_then(Value::as_str).unwrap_or_default(),
                                "arguments": "",
                            },
                        }]}),
                        None,
                        None,
                    ));
                }
            }
            "response.function_call_arguments.delta" => {
                let index = payload
                    .get("output_index")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if let Some(delta) = payload.get("delta").and_then(Value::as_str) {
                    has_tools = true;
                    out.push_str(&chat_chunk(
                        &id,
                        &model,
                        created,
                        json!({"tool_calls": [{
                            "index": index,
                            "function": {"arguments": delta},
                        }]}),
                        None,
                        None,
                    ));
                }
            }
            "response.completed" | "response.incomplete" | "response.failed" => {
                let mut finish = "stop";
                let mut usage: Option<Value> = None;
                if let Some(response) = payload.get("response") {
                    let incomplete = response.get("status").and_then(Value::as_str) == Some("incomplete")
                        && response
                            .get("incomplete_details")
                            .and_then(|details| details.get("reason"))
                            .and_then(Value::as_str)
                            == Some("max_output_tokens");
                    let calls = response
                        .get("output")
                        .and_then(Value::as_array)
                        .is_some_and(|items| {
                            items.iter().any(|item| {
                                item.get("type").and_then(Value::as_str) == Some("function_call")
                            })
                        });
                    if calls {
                        has_tools = true;
                    }
                    if incomplete {
                        finish = "length";
                    }
                    usage = response.get("usage").map(responses_usage_to_chat);
                }
                if has_tools {
                    finish = "tool_calls";
                }
                out.push_str(&chat_chunk(
                    &id,
                    &model,
                    created,
                    json!({}),
                    Some(finish),
                    usage.as_ref(),
                ));
                out.push_str("data: [DONE]\n\n");
                finished = true;
            }
            _ => {}
        }
    }

    if !finished {
        if !started {
            if id.is_empty() {
                id = "chatcmpl-converted".to_owned();
            }
            if model.is_empty() {
                model = "unknown".to_owned();
            }
            if created == 0 {
                created = now_secs();
            }
            out.push_str(&chat_chunk(
                &id,
                &model,
                created,
                json!({"role": "assistant", "content": ""}),
                None,
                None,
            ));
        }
        out.push_str(&chat_chunk(
            &id,
            &model,
            created,
            json!({}),
            Some("stop"),
            None,
        ));
        out.push_str("data: [DONE]\n\n");
    }
    out
}

fn chat_stream_to_responses_stream(text: &str) -> String {
    let mut sequence: u64 = 0;
    let mut out = String::new();
    let mut id = String::new();
    let mut model = String::new();
    let mut created = 0u64;
    let mut started = false;
    let mut message_id = String::new();
    let mut message_index = 0usize;
    let mut output_counter = 0usize;
    let mut full_text = String::new();
    let mut tools: Vec<(usize, String, usize, String)> = Vec::new();
    let mut usage: Option<Value> = None;
    let mut final_finish = String::new();

    for block in text.split("\n\n") {
        let Some((_event, payload)) = sse_event_payload(block) else {
            continue;
        };
        if id.is_empty()
            && let Some(value) = payload.get("id").and_then(Value::as_str)
        {
            id = value.to_owned();
        }
        if model.is_empty()
            && let Some(value) = payload.get("model").and_then(Value::as_str)
        {
            model = value.to_owned();
        }
        if created == 0
            && let Some(value) = payload.get("created").and_then(Value::as_u64)
        {
            created = value;
        }
        if let Some(value) = payload.get("usage")
            && value.as_object().is_some_and(|object| !object.is_empty())
        {
            usage = Some(value.clone());
        }
        let choice = payload
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first());
        let delta = choice.and_then(|choice| choice.get("delta"));
        if final_finish.is_empty()
            && let Some(reason) = choice
                .and_then(|choice| choice.get("finish_reason"))
                .and_then(Value::as_str)
        {
            final_finish = reason.to_owned();
        }
        if !started && (payload.get("id").is_some() || payload.get("choices").is_some()) {
            started = true;
            if id.is_empty() {
                id = "chatcmpl-converted".to_owned();
            }
            if model.is_empty() {
                model = "unknown".to_owned();
            }
            if created == 0 {
                created = now_secs();
            }
            let header = json!({
                "id": &id,
                "object": "response",
                "created_at": created,
                "model": &model,
                "status": "in_progress",
                "output": [],
                "error": Value::Null,
                "incomplete_details": Value::Null,
                "usage": Value::Null,
            });
            emit_response_event(
                &mut out,
                &mut sequence,
                "response.created",
                json!({"response": header.clone()}),
            );
            emit_response_event(
                &mut out,
                &mut sequence,
                "response.in_progress",
                json!({"response": header}),
            );
            message_id = format!("msg_{id}");
            message_index = output_counter;
            output_counter += 1;
            emit_response_event(
                &mut out,
                &mut sequence,
                "response.output_item.added",
                json!({
                    "output_index": message_index,
                    "item": {
                        "type": "message",
                        "id": &message_id,
                        "role": "assistant",
                        "status": "in_progress",
                        "content": [],
                    },
                }),
            );
            emit_response_event(
                &mut out,
                &mut sequence,
                "response.content_part.added",
                json!({
                    "item_id": &message_id,
                    "output_index": message_index,
                    "content_index": 0,
                    "part": {"type": "output_text", "text": "", "annotations": []},
                }),
            );
        }
        if let Some(content) = delta
            .and_then(|value| value.get("content"))
            .and_then(Value::as_str)
            && !content.is_empty()
        {
            full_text.push_str(content);
            emit_response_event(
                &mut out,
                &mut sequence,
                "response.output_text.delta",
                json!({
                    "item_id": message_id,
                    "output_index": message_index,
                    "content_index": 0,
                    "delta": content,
                }),
            );
        }
        if let Some(calls) = delta
            .and_then(|value| value.get("tool_calls"))
            .and_then(Value::as_array)
        {
            for call in calls {
                let chat_index = call.get("index").and_then(Value::as_u64).unwrap_or(0) as usize;
                let function = call.get("function");
                if !tools.iter().any(|(index, _, _, _)| *index == chat_index) {
                    let item_id = format!("fc_{chat_index}");
                    let output_index = output_counter;
                    output_counter += 1;
                    let call_id = call
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    let name = function
                        .and_then(|value| value.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    emit_response_event(
                        &mut out,
                        &mut sequence,
                        "response.output_item.added",
                        json!({
                            "output_index": output_index,
                            "item": {
                                "type": "function_call",
                                "id": &item_id,
                                "call_id": call_id,
                                "name": name,
                                "arguments": "",
                                "status": "in_progress",
                            },
                        }),
                    );
                    tools.push((chat_index, item_id, output_index, String::new()));
                }
                if let Some(entry) = tools.iter_mut().find(|(index, _, _, _)| *index == chat_index)
                    && let Some(arguments) = function
                        .and_then(|value| value.get("arguments"))
                        .and_then(Value::as_str)
                    && !arguments.is_empty()
                {
                    entry.3.push_str(arguments);
                    let item_id = entry.1.clone();
                    let output_index = entry.2;
                    emit_response_event(
                        &mut out,
                        &mut sequence,
                        "response.function_call_arguments.delta",
                        json!({
                            "item_id": item_id,
                            "output_index": output_index,
                            "delta": arguments,
                        }),
                    );
                }
            }
        }
    }

    if !started {
        if id.is_empty() {
            id = "chatcmpl-converted".to_owned();
        }
        if model.is_empty() {
            model = "unknown".to_owned();
        }
        if created == 0 {
            created = now_secs();
        }
        let header = json!({
            "id": id,
            "object": "response",
            "created_at": created,
            "model": model,
            "status": "completed",
            "output": [],
            "error": Value::Null,
            "incomplete_details": Value::Null,
            "usage": Value::Null,
        });
        emit_response_event(
            &mut out,
            &mut sequence,
            "response.created",
            json!({"response": header.clone()}),
        );
        emit_response_event(
            &mut out,
            &mut sequence,
            "response.completed",
            json!({"response": header}),
        );
        return out;
    }

    let mut output: Vec<Value> = Vec::new();
    emit_response_event(
        &mut out,
        &mut sequence,
        "response.output_text.done",
        json!({
            "item_id": &message_id,
            "output_index": message_index,
            "content_index": 0,
            "text": &full_text,
        }),
    );
    emit_response_event(
        &mut out,
        &mut sequence,
        "response.content_part.done",
        json!({
            "item_id": &message_id,
            "output_index": message_index,
            "content_index": 0,
            "part": {"type": "output_text", "text": &full_text, "annotations": []},
        }),
    );
    output.push(json!({
        "type": "message",
        "id": &message_id,
        "role": "assistant",
        "status": "completed",
        "content": [{"type": "output_text", "text": &full_text, "annotations": []}],
    }));
    emit_response_event(
        &mut out,
        &mut sequence,
        "response.output_item.done",
        json!({
            "output_index": message_index,
            "item": output.first().cloned().unwrap_or(Value::Null),
        }),
    );
    for (_, item_id, output_index, arguments) in &tools {
        emit_response_event(
            &mut out,
            &mut sequence,
            "response.function_call_arguments.done",
            json!({
                "item_id": item_id,
                "output_index": output_index,
                "arguments": arguments,
            }),
        );
        let item = json!({
            "type": "function_call",
            "id": item_id,
            "call_id": "",
            "name": "",
            "arguments": arguments,
            "status": "completed",
        });
        output.push(item.clone());
        emit_response_event(
            &mut out,
            &mut sequence,
            "response.output_item.done",
            json!({
                "output_index": output_index,
                "item": item,
            }),
        );
    }

    let incomplete = final_finish == "length";
    let mut response = json!({
        "id": id,
        "object": "response",
        "created_at": created,
        "model": model,
        "status": if incomplete { "incomplete" } else { "completed" },
        "output": output,
        "output_text": full_text,
        "error": Value::Null,
        "incomplete_details": if incomplete {
            json!({"reason": "max_output_tokens"})
        } else {
            Value::Null
        },
        "usage": usage.map(chat_usage_to_responses),
    });
    if let Some(object) = response.as_object_mut()
        && object.get("usage").is_none_or(|value| value.is_null())
    {
        object.remove("usage");
    }
    emit_response_event(
        &mut out,
        &mut sequence,
        "response.completed",
        json!({"response": response}),
    );
    out
}

fn is_health_path(path: &str) -> bool {
    matches!(
        path,
        "/health" | "/healthz" | "/ready" | "/api/health" | "/api/status"
    )
}

fn normalize_path(path: &str) -> &str {
    let normalized = path.trim_end_matches('/');
    if normalized.is_empty() {
        "/"
    } else {
        normalized
    }
}

fn is_models_path(path: &str) -> bool {
    const MODELS_PATHS: &[&str] = &[
        "/v1/models",
        "/models",
        "/api/paas/v4/models",
        "/api/v3/models",
        "/api/v1/models",
        "/v1beta/models",
    ];
    MODELS_PATHS.contains(&path)
}

fn is_claude_models_path(path: &str) -> bool {
    matches!(path, "/claude/v1/models" | "/anthropic/v1/models")
}

fn is_chat_path(path: &str) -> bool {
    const CHAT_PATHS: &[&str] = &[
        "/v1/chat/completions",
        "/chat/completions",
        "/api/paas/v4/chat/completions",
        "/api/v3/chat/completions",
        "/api/v1/chat/completions",
        "/v1beta/chat/completions",
    ];
    CHAT_PATHS.contains(&path)
}

fn is_responses_path(path: &str) -> bool {
    const RESPONSES_PATHS: &[&str] = &[
        "/v1/responses",
        "/responses",
        "/api/paas/v4/responses",
        "/api/v3/responses",
        "/api/v1/responses",
        "/v1beta/responses",
    ];
    RESPONSES_PATHS.contains(&path)
}

fn is_config_path(path: &str) -> bool {
    path == "/api/config"
}

fn is_connectivity_path(path: &str) -> bool {
    path == "/api/connectivity"
}

fn route_upstream_path(path: &str) -> String {
    if path == "/" {
        return "/".to_owned();
    }
    const PREFIXES: &[&str] = &[
        "/api/paas/v4",
        "/api/v1beta",
        "/api/v3",
        "/api/v1",
        "/claude/v1",
        "/anthropic/v1",
        "/openai/deployments/",
        "/v1beta",
        "/api",
        "/v1",
    ];
    let mut rest = path;
    for prefix in PREFIXES {
        if let Some(stripped) = rest.strip_prefix(prefix) {
            rest = stripped;
            break;
        }
    }
    if rest.is_empty() {
        "/".to_owned()
    } else {
        rest.to_owned()
    }
}

fn env_or(name: &str, fallback: &str) -> String {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_owned())
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.to_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}

fn env_seconds(name: &str, fallback: u64) -> Duration {
    Duration::from_secs(
        env::var(name)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(fallback),
    )
}

fn config_path() -> PathBuf {
    env::var("CONFIG_PATH")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH))
}

fn normalize_protocol(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "auto" | "" => Some(PROTOCOL_AUTO),
        "chat" | "openai" | "completions" => Some(PROTOCOL_CHAT),
        "responses" | "response" => Some(PROTOCOL_RESPONSES),
        _ => None,
    }
}

fn admin_allowed(auth_required: bool, authorized: bool, remote: Option<IpAddr>) -> bool {
    if auth_required {
        authorized
    } else {
        remote.is_some_and(|ip| ip.is_loopback())
    }
}

fn parse_nodes(value: &Value) -> Result<Vec<String>, String> {
    let entries = value
        .as_array()
        .ok_or_else(|| "nodes must be an array of strings".to_owned())?;
    if entries.len() > MAX_CONFIG_NODES {
        return Err(format!(
            "nodes must contain at most {MAX_CONFIG_NODES} entries"
        ));
    }
    let mut nodes: Vec<String> = Vec::new();
    for entry in entries {
        let entry = entry
            .as_str()
            .ok_or_else(|| "nodes must be an array of strings".to_owned())?;
        let node = entry.trim().trim_end_matches('/');
        if node.is_empty() {
            continue;
        }
        if !node.starts_with("http://") && !node.starts_with("https://") {
            return Err(format!("unsupported node url: {node}"));
        }
        if !nodes.iter().any(|existing| existing == node) {
            nodes.push(node.to_owned());
        }
    }
    if nodes.is_empty() {
        return Err("nodes must contain at least one entry".to_owned());
    }
    Ok(nodes)
}

fn token_patch(
    object: &Map<String, Value>,
    key: &str,
    clear_key: &str,
) -> Result<Option<Option<String>>, String> {
    let provided = match object.get(key) {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.is_empty() {
                None
            } else {
                Some(value.to_owned())
            }
        }
        Some(_) => return Err(format!("{key} must be a string")),
    };
    let clear = object
        .get(clear_key)
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if clear && provided.is_some() {
        return Err(format!("{key} and {clear_key} cannot be used together"));
    }
    if clear {
        return Ok(Some(None));
    }
    Ok(provided.map(Some))
}

fn load_stored(path: &Path) -> Option<StoredConfig> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(error) => {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!("读取配置文件 {} 失败: {error}", path.display());
            }
            return None;
        }
    };
    let value: Value = match serde_json::from_slice(&data) {
        Ok(value) => value,
        Err(error) => {
            eprintln!(
                "配置文件 {} 不是合法 JSON: {error}，已回退环境变量",
                path.display()
            );
            return None;
        }
    };
    let object = match value.as_object() {
        Some(object) => object,
        None => {
            eprintln!("配置文件 {} 顶层必须是对象，已回退环境变量", path.display());
            return None;
        }
    };
    Some(StoredConfig {
        nodes: object.get("nodes").cloned(),
        api_token: stored_token(object, "api_token"),
        auth_token: stored_token(object, "auth_token"),
        strip_free: object.get("strip_free").and_then(Value::as_bool),
        protocol: object
            .get("protocol")
            .and_then(Value::as_str)
            .and_then(normalize_protocol)
            .map(str::to_owned),
    })
}

fn stored_token(object: &Map<String, Value>, key: &str) -> Option<Option<String>> {
    match object.get(key) {
        None => None,
        Some(Value::Null) => Some(None),
        Some(Value::String(value)) => Some(Some(value.clone())),
        Some(_) => {
            eprintln!("配置文件 {key} 必须是字符串或 null，已忽略");
            None
        }
    }
}

impl StoredConfig {
    fn apply(&self, config: &mut RuntimeConfig) {
        if let Some(nodes) = &self.nodes {
            match parse_nodes(nodes) {
                Ok(nodes) => config.nodes = nodes,
                Err(message) => eprintln!("配置文件 nodes 无效，已忽略: {message}"),
            }
        }
        if let Some(value) = &self.api_token {
            config.api_token = value
                .as_deref()
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .map(str::to_owned);
        }
        if let Some(value) = &self.auth_token {
            config.auth_token = value
                .as_deref()
                .map(str::trim)
                .filter(|token| !token.is_empty())
                .map(str::to_owned);
        }
        if let Some(strip_free) = self.strip_free {
            config.strip_free = strip_free;
        }
        if let Some(protocol) = &self.protocol {
            config.protocol = protocol.clone();
        }
    }
}

fn persist_config(path: &Path, config: &RuntimeConfig) -> std::io::Result<()> {
    let payload = json!({
        "nodes": config.nodes,
        "api_token": config.api_token,
        "auth_token": config.auth_token,
        "strip_free": config.strip_free,
        "protocol": config.protocol,
    });
    let mut data = serde_json::to_vec_pretty(&payload).map_err(std::io::Error::other)?;
    data.push(b'\n');
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let mut temp = path.as_os_str().to_os_string();
    temp.push(".tmp");
    let temp = PathBuf::from(temp);
    fs::write(&temp, &data)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(&temp, fs::Permissions::from_mode(0o600));
    }
    if let Err(error) = fs::rename(&temp, path) {
        let _ = fs::remove_file(&temp);
        return Err(error);
    }
    Ok(())
}

fn split_nodes(value: &str) -> Vec<String> {
    let mut nodes = Vec::new();
    for node in value
        .split(',')
        .map(|node| node.trim().trim_end_matches('/'))
        .filter(|node| !node.is_empty())
    {
        if !nodes.iter().any(|existing| existing == node) {
            nodes.push(node.to_owned());
        }
    }
    nodes
}

fn strip_free_suffix(id: &str) -> String {
    const SUFFIX: &str = "-free";
    let lower = id.to_lowercase();
    if lower.len() > SUFFIX.len() && lower.ends_with(SUFFIX) {
        id[..id.len() - SUFFIX.len()].to_owned()
    } else {
        id.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn splits_and_deduplicates_nodes() {
        assert_eq!(
            split_nodes(" https://one.example/ , ,https://two.example/, https://one.example "),
            ["https://one.example", "https://two.example"]
        );
    }

    #[test]
    fn upstream_requests_use_opencode_user_agent() {
        let server = Server::http("127.0.0.1:0").unwrap();
        let address = server.server_addr().to_ip().unwrap();
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            let request = server.recv().unwrap();
            tx.send(request_header(&request, "User-Agent")).unwrap();
            request.respond(Response::empty(200)).unwrap();
        });
        let url = format!("http://{address}/models");
        add_token(
            client(Duration::from_secs(5), Duration::from_secs(5)).get(&url),
            &None,
        )
        .call()
        .unwrap();
        handle.join().unwrap();
        assert_eq!(rx.recv().unwrap().as_deref(), Some("opencode/1.0"));
    }

    #[test]
    fn detects_stream_request() {
        assert!(is_stream_request(&json!({"stream": true})));
        assert!(!is_stream_request(&json!({"stream": false})));
        assert!(!is_stream_request(&json!({"messages": []})));
    }

    #[test]
    fn routes_upstream_paths_correctly() {
        assert_eq!(normalize_path("/"), "/");
        assert_eq!(normalize_path("///"), "/");
        assert_eq!(normalize_path("/v1/models/"), "/v1/models");
        assert_eq!(
            normalize_path("/v1/chat/completions///"),
            "/v1/chat/completions"
        );
        assert_eq!(route_upstream_path("/v1/models"), "/models");
        assert_eq!(route_upstream_path("/models"), "/models");
        assert_eq!(route_upstream_path("/api/paas/v4/models"), "/models");
        assert_eq!(
            route_upstream_path("/api/v3/chat/completions"),
            "/chat/completions"
        );
        assert_eq!(route_upstream_path("/api/v1/models"), "/models");
        assert_eq!(route_upstream_path("/claude/v1/models"), "/models");
        assert_eq!(route_upstream_path("/anthropic/v1/models"), "/models");
        assert_eq!(route_upstream_path("/v1beta/models"), "/models");
        assert_eq!(
            route_upstream_path("/api/v1beta/chat/completions"),
            "/chat/completions"
        );
        assert_eq!(
            route_upstream_path("/v1/chat/completions"),
            "/chat/completions"
        );
        assert_eq!(
            route_upstream_path("/openai/v1/models"),
            "/openai/v1/models"
        );
        assert_eq!(route_upstream_path("/v2/models"), "/v2/models");
        assert_eq!(route_upstream_path("/"), "/");
    }

    #[test]
    fn admin_allowed_requires_token_or_loopback() {
        let loopback = Some(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST));
        let lan = Some(IpAddr::V4(std::net::Ipv4Addr::new(192, 168, 1, 20)));
        assert!(admin_allowed(true, true, None));
        assert!(!admin_allowed(true, false, None));
        assert!(admin_allowed(true, true, lan));
        assert!(admin_allowed(false, false, loopback));
        assert!(!admin_allowed(false, false, lan));
        assert!(!admin_allowed(false, false, None));
    }

    #[test]
    fn validates_and_normalizes_config_nodes() {
        let nodes = parse_nodes(&json!([
            " https://one.example/ ",
            "https://two.example",
            "https://one.example",
            "",
            "   "
        ]))
        .expect("valid nodes");
        assert_eq!(nodes, ["https://one.example", "https://two.example"]);
        assert!(parse_nodes(&json!("https://one.example")).is_err());
        assert!(parse_nodes(&json!(["ftp://one.example"])).is_err());
        assert!(parse_nodes(&json!([])).is_err());
        assert!(parse_nodes(&json!(["", "   "])).is_err());
        assert!(parse_nodes(&json!([42])).is_err());
    }

    #[test]
    fn token_patch_handles_set_clear_and_conflicts() {
        let object: Map<String, Value> =
            serde_json::from_str(r#"{"api_token": "  secret  "}"#).unwrap();
        assert_eq!(
            token_patch(&object, "api_token", "clear_api_token").unwrap(),
            Some(Some("secret".to_owned()))
        );

        let object: Map<String, Value> =
            serde_json::from_str(r#"{"clear_api_token": true}"#).unwrap();
        assert_eq!(
            token_patch(&object, "api_token", "clear_api_token").unwrap(),
            Some(None)
        );

        let object: Map<String, Value> = serde_json::from_str("{}").unwrap();
        assert_eq!(
            token_patch(&object, "api_token", "clear_api_token").unwrap(),
            None
        );

        let object: Map<String, Value> = serde_json::from_str(r#"{"api_token": ""}"#).unwrap();
        assert_eq!(
            token_patch(&object, "api_token", "clear_api_token").unwrap(),
            None
        );

        let object: Map<String, Value> =
            serde_json::from_str(r#"{"api_token": "x", "clear_api_token": true}"#).unwrap();
        assert!(token_patch(&object, "api_token", "clear_api_token").is_err());

        let object: Map<String, Value> = serde_json::from_str(r#"{"api_token": 42}"#).unwrap();
        assert!(token_patch(&object, "api_token", "clear_api_token").is_err());
    }

    #[test]
    fn responses_paths_are_recognized() {
        for path in [
            "/v1/responses",
            "/responses",
            "/api/v1/responses",
            "/api/v3/responses",
            "/api/paas/v4/responses",
            "/v1beta/responses",
        ] {
            assert!(is_responses_path(path), "{path} should be a responses path");
        }
        assert!(!is_responses_path("/v1/chat/completions"));
        assert!(!is_responses_path("/api/config"));
        assert!(is_config_path("/api/config"));
        assert!(!is_config_path("/api/configs"));
    }

    #[test]
    fn protocol_values_are_normalized() {
        assert_eq!(normalize_protocol("auto"), Some(PROTOCOL_AUTO));
        assert_eq!(normalize_protocol(" responses "), Some(PROTOCOL_RESPONSES));
        assert_eq!(normalize_protocol("openai"), Some(PROTOCOL_CHAT));
        assert_eq!(normalize_protocol("COMPLETIONS"), Some(PROTOCOL_CHAT));
        assert_eq!(normalize_protocol(""), Some(PROTOCOL_AUTO));
        assert_eq!(normalize_protocol("grpc"), None);
    }

    #[test]
    fn chat_request_converts_to_responses() {
        let chat = json!({
            "model": "gpt-4o",
            "messages": [
                {"role": "system", "content": "be nice"},
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"},
                {"role": "assistant", "content": null, "tool_calls": [
                    {"id": "call_1", "type": "function",
                     "function": {"name": "lookup", "arguments": "{\"q\":\"x\"}"}}
                ]},
                {"role": "tool", "tool_call_id": "call_1", "content": "found"}
            ],
            "max_tokens": 128,
            "temperature": 0.4,
            "stream": true,
            "reasoning_effort": "high",
            "tools": [{
                "type": "function",
                "function": {"name": "lookup", "description": "d", "parameters": {"type": "object"}}
            }],
            "tool_choice": "auto"
        });
        let converted = chat_to_responses_request(&chat);
        assert_eq!(converted["model"], json!("gpt-4o"));
        assert_eq!(converted["instructions"], json!("be nice"));
        assert_eq!(converted["max_output_tokens"], json!(128));
        assert_eq!(converted["temperature"], json!(0.4));
        assert_eq!(converted["stream"], json!(true));
        assert_eq!(converted["reasoning"], json!({"effort": "high"}));
        assert_eq!(converted["tool_choice"], json!("auto"));
        assert_eq!(converted["tools"][0]["name"], json!("lookup"));
        assert!(converted["tools"][0].get("function").is_none());
        let input = converted["input"].as_array().expect("input array");
        assert_eq!(input.len(), 4, "system moves to instructions, null content is dropped");
        assert_eq!(input[0]["type"], json!("message"));
        assert_eq!(input[0]["role"], json!("user"));
        assert_eq!(input[0]["content"][0]["type"], json!("input_text"));
        assert_eq!(input[1]["role"], json!("assistant"));
        assert_eq!(input[1]["content"][0]["type"], json!("output_text"));
        assert_eq!(input[2]["type"], json!("function_call"));
        assert_eq!(input[2]["call_id"], json!("call_1"));
        assert_eq!(input[3]["type"], json!("function_call_output"));
        assert_eq!(input[3]["call_id"], json!("call_1"));
        assert_eq!(input[3]["output"], json!("found"));
    }

    #[test]
    fn responses_request_converts_to_chat() {
        let responses = json!({
            "model": "gpt-4o",
            "instructions": "be nice",
            "input": [
                {"type": "message", "role": "user",
                 "content": [{"type": "input_text", "text": "hi"}]},
                {"type": "function_call", "call_id": "call_1", "name": "lookup", "arguments": "{}"},
                {"type": "function_call_output", "call_id": "call_1", "output": "found"}
            ],
            "max_output_tokens": 64,
            "reasoning": {"effort": "medium"},
            "stream": false,
            "tools": [{"type": "function", "name": "lookup", "parameters": {"type": "object"}}]
        });
        let chat = responses_request_to_chat(&responses);
        assert_eq!(chat["model"], json!("gpt-4o"));
        assert_eq!(chat["max_tokens"], json!(64));
        assert_eq!(chat["stream"], json!(false));
        assert_eq!(chat["reasoning_effort"], json!("medium"));
        assert_eq!(chat["tools"][0]["function"]["name"], json!("lookup"));
        let messages = chat["messages"].as_array().expect("messages array");
        assert_eq!(messages.len(), 4, "instructions become the system message");
        assert_eq!(messages[0]["role"], json!("system"));
        assert_eq!(messages[0]["content"], json!("be nice"));
        assert_eq!(messages[1]["role"], json!("user"));
        assert_eq!(messages[1]["content"], json!("hi"), "single text part collapses to a string");
        assert_eq!(messages[2]["role"], json!("assistant"));
        assert_eq!(messages[2]["tool_calls"][0]["id"], json!("call_1"));
        assert_eq!(
            messages[2]["tool_calls"][0]["function"]["name"],
            json!("lookup")
        );
        assert_eq!(messages[3]["role"], json!("tool"));
        assert_eq!(messages[3]["tool_call_id"], json!("call_1"));
        assert_eq!(messages[3]["content"], json!("found"));
    }

    #[test]
    fn responses_response_converts_to_chat() {
        let responses = json!({
            "id": "resp_1",
            "object": "response",
            "created_at": 1700000000,
            "model": "gpt-4o",
            "status": "completed",
            "output": [
                {"type": "message", "content": [{"type": "output_text", "text": "answer"}]},
                {"type": "function_call", "call_id": "call_9", "name": "lookup", "arguments": "{}"}
            ],
            "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}
        });
        let chat = responses_to_chat_response(&responses);
        assert_eq!(chat["id"], json!("resp_1"));
        assert_eq!(chat["object"], json!("chat.completion"));
        assert_eq!(chat["created"], json!(1700000000));
        assert_eq!(chat["model"], json!("gpt-4o"));
        assert_eq!(chat["choices"][0]["message"]["content"], json!("answer"));
        assert_eq!(chat["choices"][0]["finish_reason"], json!("tool_calls"));
        assert_eq!(
            chat["choices"][0]["message"]["tool_calls"][0]["id"],
            json!("call_9")
        );
        assert_eq!(
            chat["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
            json!("lookup")
        );
        assert_eq!(chat["usage"]["prompt_tokens"], json!(10));
        assert_eq!(chat["usage"]["completion_tokens"], json!(5));
        assert_eq!(chat["usage"]["total_tokens"], json!(15));
    }

    #[test]
    fn chat_response_converts_to_responses() {
        let chat = json!({
            "id": "chatcmpl-1",
            "model": "gpt-4o",
            "created": 1700000001,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "partial", "reasoning_content": "thinking"},
                "finish_reason": "length"
            }],
            "usage": {"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10}
        });
        let responses = chat_to_responses_response(&chat);
        assert_eq!(responses["id"], json!("chatcmpl-1"));
        assert_eq!(responses["object"], json!("response"));
        assert_eq!(responses["created_at"], json!(1700000001));
        assert_eq!(responses["status"], json!("incomplete"));
        assert_eq!(
            responses["incomplete_details"]["reason"],
            json!("max_output_tokens")
        );
        assert_eq!(responses["output_text"], json!("partial"));
        assert_eq!(responses["output"][0]["type"], json!("reasoning"));
        assert_eq!(responses["output"][1]["type"], json!("message"));
        assert_eq!(responses["output"][1]["content"][0]["type"], json!("output_text"));
        assert_eq!(responses["usage"]["input_tokens"], json!(7));
        assert_eq!(responses["usage"]["output_tokens"], json!(3));
        assert_eq!(responses["usage"]["total_tokens"], json!(10));
    }

    #[test]
    fn responses_stream_converts_to_chat_stream() {
        let upstream = [
            "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_9\",\"model\":\"gpt-4o\",\"created_at\":1700000002,\"status\":\"in_progress\"}}",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"Hel\"}",
            "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"lo\"}",
            "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_9\",\"model\":\"gpt-4o\",\"created_at\":1700000002,\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}",
        ]
        .join("\n\n");
        let converted = responses_stream_to_chat_stream(&upstream);
        assert!(converted.contains("\"role\":\"assistant\""), "{converted}");
        assert!(converted.contains("\"content\":\"Hel\""), "{converted}");
        assert!(converted.contains("\"content\":\"lo\""), "{converted}");
        assert!(converted.contains("\"model\":\"gpt-4o\""), "{converted}");
        assert!(converted.contains("\"created\":1700000002"), "{converted}");
        assert!(converted.contains("\"finish_reason\":\"stop\""), "{converted}");
        assert!(converted.contains("\"prompt_tokens\":3"), "{converted}");
        assert!(converted.contains("\"completion_tokens\":2"), "{converted}");
        assert!(
            converted.trim_end().ends_with("data: [DONE]"),
            "{converted}"
        );
    }

    #[test]
    fn chat_stream_converts_to_responses_stream() {
        let chunks = [
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000003,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"\"},\"finish_reason\":null}]}",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000003,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}",
            "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1700000003,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1,\"total_tokens\":5}}",
            "data: [DONE]",
        ]
        .join("\n\n");
        let converted = chat_stream_to_responses_stream(&chunks);
        assert!(converted.contains("event: response.created"), "{converted}");
        assert!(
            converted.contains("event: response.output_item.added"),
            "{converted}"
        );
        assert!(
            converted.contains("event: response.content_part.added"),
            "{converted}"
        );
        assert!(
            converted.contains("\"type\":\"response.output_text.delta\""),
            "{converted}"
        );
        assert!(converted.contains("\"delta\":\"Hi\""), "{converted}");
        assert!(converted.contains("\"text\":\"Hi\""), "{converted}");
        assert!(
            converted.contains("event: response.completed"),
            "{converted}"
        );
        assert!(converted.contains("\"input_tokens\":4"), "{converted}");
        assert!(
            !converted.contains("data: [DONE]"),
            "responses streams end with response.completed: {converted}"
        );
    }

    fn temp_config_path(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("opencode-free-api-tests-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("create temp dir");
        dir.join(format!("{name}-{stamp}.json"))
    }

    fn test_agent() -> ureq::Agent {
        ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(15)))
            .http_status_as_error(false)
            .build()
            .new_agent()
    }

    fn read_json(response: http::Response<ureq::Body>) -> Value {
        serde_json::from_slice(&read_body(response.into_body())).expect("response is JSON")
    }

    #[test]
    fn persists_and_loads_config_file() {
        let path = temp_config_path("roundtrip");
        let config = RuntimeConfig {
            nodes: vec!["https://one.example".to_owned()],
            api_token: Some("api".to_owned()),
            auth_token: None,
            strip_free: true,
            protocol: PROTOCOL_RESPONSES.to_owned(),
        };
        persist_config(&path, &config).expect("persist config");
        let stored = load_stored(&path).expect("load config");
        let mut restored = RuntimeConfig {
            nodes: vec![DEFAULT_NODE.to_owned()],
            api_token: None,
            auth_token: Some("env".to_owned()),
            strip_free: false,
            protocol: PROTOCOL_AUTO.to_owned(),
        };
        stored.apply(&mut restored);
        assert_eq!(restored.nodes, ["https://one.example"]);
        assert_eq!(restored.api_token.as_deref(), Some("api"));
        assert_eq!(
            restored.auth_token, None,
            "null in the file clears the env value"
        );
        assert!(restored.strip_free);
        assert_eq!(restored.protocol, PROTOCOL_RESPONSES);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}.tmp", path.display()));
    }

    #[test]
    fn partial_config_file_keeps_env_values() {
        let path = temp_config_path("partial");
        fs::write(&path, br#"{"nodes": ["https://three.example"]}"#).expect("write partial file");
        let stored = load_stored(&path).expect("load partial config");
        let mut restored = RuntimeConfig {
            nodes: vec![DEFAULT_NODE.to_owned()],
            api_token: Some("env-api".to_owned()),
            auth_token: Some("env-auth".to_owned()),
            strip_free: false,
            protocol: PROTOCOL_CHAT.to_owned(),
        };
        stored.apply(&mut restored);
        assert_eq!(restored.nodes, ["https://three.example"]);
        assert_eq!(
            restored.api_token.as_deref(),
            Some("env-api"),
            "absent keys keep env values"
        );
        assert_eq!(restored.auth_token.as_deref(), Some("env-auth"));
        assert!(!restored.strip_free);
        assert_eq!(
            restored.protocol, PROTOCOL_CHAT,
            "absent protocol keeps env value"
        );
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn corrupt_config_file_falls_back_to_env() {
        let path = temp_config_path("corrupt");
        fs::write(&path, b"{ not json").expect("write corrupt file");
        assert!(load_stored(&path).is_none());
        let _ = fs::remove_file(&path);
    }

    fn app_for_test() -> App {
        App {
            config: Arc::new(Mutex::new(RuntimeConfig {
                nodes: vec![DEFAULT_NODE.to_owned()],
                api_token: None,
                auth_token: None,
                strip_free: false,
                protocol: PROTOCOL_AUTO.to_owned(),
            })),
            config_path: PathBuf::from(DEFAULT_CONFIG_PATH),
            free_map: Arc::new(Mutex::new(FreeMap::new())),
            client: client(Duration::from_secs(15), Duration::from_secs(15)),
            stream_client: client(Duration::from_secs(15), Duration::from_secs(15)),
            requests: Arc::new(AtomicU64::new(0)),
            port: "8788".to_owned(),
            local_ip: "127.0.0.1".to_owned(),
            public_ip: None,
            started: 0,
        }
    }

    fn app_with_strip_free() -> App {
        let app = app_for_test();
        app.config.lock().expect("config lock").strip_free = true;
        app
    }

    fn app_with_nodes(node: &str) -> App {
        let app = app_for_test();
        app.config.lock().expect("config lock").nodes = vec![node.to_owned()];
        app
    }

    fn app_with_protocol(protocol: &str) -> App {
        let app = app_for_test();
        app.config.lock().expect("config lock").protocol = protocol.to_owned();
        app
    }

    fn live_models(app: &App) -> Vec<Value> {
        let response = app
            .upstream("/models", Method::Get, Vec::new(), None, false)
            .expect("official zen /v1/models reachable");
        let payload: Value =
            serde_json::from_slice(&response.into_body()).expect("parse /v1/models response");
        app.process_models(payload)
    }

    #[test]
    fn fetches_free_models_from_official_zen() {
        let app = app_for_test();
        let models = live_models(&app);
        assert!(
            !models.is_empty(),
            "expected free models from the official list"
        );
        assert!(models.iter().all(|model| {
            model
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| app.is_free_model(id))
        }));
        assert!(models.iter().any(|model| {
            model
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|id| id.to_lowercase().contains("free"))
        }));
    }

    #[test]
    fn strips_free_suffix_on_live_list_and_maps_back() {
        let app = app_with_strip_free();
        let models = live_models(&app);
        assert!(
            !models.is_empty(),
            "expected free models from the official list"
        );
        for model in &models {
            let id = model["id"].as_str().expect("model id");
            assert!(
                !id.to_lowercase().ends_with("-free"),
                "{id} still carries the -free suffix"
            );
        }
        let map = app.free_map.lock().unwrap();
        assert!(!map.entries.is_empty());
        for (stripped, original) in map.entries.iter() {
            assert_eq!(stripped, &strip_free_suffix(original));
        }
    }

    #[test]
    fn refresh_free_map_populates_empty_map() {
        let app = app_with_strip_free();
        let map = app.free_map.lock().unwrap();
        assert!(map.entries.is_empty(), "fresh app should have empty map");
        drop(map);
        app.refresh_free_map();
        let map = app.free_map.lock().unwrap();
        assert!(
            !map.entries.is_empty(),
            "refresh_free_map should populate the map"
        );
        for (stripped, original) in map.entries.iter() {
            assert_eq!(stripped, &strip_free_suffix(original));
        }
    }

    #[test]
    fn remap_chat_body_primes_map_when_strip_free() {
        let app = app_with_strip_free();
        assert!(
            app.free_map.lock().unwrap().entries.is_empty(),
            "fresh app should have empty map"
        );
        app.refresh_free_map();
        let mapping = app.free_map.lock().unwrap();
        let (stripped, original) = mapping
            .entries
            .iter()
            .find(|(key, value)| key != value)
            .expect("strip_free should produce at least one mapped id");
        let stripped = stripped.clone();
        let original = original.clone();
        drop(mapping);
        let body = app.remap_chat_body(json!({
            "model": stripped,
            "messages": [],
            "stream": false
        }));
        let parsed: Value = serde_json::from_slice(&body).expect("valid json out");
        assert_eq!(
            parsed["model"], original,
            "chat body should have been remapped to the real -free id after priming, got {parsed}"
        );
    }

    #[test]
    fn thinking_mode_detects_thinking_and_effort() {
        assert!(thinking_mode(&json!({"thinking": {"enabled": true}})));
        assert!(!thinking_mode(&json!({"thinking": {"enabled": false}})));
        assert!(thinking_mode(&json!({"reasoning_effort": "high"})));
        assert!(thinking_mode(&json!({"reasoning": {"effort": "high"}})));
        assert!(!thinking_mode(&json!({"reasoning": {"effort": ""}})));
        assert!(!thinking_mode(&json!({"messages": []})));
    }

    #[test]
    fn remap_injects_empty_reasoning_in_thinking_mode() {
        let app = app_for_test();
        let body = app.remap_chat_body(json!({
            "model": "some-model",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"}
            ],
            "thinking": {"enabled": true},
            "stream": true
        }));
        let parsed: Value = serde_json::from_slice(&body).expect("valid json out");
        assert_eq!(parsed["reasoning_content"], json!(""));
        let assistant = parsed["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "assistant")
            .expect("assistant message present");
        assert_eq!(assistant["reasoning_content"], json!(""));
    }

    #[test]
    fn remap_merges_reasoning_effort_object_form() {
        let app = app_for_test();
        let body = app.remap_chat_body(json!({
            "model": "some-model",
            "messages": [{"role": "user", "content": "hi"}],
            "reasoning": {"effort": "high"}
        }));
        let parsed: Value = serde_json::from_slice(&body).expect("valid json out");
        assert_eq!(parsed["reasoning_effort"], json!("high"));
        assert_eq!(
            parsed["reasoning_content"],
            json!(""),
            "thinking mode should kick in via merged effort"
        );
    }

    #[test]
    fn remap_prefers_explicit_reasoning_effort() {
        let app = app_for_test();
        let body = app.remap_chat_body(json!({
            "model": "some-model",
            "messages": [],
            "reasoning_effort": "low",
            "reasoning": {"effort": "high"}
        }));
        let parsed: Value = serde_json::from_slice(&body).expect("valid json out");
        assert_eq!(parsed["reasoning_effort"], json!("low"));
    }

    #[test]
    fn ensure_completion_reasoning_injects_when_tokens_only() {
        let mut body = serde_json::to_vec(&json!({
            "id": "chatcmpl-1",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
            "usage": {"completion_tokens": 10, "completion_tokens_details": {"reasoning_tokens": 5}}
        }))
        .unwrap();
        ensure_completion_reasoning(&mut body);
        let parsed: Value = serde_json::from_slice(&body).expect("valid json out");
        assert_eq!(parsed["reasoning_content"], json!(""));
        assert_eq!(
            parsed["choices"][0]["message"]["reasoning_content"],
            json!("")
        );
    }

    #[test]
    fn ensure_completion_reasoning_keeps_existing_content() {
        let mut body = serde_json::to_vec(&json!({
            "reasoning_content": "thoughts",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi", "reasoning_content": "inner"}}],
            "usage": {"completion_tokens_details": {"reasoning_tokens": 5}}
        }))
        .unwrap();
        ensure_completion_reasoning(&mut body);
        let parsed: Value = serde_json::from_slice(&body).expect("valid json out");
        assert_eq!(parsed["reasoning_content"], json!("thoughts"));
        assert_eq!(
            parsed["choices"][0]["message"]["reasoning_content"],
            json!("inner")
        );
    }

    #[test]
    fn ensure_completion_reasoning_injects_without_usage_too() {
        let mut body = serde_json::to_vec(&json!({
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}}],
            "usage": {"completion_tokens": 10}
        }))
        .unwrap();
        ensure_completion_reasoning(&mut body);
        let parsed: Value = serde_json::from_slice(&body).expect("valid json out");
        assert_eq!(parsed["reasoning_content"], json!(""));
        assert_eq!(
            parsed["choices"][0]["message"]["reasoning_content"],
            json!("")
        );
    }

    #[test]
    fn ensure_completion_reasoning_noop_without_choices() {
        let original = json!({"error": {"message": "boom"}});
        let mut body = serde_json::to_vec(&original).unwrap();
        ensure_completion_reasoning(&mut body);
        let parsed: Value = serde_json::from_slice(&body).expect("valid json out");
        assert_eq!(parsed, original);
    }

    #[test]
    fn ensure_completion_reasoning_ignores_non_json() {
        let mut body = b"<html>not json</html>".to_vec();
        ensure_completion_reasoning(&mut body);
        assert_eq!(body, b"<html>not json</html>");
    }

    #[test]
    fn stream_has_reasoning_detects_deltas() {
        let seen = r#"data: {"choices":[{"delta":{"reasoning_content":"hmm"}}]}
data: [DONE]"#;
        assert!(stream_has_reasoning(seen));
        assert!(!stream_has_reasoning("data: {\"usage\":{}}\n"));
    }

    #[test]
    fn transform_stream_injects_into_finish_chunk() {
        let stream = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens_details\":{\"reasoning_tokens\":7}}}\n\ndata: [DONE]\n\n";
        let mut body = stream.as_bytes().to_vec();
        transform_stream_body(&mut body);
        let transformed = String::from_utf8(body).unwrap();
        let finish_line = transformed
            .lines()
            .find(|line| line.contains("finish_reason"))
            .expect("finish chunk present");
        assert!(
            finish_line.contains("\"reasoning_content\":\"\""),
            "finish chunk should carry the fallback reasoning_content, got {finish_line}"
        );
        assert_eq!(transformed.matches("reasoning_content").count(), 1);
    }

    #[test]
    fn transform_stream_appends_fallback_without_finish_chunk() {
        let stream = "data: {\"usage\":{\"completion_tokens_details\":{\"reasoning_tokens\":7}}}\n\ndata: [DONE]\n\n";
        let mut body = stream.as_bytes().to_vec();
        transform_stream_body(&mut body);
        let transformed = String::from_utf8(body).unwrap();
        let done = transformed.rfind("data: [DONE]").expect("DONE present");
        let fallback = transformed
            .rfind("reasoning_content")
            .expect("fallback chunk present");
        assert!(fallback < done, "fallback chunk must precede [DONE]");
    }

    #[test]
    fn transform_stream_fallback_carries_chunk_identity() {
        let stream = "data: {\"id\":\"chatcmpl-1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"usage\":{\"completion_tokens_details\":{\"reasoning_tokens\":7}}}\n\ndata: [DONE]\n\n";
        let mut body = stream.as_bytes().to_vec();
        transform_stream_body(&mut body);
        let transformed = String::from_utf8(body).unwrap();
        let fallback_line = transformed
            .lines()
            .find(|line| line.contains("reasoning_content"))
            .expect("fallback chunk present");
        let payload: Value =
            serde_json::from_str(fallback_line.strip_prefix("data: ").unwrap()).unwrap();
        assert_eq!(payload["id"], json!("chatcmpl-1"));
        assert_eq!(payload["object"], json!("chat.completion.chunk"));
        assert_eq!(payload["model"], json!("m"));
    }

    #[test]
    fn transform_stream_fallback_without_identity_still_valid() {
        let stream = "data: {\"usage\":{\"completion_tokens_details\":{\"reasoning_tokens\":7}}}\n\ndata: [DONE]\n\n";
        let mut body = stream.as_bytes().to_vec();
        transform_stream_body(&mut body);
        let transformed = String::from_utf8(body).unwrap();
        let fallback_line = transformed
            .lines()
            .find(|line| line.contains("reasoning_content"))
            .expect("fallback chunk present");
        let payload: Value =
            serde_json::from_str(fallback_line.strip_prefix("data: ").unwrap()).unwrap();
        assert_eq!(
            payload["choices"][0]["delta"]["reasoning_content"],
            json!("")
        );
    }

    #[test]
    fn transform_stream_handles_crlf() {
        let stream = "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens_details\":{\"reasoning_tokens\":3}}}\r\n\r\ndata: [DONE]\r\n\r\n";
        let mut body = stream.as_bytes().to_vec();
        transform_stream_body(&mut body);
        let transformed = String::from_utf8(body).unwrap();
        assert!(transformed.contains("\"reasoning_content\":\"\""));
    }

    #[test]
    fn transform_stream_noop_when_upstream_already_streams_reasoning() {
        let stream = "data: {\"choices\":[{\"delta\":{\"reasoning_content\":\"thinking...\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens_details\":{\"reasoning_tokens\":7}}}\n\ndata: [DONE]\n\n";
        let mut body = stream.as_bytes().to_vec();
        let original = body.clone();
        transform_stream_body(&mut body);
        assert_eq!(body, original);
    }

    #[test]
    fn transform_stream_injects_without_any_usage_chunk() {
        let stream =
            "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        let mut body = stream.as_bytes().to_vec();
        transform_stream_body(&mut body);
        let transformed = String::from_utf8(body).unwrap();
        assert!(transformed.contains("\"reasoning_content\":\"\""));
    }

    fn start_mock_upstream(
        capture: Arc<Mutex<Vec<u8>>>,
        response_body: Vec<u8>,
        content_type: &'static str,
    ) -> String {
        let server = Server::http("127.0.0.1:0").unwrap();
        let address = server.server_addr().to_ip().unwrap();
        thread::spawn(move || {
            for mut request in server.incoming_requests() {
                let mut body = Vec::new();
                let _ = request.as_reader().read_to_end(&mut body);
                *capture.lock().unwrap() = body;
                let len = response_body.len();
                let resp = Response::new(
                    StatusCode(200),
                    vec![Header::from_bytes("Content-Type", content_type).unwrap()],
                    Cursor::new(response_body.clone()),
                    Some(len),
                    None,
                );
                let _ = request.respond(resp);
            }
        });
        format!("http://{address}")
    }

    fn start_app_server(app: App) -> String {
        let server = Server::http("127.0.0.1:0").unwrap();
        let address = server.server_addr().to_ip().unwrap();
        let (tx, rx) = mpsc::channel::<HttpRequest>();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..2 {
            let rx = Arc::clone(&rx);
            let app = app.clone();
            thread::spawn(move || {
                while let Ok(mut request) = rx.lock().expect("request queue lock").recv() {
                    let response = app.handle(&mut request);
                    let _ = request.respond(response);
                }
            });
        }
        thread::spawn(move || {
            for request in server.incoming_requests() {
                let _ = tx.send(request);
            }
        });
        format!("http://{address}")
    }

    #[test]
    fn e2e_request_side_injects_reasoning_content() {
        let capture = Arc::new(Mutex::new(Vec::new()));
        let upstream_body = serde_json::to_vec(&json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
            "usage": {"completion_tokens": 3}
        }))
        .unwrap();
        let upstream_url =
            start_mock_upstream(Arc::clone(&capture), upstream_body, "application/json");
        let app = app_with_nodes(&upstream_url);
        let base = start_app_server(app);
        let client = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .build()
            .new_agent();
        let request_body = json!({
            "model": "some-model",
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"}
            ],
            "reasoning_effort": "high",
            "stream": false
        });
        let response = client
            .post(&format!("{base}/v1/chat/completions"))
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&request_body).unwrap())
            .expect("app should answer");
        assert_eq!(response.status().as_u16(), 200);
        let forwarded: Value =
            serde_json::from_slice(&capture.lock().unwrap()).expect("forwarded body is JSON");
        assert_eq!(
            forwarded["reasoning_content"],
            json!(""),
            "top-level reasoning_content should be injected, got {forwarded}"
        );
        let assistant = forwarded["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "assistant")
            .expect("assistant message forwarded");
        assert_eq!(assistant["reasoning_content"], json!(""));
    }

    #[test]
    fn e2e_response_side_adds_missing_reasoning_content() {
        let capture = Arc::new(Mutex::new(Vec::new()));
        let upstream_body = serde_json::to_vec(&json!({
            "id": "chatcmpl-1",
            "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "answer"}, "finish_reason": "stop"}],
            "usage": {"completion_tokens": 12, "completion_tokens_details": {"reasoning_tokens": 9}}
        }))
        .unwrap();
        let upstream_url =
            start_mock_upstream(Arc::clone(&capture), upstream_body, "application/json");
        let app = app_with_nodes(&upstream_url);
        let base = start_app_server(app);
        let client = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .build()
            .new_agent();
        let request_body = json!({
            "model": "some-model",
            "messages": [{"role": "user", "content": "think hard"}],
            "reasoning_effort": "high",
            "stream": false
        });
        let response = client
            .post(&format!("{base}/v1/chat/completions"))
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&request_body).unwrap())
            .expect("app should answer");
        let payload: Value =
            serde_json::from_slice(&read_body(response.into_body())).expect("JSON response");
        assert_eq!(
            payload["reasoning_content"],
            json!(""),
            "missing reasoning_content should be filled, got {payload}"
        );
        assert_eq!(
            payload["choices"][0]["message"]["reasoning_content"],
            json!("")
        );
    }

    #[test]
    fn e2e_stream_side_adds_missing_reasoning_delta() {
        let capture = Arc::new(Mutex::new(Vec::new()));
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"completion_tokens_details\":{\"reasoning_tokens\":7}}}\n\ndata: [DONE]\n\n";
        let upstream_url = start_mock_upstream(
            Arc::clone(&capture),
            sse.as_bytes().to_vec(),
            "text/event-stream; charset=utf-8",
        );
        let app = app_with_nodes(&upstream_url);
        let base = start_app_server(app);
        let client = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(10)))
            .build()
            .new_agent();
        let request_body = json!({
            "model": "some-model",
            "messages": [{"role": "user", "content": "think"}],
            "reasoning_effort": "high",
            "stream": true
        });
        let response = client
            .post(&format!("{base}/v1/chat/completions"))
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&request_body).unwrap())
            .expect("app should answer");
        let body = read_body(response.into_body());
        let text = String::from_utf8(body).unwrap();
        let finish_line = text
            .lines()
            .find(|line| line.contains("finish_reason"))
            .expect("finish chunk present");
        assert!(
            finish_line.contains("\"reasoning_content\":\"\""),
            "finish chunk should carry fallback reasoning_content, got {finish_line}"
        );
    }

    #[test]
    fn e2e_responses_remaps_stripped_model_and_forwards() {
        let capture = Arc::new(Mutex::new(Vec::new()));
        let upstream_body = serde_json::to_vec(&json!({
            "id": "resp_1",
            "object": "response",
            "status": "completed"
        }))
        .unwrap();
        let upstream_url =
            start_mock_upstream(Arc::clone(&capture), upstream_body, "application/json");
        let app = app_with_nodes(&upstream_url);
        app.config.lock().expect("config lock").strip_free = true;
        app.free_map
            .lock()
            .expect("model map lock")
            .entries
            .insert("big-pickle".to_owned(), "big-pickle-free".to_owned());
        let base = start_app_server(app);
        let client = test_agent();
        let request_body = json!({"model": "big-pickle", "input": "hi", "stream": false});
        let response = client
            .post(&format!("{base}/v1/responses"))
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&request_body).unwrap())
            .expect("app should answer");
        assert_eq!(response.status().as_u16(), 200);
        let forwarded: Value =
            serde_json::from_slice(&capture.lock().expect("capture lock")).expect("JSON body");
        assert_eq!(forwarded["model"], json!("big-pickle-free"));
    }

    #[test]
    fn e2e_responses_stream_passes_sse_through() {
        let capture = Arc::new(Mutex::new(Vec::new()));
        let sse =
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\ndata: [DONE]\n\n";
        let upstream_url = start_mock_upstream(
            Arc::clone(&capture),
            sse.as_bytes().to_vec(),
            "text/event-stream; charset=utf-8",
        );
        let app = app_with_nodes(&upstream_url);
        let base = start_app_server(app);
        let client = test_agent();
        let request_body = json!({"model": "some-model", "input": "hi", "stream": true});
        let response = client
            .post(&format!("{base}/v1/responses"))
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&request_body).unwrap())
            .expect("app should answer");
        assert_eq!(response.status().as_u16(), 200);
        let text = String::from_utf8(read_body(response.into_body())).unwrap();
        assert!(
            text.contains("response.output_text.delta"),
            "SSE should pass through, got {text}"
        );
    }

    #[test]
    fn e2e_config_endpoints_round_trip() {
        let path = temp_config_path("http");
        let mut app = app_for_test();
        app.config_path = path.clone();
        let base = start_app_server(app);
        let client = test_agent();

        let view = read_json(
            client
                .get(&format!("{base}/api/config"))
                .call()
                .expect("config view"),
        );
        assert_eq!(view["admin"], json!(true), "loopback should be admin");
        assert_eq!(view["auth_required"], json!(false));
        assert_eq!(view["strip_free"], json!(false));
        assert!(view["nodes"].as_array().is_some());

        let patch = json!({
            "nodes": [" https://api-one.example/ ", "https://api-two.example"],
            "strip_free": true,
            "api_token": "zen-key"
        });
        let saved = read_json(
            client
                .post(&format!("{base}/api/config"))
                .header("Content-Type", "application/json")
                .send(serde_json::to_vec(&patch).unwrap())
                .expect("config update"),
        );
        assert_eq!(saved["saved"], json!(true));
        assert_eq!(saved["persisted"], json!(true));
        assert_eq!(saved["strip_free"], json!(true));
        assert_eq!(saved["api_token"], json!("zen-key"));

        let stored = load_stored(&path).expect("persisted config");
        assert_eq!(stored.strip_free, Some(true));
        assert_eq!(stored.api_token, Some(Some("zen-key".to_owned())));
        assert_eq!(
            stored.nodes,
            Some(json!([
                "https://api-one.example",
                "https://api-two.example"
            ]))
        );

        let rejected = client
            .post(&format!("{base}/api/config"))
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&json!({"nodes": ["ftp://bad.example"]})).unwrap())
            .expect("invalid config update should answer");
        assert_eq!(rejected.status().as_u16(), 400);
        let still = read_json(
            client
                .get(&format!("{base}/api/config"))
                .call()
                .expect("config view"),
        );
        assert_eq!(
            still["nodes"],
            json!(["https://api-one.example", "https://api-two.example"]),
            "invalid patch must not change stored nodes"
        );

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}.tmp", path.display()));
    }

    #[test]
    fn e2e_index_stays_reachable_with_auth_token() {
        let app = app_for_test();
        app.config.lock().expect("config lock").auth_token = Some("secret".to_owned());
        let base = start_app_server(app);
        let client = test_agent();

        let index = client
            .get(&format!("{base}/"))
            .call()
            .expect("index should stay reachable");
        assert_eq!(index.status().as_u16(), 200);

        let models = client
            .get(&format!("{base}/v1/models"))
            .call()
            .expect("models should answer");
        assert_eq!(models.status().as_u16(), 401);

        let wrong = client
            .get(&format!("{base}/v1/models"))
            .header("Authorization", "Bearer wrong")
            .call()
            .expect("wrong token should answer");
        assert_eq!(wrong.status().as_u16(), 401);

        let view = read_json(
            client
                .get(&format!("{base}/api/config"))
                .header("Authorization", "Bearer secret")
                .call()
                .expect("authorized config view"),
        );
        assert_eq!(view["admin"], json!(true));
        assert_eq!(view["auth_token"], json!("secret"));
    }

    #[test]
    fn e2e_chat_client_converted_to_responses_upstream() {
        let capture = Arc::new(Mutex::new(Vec::new()));
        let upstream_body = serde_json::to_vec(&json!({
            "id": "resp_1",
            "object": "response",
            "model": "gpt-4o",
            "created_at": 1700000010,
            "status": "completed",
            "output": [{
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": "converted", "annotations": []}],
            }],
            "usage": {"input_tokens": 3, "output_tokens": 2, "total_tokens": 5},
        }))
        .unwrap();
        let upstream_url =
            start_mock_upstream(Arc::clone(&capture), upstream_body, "application/json");
        let app = app_with_protocol(PROTOCOL_RESPONSES);
        app.config.lock().expect("config lock").nodes = vec![upstream_url];
        let base = start_app_server(app);
        let client = test_agent();
        let request_body = json!({
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": false,
        });
        let response = client
            .post(&format!("{base}/v1/chat/completions"))
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&request_body).unwrap())
            .expect("app should answer");
        assert_eq!(response.status().as_u16(), 200);
        let forwarded: Value =
            serde_json::from_slice(&capture.lock().expect("capture lock")).expect("JSON body");
        assert_eq!(forwarded["input"][0]["content"][0]["text"], json!("hi"));
        assert!(
            forwarded.get("messages").is_none(),
            "request must be converted to responses format, got {forwarded}"
        );
        let payload = read_json(response);
        assert_eq!(payload["object"], json!("chat.completion"));
        assert_eq!(
            payload["choices"][0]["message"]["content"],
            json!("converted")
        );
        assert_eq!(payload["usage"]["prompt_tokens"], json!(3));
        assert_eq!(payload["usage"]["total_tokens"], json!(5));
    }

    #[test]
    fn e2e_responses_client_converted_to_chat_upstream() {
        let capture = Arc::new(Mutex::new(Vec::new()));
        let upstream_body = serde_json::to_vec(&json!({
            "id": "chatcmpl-9",
            "object": "chat.completion",
            "model": "gpt-4o",
            "created": 1700000011,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "converted back"},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": 4, "completion_tokens": 2, "total_tokens": 6},
        }))
        .unwrap();
        let upstream_url =
            start_mock_upstream(Arc::clone(&capture), upstream_body, "application/json");
        let app = app_with_protocol(PROTOCOL_CHAT);
        app.config.lock().expect("config lock").nodes = vec![upstream_url];
        let base = start_app_server(app);
        let client = test_agent();
        let request_body = json!({"model": "gpt-4o", "input": "hi", "stream": false});
        let response = client
            .post(&format!("{base}/v1/responses"))
            .header("Content-Type", "application/json")
            .send(serde_json::to_vec(&request_body).unwrap())
            .expect("app should answer");
        assert_eq!(response.status().as_u16(), 200);
        let forwarded: Value =
            serde_json::from_slice(&capture.lock().expect("capture lock")).expect("JSON body");
        assert_eq!(forwarded["messages"][0]["content"], json!("hi"));
        assert!(
            forwarded.get("input").is_none(),
            "request must be converted to chat format, got {forwarded}"
        );
        let payload = read_json(response);
        assert_eq!(payload["object"], json!("response"));
        assert_eq!(payload["status"], json!("completed"));
        assert_eq!(payload["output_text"], json!("converted back"));
        assert_eq!(payload["usage"]["input_tokens"], json!(4));
        assert_eq!(payload["usage"]["total_tokens"], json!(6));
    }

    #[test]
    fn e2e_connectivity_reports_node_status() {
        let list_body = serde_json::to_vec(&json!({
            "object": "list",
            "data": [{"id": "a-free"}, {"id": "b-free"}],
        }))
        .unwrap();
        let good_url = start_mock_upstream(Arc::new(Mutex::new(Vec::new())), list_body, "application/json");
        let app = app_with_nodes(&good_url);
        app.config
            .lock()
            .expect("config lock")
            .nodes
            .push("http://127.0.0.1:1".to_owned());
        let base = start_app_server(app);
        let client = test_agent();
        let payload = read_json(
            client
                .get(&format!("{base}/api/connectivity"))
                .call()
                .expect("connectivity"),
        );
        assert_eq!(payload["tested"], json!(2), "{payload}");
        let results = payload["results"].as_array().expect("results array");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["node"], json!(good_url), "{payload}");
        assert_eq!(results[0]["ok"], json!(true), "{payload}");
        assert_eq!(results[0]["status"], json!(200), "{payload}");
        assert_eq!(results[0]["models"], json!(2), "{payload}");
        assert!(results[0]["latency_ms"].as_u64().is_some(), "{payload}");
        assert!(results[0]["error"].is_null(), "{payload}");
        assert_eq!(results[1]["node"], json!("http://127.0.0.1:1"));
        assert_eq!(results[1]["ok"], json!(false), "{payload}");
        assert!(results[1]["error"].is_string(), "{payload}");
    }
}
