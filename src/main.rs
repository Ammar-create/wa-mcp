//! wa-cli: headless WhatsApp client on top of whatsapp-rust.
//! `wa pair <phone>` links with an 8-character pair code, `wa serve` keeps the
//! session alive and exposes a small local JSON API for the MCP wrapper.

use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::sync::mpsc::Sender as SyncSender;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;
use tiny_http::{Header, Method, Response, Server};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
use whatsapp_rust::pair_code::PairCodeOptions;
use whatsapp_rust::prelude::*;

const DATA_DIR: &str = "/root/.local/share/wa-cli";
const DEFAULT_PORT: u16 = 8899;

#[derive(Serialize, Clone)]
struct Msg {
    ts: u64,
    chat: String,
    sender: String,
    text: String,
    id: String,
}

struct State {
    connected: bool,
    code: Option<String>,
    messages: Vec<Msg>,
}

struct SendJob {
    to: String,
    text: String,
    reply: SyncSender<String>,
}

fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn,wa_cli=info")).init();
    std::fs::create_dir_all(DATA_DIR).context("create data dir")?;

    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).cloned().unwrap_or_else(|| "help".into());
    let port: u16 = flag(&args, "--port").and_then(|p| p.parse().ok()).unwrap_or(DEFAULT_PORT);
    let phone = flag(&args, "--phone").or_else(|| {
        if cmd == "pair" { args.get(2).cloned() } else { None }
    });
    let code = flag(&args, "--code");

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    rt.block_on(run(cmd, phone, code, port))
}

async fn run(cmd: String, phone: Option<String>, code: Option<String>, port: u16) -> Result<()> {
    if cmd == "help" {
        println!("wa pair <phone> [--code ABC12345]   link a device with a pair code");
        println!("wa serve [--port {DEFAULT_PORT}]        run headless, expose local JSON API");
        return Ok(());
    }

    let state = Arc::new(Mutex::new(State { connected: false, code: None, messages: Vec::new() }));
    let (tx, mut rx) = unbounded_channel::<SendJob>();

    let st = state.clone();
    let archive = format!("{DATA_DIR}/messages.jsonl");

    let store = SqliteStore::new(&format!("{DATA_DIR}/wa.db")).await.context("open session store")?;

    let mut builder = Bot::builder()
        .with_backend(store)
        .on_qr_code(|qr, _timeout| async move {
            eprintln!("QR: {qr}");
        })
        .on_pair_code(move |pair, _timeout| {
            let st = st.clone();
            async move {
                println!("PAIR_CODE={pair}");
                eprintln!("Type this in WhatsApp > Linked devices > Link with phone number instead: {pair}");
                if let Ok(mut s) = st.lock() { s.code = Some(pair.to_string()); }
            }
        })
        .on_connected(move |_client| {
            let st = state.clone();
            async move {
                if let Ok(mut s) = st.lock() { s.connected = true; s.code = None; }
                eprintln!("connected");
            }
        })
        .on_logged_out(|info| async move {
            eprintln!("logged out: {info:?}");
        })
        .on_message(move |ctx| {
            let st = state.clone();
            let path = archive.clone();
            async move {
                let Some(text) = ctx.message.text_content() else { return };
                let chat = ctx.info.source.chat.to_string();
                let sender = ctx.info.source.sender.to_string();
                let id = format!("{}", ctx.info.id);
                let m = Msg { ts: now(), chat, sender, text: text.to_string(), id };
                if let Ok(mut s) = st.lock() { s.messages.push(m.clone()); }
                if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(&path) {
                    if let Ok(line) = serde_json::to_string(&m) { let _ = writeln!(f, "{line}"); }
                }
            }
        });

    if let Some(p) = phone {
        builder = builder.with_pair_code(PairCodeOptions { phone_number: p, custom_code: code, ..Default::default() });
    }

    let bot = builder.build().await.context("build bot")?;
    let client = bot.client();

    tokio::spawn(async move {
        while let Some(job) = rx.recv().await {
            let jid_s = if job.to.contains('@') { job.to.clone() } else { format!("{}@s.whatsapp.net", job.to) };
            let out = match jid_s.parse::<Jid>() {
                Ok(jid) => match client.send_message(jid, wa::Message::text(job.text)).await {
                    Ok(sent) => format!("{{\"ok\":true,\"id\":\"{}\"}}", sent.message_id),
                    Err(e) => format!("{{\"ok\":false,\"error\":\"{}\"}}", e.to_string().replace('"', "'")),
                },
                Err(e) => format!("{{\"ok\":false,\"error\":\"bad jid: {}\"}}", e),
            };
            let _ = job.reply.send(out);
        }
    });

    let http_state = state.clone();
    let http_tx = tx.clone();
    std::thread::spawn(move || {
        let addr = format!("127.0.0.1:{port}");
        let server = match Server::http(&addr) {
            Ok(s) => s,
            Err(e) => { eprintln!("http bind failed: {e}"); return; }
        };
        eprintln!("http listening on {addr}");
        for request in server.incoming_requests() {
            let url = request.url().to_string();
            let method = request.method().clone();
            let mut body = String::new();
            let _ = request.as_reader().read_to_string(&mut body);
            let (code, out) = handle(&http_state, &http_tx, &method, &url, &body);
            let header = Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
            let _ = request.respond(Response::from_string(out).with_status_code(code).with_header(header));
        }
    });

    eprintln!("running; api on http://127.0.0.1:{port}");
    bot.run().await;
    Ok(())
}

fn handle(state: &Arc<Mutex<State>>, tx: &UnboundedSender<SendJob>, method: &Method, url: &str, body: &str) -> (u16, String) {
    let (path, query) = match url.split_once('?') { Some((p, q)) => (p, q), None => (url, "") };
    let param = |key: &str| -> Option<String> {
        query.split('&').find_map(|kv| {
            let (k, v) = kv.split_once('=')?;
            if k == key { Some(v.replace("%40", "@").replace("%2B", "+")) } else { None }
        })
    };

    match (method.as_str(), path) {
        ("GET", "/status") => {
            let s = state.lock().unwrap();
            (200, format!("{{\"ok\":true,\"connected\":{},\"pair_code\":{},\"messages\":{}}}",
                s.connected,
                s.code.as_ref().map(|c| format!("\"{c}\"")).unwrap_or("null".into()),
                s.messages.len()))
        }
        ("GET", "/chats") => {
            let s = state.lock().unwrap();
            let mut seen: Vec<String> = Vec::new();
            let mut out: Vec<String> = Vec::new();
            for m in s.messages.iter().rev() {
                if seen.contains(&m.chat) { continue }
                seen.push(m.chat.clone());
                out.push(format!("{{\"chat\":\"{}\",\"last\":\"{}\",\"ts\":{}}}",
                    m.chat, esc(&m.text), m.ts));
                if out.len() >= 100 { break }
            }
            (200, format!("{{\"ok\":true,\"chats\":[{}]}}", out.join(",")))
        }
        ("GET", "/messages") => {
            let chat = param("chat");
            let limit: usize = param("limit").and_then(|l| l.parse().ok()).unwrap_or(50);
            let s = state.lock().unwrap();
            let out: Vec<String> = s.messages.iter().rev()
                .filter(|m| chat.as_ref().map(|c| &m.chat == c).unwrap_or(true))
                .take(limit)
                .map(|m| format!("{{\"ts\":{},\"chat\":\"{}\",\"from\":\"{}\",\"text\":\"{}\"}}",
                    m.ts, m.chat, m.sender, esc(&m.text)))
                .collect();
            (200, format!("{{\"ok\":true,\"messages\":[{}]}}", out.join(",")))
        }
        ("POST", "/send") => {
            let v: serde_json::Value = match serde_json::from_str(body) {
                Ok(v) => v,
                Err(e) => return (400, format!("{{\"ok\":false,\"error\":\"json: {e}\"}}")),
            };
            let to = v.get("to").and_then(|x| x.as_str()).unwrap_or("").to_string();
            let text = v.get("text").and_then(|x| x.as_str()).unwrap_or("").to_string();
            if to.is_empty() || text.is_empty() {
                return (400, "{\"ok\":false,\"error\":\"need to and text\"}".into());
            }
            let (rtx, rrx) = std::sync::mpsc::channel();
            if tx.send(SendJob { to, text, reply: rtx }).is_err() {
                return (500, "{\"ok\":false,\"error\":\"sender task gone\"}".into());
            }
            match rrx.recv_timeout(std::time::Duration::from_secs(30)) {
                Ok(out) => (200, out),
                Err(_) => (504, "{\"ok\":false,\"error\":\"timeout\"}".into()),
            }
        }
        _ => (404, "{\"ok\":false,\"error\":\"no route\"}".into()),
    }
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n").replace('\r', "")
}
