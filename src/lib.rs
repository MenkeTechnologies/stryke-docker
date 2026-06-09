//! stryke-docker — Docker cdylib loaded in-process by stryke via dlopen.
//!
//! Each `#[no_mangle] extern "C" fn docker__*` is a JSON-string-in /
//! JSON-string-out wrapper around `bollard`'s async Docker API. stryke's
//! FFI bridge (`rust_ffi.rs::load_cdylib`) resolves these symbols at first
//! `use Docker`, registers each one as a stryke-callable function, and on
//! each call passes a JSON-encoded args dict and copies the returned JSON
//! into a stryke string.
//!
//! Persistent state:
//!   * `RUNTIME` — one shared `tokio` runtime drives every async call.
//!   * `CLIENTS` — `bollard::Docker` cache per `DOCKER_HOST` (socket path /
//!     tcp url). v1 helper opened a fresh dockerd connection per fork; this
//!     reuses the same client + underlying HTTP pool across calls.

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::panic::AssertUnwindSafe;

use anyhow::{anyhow, Result};
use bollard::container::{
    Config, CreateContainerOptions, ListContainersOptions, LogsOptions, RemoveContainerOptions,
    StopContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::image::{CreateImageOptions, ListImagesOptions, RemoveImageOptions};
use bollard::network::{CreateNetworkOptions, ListNetworksOptions};
use bollard::volume::{CreateVolumeOptions, ListVolumesOptions};
use bollard::Docker;
use futures_util::stream::TryStreamExt;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::runtime::{Builder, Runtime};

// ── runtime + client cache ──────────────────────────────────────────────────

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn rt() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
    })
}

static CLIENTS: OnceCell<Mutex<HashMap<String, Docker>>> = OnceCell::new();

fn clients() -> &'static Mutex<HashMap<String, Docker>> {
    CLIENTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_client(opts: &Value) -> Result<Docker> {
    let host = opts
        .get("host")
        .and_then(|v| v.as_str())
        .map(String::from)
        .or_else(|| std::env::var("DOCKER_HOST").ok())
        .unwrap_or_else(|| "default".to_string());
    {
        let map = clients().lock();
        if let Some(c) = map.get(&host) {
            return Ok(c.clone());
        }
    }
    let docker = if host == "default" {
        Docker::connect_with_local_defaults()?
    } else if host.starts_with("unix://") {
        Docker::connect_with_socket(&host, 4, bollard::API_DEFAULT_VERSION)?
    } else {
        Docker::connect_with_http(&host, 4, bollard::API_DEFAULT_VERSION)?
    };
    clients().lock().insert(host, docker.clone());
    Ok(docker)
}

fn to_value<T: serde::Serialize>(v: T) -> Value {
    serde_json::to_value(v).unwrap_or(Value::Null)
}

// ── ops ─────────────────────────────────────────────────────────────────────

async fn op_version(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let v = d.version().await?;
    Ok(to_value(v))
}

async fn op_ping(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let s = d.ping().await?;
    Ok(json!({"value": s}))
}

async fn op_info(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let i = d.info().await?;
    Ok(to_value(i))
}

async fn op_ps(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let all = opts["all"].as_bool().unwrap_or(false);
    let containers = d
        .list_containers(Some(ListContainersOptions::<String> {
            all,
            ..Default::default()
        }))
        .await?;
    Ok(json!({"containers": to_value(containers)}))
}

async fn op_inspect(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    let info = d.inspect_container(name, None).await?;
    Ok(to_value(info))
}

async fn op_create(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let image = opts["image"]
        .as_str()
        .ok_or_else(|| anyhow!("missing image"))?
        .to_string();
    let name = opts["name"].as_str().map(String::from);
    let cmd = opts["cmd"].as_array().map(|a| {
        a.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>()
    });
    let env = opts["env"].as_array().map(|a| {
        a.iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>()
    });
    let config = Config::<String> {
        image: Some(image),
        cmd,
        env,
        ..Default::default()
    };
    let options = name.as_ref().map(|n| CreateContainerOptions::<String> {
        name: n.clone(),
        platform: None,
    });
    let resp = d.create_container(options, config).await?;
    Ok(to_value(resp))
}

async fn op_start(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    d.start_container::<String>(name, None).await?;
    Ok(json!({"ok": true}))
}

async fn op_stop(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    let t = opts["timeout"].as_i64();
    d.stop_container(name, t.map(|secs| StopContainerOptions { t: secs }))
        .await?;
    Ok(json!({"ok": true}))
}

async fn op_restart(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    d.restart_container(name, None).await?;
    Ok(json!({"ok": true}))
}

async fn op_kill(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    let signal = opts["signal"].as_str().unwrap_or("SIGKILL");
    d.kill_container(
        name,
        Some(bollard::container::KillContainerOptions { signal }),
    )
    .await?;
    Ok(json!({"ok": true}))
}

async fn op_rm(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    let force = opts["force"].as_bool().unwrap_or(false);
    let v = opts["volumes"].as_bool().unwrap_or(false);
    d.remove_container(
        name,
        Some(RemoveContainerOptions {
            force,
            v,
            ..Default::default()
        }),
    )
    .await?;
    Ok(json!({"ok": true}))
}

async fn op_logs(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    let tail = opts["tail"].as_u64();
    let mut stream = d.logs(
        name,
        Some(LogsOptions::<String> {
            stdout: true,
            stderr: true,
            follow: false,
            tail: tail
                .map(|n| n.to_string())
                .unwrap_or_else(|| "all".to_string()),
            ..Default::default()
        }),
    );
    let mut buf = String::new();
    while let Some(line) = stream.try_next().await? {
        buf.push_str(&line.to_string());
    }
    Ok(json!({"logs": buf}))
}

async fn op_exec(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))?;
    let cmd: Vec<String> = opts["cmd"]
        .as_array()
        .ok_or_else(|| anyhow!("missing cmd (array)"))?
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    let exec = d
        .create_exec(
            name,
            CreateExecOptions {
                cmd: Some(cmd),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                ..Default::default()
            },
        )
        .await?;
    let mut out = String::new();
    if let StartExecResults::Attached { mut output, .. } = d.start_exec(&exec.id, None).await? {
        while let Some(chunk) = output.try_next().await? {
            out.push_str(&chunk.to_string());
        }
    }
    Ok(json!({"output": out}))
}

async fn op_images(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let all = opts["all"].as_bool().unwrap_or(false);
    let images = d
        .list_images(Some(ListImagesOptions::<String> {
            all,
            ..Default::default()
        }))
        .await?;
    Ok(json!({"images": to_value(images)}))
}

async fn op_pull(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let image = opts["image"]
        .as_str()
        .ok_or_else(|| anyhow!("missing image"))?
        .to_string();
    let mut stream = d.create_image(
        Some(CreateImageOptions::<String> {
            from_image: image.clone(),
            ..Default::default()
        }),
        None,
        None,
    );
    while let Some(_chunk) = stream.try_next().await? {}
    Ok(json!({"image": image}))
}

async fn op_rmi(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let image = opts["image"]
        .as_str()
        .ok_or_else(|| anyhow!("missing image"))?;
    let force = opts["force"].as_bool().unwrap_or(false);
    d.remove_image(
        image,
        Some(RemoveImageOptions {
            force,
            ..Default::default()
        }),
        None,
    )
    .await?;
    Ok(json!({"ok": true}))
}

async fn op_tag(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let source = opts["source"]
        .as_str()
        .ok_or_else(|| anyhow!("missing source"))?;
    let target = opts["target"]
        .as_str()
        .ok_or_else(|| anyhow!("missing target"))?;
    // bollard's tag expects repo + tag separately.
    let (repo, tag) = match target.rsplit_once(':') {
        Some((r, t)) => (r.to_string(), Some(t.to_string())),
        None => (target.to_string(), None),
    };
    d.tag_image(
        source,
        Some(bollard::image::TagImageOptions {
            repo,
            tag: tag.unwrap_or_default(),
        }),
    )
    .await?;
    Ok(json!({"ok": true}))
}

async fn op_networks(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let nets = d
        .list_networks(Some(ListNetworksOptions::<String> {
            ..Default::default()
        }))
        .await?;
    Ok(json!({"networks": to_value(nets)}))
}

async fn op_network_create(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?
        .to_string();
    let driver = opts["driver"].as_str().unwrap_or("bridge").to_string();
    let resp = d
        .create_network(CreateNetworkOptions {
            name,
            driver,
            ..Default::default()
        })
        .await?;
    Ok(to_value(resp))
}

async fn op_network_rm(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    d.remove_network(name).await?;
    Ok(json!({"ok": true}))
}

async fn op_volumes(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let vols = d
        .list_volumes::<String>(Some(ListVolumesOptions::default()))
        .await?;
    Ok(json!({"volumes": to_value(vols)}))
}

async fn op_volume_create(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?
        .to_string();
    let resp = d
        .create_volume(CreateVolumeOptions::<String> {
            name,
            ..Default::default()
        })
        .await?;
    Ok(to_value(resp))
}

async fn op_volume_rm(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name"))?;
    let force = opts["force"].as_bool().unwrap_or(false);
    d.remove_volume(name, Some(bollard::volume::RemoveVolumeOptions { force }))
        .await?;
    Ok(json!({"ok": true}))
}

async fn op_prune(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let kind = opts["kind"].as_str().unwrap_or("containers");
    match kind {
        "containers" => {
            let r = d.prune_containers::<String>(None).await?;
            Ok(to_value(r))
        }
        "images" => {
            let r = d.prune_images::<String>(None).await?;
            Ok(to_value(r))
        }
        "volumes" => {
            let r = d.prune_volumes::<String>(None).await?;
            Ok(to_value(r))
        }
        "networks" => {
            let r = d.prune_networks::<String>(None).await?;
            Ok(to_value(r))
        }
        _ => Err(anyhow!(
            "prune kind must be containers|images|volumes|networks, got {}",
            kind
        )),
    }
}

// ── FFI plumbing ────────────────────────────────────────────────────────────

fn ffi_call_async<F, Fut>(args: *const c_char, handler: F) -> *const c_char
where
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Result<Value>>,
{
    let input = if args.is_null() {
        Value::Null
    } else {
        let cs = unsafe { CStr::from_ptr(args) };
        serde_json::from_slice::<Value>(cs.to_bytes()).unwrap_or(Value::Null)
    };
    let fut = handler(input);
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| rt().block_on(fut)));
    let out = match result {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => json!({ "error": e.to_string() }),
        Err(_) => json!({ "error": "stryke-docker handler panicked" }),
    };
    let s =
        serde_json::to_string(&out).unwrap_or_else(|_| String::from(r#"{"error":"serialize"}"#));
    match CString::new(s) {
        Ok(c) => c.into_raw() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Free a C string allocated by any export from this cdylib.
///
/// # Safety
///
/// `p` must be a pointer previously returned by an export from this cdylib,
/// or null.
#[no_mangle]
pub unsafe extern "C" fn stryke_free_cstring(p: *mut c_char) {
    if p.is_null() {
        return;
    }
    drop(CString::from_raw(p));
}

// ── exports ─────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn docker__pkg_version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |_| async {
        Ok(json!({"version": env!("CARGO_PKG_VERSION")}))
    })
}

#[no_mangle]
pub extern "C" fn docker__version(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_version)
}

#[no_mangle]
pub extern "C" fn docker__ping(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ping)
}

#[no_mangle]
pub extern "C" fn docker__info(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_info)
}

#[no_mangle]
pub extern "C" fn docker__ps(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_ps)
}

#[no_mangle]
pub extern "C" fn docker__inspect(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_inspect)
}

#[no_mangle]
pub extern "C" fn docker__create(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_create)
}

#[no_mangle]
pub extern "C" fn docker__start(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_start)
}

#[no_mangle]
pub extern "C" fn docker__stop(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_stop)
}

#[no_mangle]
pub extern "C" fn docker__restart(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_restart)
}

#[no_mangle]
pub extern "C" fn docker__kill(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_kill)
}

#[no_mangle]
pub extern "C" fn docker__rm(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_rm)
}

#[no_mangle]
pub extern "C" fn docker__logs(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_logs)
}

#[no_mangle]
pub extern "C" fn docker__exec(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_exec)
}

#[no_mangle]
pub extern "C" fn docker__images(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_images)
}

#[no_mangle]
pub extern "C" fn docker__pull(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pull)
}

#[no_mangle]
pub extern "C" fn docker__rmi(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_rmi)
}

#[no_mangle]
pub extern "C" fn docker__tag(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_tag)
}

#[no_mangle]
pub extern "C" fn docker__networks(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_networks)
}

#[no_mangle]
pub extern "C" fn docker__network_create(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_network_create)
}

#[no_mangle]
pub extern "C" fn docker__network_rm(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_network_rm)
}

#[no_mangle]
pub extern "C" fn docker__volumes(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_volumes)
}

#[no_mangle]
pub extern "C" fn docker__volume_create(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_volume_create)
}

#[no_mangle]
pub extern "C" fn docker__volume_rm(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_volume_rm)
}

#[no_mangle]
pub extern "C" fn docker__prune(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_prune)
}
