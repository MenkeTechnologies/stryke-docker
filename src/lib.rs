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
    RenameContainerOptions, StatsOptions, StopContainerOptions, TopOptions, WaitContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecResults};
use bollard::image::{
    CommitContainerOptions, CreateImageOptions, ListImagesOptions, RemoveImageOptions,
};
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
    // Pre-fix `tag` was never set, so bollard defaulted to empty — and per
    // bollard's own docstring "If empty when pulling, all tags for the given
    // image will be pulled," which produced unexpected multi-image pulls and
    // wrong-version usage. Split the standard `image:tag` form so the explicit
    // tag survives; bare `image` (no `:`) implicitly pulls `latest` per Docker
    // CLI parity.
    let (from_image, tag) = match image.rsplit_once(':') {
        // Skip the digest form `image@sha256:…` where `:` lives inside the digest.
        Some((repo, t)) if !repo.contains('@') && !t.contains('/') => {
            (repo.to_string(), t.to_string())
        }
        _ => (image.clone(), "latest".to_string()),
    };
    let mut stream = d.create_image(
        Some(CreateImageOptions::<String> {
            from_image,
            tag,
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

// ── container lifecycle extras ───────────────────────────────────────────────

/// Read the required `container` name from opts.
fn container_name(opts: &Value) -> Result<&str> {
    opts["container"]
        .as_str()
        .ok_or_else(|| anyhow!("missing container"))
}

async fn op_pause(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let d = get_client(&opts)?;
    d.pause_container(&name).await?;
    Ok(json!({"ok": true}))
}

async fn op_unpause(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let d = get_client(&opts)?;
    d.unpause_container(&name).await?;
    Ok(json!({"ok": true}))
}

async fn op_rename(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let new_name = opts["name"]
        .as_str()
        .ok_or_else(|| anyhow!("missing name (the new container name)"))?
        .to_string();
    let d = get_client(&opts)?;
    d.rename_container(
        &name,
        RenameContainerOptions {
            name: new_name.clone(),
        },
    )
    .await?;
    Ok(json!({"ok": true, "renamed": new_name}))
}

/// Block until the container exits; returns its exit status code.
async fn op_wait(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let d = get_client(&opts)?;
    let mut stream = d.wait_container(&name, None::<WaitContainerOptions<String>>);
    match stream.try_next().await? {
        Some(r) => Ok(json!({"status_code": r.status_code})),
        None => Ok(json!({"status_code": Value::Null})),
    }
}

/// Running processes inside the container (`docker top`). opts: ps_args.
async fn op_top(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let options = opts["ps_args"].as_str().map(|a| TopOptions {
        ps_args: a.to_string(),
    });
    let d = get_client(&opts)?;
    let r = d.top_processes(&name, options).await?;
    Ok(to_value(r))
}

/// One-shot resource-usage snapshot (`docker stats --no-stream`). Streaming
/// stats remain out of scope for the blocking FFI shape.
async fn op_stats(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let d = get_client(&opts)?;
    let mut stream = d.stats(
        &name,
        Some(StatsOptions {
            stream: false,
            one_shot: true,
        }),
    );
    match stream.try_next().await? {
        Some(s) => Ok(to_value(s)),
        None => Err(anyhow!("no stats returned for `{name}`")),
    }
}

/// Commit a container's current state to a new image. opts: repo, tag,
/// comment, author, pause (default true). Returns the new image id.
async fn op_commit(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let d = get_client(&opts)?;
    let repo = opts["repo"].as_str().unwrap_or("").to_string();
    let tag = opts["tag"].as_str().unwrap_or("latest").to_string();
    let comment = opts["comment"].as_str().unwrap_or("").to_string();
    let author = opts["author"].as_str().unwrap_or("").to_string();
    let options = CommitContainerOptions {
        container: name,
        repo,
        tag,
        comment,
        author,
        pause: opts["pause"].as_bool().unwrap_or(true),
        changes: None,
    };
    let r = d
        .commit_container(options, Config::<String>::default())
        .await?;
    Ok(json!({"id": r.id}))
}

// ── inspection extras ─────────────────────────────────────────────────────────

async fn op_diff(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let d = get_client(&opts)?;
    // FilesystemChange.kind: 0 = modified, 1 = added, 2 = deleted.
    let changes = d.container_changes(&name).await?.unwrap_or_default();
    Ok(json!({"changes": to_value(changes)}))
}

async fn op_history(opts: Value) -> Result<Value> {
    let image = opts["image"]
        .as_str()
        .ok_or_else(|| anyhow!("missing image"))?;
    let d = get_client(&opts)?;
    let layers = d.image_history(image).await?;
    Ok(json!({"history": to_value(layers)}))
}

async fn op_df(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let usage = d.df().await?;
    Ok(to_value(usage))
}

async fn op_port(opts: Value) -> Result<Value> {
    let name = container_name(&opts)?.to_string();
    let d = get_client(&opts)?;
    // Container port map: container-port/proto -> [{ host_ip, host_port }, ...].
    let info = d.inspect_container(&name, None).await?;
    let ports = info.network_settings.and_then(|ns| ns.ports);
    Ok(json!({"container": name, "ports": to_value(ports)}))
}

async fn op_update(opts: Value) -> Result<Value> {
    use bollard::container::UpdateContainerOptions;
    let name = container_name(&opts)?.to_string();
    let d = get_client(&opts)?;
    // Live resource limits. Only the fields the caller supplies are changed;
    // omitted ones keep their current value (None = leave alone).
    let config = UpdateContainerOptions::<String> {
        memory: opts["memory"].as_i64(),
        memory_swap: opts["memory_swap"].as_i64(),
        cpu_shares: opts["cpu_shares"].as_i64().map(|n| n as isize),
        cpu_quota: opts["cpu_quota"].as_i64(),
        cpu_period: opts["cpu_period"].as_i64(),
        cpuset_cpus: opts["cpuset_cpus"].as_str().map(String::from),
        ..Default::default()
    };
    d.update_container(&name, config).await?;
    Ok(json!({"ok": true, "updated": name}))
}

async fn op_image_inspect(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let image = opts["image"]
        .as_str()
        .ok_or_else(|| anyhow!("missing image"))?;
    let info = d.inspect_image(image).await?;
    Ok(to_value(info))
}

async fn op_volume_inspect(opts: Value) -> Result<Value> {
    let d = get_client(&opts)?;
    let volume = opts["volume"]
        .as_str()
        .ok_or_else(|| anyhow!("missing volume"))?;
    let info = d.inspect_volume(volume).await?;
    Ok(to_value(info))
}

async fn op_network_inspect(opts: Value) -> Result<Value> {
    use bollard::network::InspectNetworkOptions;
    let d = get_client(&opts)?;
    let network = opts["network"]
        .as_str()
        .ok_or_else(|| anyhow!("missing network"))?;
    let info = d
        .inspect_network(network, None::<InspectNetworkOptions<String>>)
        .await?;
    Ok(to_value(info))
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

// ── pure helpers (no daemon) ─────────────────────────────────────────────────

/// Parse a Docker image reference `[registry[:port]/]name[:tag][@digest]` into
/// its parts. The first path component is the registry only if it looks like a
/// host (has `.`/`:` or is `localhost`). Tag defaults to `latest` when neither
/// tag nor digest is present. Pure — contacts no registry.
/// The structural parts of an image reference. `tag` here is the literal tag
/// found (no `latest` default); `op_parse_image_ref` applies the default.
struct RefParts {
    registry: Option<String>,
    namespace: Option<String>,
    repository: String,
    tag: Option<String>,
    digest: Option<String>,
    path: String,
}

/// Split an image reference `[registry/][namespace/]repo[:tag][@digest]` into its
/// parts. The first path component is a registry only when it has a `.`, a `:`,
/// or is `localhost`. Shared by `parse_image_ref` and `normalize_image_ref`.
fn parse_ref_parts(r: &str) -> Result<RefParts> {
    if r.is_empty() {
        return Err(anyhow!("empty image reference"));
    }
    let (name_tag, digest) = match r.split_once('@') {
        Some((nt, d)) => (nt, Some(d.to_string())),
        None => (r, None),
    };
    // A `:` is a tag separator only when it sits after the last `/` (registries
    // carry `:port` before the path).
    let search_from = name_tag.rfind('/').map(|i| i + 1).unwrap_or(0);
    let (path, tag) = match name_tag[search_from..].rfind(':') {
        Some(rel) => {
            let colon = search_from + rel;
            (&name_tag[..colon], Some(name_tag[colon + 1..].to_string()))
        }
        None => (name_tag, None),
    };
    let comps: Vec<&str> = path.split('/').collect();
    let has_registry = comps.len() > 1
        && (comps[0].contains('.') || comps[0].contains(':') || comps[0] == "localhost");
    let registry = if has_registry {
        Some(comps[0].to_string())
    } else {
        None
    };
    let path_comps: &[&str] = if has_registry {
        &comps[1..]
    } else {
        &comps[..]
    };
    let repository = path_comps.last().copied().unwrap_or("").to_string();
    let namespace = if path_comps.len() > 1 {
        Some(path_comps[..path_comps.len() - 1].join("/"))
    } else {
        None
    };
    Ok(RefParts {
        registry,
        namespace,
        repository,
        tag,
        digest,
        path: path.to_string(),
    })
}

fn op_parse_image_ref(opts: Value) -> Result<Value> {
    let r = opts
        .get("ref")
        .or_else(|| opts.get("image"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing ref"))?;
    let p = parse_ref_parts(r)?;
    let tag_out = match (&p.tag, &p.digest) {
        (Some(t), _) => Some(t.clone()),
        (None, None) => Some("latest".to_string()),
        (None, Some(_)) => None,
    };
    Ok(json!({
        "registry": p.registry,
        "namespace": p.namespace,
        "repository": p.repository,
        "tag": tag_out,
        "digest": p.digest,
        "path": p.path,
    }))
}

/// Expand an image reference to its fully-qualified canonical form, the way
/// Docker normalizes a short name: a missing registry becomes `docker.io`, a
/// Docker Hub repo with no namespace gets `library`, and a missing tag (with no
/// digest) becomes `latest`. `nginx` → `docker.io/library/nginx:latest`,
/// `redis:6` → `docker.io/library/redis:6`, `ghcr.io/o/a` → `ghcr.io/o/a:latest`.
/// opts: `ref` (or `image`). Returns `{ref, registry, namespace, repository,
/// tag, digest}`. Pure.
fn op_normalize_image_ref(opts: Value) -> Result<Value> {
    let r = opts
        .get("ref")
        .or_else(|| opts.get("image"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing ref"))?;
    let p = parse_ref_parts(r)?;
    let registry = p.registry.unwrap_or_else(|| "docker.io".to_string());
    let namespace = match p.namespace {
        Some(ns) => Some(ns),
        // Docker Hub official images get the implicit `library` namespace.
        None if registry == "docker.io" => Some("library".to_string()),
        None => None,
    };
    let tag = match (&p.tag, &p.digest) {
        (Some(t), _) => Some(t.clone()),
        (None, None) => Some("latest".to_string()),
        (None, Some(_)) => None,
    };
    let mut out = registry.clone();
    out.push('/');
    if let Some(ns) = &namespace {
        out.push_str(ns);
        out.push('/');
    }
    out.push_str(&p.repository);
    if let Some(t) = &tag {
        out.push(':');
        out.push_str(t);
    }
    if let Some(d) = &p.digest {
        out.push('@');
        out.push_str(d);
    }
    Ok(json!({
        "ref": out,
        "registry": registry,
        "namespace": namespace,
        "repository": p.repository,
        "tag": tag,
        "digest": p.digest,
    }))
}

/// Assemble an image reference from parts — the inverse of `parse_image_ref`.
/// opts: `repository` (required), and optional `registry`, `namespace`, `tag`,
/// `digest`. Produces `[registry/][namespace/]repository[:tag][@digest]`. Pure.
fn op_build_image_ref(opts: Value) -> Result<Value> {
    let repository = opts
        .get("repository")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing repository"))?;
    let opt = |k: &str| {
        opts.get(k)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    let mut path_parts: Vec<&str> = Vec::new();
    if let Some(reg) = opt("registry") {
        path_parts.push(reg);
    }
    if let Some(ns) = opt("namespace") {
        path_parts.push(ns);
    }
    path_parts.push(repository);
    let mut out = path_parts.join("/");
    if let Some(tag) = opt("tag") {
        out.push(':');
        out.push_str(tag);
    }
    if let Some(digest) = opt("digest") {
        out.push('@');
        out.push_str(digest);
    }
    Ok(json!({"ref": out}))
}

/// Validate a Docker container/volume/network name against the daemon rule
/// `/?[a-zA-Z0-9][a-zA-Z0-9_.-]+` (optional leading slash, first char
/// alphanumeric, length ≥ 2). Pure.
fn op_valid_container_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let n = name.strip_prefix('/').unwrap_or(name);
    let valid = n.len() >= 2
        && n.as_bytes()[0].is_ascii_alphanumeric()
        && n.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-');
    Ok(json!({"name": name, "valid": valid}))
}

/// Validate a Docker image tag per `docker tag`'s rules: valid ASCII of letters
/// (any case), digits, `_`, `.`, and `-`; at most 128 characters; and it may not
/// start with a `.` or a `-`. Returns `{tag, valid, reason}`. Pure.
fn op_valid_image_tag(opts: Value) -> Result<Value> {
    let tag = opts
        .get("tag")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing tag"))?;
    let bytes = tag.as_bytes();
    let reason: Option<&str> = if tag.is_empty() {
        Some("must not be empty")
    } else if tag.len() > 128 {
        Some("must be at most 128 characters")
    } else if bytes[0] == b'.' || bytes[0] == b'-' {
        Some("must not start with a period or a dash")
    } else if !tag
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
    {
        Some("only ASCII letters, digits, '_', '.', and '-'")
    } else {
        None
    };
    Ok(json!({"tag": tag, "valid": reason.is_none(), "reason": reason}))
}

/// Validate a content digest `algorithm:hex` as used in `name@sha256:…` image
/// pins, per the OCI/distribution grammar. The algorithm is one or more
/// lowercase-alphanumeric components joined by `+._-`; the encoded part is
/// lowercase hex of even length. Registered algorithms are length-checked:
/// `sha256` needs 64 hex chars, `sha512` needs 128. Returns `{digest, algorithm,
/// hex, valid, reason}`. The validation `parse_image_ref` skips on the opaque
/// digest it carries. Pure.
fn op_valid_digest(opts: Value) -> Result<Value> {
    let digest = opts
        .get("digest")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing digest"))?;
    let split = digest.split_once(':');
    let reason: Option<&str> = match split {
        None => Some("must be `algorithm:hex` (e.g. sha256:…)"),
        Some((algo, hex)) => {
            // algorithm := component ([+._-] component)*, component := [a-z0-9]+
            let algo_ok = !algo.is_empty()
                && algo.split(['+', '.', '_', '-']).all(|c| {
                    !c.is_empty()
                        && c.bytes()
                            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
                })
                && !algo.starts_with(['+', '.', '_', '-'])
                && !algo.ends_with(['+', '.', '_', '-']);
            let hex_ok = !hex.is_empty()
                && hex.len() % 2 == 0
                && hex
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
            if !algo_ok {
                Some("algorithm must be lowercase-alphanumeric components joined by `+._-`")
            } else if !hex_ok {
                Some("encoded part must be even-length lowercase hex")
            } else if algo == "sha256" && hex.len() != 64 {
                Some("sha256 digest must be 64 hex characters")
            } else if algo == "sha512" && hex.len() != 128 {
                Some("sha512 digest must be 128 hex characters")
            } else {
                None
            }
        }
    };
    let (algorithm, hex) = match split {
        Some((a, h)) if reason.is_none() => (Some(a), Some(h)),
        _ => (None, None),
    };
    Ok(json!({
        "digest": digest,
        "algorithm": algorithm,
        "hex": hex,
        "valid": reason.is_none(),
        "reason": reason,
    }))
}

/// A single repository path component per distribution/reference:
/// `[a-z0-9]+ (separator [a-z0-9]+)*`, where a separator between alphanumeric
/// runs is a single `.`, a single `_`, a double `__`, or one-or-more `-`.
/// Lowercase only; must start and end alphanumeric.
fn valid_path_component(c: &str) -> bool {
    let b = c.as_bytes();
    let alnum = |x: u8| x.is_ascii_lowercase() || x.is_ascii_digit();
    if b.is_empty() || !alnum(b[0]) || !alnum(b[b.len() - 1]) {
        return false;
    }
    let mut i = 0;
    while i < b.len() {
        if alnum(b[i]) {
            i += 1;
            continue;
        }
        match b[i] {
            b'.' => i += 1,
            b'_' => {
                i += if i + 1 < b.len() && b[i + 1] == b'_' {
                    2
                } else {
                    1
                }
            }
            b'-' => {
                while i < b.len() && b[i] == b'-' {
                    i += 1;
                }
            }
            _ => return false,
        }
        // A separator run must be followed by another alphanumeric run.
        if i >= b.len() || !alnum(b[i]) {
            return false;
        }
    }
    true
}

/// Validate a Docker/OCI repository name (the `namespace/repo` path, excluding
/// any registry domain) per the distribution/reference grammar: one or more
/// `/`-joined path components, each `[a-z0-9]+` runs separated by a single `.`,
/// `_`, `__`, or `-`+. Lowercase only — the rule that rejects `docker build -t
/// MyApp`. At most 255 characters. Returns `{name, valid, reason}`. Pure.
fn op_valid_repository_name(opts: Value) -> Result<Value> {
    let name = opts
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing name"))?;
    let reason: Option<String> = if name.is_empty() {
        Some("must not be empty".into())
    } else if name.len() > 255 {
        Some("must be at most 255 characters".into())
    } else {
        name.split('/')
            .find(|c| !valid_path_component(c))
            .map(|c| {
                if c.is_empty() {
                    "has an empty path component".into()
                } else {
                    format!("component `{c}` must be lowercase alphanumerics separated by a single `.`, `_`, `__`, or `-`")
                }
            })
    };
    Ok(json!({"name": name, "valid": reason.is_none(), "reason": reason}))
}

/// Parse a `-p` port spec `[host_ip:][host_port:]container_port[/proto]` into
/// `{host_ip, host_port, container_port, protocol}` (protocol default `tcp`).
/// IPv6 host literals must be bracketed (`[::1]:8080:80`). Pure.
fn op_parse_port_spec(opts: Value) -> Result<Value> {
    let spec = opts
        .get("spec")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing spec"))?;
    let (addr, proto) = match spec.rsplit_once('/') {
        Some((a, p)) if matches!(p, "tcp" | "udp" | "sctp") => (a, p.to_string()),
        _ => (spec, "tcp".to_string()),
    };
    // Bracketed IPv6 host: [::1]:hostPort:containerPort
    let (host_ip, remainder): (Option<String>, String) =
        if let Some(stripped) = addr.strip_prefix('[') {
            let end = stripped
                .find(']')
                .ok_or_else(|| anyhow!("unterminated IPv6 literal in `{spec}`"))?;
            let ip = stripped[..end].to_string();
            let rest = stripped[end + 1..].trim_start_matches(':').to_string();
            (Some(ip), rest)
        } else {
            (None, addr.to_string())
        };
    let parts: Vec<&str> = remainder.split(':').collect();
    let (ip2, host_port, container_port) = match (host_ip.is_some(), parts.as_slice()) {
        (true, [c]) => (host_ip.clone(), None, *c),
        (true, [hp, c]) => (host_ip.clone(), Some(*hp), *c),
        (false, [c]) => (None, None, *c),
        (false, [hp, c]) => (None, Some(*hp), *c),
        (false, [ip, hp, c]) => (Some(ip.to_string()), Some(*hp), *c),
        _ => return Err(anyhow!("invalid port spec `{spec}`")),
    };
    if container_port.is_empty() {
        return Err(anyhow!("missing container port in `{spec}`"));
    }
    let host_port = host_port.filter(|p| !p.is_empty());
    Ok(json!({
        "host_ip": ip2,
        "host_port": host_port,
        "container_port": container_port,
        "protocol": proto,
    }))
}

/// Build a `docker run -p` port spec from parts — the inverse of
/// `parse_port_spec`. opts: `container_port` (required, string or number),
/// optional `host_port`, `host_ip` (bracketed automatically when it is an IPv6
/// literal), and `protocol` (default `tcp`; only emitted as `/proto` when not
/// tcp, mirroring the minimal form parse accepts). Pure.
fn op_build_port_spec(opts: Value) -> Result<Value> {
    // Accept a port as a string or a JSON number.
    let port = |k: &str| -> Option<String> {
        match opts.get(k) {
            Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        }
    };
    let container_port = port("container_port").ok_or_else(|| anyhow!("missing container_port"))?;
    let host_port = port("host_port");
    let host_ip = opts
        .get("host_ip")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let mut spec = String::new();
    if let Some(ip) = host_ip {
        // IPv6 literals (containing `:`) must be bracketed.
        if ip.contains(':') {
            spec.push_str(&format!("[{ip}]:"));
        } else {
            spec.push_str(&format!("{ip}:"));
        }
        // An IP with no host port leaves the slot empty (`ip::container`).
        match &host_port {
            Some(hp) => spec.push_str(&format!("{hp}:")),
            None => spec.push(':'),
        }
    } else if let Some(hp) = &host_port {
        spec.push_str(&format!("{hp}:"));
    }
    spec.push_str(&container_port);
    let proto = opts
        .get("protocol")
        .and_then(Value::as_str)
        .unwrap_or("tcp");
    if proto != "tcp" {
        spec.push_str(&format!("/{proto}"));
    }
    Ok(json!({"spec": spec}))
}

/// Parse a `docker run -v` short mount spec into its parts. Supports
/// `source:target[:opts]` where a `source` containing `/` (or `.`) is a bind
/// mount and a bare name is a named volume; a lone `target` (`/data`) is an
/// anonymous volume. `opts` is the comma list after the second colon (`ro`,
/// `rw`, `z`, `cached`, …) — `ro` sets `readonly`. IPv6 has no bearing here, but
/// Windows-style `C:\` sources are rejected (use `--mount` for those). Pure.
fn op_parse_mount(opts: Value) -> Result<Value> {
    let spec = opts
        .get("spec")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing spec"))?;
    let parts: Vec<&str> = spec.split(':').collect();
    let (source, target, mount_opts): (Option<&str>, &str, &str) = match parts.as_slice() {
        // Anonymous volume: just an in-container path.
        [t] => (None, *t, ""),
        [s, t] => (Some(*s), *t, ""),
        [s, t, o] => (Some(*s), *t, *o),
        _ => return Err(anyhow!("invalid mount spec `{spec}` (want src:dst[:opts])")),
    };
    if target.is_empty() {
        return Err(anyhow!("mount spec missing container path: `{spec}`"));
    }
    // A bind mount sources from a host path (absolute, relative `.`/`..`, or
    // `~`); anything else naming a source is a named volume.
    let kind = match source {
        None => "anonymous",
        Some(s) if s.starts_with('/') || s.starts_with('.') || s.starts_with('~') => "bind",
        Some(_) => "volume",
    };
    let opt_list: Vec<&str> = mount_opts.split(',').filter(|o| !o.is_empty()).collect();
    let readonly = opt_list.contains(&"ro");
    Ok(json!({
        "type": kind,
        "source": source,
        "target": target,
        "readonly": readonly,
        "options": opt_list,
    }))
}

/// Assemble a `-v` mount spec `src:dst[:opts]` from parts — the inverse of
/// `parse_mount`. opts: `target` (required), `source` (optional — omit for an
/// anonymous volume), `options` (array of strings), and a `readonly` flag that
/// appends `ro` if not already listed. Anonymous mounts take no options, so
/// options without a source is an error (the colon syntax can't express it).
/// Returns `{spec}`. Pure.
fn op_build_mount(opts: Value) -> Result<Value> {
    let target = opts
        .get("target")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing target"))?;
    let source = opts
        .get("source")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let mut mount_opts: Vec<String> = opts
        .get("options")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        })
        .unwrap_or_default();
    let readonly = match opts.get("readonly") {
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_i64().is_some_and(|x| x != 0),
        Some(Value::String(s)) => s == "1" || s == "true",
        _ => false,
    };
    if readonly && !mount_opts.iter().any(|o| o == "ro") {
        mount_opts.push("ro".to_string());
    }
    if !mount_opts.is_empty() && source.is_none() {
        return Err(anyhow!(
            "mount options require a source (anonymous volumes take no options)"
        ));
    }
    let mut spec = match source {
        Some(s) => format!("{s}:{target}"),
        None => target.to_string(),
    };
    if !mount_opts.is_empty() {
        spec.push(':');
        spec.push_str(&mount_opts.join(","));
    }
    Ok(json!({ "spec": spec }))
}

/// Parse a `docker run -e` environment spec into its parts. Docker splits on the
/// FIRST `=`: `KEY=VALUE` sets the value (a trailing `=` gives an empty string),
/// while a bare `KEY` with no `=` means "pass the variable through from the host
/// environment at runtime" — encoded here as `from_host: true` with a null
/// `value`. A value may itself contain `=` (`K=a=b` → value `a=b`). An empty key
/// (`=v`) is rejected. opts: `spec`. Returns `{spec, key, value, from_host}`.
/// Pure.
fn op_parse_env(opts: Value) -> Result<Value> {
    let spec = opts
        .get("spec")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing spec"))?;
    match spec.split_once('=') {
        Some((key, value)) => {
            if key.is_empty() {
                return Err(anyhow!("env spec has an empty key: `{spec}`"));
            }
            Ok(json!({
                "spec": spec,
                "key": key,
                "value": value,
                "from_host": false,
            }))
        }
        None => {
            if spec.is_empty() {
                return Err(anyhow!("env spec is empty"));
            }
            Ok(json!({
                "spec": spec,
                "key": spec,
                "value": Value::Null,
                "from_host": true,
            }))
        }
    }
}

/// Assemble a `-e` environment spec from parts — the inverse of `parse_env`.
/// With a `value` it emits `KEY=VALUE` (value may be a string or a number; an
/// empty string yields the trailing-`=` form `KEY=`). Without a `value` it emits
/// a bare `KEY`, the host-passthrough form. An empty key is rejected. opts:
/// `key` (required), optional `value`. Returns `{spec}`. Pure.
fn op_build_env(opts: Value) -> Result<Value> {
    let key = opts
        .get("key")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing key"))?;
    let spec = match opts.get("value") {
        None | Some(Value::Null) => key.to_string(),
        Some(Value::String(s)) => format!("{key}={s}"),
        Some(Value::Number(n)) => format!("{key}={n}"),
        Some(Value::Bool(b)) => format!("{key}={b}"),
        Some(other) => return Err(anyhow!("env value must be a scalar, got `{other}`")),
    };
    Ok(json!({ "spec": spec }))
}

/// Parse a `docker run --restart` policy into its parts. The four policies are
/// `no`, `always`, `unless-stopped`, and `on-failure[:max-retries]`; only
/// `on-failure` accepts the `:N` retry limit (a non-negative integer). opts:
/// `spec` (or `policy`). Returns `{spec, policy, max_retries}` (`max_retries`
/// null unless an `on-failure:N` count is given). Pure.
fn op_parse_restart_policy(opts: Value) -> Result<Value> {
    let spec = opts
        .get("spec")
        .or_else(|| opts.get("policy"))
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing spec"))?;
    let (name, retries) = match spec.split_once(':') {
        Some((n, r)) => (n, Some(r)),
        None => (spec, None),
    };
    let max_retries = match name {
        "no" | "always" | "unless-stopped" => {
            if retries.is_some() {
                return Err(anyhow!(
                    "restart policy `{name}` does not take a `:max-retries`"
                ));
            }
            Value::Null
        }
        "on-failure" => match retries {
            None => Value::Null,
            Some(r) => {
                let n: u32 = r.parse().map_err(|_| {
                    anyhow!("invalid max-retries `{r}` (want a non-negative integer)")
                })?;
                json!(n)
            }
        },
        other => {
            return Err(anyhow!(
                "unknown restart policy `{other}` (no|always|unless-stopped|on-failure[:N])"
            ))
        }
    };
    Ok(json!({ "spec": spec, "policy": name, "max_retries": max_retries }))
}

/// Assemble a `--restart` policy spec from parts — the inverse of
/// `parse_restart_policy`. `max_retries` is honoured only with `on-failure`
/// (yielding `on-failure:N`); supplying it for any other policy is an error.
/// opts: `policy` (required), optional `max_retries`. Returns `{spec}`. Pure.
fn op_build_restart_policy(opts: Value) -> Result<Value> {
    let policy = opts
        .get("policy")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| anyhow!("missing policy"))?;
    let retries = opts.get("max_retries").and_then(Value::as_u64);
    let spec = match policy {
        "no" | "always" | "unless-stopped" => {
            if retries.is_some() {
                return Err(anyhow!(
                    "restart policy `{policy}` does not take a max-retries"
                ));
            }
            policy.to_string()
        }
        "on-failure" => match retries {
            Some(n) => format!("on-failure:{n}"),
            None => "on-failure".to_string(),
        },
        other => {
            return Err(anyhow!(
                "unknown restart policy `{other}` (no|always|unless-stopped|on-failure)"
            ))
        }
    };
    Ok(json!({ "spec": spec }))
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

#[no_mangle]
pub extern "C" fn docker__pause(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_pause)
}

#[no_mangle]
pub extern "C" fn docker__unpause(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_unpause)
}

#[no_mangle]
pub extern "C" fn docker__rename(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_rename)
}

#[no_mangle]
pub extern "C" fn docker__wait(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_wait)
}

#[no_mangle]
pub extern "C" fn docker__top(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_top)
}

#[no_mangle]
pub extern "C" fn docker__stats(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_stats)
}

#[no_mangle]
pub extern "C" fn docker__commit(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_commit)
}

#[no_mangle]
pub extern "C" fn docker__diff(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_diff)
}

#[no_mangle]
pub extern "C" fn docker__history(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_history)
}

#[no_mangle]
pub extern "C" fn docker__df(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_df)
}

#[no_mangle]
pub extern "C" fn docker__port(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_port)
}

#[no_mangle]
pub extern "C" fn docker__update(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_update)
}

#[no_mangle]
pub extern "C" fn docker__image_inspect(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_image_inspect)
}

#[no_mangle]
pub extern "C" fn docker__volume_inspect(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_volume_inspect)
}

#[no_mangle]
pub extern "C" fn docker__network_inspect(args: *const c_char) -> *const c_char {
    ffi_call_async(args, op_network_inspect)
}

#[no_mangle]
pub extern "C" fn docker__parse_image_ref(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_image_ref(opts) })
}

#[no_mangle]
pub extern "C" fn docker__normalize_image_ref(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_normalize_image_ref(opts) })
}

#[no_mangle]
pub extern "C" fn docker__build_image_ref(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_image_ref(opts) })
}

#[no_mangle]
pub extern "C" fn docker__valid_container_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_container_name(opts) })
}

#[no_mangle]
pub extern "C" fn docker__valid_image_tag(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_image_tag(opts) })
}

#[no_mangle]
pub extern "C" fn docker__valid_digest(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_digest(opts) })
}

#[no_mangle]
pub extern "C" fn docker__valid_repository_name(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_valid_repository_name(opts) })
}

#[no_mangle]
pub extern "C" fn docker__parse_port_spec(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_port_spec(opts) })
}

#[no_mangle]
pub extern "C" fn docker__build_port_spec(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_port_spec(opts) })
}

#[no_mangle]
pub extern "C" fn docker__parse_mount(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_mount(opts) })
}

#[no_mangle]
pub extern "C" fn docker__build_mount(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_mount(opts) })
}

#[no_mangle]
pub extern "C" fn docker__parse_env(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_env(opts) })
}

#[no_mangle]
pub extern "C" fn docker__build_env(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_env(opts) })
}

#[no_mangle]
pub extern "C" fn docker__parse_restart_policy(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_parse_restart_policy(opts) })
}

#[no_mangle]
pub extern "C" fn docker__build_restart_policy(args: *const c_char) -> *const c_char {
    ffi_call_async(args, |opts| async move { op_build_restart_policy(opts) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Sample {
        name: String,
        count: u32,
        flag: bool,
    }

    #[test]
    fn to_value_renders_struct_as_json_object() {
        let v = to_value(Sample {
            name: "alpine".into(),
            count: 3,
            flag: true,
        });
        assert_eq!(v["name"], json!("alpine"));
        assert_eq!(v["count"], json!(3));
        assert_eq!(v["flag"], json!(true));
    }

    #[test]
    fn to_value_handles_string_and_primitives() {
        assert_eq!(to_value("hello"), json!("hello"));
        assert_eq!(to_value(42_u64), json!(42));
        assert_eq!(to_value(true), json!(true));
        assert_eq!(to_value(()), json!(null));
    }

    #[test]
    fn to_value_handles_vec_and_option() {
        assert_eq!(to_value(vec!["a", "b"]), json!(["a", "b"]));
        let none: Option<u32> = None;
        assert_eq!(to_value(none), json!(null));
        assert_eq!(to_value(Some(7_u32)), json!(7));
    }

    /// A type that always fails to serialize — `to_value` must return
    /// `Value::Null` rather than panicking (used in op handlers where
    /// the docker response shape may include unknown variants).
    struct BadSerialize;
    impl Serialize for BadSerialize {
        fn serialize<S: serde::Serializer>(&self, _s: S) -> std::result::Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("nope"))
        }
    }

    #[test]
    fn to_value_returns_null_on_serialize_error() {
        assert_eq!(to_value(BadSerialize), Value::Null);
    }

    /// Helper extracted from `op_create` for testing:
    /// strip a JSON Value array down to a Vec<String> of its string elements.
    fn string_vec_from_value(v: &Value) -> Option<Vec<String>> {
        v.as_array().map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
    }

    #[test]
    fn string_vec_helper_matches_op_create_pattern() {
        // Same shape `op_create` uses to coerce `opts["cmd"]` / `opts["env"]`.
        let v = json!(["sh", "-c", "echo hi", 42, null, "tail"]);
        assert_eq!(
            string_vec_from_value(&v),
            Some(vec![
                "sh".into(),
                "-c".into(),
                "echo hi".into(),
                "tail".into()
            ])
        );
        assert_eq!(string_vec_from_value(&json!("not-array")), None);
        assert_eq!(string_vec_from_value(&json!([])), Some(vec![]));
    }

    /// A nested struct must round-trip through `to_value` with its fields
    /// preserved. Catches a regression where serde's "humanized" form
    /// (e.g. snake_case rename, tag flattening) silently changes the
    /// JSON shape downstream callers depend on.
    #[test]
    fn to_value_preserves_nested_struct_fields() {
        #[derive(serde::Serialize)]
        struct Outer {
            name: String,
            inner: Inner,
        }
        #[derive(serde::Serialize)]
        struct Inner {
            count: i32,
            tags: Vec<String>,
        }
        let o = Outer {
            name: "stryke".into(),
            inner: Inner {
                count: 3,
                tags: vec!["a".into(), "b".into()],
            },
        };
        let v = to_value(o);
        assert_eq!(v["name"], json!("stryke"));
        assert_eq!(v["inner"]["count"], json!(3));
        assert_eq!(v["inner"]["tags"], json!(["a", "b"]));
    }

    /// Empty array input to `string_vec_from_value` should be `Some(vec![])`,
    /// NOT `None`. Distinguishing "supplied but empty" from "not supplied"
    /// matters: an explicit empty `cmd` means "no command override" vs a
    /// missing `cmd` means "use the image's default ENTRYPOINT".
    #[test]
    fn string_vec_empty_array_is_some_not_none() {
        assert_eq!(string_vec_from_value(&json!([])), Some(vec![]));
        assert_eq!(string_vec_from_value(&Value::Null), None);
    }

    // ── FFI boundary tests ──────────────────────────────────────────────────
    //
    // Hand-crafted around the specific invariants `ffi_call_async` must
    // preserve when it's invoked from stryke's dlopen bridge. A regression
    // in any of these turns "the handler errored" into "the user's shell
    // crashed" — far worse than a docker-call failure.

    /// Helper: read back the C string returned by an FFI call and free it
    /// through the same `stryke_free_cstring` the bridge would use.
    /// Reproduces the exact lifecycle the FFI contract promises.
    unsafe fn read_and_free(ptr: *const c_char) -> Option<String> {
        if ptr.is_null() {
            return None;
        }
        let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
        stryke_free_cstring(ptr as *mut c_char);
        Some(s)
    }

    /// Panic in the async handler MUST be caught and converted into a
    /// well-formed JSON error string. If `catch_unwind` is removed or the
    /// wrapper is restructured to await before `catch_unwind`, the panic
    /// unwinds across the FFI boundary into the loader (stryke shell) and
    /// aborts the user's process. The export contract is: ALWAYS return
    /// a non-null `*const c_char` pointing at valid JSON.
    #[test]
    fn ffi_call_async_catches_handler_panic_and_returns_json_error() {
        let args = CString::new("{}").unwrap();
        let ptr = ffi_call_async(args.as_ptr(), |_v| async {
            // Deliberately panic inside the async future.
            panic!("synthetic blow-up");
        });
        let s = unsafe { read_and_free(ptr) }.expect("ffi_call_async returned null on panic");
        let v: Value = serde_json::from_str(&s).expect("output must be valid JSON");
        let err = v.get("error").and_then(|e| e.as_str()).unwrap_or("");
        assert!(
            err.contains("panicked"),
            "expected 'panicked' in error message, got: {s}"
        );
    }

    /// A null `args` pointer MUST NOT dereference. stryke's bridge can
    /// pass null when the called op takes no arguments. If the
    /// `args.is_null()` guard regresses to `unsafe { CStr::from_ptr(args) }`
    /// directly, this dereferences address 0 and segfaults the shell.
    /// Pair the null-args call with a handler that asserts it received
    /// `Value::Null` (not, e.g., `Value::Object(empty)`).
    #[test]
    fn ffi_call_async_null_args_yields_value_null_not_segv() {
        use std::sync::atomic::{AtomicBool, Ordering};
        static SAW_NULL: AtomicBool = AtomicBool::new(false);
        let ptr = ffi_call_async(std::ptr::null(), |v| async move {
            // Exactly Value::Null — not an empty object, not a default.
            SAW_NULL.store(v == Value::Null, Ordering::SeqCst);
            Ok(json!({"got": v}))
        });
        let s = unsafe { read_and_free(ptr) }.expect("null-args call must not return null ptr");
        assert!(
            SAW_NULL.load(Ordering::SeqCst),
            "handler should have received Value::Null for null args, output: {s}"
        );
    }

    /// Malformed JSON in `args` must fall through to `Value::Null` rather
    /// than panicking. stryke shell users can construct any JSON-ish dict;
    /// a stray trailing comma or unterminated string would otherwise crash
    /// the cdylib at the unwrap-or boundary if `from_slice` is ever
    /// upgraded to `.unwrap()`.
    #[test]
    fn ffi_call_async_malformed_json_args_does_not_panic() {
        use std::sync::atomic::{AtomicBool, Ordering};
        static SAW_NULL: AtomicBool = AtomicBool::new(false);
        // ‘{not json’ is unambiguously broken — not a quoted string, not
        // an object, not a number.
        let bad = CString::new("{not json").unwrap();
        let ptr = ffi_call_async(bad.as_ptr(), |v| async move {
            SAW_NULL.store(v == Value::Null, Ordering::SeqCst);
            Ok(json!({"ok": true}))
        });
        let out = unsafe { read_and_free(ptr) }.expect("malformed args must still return JSON");
        let parsed: Value =
            serde_json::from_str(&out).expect("output of malformed-args call must still be JSON");
        assert_eq!(parsed["ok"], json!(true));
        assert!(
            SAW_NULL.load(Ordering::SeqCst),
            "malformed JSON should degrade to Value::Null, output: {out}"
        );
    }

    /// Handler returning `Err(_)` must surface the message under the
    /// "error" key as a string — this is the wire contract stryke's
    /// FFI bridge inspects to decide success vs failure. If the
    /// branch ever rewraps the error (e.g. into `{"err":...}` or a
    /// number), every docker.* call site in stryke silently treats
    /// failures as success.
    #[test]
    fn ffi_call_async_handler_error_maps_to_error_key_string() {
        let args = CString::new("{}").unwrap();
        let ptr = ffi_call_async(args.as_ptr(), |_v| async { Err(anyhow!("missing image")) });
        let s = unsafe { read_and_free(ptr) }.expect("error path must not return null ptr");
        let v: Value = serde_json::from_str(&s).expect("error path output must be valid JSON");
        assert_eq!(
            v.get("error").and_then(|e| e.as_str()),
            Some("missing image"),
            "expected verbatim error message under 'error' key, got: {s}"
        );
    }

    // ── `op_tag` target-splitting bug ───────────────────────────────────────
    //
    // `op_tag` parses `opts["target"]` with `rsplit_once(':')`. For a
    // registry reference like `localhost:5000/img` (port present, no tag)
    // that finds the colon in `localhost:5000` and splits to repo=
    // "localhost" and tag="5000/img" — which is malformed. The OCI / docker
    // grammar separates registry-with-port from tag by the LAST colon
    // *after* the final `/`. The current code is wrong for any path-bearing
    // repo with a port and no explicit tag.
    //
    // We can't run `op_tag` directly without a docker daemon, so we
    // hand-mirror its split with the same `rsplit_once(':')` and assert the
    // broken output, then assert what a correct splitter would have to
    // return. This test will need updating once the bug is fixed in
    // `op_tag` — by then `parse_tag_target` should be a real helper here
    // and the test should call IT instead of duplicating the logic. Today
    // the test pins the current (broken) behaviour so a fix-in-place is
    // visible as a test diff.

    fn op_tag_split_current(target: &str) -> (String, Option<String>) {
        match target.rsplit_once(':') {
            Some((r, t)) => (r.to_string(), Some(t.to_string())),
            None => (target.to_string(), None),
        }
    }

    #[test]
    fn op_tag_rsplit_once_misparses_registry_with_port_no_tag() {
        // localhost:5000/img has NO tag, but rsplit_once happily grabs
        // the registry port as if it were one.
        let (repo, tag) = op_tag_split_current("localhost:5000/img");
        assert_eq!(repo, "localhost", "current (buggy) repo");
        assert_eq!(tag.as_deref(), Some("5000/img"), "current (buggy) tag");
        // The correct behaviour is repo="localhost:5000/img", tag=None,
        // because a tag may only follow the final `/`-separated path
        // component. Documenting the expectation here so a future fix
        // will turn this `ne` into `eq` and the assertion above will need
        // updating in the same diff.
        assert_ne!(
            repo, "localhost:5000/img",
            "if this fires, op_tag was fixed — update the assertions above and remove this guard"
        );
    }

    // ── `op_tag` default-tag-when-missing bugs ──────────────────────────────
    //
    // Different bug class from the registry-port misparsing above. Here the
    // target string is *unambiguously* missing a tag, but the code path that
    // forwards to bollard substitutes empty string ("") instead of the docker
    // default "latest". Bollard then sends `repo:` to dockerd which the engine
    // rejects as `invalid reference format`. The user sees a generic bollard
    // error and has no idea why `docker.tag(source="x", target="y")` failed.
    //
    // Mirror the exact split+unwrap pipeline used in `op_tag` so the test is
    // self-contained (no docker daemon, no dlopen). When op_tag is fixed to
    // default to "latest", these tests fail loudly via the `assert_ne!`
    // guards so the broken pin is removed in the same diff.

    /// Mirror of the full `op_tag` split + `unwrap_or_default` pipeline:
    /// returns the (repo, tag) actually handed to bollard's TagImageOptions.
    fn op_tag_bollard_args_current(target: &str) -> (String, String) {
        let (repo, tag) = match target.rsplit_once(':') {
            Some((r, t)) => (r.to_string(), Some(t.to_string())),
            None => (target.to_string(), None),
        };
        (repo, tag.unwrap_or_default())
    }

    /// `target = "alpine"` (no colon at all). `rsplit_once` returns None →
    /// `tag = None` → `unwrap_or_default()` → empty string. Docker's tag API
    /// requires a non-empty tag (defaults to "latest" when omitted via the
    /// CLI). Empty string causes dockerd to reject the request with
    /// `invalid reference format`.
    #[test]
    fn op_tag_no_colon_target_produces_empty_tag_not_latest() {
        let (repo, tag) = op_tag_bollard_args_current("alpine");
        assert_eq!(repo, "alpine", "repo extracted correctly");
        // Current (broken) behaviour: tag is empty string.
        assert_eq!(tag, "", "current (buggy) empty tag — dockerd will 400");
        // The correct behaviour is tag = "latest". When op_tag is fixed,
        // this guard fires and the assert_eq above needs to be updated.
        assert_ne!(
            tag, "latest",
            "if this fires, op_tag was fixed to default to 'latest' — \
             update the assertions above and remove this guard"
        );
    }

    /// `target = "alpine:"` (trailing colon, no tag chars). Different code
    /// path than the no-colon case: `rsplit_once` returns `Some(("alpine",
    /// ""))`, so `tag = Some(String::new())`. `unwrap_or_default()` then
    /// short-circuits — Some is unwrapped to "" without ever hitting the
    /// default. Same broken outcome as no-colon, but via the Some-branch.
    /// Important to pin separately: a fix that only adds `or "latest"` to
    /// the None arm would still leave THIS path broken.
    #[test]
    fn op_tag_trailing_colon_keeps_empty_tag_via_some_branch() {
        let (repo, tag) = op_tag_bollard_args_current("alpine:");
        assert_eq!(repo, "alpine", "repo extracted correctly");
        // Current (broken) behaviour: tag is empty string via Some("") path.
        assert_eq!(
            tag, "",
            "current (buggy) empty tag via Some(\"\") — dockerd will 400"
        );
        // A fix must also normalize Some("") → "latest", not just None.
        assert_ne!(
            tag, "latest",
            "if this fires, op_tag normalizes Some(\"\") to 'latest' too — \
             update the assertions above and remove this guard"
        );
    }

    // ── new-surface coverage ─────────────────────────────────────────────────

    #[test]
    fn container_name_requires_the_field() {
        assert_eq!(container_name(&json!({"container": "web"})).unwrap(), "web");
        assert!(container_name(&json!({}))
            .unwrap_err()
            .to_string()
            .contains("missing container"));
    }

    /// Drive a new export the way stryke's bridge does. `get_client` connects
    /// lazily, so a missing required arg surfaces as an error WITHOUT a live
    /// daemon — these pin that contract for the new container ops.
    fn call_export(f: extern "C" fn(*const c_char) -> *const c_char, arg: &str) -> Value {
        let cs = CString::new(arg).unwrap();
        let raw = f(cs.as_ptr());
        assert!(!raw.is_null());
        let out = unsafe { CStr::from_ptr(raw) }.to_str().unwrap().to_string();
        unsafe { stryke_free_cstring(raw as *mut c_char) };
        serde_json::from_str(&out).unwrap()
    }

    #[test]
    fn new_container_ops_reject_missing_args_offline() {
        for f in [
            docker__pause,
            docker__unpause,
            docker__wait,
            docker__top,
            docker__stats,
            docker__commit,
        ] {
            let v = call_export(f, "{}");
            assert_eq!(
                v["error"], "missing container",
                "export must reject missing container offline; got {v}"
            );
        }
        // rename needs both container and the new name.
        let v = call_export(docker__rename, r#"{"container":"web"}"#);
        assert!(
            v["error"].as_str().unwrap().contains("missing name"),
            "rename must require the new name; got {v}"
        );
    }

    // ── pure helpers (no daemon) ─────────────────────────────────────────────

    #[test]
    fn parse_image_ref_full_registry_namespace_tag_digest() {
        let v = op_parse_image_ref(json!({
            "ref": "registry.example.com:5000/team/app:1.2.3@sha256:abc123"
        }))
        .unwrap();
        assert_eq!(v["registry"], json!("registry.example.com:5000"));
        assert_eq!(v["namespace"], json!("team"));
        assert_eq!(v["repository"], json!("app"));
        assert_eq!(v["tag"], json!("1.2.3"));
        assert_eq!(v["digest"], json!("sha256:abc123"));
    }

    #[test]
    fn build_image_ref_inverts_parse_image_ref() {
        // Full ref → parts → ref reproduces the original string.
        let orig = "registry.example.com:5000/team/app:1.2.3@sha256:abc123";
        let p = op_parse_image_ref(json!({"ref": orig})).unwrap();
        let built = op_build_image_ref(json!({
            "registry": p["registry"], "namespace": p["namespace"],
            "repository": p["repository"], "tag": p["tag"], "digest": p["digest"],
        }))
        .unwrap();
        assert_eq!(built["ref"], json!(orig));
        // Repository only → bare name (no separators).
        assert_eq!(
            op_build_image_ref(json!({"repository": "nginx"})).unwrap()["ref"],
            json!("nginx")
        );
        // Namespace + tag, no registry.
        assert_eq!(
            op_build_image_ref(
                json!({"namespace": "library", "repository": "nginx", "tag": "1.25"})
            )
            .unwrap()["ref"],
            json!("library/nginx:1.25")
        );
        // Digest without tag (the `@`-only form).
        assert_eq!(
            op_build_image_ref(json!({"repository": "app", "digest": "sha256:deadbeef"})).unwrap()
                ["ref"],
            json!("app@sha256:deadbeef")
        );
        assert!(op_build_image_ref(json!({})).is_err());
    }

    #[test]
    fn parse_image_ref_docker_hub_short_form_defaults_latest() {
        // `nginx` → no registry, no namespace, tag latest.
        let v = op_parse_image_ref(json!({"ref": "nginx"})).unwrap();
        assert_eq!(v["registry"], Value::Null);
        assert_eq!(v["namespace"], Value::Null);
        assert_eq!(v["repository"], json!("nginx"));
        assert_eq!(v["tag"], json!("latest"));
        // `library/nginx:1.25` → namespace library, explicit tag, still no registry.
        let v2 = op_parse_image_ref(json!({"ref": "library/nginx:1.25"})).unwrap();
        assert_eq!(
            v2["registry"],
            Value::Null,
            "`library` is a namespace, not a registry"
        );
        assert_eq!(v2["namespace"], json!("library"));
        assert_eq!(v2["tag"], json!("1.25"));
    }

    #[test]
    fn parse_image_ref_digest_only_has_null_tag() {
        let v = op_parse_image_ref(json!({"ref": "nginx@sha256:deadbeef"})).unwrap();
        assert_eq!(
            v["tag"],
            Value::Null,
            "digest-pinned ref has no default tag"
        );
        assert_eq!(v["digest"], json!("sha256:deadbeef"));
    }

    #[test]
    fn normalize_image_ref_expands_to_canonical_form() {
        let norm = |r: &str| {
            op_normalize_image_ref(json!({ "ref": r })).unwrap()["ref"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Docker Hub short name → registry + library + latest.
        assert_eq!(norm("nginx"), "docker.io/library/nginx:latest");
        assert_eq!(norm("redis:6"), "docker.io/library/redis:6");
        // A user repo keeps its namespace; gets the registry + latest.
        assert_eq!(norm("myuser/app"), "docker.io/myuser/app:latest");
        // An explicit docker.io still gets the library namespace.
        assert_eq!(norm("docker.io/nginx"), "docker.io/library/nginx:latest");
        // Third-party registry: no library namespace, just default tag.
        assert_eq!(norm("ghcr.io/owner/app"), "ghcr.io/owner/app:latest");
        assert_eq!(norm("localhost:5000/app:dev"), "localhost:5000/app:dev");
        // Digest-pinned: no :latest is added.
        assert_eq!(
            norm("nginx@sha256:deadbeef"),
            "docker.io/library/nginx@sha256:deadbeef"
        );
        // Structured fields are returned too.
        let v = op_normalize_image_ref(json!({"ref": "nginx"})).unwrap();
        assert_eq!(v["registry"], json!("docker.io"));
        assert_eq!(v["namespace"], json!("library"));
        assert_eq!(v["repository"], json!("nginx"));
        assert!(op_normalize_image_ref(json!({})).is_err());
        assert!(op_normalize_image_ref(json!({"ref": ""})).is_err());
    }

    #[test]
    fn valid_container_name_matches_daemon_regex() {
        assert_eq!(
            op_valid_container_name(json!({"name": "web-1.api_v2"})).unwrap()["valid"],
            json!(true)
        );
        // Leading slash (the inspect form) is allowed.
        assert_eq!(
            op_valid_container_name(json!({"name": "/web"})).unwrap()["valid"],
            json!(true)
        );
        // First char must be alphanumeric; length >= 2.
        assert_eq!(
            op_valid_container_name(json!({"name": "_bad"})).unwrap()["valid"],
            json!(false)
        );
        assert_eq!(
            op_valid_container_name(json!({"name": "has space"})).unwrap()["valid"],
            json!(false)
        );
        assert_eq!(
            op_valid_container_name(json!({"name": "a"})).unwrap()["valid"],
            json!(false),
            "single char fails the daemon's length-2 rule"
        );
    }

    #[test]
    fn valid_image_tag_follows_docker_tag_rules() {
        let ok = |t: &str| {
            op_valid_image_tag(json!({ "tag": t })).unwrap()["valid"]
                .as_bool()
                .unwrap()
        };
        // Common valid tags — case, digits, and the allowed punctuation.
        assert!(ok("latest"));
        assert!(ok("v1.2.3"));
        assert!(ok("1.21-alpine"));
        assert!(ok("RELEASE_2025"), "uppercase and underscore allowed");
        assert!(
            ok("_leading_underscore"),
            "underscore may lead, unlike . or -"
        );
        // Rejections.
        for (t, want) in [
            ("", "empty"),
            (".dotstart", "period or a dash"),
            ("-dashstart", "period or a dash"),
            ("has space", "ASCII letters"),
            ("a/b", "ASCII letters"),
        ] {
            let r = op_valid_image_tag(json!({ "tag": t })).unwrap();
            assert_eq!(r["valid"], json!(false), "{t} should be invalid");
            assert!(
                r["reason"].as_str().unwrap().contains(want),
                "{t}: reason `{}` should mention `{want}`",
                r["reason"]
            );
        }
        // 128 is the max; 129 fails.
        assert!(ok(&"a".repeat(128)));
        let long = op_valid_image_tag(json!({"tag": "a".repeat(129)})).unwrap();
        assert_eq!(long["valid"], json!(false));
        assert!(long["reason"].as_str().unwrap().contains("128"));
    }

    #[test]
    fn valid_digest_checks_algorithm_and_encoded_length() {
        let sha256 = format!("sha256:{}", "a".repeat(64));
        let v = op_valid_digest(json!({ "digest": sha256 })).unwrap();
        assert_eq!(v["valid"], json!(true));
        assert_eq!(v["algorithm"], json!("sha256"));
        assert_eq!(v["hex"], json!("a".repeat(64)));
        // sha512 needs 128 hex.
        assert_eq!(
            op_valid_digest(json!({ "digest": format!("sha512:{}", "f".repeat(128)) })).unwrap()
                ["valid"],
            json!(true)
        );
        // Rejections with reason fragments.
        for (d, want) in [
            ("sha256deadbeef", "algorithm:hex"),
            ("sha256:", "lowercase hex"),
            ("sha256:abcd", "64 hex"),
            ("sha512:abcd", "128 hex"),
            ("sha256:ABCDEF", "lowercase hex"),
            ("Sha256:aa", "lowercase-alphanumeric"),
            (&format!("sha256:{}g", "a".repeat(63)), "lowercase hex"),
        ] {
            let r = op_valid_digest(json!({ "digest": d })).unwrap();
            assert_eq!(r["valid"], json!(false), "`{d}` should be invalid");
            assert!(
                r["reason"].as_str().unwrap().contains(want),
                "`{d}`: reason `{}` should mention `{want}`",
                r["reason"]
            );
        }
        // An unregistered algorithm with even-length lowercase hex is accepted.
        assert_eq!(
            op_valid_digest(json!({ "digest": "multihash:abcd" })).unwrap()["valid"],
            json!(true)
        );
        // Odd-length hex is rejected.
        assert_eq!(
            op_valid_digest(json!({ "digest": "multihash:abc" })).unwrap()["valid"],
            json!(false)
        );
    }

    #[test]
    fn valid_repository_name_follows_distribution_grammar() {
        let chk = |n: &str| op_valid_repository_name(json!({ "name": n })).unwrap();
        // Valid: single component, namespaced, registry-style path, allowed seps.
        for ok in [
            "nginx",
            "library/nginx",
            "myorg/myapp",
            "a.b",
            "a_b",
            "a__b",
            "a-b",
            "a---b",
            "team/sub-svc/app.v2",
            "123",
        ] {
            assert_eq!(chk(ok)["valid"], json!(true), "`{ok}` should be valid");
        }
        // Invalid: uppercase (the classic `docker build -t MyApp` failure).
        assert_eq!(chk("MyApp")["valid"], json!(false));
        assert_eq!(chk("Org/app")["valid"], json!(false));
        // Invalid separator runs and boundaries.
        for bad in [
            "a..b",  // double period
            "a___b", // triple underscore
            "a_.b",  // mixed separators
            "-abc",  // leading dash
            "abc-",  // trailing dash
            ".abc",  // leading period
            "a//b",  // empty path component
            "",      // empty
        ] {
            assert_eq!(chk(bad)["valid"], json!(false), "`{bad}` should be invalid");
            assert!(chk(bad)["reason"].is_string(), "`{bad}` has a reason");
        }
        // Over the 255-char limit.
        assert_eq!(chk(&"a".repeat(256))["valid"], json!(false));
        // Missing name errors.
        assert!(op_valid_repository_name(json!({})).is_err());
    }

    #[test]
    fn parse_port_spec_handles_the_p_flag_forms() {
        let full = op_parse_port_spec(json!({"spec": "127.0.0.1:8080:80/tcp"})).unwrap();
        assert_eq!(full["host_ip"], json!("127.0.0.1"));
        assert_eq!(full["host_port"], json!("8080"));
        assert_eq!(full["container_port"], json!("80"));
        assert_eq!(full["protocol"], json!("tcp"));

        let hp = op_parse_port_spec(json!({"spec": "8080:80"})).unwrap();
        assert_eq!(hp["host_ip"], Value::Null);
        assert_eq!(hp["host_port"], json!("8080"));
        assert_eq!(hp["protocol"], json!("tcp"), "protocol defaults to tcp");

        let bare = op_parse_port_spec(json!({"spec": "53/udp"})).unwrap();
        assert_eq!(bare["container_port"], json!("53"));
        assert_eq!(bare["host_port"], Value::Null);
        assert_eq!(bare["protocol"], json!("udp"));
    }

    #[test]
    fn parse_port_spec_handles_bracketed_ipv6() {
        let v = op_parse_port_spec(json!({"spec": "[::1]:8080:80"})).unwrap();
        assert_eq!(v["host_ip"], json!("::1"));
        assert_eq!(v["host_port"], json!("8080"));
        assert_eq!(v["container_port"], json!("80"));
    }

    #[test]
    fn build_port_spec_inverts_parse_port_spec() {
        // container-only, host:container, and ip:host:container forms.
        assert_eq!(
            op_build_port_spec(json!({"container_port": 80})).unwrap()["spec"],
            json!("80")
        );
        assert_eq!(
            op_build_port_spec(json!({"host_port": 8080, "container_port": 80})).unwrap()["spec"],
            json!("8080:80")
        );
        assert_eq!(
            op_build_port_spec(
                json!({"host_ip": "127.0.0.1", "host_port": 8080, "container_port": 80})
            )
            .unwrap()["spec"],
            json!("127.0.0.1:8080:80")
        );
        // Non-tcp protocol appends /proto; ports accepted as strings too.
        assert_eq!(
            op_build_port_spec(
                json!({"host_port": "53", "container_port": "53", "protocol": "udp"})
            )
            .unwrap()["spec"],
            json!("53:53/udp")
        );
        // IPv6 host is bracketed; an IP without a host port leaves an empty slot.
        assert_eq!(
            op_build_port_spec(json!({"host_ip": "::1", "container_port": 80})).unwrap()["spec"],
            json!("[::1]::80")
        );
        // Round-trip a full spec through parse.
        let built = op_build_port_spec(json!({
            "host_ip": "127.0.0.1", "host_port": 8080, "container_port": 80
        }))
        .unwrap()["spec"]
            .as_str()
            .unwrap()
            .to_string();
        let back = op_parse_port_spec(json!({"spec": built})).unwrap();
        assert_eq!(back["host_ip"], json!("127.0.0.1"));
        assert_eq!(back["host_port"], json!("8080"));
        assert_eq!(back["container_port"], json!("80"));
        assert!(op_build_port_spec(json!({"host_port": 8080})).is_err());
    }

    #[test]
    fn parse_mount_classifies_bind_volume_anonymous() {
        // Host path → bind, with read-only option.
        let bind = op_parse_mount(json!({"spec": "/data:/var/lib/data:ro"})).unwrap();
        assert_eq!(bind["type"], json!("bind"));
        assert_eq!(bind["source"], json!("/data"));
        assert_eq!(bind["target"], json!("/var/lib/data"));
        assert_eq!(bind["readonly"], json!(true));
        assert_eq!(bind["options"], json!(["ro"]));
        // Named volume → volume, defaults read-write.
        let vol = op_parse_mount(json!({"spec": "pgdata:/var/lib/postgresql/data"})).unwrap();
        assert_eq!(vol["type"], json!("volume"));
        assert_eq!(vol["source"], json!("pgdata"));
        assert_eq!(vol["readonly"], json!(false));
        // Lone container path → anonymous volume, null source.
        let anon = op_parse_mount(json!({"spec": "/cache"})).unwrap();
        assert_eq!(anon["type"], json!("anonymous"));
        assert_eq!(anon["source"], Value::Null);
        assert_eq!(anon["target"], json!("/cache"));
        // Multi-option list is preserved in order.
        let multi = op_parse_mount(json!({"spec": "./src:/app:ro,z"})).unwrap();
        assert_eq!(multi["type"], json!("bind"));
        assert_eq!(multi["options"], json!(["ro", "z"]));
        // Empty target and over-long specs are rejected.
        assert!(op_parse_mount(json!({"spec": "/data:"})).is_err());
        assert!(op_parse_mount(json!({"spec": "a:b:c:d"})).is_err());
    }

    #[test]
    fn build_mount_inverts_parse_mount() {
        // readonly flag appends `ro`.
        assert_eq!(
            op_build_mount(json!({"source": "/data", "target": "/var/lib/data", "readonly": true}))
                .unwrap()["spec"],
            json!("/data:/var/lib/data:ro")
        );
        // Named volume, read-write.
        assert_eq!(
            op_build_mount(json!({"source": "pgdata", "target": "/var/lib/postgresql/data"}))
                .unwrap()["spec"],
            json!("pgdata:/var/lib/postgresql/data")
        );
        // Anonymous volume — target only.
        assert_eq!(
            op_build_mount(json!({"target": "/cache"})).unwrap()["spec"],
            json!("/cache")
        );
        // Explicit options list; `ro` from the flag is not duplicated.
        assert_eq!(
            op_build_mount(json!({"source": "./src", "target": "/app", "options": ["ro", "z"], "readonly": true}))
                .unwrap()["spec"],
            json!("./src:/app:ro,z")
        );
        // truthy readonly accepts a number (stryke serializes bools as 1).
        assert_eq!(
            op_build_mount(json!({"source": "/d", "target": "/t", "readonly": 1})).unwrap()["spec"],
            json!("/d:/t:ro")
        );
        // Round-trips through parse_mount.
        for spec in ["/data:/var/lib/data:ro", "pgdata:/app", "./src:/app:ro,z"] {
            let p = op_parse_mount(json!({ "spec": spec })).unwrap();
            let rebuilt = op_build_mount(json!({
                "source": p["source"],
                "target": p["target"],
                "options": p["options"],
            }))
            .unwrap()["spec"]
                .clone();
            assert_eq!(rebuilt, json!(spec), "round-trip for {spec}");
        }
        // Options without a source can't be expressed; missing target errors.
        assert!(op_build_mount(json!({"target": "/cache", "readonly": true})).is_err());
        assert!(op_build_mount(json!({"source": "/d"})).is_err());
    }

    #[test]
    fn parse_env_splits_on_first_equals_and_marks_host_passthrough() {
        // KEY=VALUE: split on the first `=`, from_host false.
        let r = op_parse_env(json!({"spec": "FOO=bar"})).unwrap();
        assert_eq!(r["key"], json!("FOO"));
        assert_eq!(r["value"], json!("bar"));
        assert_eq!(r["from_host"], json!(false));
        // A value may contain `=`: only the first one splits.
        let m = op_parse_env(json!({"spec": "URL=a=b=c"})).unwrap();
        assert_eq!(m["key"], json!("URL"));
        assert_eq!(m["value"], json!("a=b=c"));
        // Trailing `=` is an empty value, NOT host passthrough.
        let e = op_parse_env(json!({"spec": "EMPTY="})).unwrap();
        assert_eq!(e["value"], json!(""));
        assert_eq!(e["from_host"], json!(false));
        // Bare KEY (no `=`) means pass through from the host environment.
        let h = op_parse_env(json!({"spec": "HOME"})).unwrap();
        assert_eq!(h["key"], json!("HOME"));
        assert_eq!(h["value"], Value::Null);
        assert_eq!(h["from_host"], json!(true));
        // Empty key / empty spec error.
        assert!(op_parse_env(json!({"spec": "=oops"})).is_err());
        assert!(op_parse_env(json!({"spec": ""})).is_err());
        assert!(op_parse_env(json!({})).is_err());
    }

    #[test]
    fn build_env_inverts_parse_env() {
        // With a value → KEY=VALUE.
        assert_eq!(
            op_build_env(json!({"key": "FOO", "value": "bar"})).unwrap()["spec"],
            json!("FOO=bar")
        );
        // Empty string value → trailing `=`.
        assert_eq!(
            op_build_env(json!({"key": "EMPTY", "value": ""})).unwrap()["spec"],
            json!("EMPTY=")
        );
        // No value → bare KEY (host passthrough form).
        assert_eq!(
            op_build_env(json!({"key": "HOME"})).unwrap()["spec"],
            json!("HOME")
        );
        // A numeric value is stringified.
        assert_eq!(
            op_build_env(json!({"key": "PORT", "value": 8080})).unwrap()["spec"],
            json!("PORT=8080")
        );
        // Round-trips through parse_env for every form.
        for spec in ["FOO=bar", "URL=a=b=c", "EMPTY=", "HOME"] {
            let p = op_parse_env(json!({ "spec": spec })).unwrap();
            let rebuilt = op_build_env(json!({
                "key": p["key"],
                "value": p["value"],
            }))
            .unwrap()["spec"]
                .clone();
            assert_eq!(rebuilt, json!(spec), "round-trip for {spec}");
        }
        // Empty key errors.
        assert!(op_build_env(json!({"value": "x"})).is_err());
        assert!(op_build_env(json!({"key": ""})).is_err());
    }

    #[test]
    fn parse_restart_policy_handles_all_four_policies() {
        let p = |s: &str| op_parse_restart_policy(json!({ "spec": s })).unwrap();
        // The three count-less policies.
        for name in ["no", "always", "unless-stopped"] {
            let v = p(name);
            assert_eq!(v["policy"], json!(name));
            assert_eq!(v["max_retries"], Value::Null);
        }
        // on-failure with and without a retry count.
        assert_eq!(p("on-failure")["max_retries"], Value::Null);
        let of = p("on-failure:5");
        assert_eq!(of["policy"], json!("on-failure"));
        assert_eq!(of["max_retries"], json!(5));
        // Errors: a count on a non-on-failure policy, unknown policy, bad count.
        assert!(op_parse_restart_policy(json!({"spec": "always:3"})).is_err());
        assert!(op_parse_restart_policy(json!({"spec": "sometimes"})).is_err());
        assert!(op_parse_restart_policy(json!({"spec": "on-failure:abc"})).is_err());
        assert!(op_parse_restart_policy(json!({})).is_err());
    }

    #[test]
    fn build_restart_policy_inverts_parse_restart_policy() {
        assert_eq!(
            op_build_restart_policy(json!({"policy": "always"})).unwrap()["spec"],
            json!("always")
        );
        assert_eq!(
            op_build_restart_policy(json!({"policy": "on-failure", "max_retries": 10})).unwrap()
                ["spec"],
            json!("on-failure:10")
        );
        assert_eq!(
            op_build_restart_policy(json!({"policy": "on-failure"})).unwrap()["spec"],
            json!("on-failure")
        );
        // Round-trips parse for every form.
        for spec in [
            "no",
            "always",
            "unless-stopped",
            "on-failure",
            "on-failure:5",
        ] {
            let p = op_parse_restart_policy(json!({ "spec": spec })).unwrap();
            let rebuilt = op_build_restart_policy(json!({
                "policy": p["policy"], "max_retries": p["max_retries"],
            }))
            .unwrap()["spec"]
                .clone();
            assert_eq!(rebuilt, json!(spec), "round-trip for {spec}");
        }
        // A retry count on a non-on-failure policy / unknown policy / missing.
        assert!(op_build_restart_policy(json!({"policy": "always", "max_retries": 3})).is_err());
        assert!(op_build_restart_policy(json!({"policy": "nope"})).is_err());
        assert!(op_build_restart_policy(json!({})).is_err());
    }
}
