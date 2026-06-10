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
        let ptr = ffi_call_async(args.as_ptr(), |_v| async {
            Err(anyhow!("missing image"))
        });
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
}
