//! `stryke-docker-helper` — Docker daemon bridge binary.
//!
//! Wraps the `bollard` async client. Output is NDJSON for streams
//! (ps, images, logs --follow, build, pull, push, events, exec, stats)
//! and a single JSON object otherwise. Connects via the local socket
//! by default (or `$DOCKER_HOST` / `$DOCKER_TLS_VERIFY` / `$DOCKER_CERT_PATH`).

use std::collections::HashMap;
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use bollard::container::{
    AttachContainerOptions, Config as ContainerConfig, CreateContainerOptions,
    KillContainerOptions, ListContainersOptions, LogsOptions, RemoveContainerOptions,
    RestartContainerOptions, StartContainerOptions, StatsOptions, StopContainerOptions,
};
use bollard::exec::{CreateExecOptions, StartExecOptions, StartExecResults};
use bollard::image::{BuildImageOptions, CreateImageOptions, ListImagesOptions, PushImageOptions, RemoveImageOptions, TagImageOptions};
use bollard::network::{CreateNetworkOptions, ListNetworksOptions};
use bollard::secret::{HostConfig, PortBinding, RestartPolicy, RestartPolicyNameEnum};
use bollard::system::EventsOptions;
use bollard::volume::{CreateVolumeOptions, ListVolumesOptions};
use bollard::Docker;
use clap::{Args, Parser, Subcommand};
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::StreamExt;
use serde_json::{json, Value as JsonValue};

#[derive(Parser, Debug)]
#[command(
    name = "stryke-docker-helper",
    version,
    about = "Docker client for the stryke `docker` package"
)]
struct Cli {
    #[command(flatten)]
    conn: Conn,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Debug, Clone)]
struct Conn {
    /// Daemon URL — `unix:///var/run/docker.sock` or `tcp://host:2376`.
    #[arg(long, env = "DOCKER_HOST", global = true)]
    host: Option<String>,
    /// API timeout, seconds.
    #[arg(long, default_value_t = 120, global = true)]
    timeout: u64,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// `docker ping` round-trip → `{ok}`.
    Ping,
    /// Daemon /version.
    Version,
    /// Daemon /info.
    Info,
    /// NDJSON event stream until Ctrl-C.
    Events {
        #[arg(long)]
        since: Option<String>,
        #[arg(long)]
        until: Option<String>,
        /// `--filter key=value`, repeatable.
        #[arg(long = "filter", value_parser = parse_kv, num_args = 0..)]
        filters: Vec<(String, String)>,
    },

    /// List containers as NDJSON.
    Ps {
        #[arg(long, short = 'a')]
        all: bool,
        #[arg(long)]
        limit: Option<isize>,
        #[arg(long)]
        size: bool,
        #[arg(long = "filter", value_parser = parse_kv, num_args = 0..)]
        filters: Vec<(String, String)>,
    },
    /// Inspect a container.
    Inspect { container: String },
    /// Create + start a container. Combine of `create` + `start`.
    Run {
        image: String,
        #[arg(long)]
        name: Option<String>,
        /// argv after the image: `--cmd echo -- hello`.
        #[arg(long = "cmd", num_args = 0.., allow_hyphen_values = true)]
        cmd: Vec<String>,
        #[arg(long = "env", short = 'e', num_args = 0..)]
        env: Vec<String>,
        /// Publish port: `host:container[/proto]`. Repeatable.
        #[arg(long = "port", short = 'p', num_args = 0..)]
        port: Vec<String>,
        /// Volume mount: `host:container[:ro]`. Repeatable.
        #[arg(long = "volume", short = 'v', num_args = 0..)]
        volume: Vec<String>,
        #[arg(long = "label", short = 'l', num_args = 0..)]
        label: Vec<String>,
        #[arg(long)]
        network: Option<String>,
        #[arg(long)]
        workdir: Option<String>,
        #[arg(long)]
        user: Option<String>,
        #[arg(long)]
        hostname: Option<String>,
        #[arg(long)]
        restart: Option<String>,
        #[arg(long)]
        rm: bool,
        #[arg(long, short = 'd', default_value_t = true)]
        detach: bool,
        #[arg(long)]
        tty: bool,
    },
    /// Create a container without starting.
    Create {
        image: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long = "cmd", num_args = 0.., allow_hyphen_values = true)]
        cmd: Vec<String>,
        #[arg(long = "env", short = 'e', num_args = 0..)]
        env: Vec<String>,
        #[arg(long = "port", short = 'p', num_args = 0..)]
        port: Vec<String>,
        #[arg(long = "volume", short = 'v', num_args = 0..)]
        volume: Vec<String>,
        #[arg(long = "label", short = 'l', num_args = 0..)]
        label: Vec<String>,
        #[arg(long)]
        network: Option<String>,
        #[arg(long)]
        workdir: Option<String>,
        #[arg(long)]
        user: Option<String>,
        #[arg(long)]
        hostname: Option<String>,
        #[arg(long)]
        restart: Option<String>,
        #[arg(long)]
        tty: bool,
    },
    Start { container: String },
    Stop {
        container: String,
        #[arg(long, short = 't')]
        time: Option<i64>,
    },
    Restart {
        container: String,
        #[arg(long, short = 't')]
        time: Option<isize>,
    },
    Kill {
        container: String,
        #[arg(long, short = 's')]
        signal: Option<String>,
    },
    Rm {
        container: String,
        #[arg(long, short = 'f')]
        force: bool,
        #[arg(long, short = 'v')]
        volumes: bool,
    },

    /// Tail/logs. NDJSON when `--follow`, raw text otherwise.
    Logs {
        container: String,
        #[arg(long)]
        tail: Option<String>,
        #[arg(long)]
        since: Option<i64>,
        #[arg(long)]
        until: Option<i64>,
        #[arg(long, short = 'f')]
        follow: bool,
        #[arg(long)]
        timestamps: bool,
        #[arg(long, default_value_t = true)]
        stdout: bool,
        #[arg(long, default_value_t = true)]
        stderr: bool,
    },
    /// Exec a command. Streams NDJSON `{stream:"stdout"|"stderr", data}`.
    Exec {
        container: String,
        #[arg(long = "cmd", num_args = 1.., required = true, allow_hyphen_values = true)]
        cmd: Vec<String>,
        #[arg(long = "env", short = 'e', num_args = 0..)]
        env: Vec<String>,
        #[arg(long)]
        workdir: Option<String>,
        #[arg(long, short = 'u')]
        user: Option<String>,
        #[arg(long)]
        tty: bool,
    },
    /// Resource usage. Without `--stream`, one snapshot.
    Stats {
        container: String,
        #[arg(long)]
        stream: bool,
    },

    /// List images.
    Images {
        #[arg(long, short = 'a')]
        all: bool,
        #[arg(long = "filter", value_parser = parse_kv, num_args = 0..)]
        filters: Vec<(String, String)>,
    },
    /// Pull an image. NDJSON progress events.
    Pull {
        image: String,
        #[arg(long)]
        platform: Option<String>,
    },
    /// Push an image. NDJSON progress events.
    Push {
        image: String,
    },
    /// Remove an image.
    Rmi {
        image: String,
        #[arg(long, short = 'f')]
        force: bool,
        #[arg(long)]
        noprune: bool,
    },
    /// Tag `source` as `target`.
    Tag {
        source: String,
        target: String,
    },
    /// Build an image. `dir` is the build context. NDJSON progress.
    Build {
        dir: PathBuf,
        #[arg(long, short = 't')]
        tag: Option<String>,
        #[arg(long, short = 'f')]
        dockerfile: Option<String>,
        #[arg(long = "build-arg", value_parser = parse_kv, num_args = 0..)]
        build_args: Vec<(String, String)>,
        #[arg(long)]
        no_cache: bool,
        #[arg(long)]
        pull: bool,
        #[arg(long)]
        rm: bool,
    },

    /// List networks.
    Networks {
        #[arg(long = "filter", value_parser = parse_kv, num_args = 0..)]
        filters: Vec<(String, String)>,
    },
    /// Create a network.
    NetworkCreate {
        name: String,
        #[arg(long, default_value = "bridge")]
        driver: String,
        #[arg(long)]
        subnet: Option<String>,
        #[arg(long)]
        gateway: Option<String>,
        #[arg(long = "label", num_args = 0..)]
        label: Vec<String>,
    },
    /// Remove a network.
    NetworkRm { name: String },

    /// List volumes.
    Volumes {
        #[arg(long = "filter", value_parser = parse_kv, num_args = 0..)]
        filters: Vec<(String, String)>,
    },
    /// Create a volume.
    VolumeCreate {
        name: String,
        #[arg(long, default_value = "local")]
        driver: String,
        #[arg(long = "label", num_args = 0..)]
        label: Vec<String>,
    },
    /// Remove a volume.
    VolumeRm {
        name: String,
        #[arg(long, short = 'f')]
        force: bool,
    },

    /// Reclaim space (default: containers + images + networks).
    Prune {
        #[arg(long)]
        containers: bool,
        #[arg(long)]
        images: bool,
        #[arg(long)]
        volumes: bool,
        #[arg(long)]
        networks: bool,
        /// Shortcut for --containers --images --networks --volumes.
        #[arg(long)]
        all: bool,
    },
}

fn parse_kv(s: &str) -> Result<(String, String), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("expected key=value, got `{s}`"))?;
    Ok((k.to_string(), v.to_string()))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("stryke-docker-helper: {e:#}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let docker = make_client(&cli.conn)?;
    match cli.cmd {
        Cmd::Ping => cmd_ping(&docker).await,
        Cmd::Version => cmd_version(&docker).await,
        Cmd::Info => cmd_info(&docker).await,
        Cmd::Events { since, until, filters } => cmd_events(&docker, since, until, filters).await,
        Cmd::Ps { all, limit, size, filters } => cmd_ps(&docker, all, limit, size, filters).await,
        Cmd::Inspect { container } => cmd_inspect(&docker, &container).await,
        Cmd::Run {
            image, name, cmd, env, port, volume, label, network, workdir, user, hostname,
            restart, rm, detach: _, tty,
        } => {
            cmd_create_or_run(
                &docker, &image, name.as_deref(), &cmd, &env, &port, &volume, &label,
                network.as_deref(), workdir.as_deref(), user.as_deref(),
                hostname.as_deref(), restart.as_deref(), rm, tty, true,
            )
            .await
        }
        Cmd::Create {
            image, name, cmd, env, port, volume, label, network, workdir, user, hostname,
            restart, tty,
        } => {
            cmd_create_or_run(
                &docker, &image, name.as_deref(), &cmd, &env, &port, &volume, &label,
                network.as_deref(), workdir.as_deref(), user.as_deref(),
                hostname.as_deref(), restart.as_deref(), false, tty, false,
            )
            .await
        }
        Cmd::Start { container } => cmd_start(&docker, &container).await,
        Cmd::Stop { container, time } => cmd_stop(&docker, &container, time).await,
        Cmd::Restart { container, time } => cmd_restart(&docker, &container, time).await,
        Cmd::Kill { container, signal } => cmd_kill(&docker, &container, signal.as_deref()).await,
        Cmd::Rm { container, force, volumes } => cmd_rm(&docker, &container, force, volumes).await,
        Cmd::Logs { container, tail, since, until, follow, timestamps, stdout, stderr } => {
            cmd_logs(&docker, &container, tail.as_deref(), since, until, follow, timestamps, stdout, stderr).await
        }
        Cmd::Exec { container, cmd, env, workdir, user, tty } => {
            cmd_exec(&docker, &container, &cmd, &env, workdir.as_deref(), user.as_deref(), tty).await
        }
        Cmd::Stats { container, stream } => cmd_stats(&docker, &container, stream).await,
        Cmd::Images { all, filters } => cmd_images(&docker, all, filters).await,
        Cmd::Pull { image, platform } => cmd_pull(&docker, &image, platform.as_deref()).await,
        Cmd::Push { image } => cmd_push(&docker, &image).await,
        Cmd::Rmi { image, force, noprune } => cmd_rmi(&docker, &image, force, noprune).await,
        Cmd::Tag { source, target } => cmd_tag(&docker, &source, &target).await,
        Cmd::Build {
            dir, tag, dockerfile, build_args, no_cache, pull, rm,
        } => {
            cmd_build(
                &docker, &dir, tag.as_deref(), dockerfile.as_deref(),
                &build_args, no_cache, pull, rm,
            )
            .await
        }
        Cmd::Networks { filters } => cmd_networks(&docker, filters).await,
        Cmd::NetworkCreate { name, driver, subnet, gateway, label } => {
            cmd_network_create(&docker, &name, &driver, subnet.as_deref(), gateway.as_deref(), &label).await
        }
        Cmd::NetworkRm { name } => cmd_network_rm(&docker, &name).await,
        Cmd::Volumes { filters } => cmd_volumes(&docker, filters).await,
        Cmd::VolumeCreate { name, driver, label } => {
            cmd_volume_create(&docker, &name, &driver, &label).await
        }
        Cmd::VolumeRm { name, force } => cmd_volume_rm(&docker, &name, force).await,
        Cmd::Prune { containers, images, volumes, networks, all } => {
            cmd_prune(&docker, containers || all, images || all, volumes || all, networks || all).await
        }
    }
}

/* ------------------------------------------------------------------------- */
/* connection + helpers                                                      */
/* ------------------------------------------------------------------------- */

fn make_client(c: &Conn) -> Result<Docker> {
    let docker = match &c.host {
        Some(url) if url.starts_with("unix://") => {
            let path = url.trim_start_matches("unix://");
            Docker::connect_with_unix(path, c.timeout, bollard::API_DEFAULT_VERSION)
                .context("connect_with_unix")?
        }
        Some(url) if url.starts_with("tcp://") || url.starts_with("http://") || url.starts_with("https://") => {
            Docker::connect_with_http(url, c.timeout, bollard::API_DEFAULT_VERSION)
                .context("connect_with_http")?
        }
        None => Docker::connect_with_local_defaults().context("connect_with_local_defaults")?,
        Some(other) => bail!("unrecognised DOCKER_HOST scheme: {other}"),
    };
    Ok(docker)
}

fn emit_json<T: serde::Serialize>(v: &T) -> Result<()> {
    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());
    serde_json::to_writer(&mut w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn emit_ndjson<T: serde::Serialize, W: Write>(w: &mut W, v: &T) -> Result<()> {
    serde_json::to_writer(&mut *w, v)?;
    w.write_all(b"\n")?;
    Ok(())
}

fn kv_pairs(pairs: &[(String, String)]) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    for (k, v) in pairs {
        out.entry(k.clone()).or_default().push(v.clone());
    }
    out
}

fn parse_port_binding(spec: &str) -> Result<(String, String, Option<String>)> {
    // host[:container][/proto]    or    host:container/proto
    let (left, proto) = match spec.split_once('/') {
        Some((l, p)) => (l, Some(p.to_string())),
        None => (spec, None),
    };
    let (host, container) = match left.split_once(':') {
        Some((h, c)) => (h.to_string(), c.to_string()),
        None => (left.to_string(), left.to_string()),
    };
    Ok((host, container, proto))
}

fn label_vec_to_map(v: &[String]) -> HashMap<String, String> {
    v.iter()
        .filter_map(|s| s.split_once('=').map(|(k, v)| (k.to_string(), v.to_string())))
        .collect()
}

/* ------------------------------------------------------------------------- */
/* daemon                                                                    */
/* ------------------------------------------------------------------------- */

async fn cmd_ping(d: &Docker) -> Result<()> {
    let ok = d.ping().await.is_ok();
    emit_json(&json!({ "ok": ok }))
}

async fn cmd_version(d: &Docker) -> Result<()> {
    let v = d.version().await.context("version")?;
    emit_json(&v)
}

async fn cmd_info(d: &Docker) -> Result<()> {
    let i = d.info().await.context("info")?;
    emit_json(&i)
}

async fn cmd_events(
    d: &Docker,
    since: Option<String>,
    until: Option<String>,
    filters: Vec<(String, String)>,
) -> Result<()> {
    let opts = EventsOptions::<String> {
        since,
        until,
        filters: kv_pairs(&filters),
    };
    let mut stream = d.events(Some(opts));
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(ev) = stream.next().await {
        let ev = ev.context("event")?;
        emit_ndjson(&mut out, &ev)?;
        out.flush().ok();
    }
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* containers                                                                */
/* ------------------------------------------------------------------------- */

async fn cmd_ps(
    d: &Docker,
    all: bool,
    limit: Option<isize>,
    size: bool,
    filters: Vec<(String, String)>,
) -> Result<()> {
    let opts = ListContainersOptions::<String> {
        all,
        limit,
        size,
        filters: kv_pairs(&filters),
    };
    let list = d.list_containers(Some(opts)).await.context("list_containers")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for c in list {
        emit_ndjson(&mut out, &c)?;
    }
    Ok(())
}

async fn cmd_inspect(d: &Docker, container: &str) -> Result<()> {
    let info = d.inspect_container(container, None).await.context("inspect")?;
    emit_json(&info)
}

#[allow(clippy::too_many_arguments)]
async fn cmd_create_or_run(
    d: &Docker,
    image: &str,
    name: Option<&str>,
    cmd: &[String],
    env: &[String],
    ports: &[String],
    volumes: &[String],
    labels: &[String],
    network: Option<&str>,
    workdir: Option<&str>,
    user: Option<&str>,
    hostname: Option<&str>,
    restart: Option<&str>,
    auto_remove: bool,
    tty: bool,
    start: bool,
) -> Result<()> {
    let mut exposed: HashMap<String, HashMap<(), ()>> = HashMap::new();
    let mut bindings: HashMap<String, Option<Vec<PortBinding>>> = HashMap::new();
    for spec in ports {
        let (host, container, proto) = parse_port_binding(spec)?;
        let proto = proto.unwrap_or_else(|| "tcp".to_string());
        let key = format!("{container}/{proto}");
        exposed.insert(key.clone(), HashMap::new());
        bindings
            .entry(key)
            .or_insert_with(|| Some(vec![]))
            .as_mut()
            .unwrap()
            .push(PortBinding {
                host_ip: None,
                host_port: Some(host),
            });
    }

    let host_cfg = HostConfig {
        binds: if volumes.is_empty() {
            None
        } else {
            Some(volumes.to_vec())
        },
        port_bindings: if bindings.is_empty() { None } else { Some(bindings) },
        auto_remove: if auto_remove { Some(true) } else { None },
        network_mode: network.map(|n| n.to_string()),
        restart_policy: restart.map(|r| RestartPolicy {
            name: Some(match r {
                "no" => RestartPolicyNameEnum::NO,
                "always" => RestartPolicyNameEnum::ALWAYS,
                "unless-stopped" => RestartPolicyNameEnum::UNLESS_STOPPED,
                "on-failure" => RestartPolicyNameEnum::ON_FAILURE,
                _ => RestartPolicyNameEnum::EMPTY,
            }),
            maximum_retry_count: None,
        }),
        ..Default::default()
    };

    let cfg = ContainerConfig::<String> {
        image: Some(image.to_string()),
        cmd: if cmd.is_empty() { None } else { Some(cmd.to_vec()) },
        env: if env.is_empty() { None } else { Some(env.to_vec()) },
        exposed_ports: if exposed.is_empty() { None } else { Some(exposed) },
        labels: if labels.is_empty() {
            None
        } else {
            Some(label_vec_to_map(labels))
        },
        working_dir: workdir.map(|s| s.to_string()),
        user: user.map(|s| s.to_string()),
        hostname: hostname.map(|s| s.to_string()),
        tty: Some(tty),
        attach_stdin: Some(false),
        attach_stdout: Some(!tty),
        attach_stderr: Some(!tty),
        host_config: Some(host_cfg),
        ..Default::default()
    };

    let create_opts = name.map(|n| CreateContainerOptions {
        name: n.to_string(),
        platform: None,
    });
    let created = d
        .create_container(create_opts, cfg)
        .await
        .context("create_container")?;
    for w in &created.warnings {
        eprintln!("stryke-docker-helper: warning: {w}");
    }

    if start {
        d.start_container(&created.id, None::<StartContainerOptions<String>>)
            .await
            .context("start_container")?;
    }

    emit_json(&json!({
        "id": created.id,
        "warnings": created.warnings,
        "started": start,
    }))
}

async fn cmd_start(d: &Docker, c: &str) -> Result<()> {
    d.start_container(c, None::<StartContainerOptions<String>>)
        .await
        .context("start_container")?;
    emit_json(&json!({ "id": c, "started": true }))
}

async fn cmd_stop(d: &Docker, c: &str, t: Option<i64>) -> Result<()> {
    d.stop_container(c, t.map(|t| StopContainerOptions { t }))
        .await
        .context("stop_container")?;
    emit_json(&json!({ "id": c, "stopped": true }))
}

async fn cmd_restart(d: &Docker, c: &str, t: Option<isize>) -> Result<()> {
    d.restart_container(c, t.map(|t| RestartContainerOptions { t }))
        .await
        .context("restart_container")?;
    emit_json(&json!({ "id": c, "restarted": true }))
}

async fn cmd_kill(d: &Docker, c: &str, signal: Option<&str>) -> Result<()> {
    d.kill_container(
        c,
        signal.map(|s| KillContainerOptions { signal: s.to_string() }),
    )
    .await
    .context("kill_container")?;
    emit_json(&json!({ "id": c, "killed": true }))
}

async fn cmd_rm(d: &Docker, c: &str, force: bool, v: bool) -> Result<()> {
    d.remove_container(
        c,
        Some(RemoveContainerOptions { force, v, link: false }),
    )
    .await
    .context("remove_container")?;
    emit_json(&json!({ "id": c, "removed": true }))
}

#[allow(clippy::too_many_arguments)]
async fn cmd_logs(
    d: &Docker,
    container: &str,
    tail: Option<&str>,
    since: Option<i64>,
    until: Option<i64>,
    follow: bool,
    timestamps: bool,
    stdout: bool,
    stderr: bool,
) -> Result<()> {
    let opts = LogsOptions::<String> {
        stdout,
        stderr,
        timestamps,
        follow,
        tail: tail.unwrap_or("all").to_string(),
        since: since.unwrap_or(0),
        until: until.unwrap_or(0),
    };
    let mut stream = d.logs(container, Some(opts));
    let stdout_h = io::stdout();
    let mut out = BufWriter::new(stdout_h.lock());
    while let Some(item) = stream.next().await {
        let item = item.context("logs")?;
        if follow {
            let (kind, bytes) = match &item {
                bollard::container::LogOutput::StdOut { message } => ("stdout", message.clone()),
                bollard::container::LogOutput::StdErr { message } => ("stderr", message.clone()),
                bollard::container::LogOutput::StdIn { message } => ("stdin", message.clone()),
                bollard::container::LogOutput::Console { message } => ("console", message.clone()),
            };
            emit_ndjson(
                &mut out,
                &json!({
                    "stream": kind,
                    "data": String::from_utf8_lossy(&bytes),
                }),
            )?;
            out.flush().ok();
        } else {
            out.write_all(item.into_bytes().as_ref())?;
        }
    }
    Ok(())
}

async fn cmd_exec(
    d: &Docker,
    container: &str,
    cmd: &[String],
    env: &[String],
    workdir: Option<&str>,
    user: Option<&str>,
    tty: bool,
) -> Result<()> {
    let exec = d
        .create_exec::<String>(
            container,
            CreateExecOptions {
                cmd: Some(cmd.to_vec()),
                env: if env.is_empty() { None } else { Some(env.to_vec()) },
                working_dir: workdir.map(|s| s.to_string()),
                user: user.map(|s| s.to_string()),
                attach_stdout: Some(true),
                attach_stderr: Some(true),
                tty: Some(tty),
                ..Default::default()
            },
        )
        .await
        .context("create_exec")?;
    let started = d
        .start_exec(&exec.id, Some(StartExecOptions { detach: false, tty, output_capacity: None }))
        .await
        .context("start_exec")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    match started {
        StartExecResults::Attached { mut output, .. } => {
            while let Some(item) = output.next().await {
                let item = item.context("exec stream")?;
                let (kind, bytes) = match item {
                    bollard::container::LogOutput::StdOut { message } => ("stdout", message),
                    bollard::container::LogOutput::StdErr { message } => ("stderr", message),
                    bollard::container::LogOutput::StdIn { message } => ("stdin", message),
                    bollard::container::LogOutput::Console { message } => ("console", message),
                };
                emit_ndjson(
                    &mut out,
                    &json!({
                        "stream": kind,
                        "data": String::from_utf8_lossy(&bytes),
                    }),
                )?;
                out.flush().ok();
            }
        }
        StartExecResults::Detached => {
            emit_ndjson(&mut out, &json!({ "detached": true }))?;
        }
    }
    let info = d.inspect_exec(&exec.id).await.context("inspect_exec")?;
    emit_ndjson(&mut out, &json!({ "exit_code": info.exit_code }))?;
    Ok(())
}

async fn cmd_stats(d: &Docker, container: &str, stream: bool) -> Result<()> {
    let opts = StatsOptions { stream, one_shot: !stream };
    let mut s = d.stats(container, Some(opts));
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(item) = s.next().await {
        let item = item.context("stats")?;
        emit_ndjson(&mut out, &item)?;
        out.flush().ok();
        if !stream {
            break;
        }
    }
    Ok(())
}

/* ------------------------------------------------------------------------- */
/* images                                                                    */
/* ------------------------------------------------------------------------- */

async fn cmd_images(
    d: &Docker,
    all: bool,
    filters: Vec<(String, String)>,
) -> Result<()> {
    let opts = ListImagesOptions::<String> {
        all,
        digests: false,
        filters: kv_pairs(&filters),
    };
    let list = d.list_images(Some(opts)).await.context("list_images")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for img in list {
        emit_ndjson(&mut out, &img)?;
    }
    Ok(())
}

async fn cmd_pull(d: &Docker, image: &str, platform: Option<&str>) -> Result<()> {
    let opts = CreateImageOptions::<String> {
        from_image: image.to_string(),
        platform: platform.unwrap_or("").to_string(),
        ..Default::default()
    };
    let mut stream = d.create_image(Some(opts), None, None);
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(item) = stream.next().await {
        let item = item.context("pull")?;
        emit_ndjson(&mut out, &item)?;
        out.flush().ok();
    }
    Ok(())
}

async fn cmd_push(d: &Docker, image: &str) -> Result<()> {
    let opts = PushImageOptions { tag: "" };
    let mut stream = d.push_image(image, Some(opts), None);
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(item) = stream.next().await {
        let item = item.context("push")?;
        emit_ndjson(&mut out, &item)?;
        out.flush().ok();
    }
    Ok(())
}

async fn cmd_rmi(d: &Docker, image: &str, force: bool, noprune: bool) -> Result<()> {
    let list = d
        .remove_image(image, Some(RemoveImageOptions { force, noprune }), None)
        .await
        .context("remove_image")?;
    emit_json(&list)
}

async fn cmd_tag(d: &Docker, source: &str, target: &str) -> Result<()> {
    // target = repo[:tag]
    let (repo, tag) = match target.rsplit_once(':') {
        Some((r, t)) if !r.is_empty() && !t.contains('/') => (r.to_string(), t.to_string()),
        _ => (target.to_string(), "latest".to_string()),
    };
    let opts = TagImageOptions { repo, tag };
    d.tag_image(source, Some(opts)).await.context("tag_image")?;
    emit_json(&json!({ "source": source, "target": target, "tagged": true }))
}

#[allow(clippy::too_many_arguments)]
async fn cmd_build(
    d: &Docker,
    dir: &Path,
    tag: Option<&str>,
    dockerfile: Option<&str>,
    build_args: &[(String, String)],
    no_cache: bool,
    pull: bool,
    rm: bool,
) -> Result<()> {
    let tar_gz = tar_gzip_directory(dir).context("tar build context")?;
    let opts = BuildImageOptions::<String> {
        dockerfile: dockerfile.unwrap_or("Dockerfile").to_string(),
        t: tag.unwrap_or("").to_string(),
        nocache: no_cache,
        pull,
        rm,
        forcerm: rm,
        buildargs: build_args
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        ..Default::default()
    };
    let mut stream = d.build_image(opts, None, Some(tar_gz.into()));
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    while let Some(item) = stream.next().await {
        let item = item.context("build")?;
        emit_ndjson(&mut out, &build_info_to_json(&item))?;
        out.flush().ok();
    }
    Ok(())
}

fn build_info_to_json(b: &bollard::models::BuildInfo) -> JsonValue {
    let mut m = serde_json::Map::new();
    if let Some(v) = &b.id { m.insert("id".into(), JsonValue::String(v.clone())); }
    if let Some(v) = &b.stream { m.insert("stream".into(), JsonValue::String(v.clone())); }
    if let Some(v) = &b.error { m.insert("error".into(), JsonValue::String(v.clone())); }
    if let Some(v) = &b.status { m.insert("status".into(), JsonValue::String(v.clone())); }
    if let Some(v) = &b.progress { m.insert("progress".into(), JsonValue::String(v.clone())); }
    if let Some(v) = &b.aux {
        if let Some(id) = &v.id {
            m.insert("aux".into(), json!({ "ID": id }));
        }
    }
    if let Some(v) = &b.progress_detail {
        m.insert(
            "progressDetail".into(),
            json!({ "current": v.current, "total": v.total }),
        );
    }
    if let Some(v) = &b.error_detail {
        m.insert(
            "errorDetail".into(),
            json!({ "code": v.code, "message": v.message }),
        );
    }
    JsonValue::Object(m)
}

fn tar_gzip_directory(dir: &Path) -> Result<Vec<u8>> {
    let dir = std::fs::canonicalize(dir)
        .with_context(|| format!("canonicalize {}", dir.display()))?;
    if !dir.is_dir() {
        bail!("build context `{}` is not a directory", dir.display());
    }
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = tar::Builder::new(enc);

    for entry in walkdir::WalkDir::new(&dir).follow_links(false) {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(&dir).unwrap();
        if rel.as_os_str().is_empty() {
            continue;
        }
        // skip .git for sanity; users can ship anything else
        if rel.components().any(|c| c.as_os_str() == ".git") {
            continue;
        }
        let meta = entry.metadata()?;
        if meta.is_dir() {
            builder
                .append_dir(rel, path)
                .with_context(|| format!("tar dir {}", rel.display()))?;
        } else if meta.is_file() {
            let mut f = std::fs::File::open(path)
                .with_context(|| format!("open {}", path.display()))?;
            builder
                .append_file(rel, &mut f)
                .with_context(|| format!("tar file {}", rel.display()))?;
        }
    }
    let enc = builder.into_inner()?;
    let gz = enc.finish().context("gzip finish")?;
    Ok(gz)
}

/* ------------------------------------------------------------------------- */
/* networks                                                                  */
/* ------------------------------------------------------------------------- */

async fn cmd_networks(
    d: &Docker,
    filters: Vec<(String, String)>,
) -> Result<()> {
    let opts = ListNetworksOptions::<String> { filters: kv_pairs(&filters) };
    let list = d.list_networks(Some(opts)).await.context("list_networks")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    for n in list {
        emit_ndjson(&mut out, &n)?;
    }
    Ok(())
}

async fn cmd_network_create(
    d: &Docker,
    name: &str,
    driver: &str,
    subnet: Option<&str>,
    gateway: Option<&str>,
    labels: &[String],
) -> Result<()> {
    use bollard::secret::{Ipam, IpamConfig};
    let ipam = if subnet.is_some() || gateway.is_some() {
        Some(Ipam {
            driver: Some("default".to_string()),
            config: Some(vec![IpamConfig {
                subnet: subnet.map(|s| s.to_string()),
                gateway: gateway.map(|s| s.to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        })
    } else {
        None
    };
    let opts = CreateNetworkOptions::<String> {
        name: name.to_string(),
        driver: driver.to_string(),
        labels: label_vec_to_map(labels),
        ipam: ipam.unwrap_or_default(),
        ..Default::default()
    };
    let r = d.create_network(opts).await.context("create_network")?;
    emit_json(&r)
}

async fn cmd_network_rm(d: &Docker, name: &str) -> Result<()> {
    d.remove_network(name).await.context("remove_network")?;
    emit_json(&json!({ "name": name, "removed": true }))
}

/* ------------------------------------------------------------------------- */
/* volumes                                                                   */
/* ------------------------------------------------------------------------- */

async fn cmd_volumes(d: &Docker, filters: Vec<(String, String)>) -> Result<()> {
    let opts = ListVolumesOptions::<String> { filters: kv_pairs(&filters) };
    let r = d.list_volumes(Some(opts)).await.context("list_volumes")?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    if let Some(vols) = r.volumes {
        for v in vols {
            emit_ndjson(&mut out, &v)?;
        }
    }
    Ok(())
}

async fn cmd_volume_create(
    d: &Docker,
    name: &str,
    driver: &str,
    labels: &[String],
) -> Result<()> {
    let opts = CreateVolumeOptions::<String> {
        name: name.to_string(),
        driver: driver.to_string(),
        labels: label_vec_to_map(labels),
        ..Default::default()
    };
    let v = d.create_volume(opts).await.context("create_volume")?;
    emit_json(&v)
}

async fn cmd_volume_rm(d: &Docker, name: &str, force: bool) -> Result<()> {
    d.remove_volume(name, Some(bollard::volume::RemoveVolumeOptions { force }))
        .await
        .context("remove_volume")?;
    emit_json(&json!({ "name": name, "removed": true }))
}

/* ------------------------------------------------------------------------- */
/* prune                                                                     */
/* ------------------------------------------------------------------------- */

async fn cmd_prune(
    d: &Docker,
    containers: bool,
    images: bool,
    volumes: bool,
    networks: bool,
) -> Result<()> {
    let mut report = serde_json::Map::new();
    if containers {
        let r = d
            .prune_containers(None::<bollard::container::PruneContainersOptions<String>>)
            .await
            .context("prune_containers")?;
        report.insert("containers".into(), serde_json::to_value(r)?);
    }
    if images {
        let r = d
            .prune_images(None::<bollard::image::PruneImagesOptions<String>>)
            .await
            .context("prune_images")?;
        report.insert("images".into(), serde_json::to_value(r)?);
    }
    if volumes {
        let r = d
            .prune_volumes(None::<bollard::volume::PruneVolumesOptions<String>>)
            .await
            .context("prune_volumes")?;
        report.insert("volumes".into(), serde_json::to_value(r)?);
    }
    if networks {
        let r = d
            .prune_networks(None::<bollard::network::PruneNetworksOptions<String>>)
            .await
            .context("prune_networks")?;
        report.insert("networks".into(), serde_json::to_value(r)?);
    }
    emit_json(&JsonValue::Object(report))
}

/* silence unused import in some build configurations */
#[allow(dead_code)]
fn _read_marker() {
    let _ = io::stdin();
    let _ = AttachContainerOptions::<String>::default();
    let _: Option<&mut dyn Read> = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_kv ────────────────────────────────────────────────────

    #[test]
    fn parse_kv_simple() {
        let (k, v) = parse_kv("FOO=bar").unwrap();
        assert_eq!(k, "FOO");
        assert_eq!(v, "bar");
    }

    #[test]
    fn parse_kv_splits_on_first_equals() {
        // Values may contain '=' (e.g. KMS=key=value); first '=' splits.
        let (k, v) = parse_kv("KEY=val=ue=more").unwrap();
        assert_eq!(k, "KEY");
        assert_eq!(v, "val=ue=more");
    }

    #[test]
    fn parse_kv_empty_value_allowed() {
        let (k, v) = parse_kv("EMPTY=").unwrap();
        assert_eq!(k, "EMPTY");
        assert_eq!(v, "");
    }

    #[test]
    fn parse_kv_empty_key_allowed() {
        // Liberal parser — empty key pinned; downstream may reject.
        let (k, v) = parse_kv("=value").unwrap();
        assert_eq!(k, "");
        assert_eq!(v, "value");
    }

    #[test]
    fn parse_kv_missing_equals_errors() {
        let err = parse_kv("noequals").unwrap_err();
        assert!(err.contains("expected key=value"));
        assert!(err.contains("noequals"));
    }

    // ─── kv_pairs ────────────────────────────────────────────────────

    #[test]
    fn kv_pairs_groups_duplicate_keys() {
        let input = vec![
            ("label".into(), "a".into()),
            ("label".into(), "b".into()),
            ("env".into(), "X=1".into()),
        ];
        let out = kv_pairs(&input);
        assert_eq!(out.get("label").unwrap(), &vec!["a", "b"]);
        assert_eq!(out.get("env").unwrap(), &vec!["X=1"]);
    }

    #[test]
    fn kv_pairs_empty_input() {
        let out = kv_pairs(&[]);
        assert!(out.is_empty());
    }

    #[test]
    fn kv_pairs_preserves_insertion_order_within_key() {
        let input = vec![
            ("k".into(), "1".into()),
            ("k".into(), "2".into()),
            ("k".into(), "3".into()),
        ];
        let out = kv_pairs(&input);
        assert_eq!(out["k"], vec!["1", "2", "3"]);
    }

    // ─── parse_port_binding ──────────────────────────────────────────

    #[test]
    fn parse_port_binding_host_only() {
        let (h, c, p) = parse_port_binding("8080").unwrap();
        assert_eq!(h, "8080");
        // No ':' → container defaults to same value as host.
        assert_eq!(c, "8080");
        assert_eq!(p, None);
    }

    #[test]
    fn parse_port_binding_host_container() {
        let (h, c, p) = parse_port_binding("8080:80").unwrap();
        assert_eq!(h, "8080");
        assert_eq!(c, "80");
        assert_eq!(p, None);
    }

    #[test]
    fn parse_port_binding_with_protocol() {
        let (h, c, p) = parse_port_binding("8080:80/tcp").unwrap();
        assert_eq!(h, "8080");
        assert_eq!(c, "80");
        assert_eq!(p.as_deref(), Some("tcp"));
    }

    #[test]
    fn parse_port_binding_proto_without_container() {
        let (h, c, p) = parse_port_binding("53/udp").unwrap();
        assert_eq!(h, "53");
        assert_eq!(c, "53");
        assert_eq!(p.as_deref(), Some("udp"));
    }

    #[test]
    fn parse_port_binding_proto_split_runs_before_colon() {
        // '/' splits first, then ':' applies to the LEFT half only.
        let (h, c, p) = parse_port_binding("a:b/c:d").unwrap();
        assert_eq!(h, "a");
        assert_eq!(c, "b");
        // Everything after first '/' is the proto, verbatim.
        assert_eq!(p.as_deref(), Some("c:d"));
    }

    // ─── label_vec_to_map ────────────────────────────────────────────

    #[test]
    fn label_vec_to_map_parses_key_value() {
        let v = vec!["a=1".into(), "b=2".into()];
        let m = label_vec_to_map(&v);
        assert_eq!(m.get("a").map(String::as_str), Some("1"));
        assert_eq!(m.get("b").map(String::as_str), Some("2"));
    }

    #[test]
    fn label_vec_to_map_silently_drops_malformed() {
        // filter_map: entries without '=' are dropped, not errored.
        let v = vec!["a=1".into(), "no-equals".into(), "b=2".into()];
        let m = label_vec_to_map(&v);
        assert_eq!(m.len(), 2);
        assert!(!m.contains_key("no-equals"));
    }

    #[test]
    fn label_vec_to_map_last_wins_on_duplicate_key() {
        // HashMap insertion order is iterator order; last collected wins.
        let v = vec!["k=first".into(), "k=second".into()];
        let m = label_vec_to_map(&v);
        assert_eq!(m.get("k").map(String::as_str), Some("second"));
    }

    #[test]
    fn label_vec_to_map_empty_vec() {
        let m = label_vec_to_map(&[]);
        assert!(m.is_empty());
    }

    // ─── emit_ndjson ─────────────────────────────────────────────────

    #[test]
    fn emit_ndjson_single_line() {
        let mut buf = Vec::new();
        emit_ndjson(&mut buf, &json!({"id": "abc"})).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "{\"id\":\"abc\"}\n");
    }

    #[test]
    fn emit_ndjson_multi_call_count() {
        let mut buf = Vec::new();
        for i in 0..5 {
            emit_ndjson(&mut buf, &json!({"i": i})).unwrap();
        }
        let s = String::from_utf8(buf).unwrap();
        assert_eq!(s.lines().count(), 5);
    }

    // ─── build_info_to_json ──────────────────────────────────────────

    #[test]
    fn build_info_to_json_empty_produces_empty_object() {
        let b = bollard::models::BuildInfo::default();
        let j = build_info_to_json(&b);
        assert_eq!(j, JsonValue::Object(serde_json::Map::new()));
    }

    #[test]
    fn build_info_to_json_populates_present_fields_only() {
        let mut b = bollard::models::BuildInfo::default();
        b.id = Some("img1".into());
        b.stream = Some("Step 1/3\n".into());
        let j = build_info_to_json(&b);
        let obj = j.as_object().unwrap();
        assert_eq!(obj.get("id").and_then(|v| v.as_str()), Some("img1"));
        assert_eq!(obj.get("stream").and_then(|v| v.as_str()), Some("Step 1/3\n"));
        // Unset fields are NOT included (skip-if-None semantics).
        assert!(!obj.contains_key("error"));
        assert!(!obj.contains_key("status"));
    }

    #[test]
    fn build_info_to_json_includes_progress_detail_when_set() {
        let mut b = bollard::models::BuildInfo::default();
        b.progress_detail = Some(bollard::models::ProgressDetail {
            current: Some(50),
            total: Some(100),
        });
        let j = build_info_to_json(&b);
        let pd = j.get("progressDetail").unwrap();
        assert_eq!(pd["current"], json!(50));
        assert_eq!(pd["total"], json!(100));
    }
}
