```
 ███████╗████████╗██████╗ ██╗   ██╗██╗  ██╗███████╗
 ██╔════╝╚══██╔══╝██╔══██╗╚██╗ ██╔╝██║ ██╔╝██╔════╝
 ███████╗   ██║   ██████╔╝ ╚████╔╝ █████╔╝ █████╗
 ╚════██║   ██║   ██╔══██╗  ╚██╔╝  ██╔═██╗ ██╔══╝
 ███████║   ██║   ██║  ██║   ██║   ██║  ██╗███████╗
 ╚══════╝   ╚═╝   ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝
                   [ d o c k e r ]
```

[![CI](https://github.com/MenkeTechnologies/stryke-docker/actions/workflows/ci.yml/badge.svg)](https://github.com/MenkeTechnologies/stryke-docker/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![stryke](https://img.shields.io/badge/stryke-package-cyan.svg)](https://github.com/MenkeTechnologies/strykelang)

### `[DOCKER CLIENT FOR STRYKE // CONTAINERS + IMAGES + NETWORKS + VOLUMES + BUILD + LOGS + EXEC + EVENTS]`

> *"The docker daemon, scriptable from a one-liner."*

Docker client for stryke. Containers, images, networks, volumes, build,
logs, exec, events against any reachable Docker daemon (Docker Desktop,
Linux daemon, Podman with the docker-API socket, remote DOCKER_HOST,
TLS-wrapped). Opt-in package tier.

> The CLI launcher is named `dock` (not `docker`) to avoid shadowing
> the real `docker` binary on $PATH after `s pkg install -g .`.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-k8s`](https://github.com/MenkeTechnologies/stryke-k8s) · [`stryke-kafka`](https://github.com/MenkeTechnologies/stryke-kafka) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] CLI: `dock`](#0x02-cli-dock)
- [\[0x03\] API reference](#0x03-api-reference)
- [\[0x04\] Helper protocol](#0x04-helper-protocol)
- [\[0x05\] Tests](#0x05-tests)
- [\[0x06\] Dev workflow](#0x06-dev-workflow)
- [\[0x07\] Layout](#0x07-layout)
- [\[0x08\] Roadmap](#0x08-roadmap)
- [\[0xFF\] License](#0xff-license)

---

## [0x00] Install

From a release (no rustc on the consumer machine):

```sh
s pkg install -g github.com/MenkeTechnologies/stryke-docker
```

From a local checkout:

```sh
cd ~/projects/stryke-docker
cargo build --release
s pkg install -g .
```

Or:

```sh
make install
```

The cdylib is dlopened in-process on first `use Docker`. A shared tokio
runtime + persistent `bollard::Docker` client is held in `OnceCell` for
the life of the process — no fork-per-call, no fresh HTTP connection
each time.

## [0x01] Quick start

```stryke
use Docker

# Connection: $DOCKER_HOST or local socket — no setup.
p Docker::version()->{Version}
exit 1 unless Docker::ping()

# Pull, run, inspect, stop, rm.
Docker::pull "nginx:alpine"

my $r = Docker::run "nginx:alpine",
    name   => "web",
    port   => ["8080:80/tcp"],
    label  => ["app=web"],
    detach => 1
p "started: $r->{id}"

# List with filters.
my @web = Docker::ps all => 1, filters => { label => ["app=web"] }
for my $c (@web) {
    p "  $c->{Id}  $c->{State}  " . join(",", @{$c->{Names}})
}

# Buffered logs.
p Docker::logs "web", tail => "50", timestamps => 1

# Streaming logs.
Docker::logs_follow "web",
    callback => sub ($stream, $data) { print $data }

# Exec.
my $r = Docker::exec "web", ["sh", "-c", "nginx -v"],
    callback => sub ($s, $d) { print $d }
p "exit_code=$r->{exit_code}"

# Stats snapshot.
my $s = Docker::stats "web"
p "mem=$s->{memory_stats}{usage} cpu_total=$s->{cpu_stats}{cpu_usage}{total_usage}"

# Build from a context dir.
Docker::build "./app",
    tag        => "my/app:latest",
    pull       => 1,
    rm         => 1,
    build_args => { COMMIT => "abc123" },
    callback   => sub ($evt) { print $evt->{stream} if defined $evt->{stream} }

# Networks + volumes.
Docker::network_create "appnet", driver => "bridge", subnet => "10.42.0.0/24"
Docker::volume_create  "appdata"
p $_->{Name} for Docker::volumes()

# Cleanup.
Docker::stop "web", time => 5
Docker::rm   "web"
Docker::network_rm "appnet"
Docker::volume_rm  "appdata"

# Reclaim space.
my $report = Docker::prune all => 1
p "freed: $report->{containers}{SpaceReclaimed} bytes"
```

Per-call connection overrides:

```stryke
my %remote = (host => "tcp://docker.example.com:2376", timeout => 30)
Docker::ps %remote
```

## [0x02] CLI: `dock`

```sh
dock ping
dock version
dock info
dock events --filter type=container

dock ps --all --filter label=app=web
dock inspect web
dock run nginx:alpine --name web -p 8080:80/tcp -l app=web
dock create nginx:alpine --name worker
dock start  web
dock stop   web --time=5
dock restart web
dock kill   web --signal=SIGTERM
dock rm     web --force --volumes
dock logs   web --tail=50 --timestamps
dock logs   web --follow
dock exec   web --cmd sh -- -c "nginx -v"
dock stats  web
dock stats  web --stream

dock images
dock pull   nginx:alpine
dock push   my/app:1.2.3
dock rmi    nginx:alpine
dock tag    sha256:abc... my/app:latest
dock build  ./app --tag my/app:latest --pull --rm --build-arg COMMIT=abc123

dock networks
dock network-create appnet --driver bridge --subnet 10.42.0.0/24
dock network-rm     appnet

dock volumes
dock volume-create appdata
dock volume-rm     appdata --force

dock prune --all
dock prune --containers --images

dock build-helper      # cargo build --release
```

Global flags (also env vars):

```
--host URL              $DOCKER_HOST       unix://… or tcp://…
--timeout SECONDS                          API timeout (default 120)
```

## [0x03] API reference

### Daemon

```stryke
Docker::ping     %opts → 1 | ""
Docker::version  %opts → \%info             # Version, ApiVersion, Os, Arch, …
Docker::info     %opts → \%info
Docker::events   %opts → $count             # callback => sub ($evt) { … }, since/until/filters
```

### Containers

```stryke
Docker::ps        %opts → @{ \%container }
Docker::inspect   $container, %opts → \%info
Docker::run       $image, %opts → { id, warnings, started }
Docker::create    $image, %opts → { id, warnings, started => false }
Docker::start     $container, %opts → { id, started }
Docker::stop      $container, %opts → { id, stopped }    # opts: time
Docker::restart   $container, %opts → { id, restarted }
Docker::kill      $container, %opts → { id, killed }     # opts: signal
Docker::rm        $container, %opts → { id, removed }    # opts: force, volumes
```

`%opts` for run/create:
`name, cmd \@argv, env \@KV, port \@spec, volume \@spec, label \@KV,
network, workdir, user, hostname, restart, rm, tty`

### Logs + exec + stats

```stryke
Docker::logs         $container, %opts → $text
Docker::logs_follow  $container, %opts → $chunks   # callback => sub ($stream, $data) { … }
Docker::exec         $container, \@cmd, %opts → { chunks, exit_code }
Docker::stats        $container, %opts → \%snapshot
Docker::stats        $container, stream => 1, callback => sub { … } → $count
```

### Images

```stryke
Docker::images   %opts → @{ \%image }
Docker::pull     $image, %opts → @events | $count       # opts: platform, callback
Docker::push     $image, %opts → @events | $count       # opts: callback
Docker::rmi      $image, %opts → \@deleted              # opts: force, noprune
Docker::tag      $source, $target, %opts → { tagged }
Docker::build    $dir,  %opts → @events | $count
    # opts: tag, dockerfile, build_args \%KV, no_cache, pull, rm, callback
```

### Networks + volumes

```stryke
Docker::networks         %opts → @{ \%network }
Docker::network_create   $name, %opts → \%network     # driver, subnet, gateway, label
Docker::network_rm       $name, %opts → { removed }

Docker::volumes          %opts → @{ \%volume }
Docker::volume_create    $name, %opts → \%volume      # driver, label
Docker::volume_rm        $name, %opts → { removed }   # force
```

### Prune

```stryke
Docker::prune  %opts → \%report
    # opts: containers, images, volumes, networks, all
```

## [0x04] FFI layer

Each `Docker::*` wrapper builds a JSON args dict and calls a sibling
`docker__*` symbol resolved out of `libstryke_docker.{dylib,so}`. The
cdylib is dlopened in-process on first `use Docker` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook) and exposes 25 entry
points covering containers, images, networks, volumes, exec, logs, and
prune.

**Persistent state:**

* `RUNTIME` — one shared `tokio` multi-thread runtime drives every
  async call.
* `CLIENTS` — `bollard::Docker` cache keyed by `DOCKER_HOST` (socket
  path / tcp url). The v1 helper opened a fresh dockerd connection per
  fork; this reuses the same client + underlying HTTP pool across
  calls.

**Deferred from v0.2.0:** streaming-only ops (`events`, `stats`,
`logs --follow`, `build`, `push`). These need a callback FFI shape that
v1's `FfiSig::StrToStr` doesn't model. Calling them dies with a clear
message.

## [0x05] Tests

```sh
cargo test                                # compiles, no live calls
DOCKER_HOST=unix:///var/run/docker.sock s test t/   # 9-test live round-trip
```

Tests pull `busybox:latest`, run a sleep container with a unique
`stryke-test-$$` name, run network/volume creates with unique names,
exec into it, and clean up at exit.

Local test daemon:

```sh
# macOS
open -a Docker

# Linux
sudo systemctl start docker

# Or any reachable daemon:
DOCKER_HOST=tcp://192.168.1.10:2375 s test t/
```

## [0x06] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x07] Layout

```
stryke-docker/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # Rust helper crate manifest
  Makefile
  src/main.rs                      # single-file helper
  lib/
    Docker.stk                     # `use Docker`
  bin/
    dock.stk                       # `dock` CLI
    dock-build.stk
  t/
    test_docker.stk                # 9-test live round-trip
  examples/
    run.stk
    logs.stk
    build.stk
  .github/workflows/
    ci.yml                         # live docker socket + 9-test round-trip
    release.yml                    # cross-compile + GH release on tag push
```

## [0x08] Roadmap

| v1 (this release) | v2+ |
|---|---|
| Local socket + DOCKER_HOST tcp/http | TLS client certs (DOCKER_CERT_PATH / DOCKER_TLS_VERIFY) |
| One-file context tar (skips `.git/`) | `.dockerignore` honoring |
| Build / pull / push as event streams | Buildx multi-platform / cross-arch |
| Exec stdout/stderr stream | Interactive TTY + stdin attach |
| Synchronous create/start/stop | docker-compose v2 file parser |
| Single-daemon | Swarm services / configs / secrets |

## [0xFF] License

MIT.
