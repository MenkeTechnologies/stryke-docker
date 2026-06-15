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

### `[DOCKER CLIENT FOR STRYKE // CONTAINERS + IMAGES + NETWORKS + VOLUMES + LOGS + EXEC + PRUNE]`

> *"The docker daemon, scriptable from a one-liner."*

Docker client for stryke. Containers, images, networks, volumes, logs,
exec, prune against any reachable Docker daemon (Docker Desktop, Linux
daemon, Podman with the docker-API socket, remote DOCKER_HOST). Opt-in
package tier.

### [`strykelang`](https://github.com/MenkeTechnologies/strykelang) &middot; [`MenkeTechnologiesMeta`](https://github.com/MenkeTechnologies/MenkeTechnologiesMeta) · [`stryke-k8s`](https://github.com/MenkeTechnologies/stryke-k8s) · [`stryke-kafka`](https://github.com/MenkeTechnologies/stryke-kafka) · [`stryke-demo`](https://github.com/MenkeTechnologies/stryke-demo)

---

## Table of Contents

- [\[0x00\] Install](#0x00-install)
- [\[0x01\] Quick start](#0x01-quick-start)
- [\[0x02\] API reference](#0x02-api-reference)
- [\[0x03\] FFI layer](#0x03-ffi-layer)
- [\[0x04\] Tests](#0x04-tests)
- [\[0x05\] Dev workflow](#0x05-dev-workflow)
- [\[0x06\] Layout](#0x06-layout)
- [\[0x07\] Roadmap](#0x07-roadmap)
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

# Exec — returns captured stdout+stderr.
p Docker::exec "web", ["sh", "-c", "nginx -v"]

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

## [0x02] API reference

### Daemon

```stryke
Docker::ping     %opts → 1 | ""
Docker::version  %opts → \%info             # Version, ApiVersion, Os, Arch, …
Docker::info     %opts → \%info
Docker::events   %opts → dies               # streaming — deferred in v0.2.x cdylib
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
Docker::pause     $container, %opts → 1 | 0
Docker::unpause   $container, %opts → 1 | 0
Docker::rename    $container, $name, %opts → 1 | 0
Docker::wait      $container, %opts → $exit_status_code  # blocks until exit
Docker::top       $container, %opts → { Titles, Processes }   # opts: ps_args
Docker::commit    $container, %opts → $image_id          # opts: repo, tag, comment, author, pause
```

`%opts` for run/create:
`name, cmd \@argv, env \@KV, port \@spec, volume \@spec, label \@KV,
network, workdir, user, hostname, restart, rm, tty`

### Logs + exec + stats

```stryke
Docker::logs         $container, %opts → $text
Docker::logs_follow  $container, %opts → dies      # streaming — deferred in v0.2.x cdylib
Docker::exec         $container, \@cmd, %opts → $output   # captured stdout+stderr
Docker::stats        $container, %opts → \%snapshot   # one-shot stats (--no-stream)
Docker::diff         $container, %opts → @{ {Path, Kind} }   # fs changes vs image (0 mod,1 add,2 del)
Docker::history      $image, %opts → @layers           # image build history
Docker::df           %opts → \%usage                   # docker system df (images/containers/volumes/cache)
Docker::port         $container, %opts → \%ports        # { "80/tcp" => [{HostIp, HostPort}] }
Docker::update       $container, %opts → \%resp         # live limits: memory, cpu_shares, cpu_quota, cpuset_cpus
Docker::image_inspect   $image, %opts → \%info          # config, layers, RootFS
Docker::volume_inspect  $volume, %opts → \%info         # driver, mountpoint, labels
Docker::network_inspect $network, %opts → \%info        # driver, subnet, connected containers
```

### Pure helpers (no daemon)

```stryke
Docker::parse_image_ref($ref)      → { registry, namespace, repository, tag, digest, path }
Docker::valid_container_name($n)   → 1 | ""    # /?[a-zA-Z0-9][a-zA-Z0-9_.-]+
Docker::parse_port_spec($spec)     → { host_ip, host_port, container_port, protocol }
Docker::parse_mount($spec)         → { type, source, target, readonly, options }   # -v src:dst[:opts]
```

`parse_mount` classifies a `-v` short mount: a host-path source (`/`, `.`, `~`)
is a `bind`, a bare name is a `volume`, and a lone container path is an
`anonymous` volume. The comma list after the second colon becomes `options`,
and an `ro` entry sets `readonly`.

### Images

```stryke
Docker::images   %opts → @{ \%image }
Docker::pull     $image, %opts → @events                # drained event list
Docker::push     $image, %opts → dies                   # deferred in v0.2.x cdylib
Docker::rmi      $image, %opts → 1 | 0                  # opts: force
Docker::tag      $source, $target, %opts → 1 | 0
Docker::build    $dir,  %opts → dies                    # deferred in v0.2.x cdylib
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

## [0x03] FFI layer

Each `Docker::*` wrapper builds a JSON args dict and calls a sibling
`docker__*` symbol resolved out of `libstryke_docker.{dylib,so}`. The
cdylib is dlopened in-process on first `use Docker` (via stryke's
`pkg::commands::try_load_ffi_for` resolver hook). Its exports cover
containers, images, networks, volumes, exec, logs, and prune, plus
daemon-free helpers (`docker__parse_image_ref`,
`docker__valid_container_name`, `docker__parse_port_spec`,
`docker__parse_mount`). The
authoritative list is `[ffi].exports` in `stryke.toml`.

**Persistent state:**

* `RUNTIME` — one shared `tokio` multi-thread runtime drives every
  async call.
* `CLIENTS` — `bollard::Docker` cache keyed by `DOCKER_HOST` (socket
  path / tcp url). The v1 helper opened a fresh dockerd connection per
  fork; this reuses the same client + underlying HTTP pool across
  calls.

**Snapshot vs. streaming:** `stats` is a one-shot snapshot (`--no-stream`).
The remaining continuous-stream ops (`events`, `logs --follow`) and the
auth/tar-heavy `build` / `push` need a callback FFI / credential / tar-stream
design that the blocking `StrToStr` shape doesn't model yet; calling them
dies with a clear message.

## [0x04] Tests

```sh
cargo test                                # compiles, no live calls
DOCKER_HOST=unix:///var/run/docker.sock s test t/   # live round-trip
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

## [0x05] Dev workflow

```sh
make             # release build
make debug
make test
make install
make clean
```

## [0x06] Layout

```
stryke-docker/
  stryke.toml                      # stryke package manifest
  Cargo.toml                       # cdylib crate manifest
  Makefile
  src/lib.rs                       # cdylib — docker__* extern "C" exports
  lib/
    Docker.stk                     # `use Docker`
  t/
    test_docker.stk                # live round-trip (gated on a reachable daemon)
    test_stryke_docker_surface.stk # wrapper-completeness pin
  examples/
    build.stk
    discover.stk
    health.stk
    logs.stk
    run.stk
  .github/workflows/
    ci.yml                         # cargo check/test/clippy + docs lint (no live daemon)
    release.yml                    # cross-compile + GH release on tag push
```

## [0x07] Roadmap

| Shipped (v0.2.x) | Later |
|---|---|
| Local socket + DOCKER_HOST tcp/http | TLS client certs (DOCKER_CERT_PATH / DOCKER_TLS_VERIFY) |
| Pull (drained event list) | Build / push / events / logs --follow (need callback FFI / tar / auth) |
| Pause/unpause/rename/wait/top/commit + one-shot stats | Interactive TTY + stdin attach |
| Exec with captured stdout+stderr | docker-compose v2 file parser |
| Synchronous create/start/stop | Swarm services / configs / secrets |

## [0xFF] License

MIT.
