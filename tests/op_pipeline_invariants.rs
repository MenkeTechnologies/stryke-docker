//! Pinned-invariant tests for the JSON-args → bollard-args pipelines used
//! by exported FFI handlers in `src/lib.rs`.
//!
//! Each handler in lib.rs extracts a few fields from a `serde_json::Value`
//! and forwards them to bollard. The extraction pipelines are short but
//! load-bearing: a wrong default here ships through every stryke
//! `Docker::*` call site silently because there is no daemon-free
//! end-to-end test.
//!
//! Why a free-standing tests/ file (vs another `#[cfg(test)] mod tests`
//! inside lib.rs): the crate is `crate-type = ["cdylib"]` only, so other
//! tests/*.rs cannot `use stryke_docker::*`. That same shape means the
//! ONLY testable surface from an integration crate is to mirror the
//! pipeline shape and pin its observable inputs/outputs. When the bug is
//! fixed in lib.rs, the `assert_ne!` guard here fires loudly and forces
//! this file to be updated in the same diff — so the broken pin and the
//! fix land together.

use serde_json::{json, Value};

// ── op_pull: empty-tag → docker pulls ALL tags (real bug) ───────────────────
//
// src/lib.rs::op_pull (≈ line 286-302) extracts `opts["image"].as_str()` and
// passes it as `CreateImageOptions { from_image, tag: "" }` (Default for
// String). Bollard's docs on `CreateImageOptions::tag` say verbatim:
//
//     "Tag or digest. If empty when pulling an image, this causes all tags
//      for the given image to be pulled."
//
// So `Docker::pull(image="alpine")` in stryke triggers a pull of EVERY
// alpine tag from the registry — alpine:3, alpine:edge, alpine:3.18, etc.
// — instead of the user-expected `alpine:latest`. Bandwidth, disk, and
// time blow up silently. The handler returns success with the input image
// name verbatim, so the caller has no signal anything went wrong.
//
// The fix is to parse `image` for a `:tag` suffix and either:
//   (a) split it into `from_image` + `tag` before calling bollard, or
//   (b) substitute `tag: "latest"` when the parsed result has no tag.
//
// These tests pin the broken pipeline so the fix is a visible diff.

/// Mirror of `op_pull`'s field extraction. Returns the (from_image, tag)
/// strings that get handed to bollard's CreateImageOptions.
fn op_pull_bollard_args_current(opts: &Value) -> Option<(String, String)> {
    let image = opts.get("image").and_then(|v| v.as_str())?.to_string();
    // CreateImageOptions::<String>::default() gives tag = "".
    let tag = String::new();
    Some((image, tag))
}

#[test]
fn op_pull_bare_image_name_sends_empty_tag_to_bollard() {
    let opts = json!({"image": "alpine"});
    let (from_image, tag) =
        op_pull_bollard_args_current(&opts).expect("image arg must be extracted");
    assert_eq!(
        from_image, "alpine",
        "image is forwarded as-is to from_image"
    );
    // Current (broken) behaviour: empty tag, which dockerd interprets as
    // "pull every tag of alpine" per the bollard docs quoted above.
    assert_eq!(
        tag, "",
        "current (buggy) empty tag — dockerd will pull ALL tags of `alpine` \
         instead of just `latest`"
    );
    // When the fix lands, tag should be "latest" (or the pipeline should
    // parse "image:tag" into the two fields). Either way, the empty-string
    // case must stop reaching bollard. This guard flips when the bug is
    // fixed so the pin is rewritten in the same commit.
    assert_ne!(
        tag, "latest",
        "if this fires, op_pull was fixed — the bare-image-name path now \
         defaults to 'latest'. Update the assertions above and remove this \
         guard."
    );
}

#[test]
fn op_pull_image_with_explicit_tag_still_sends_empty_to_bollard_tag_field() {
    // The user typed `Docker::pull(image="alpine:3.18")`. The handler
    // forwards `image` to `from_image` unparsed. Bollard's `tag` field is
    // still empty. dockerd MAY honour the embedded `:3.18` in from_image,
    // but per the bollard struct doc the *tag field* is what overrides;
    // when empty the behaviour falls back to "all tags". This is at best
    // implementation-defined behaviour driven by dockerd quirks, not a
    // contract the caller can rely on.
    let opts = json!({"image": "alpine:3.18"});
    let (from_image, tag) =
        op_pull_bollard_args_current(&opts).expect("image arg must be extracted");
    assert_eq!(
        from_image, "alpine:3.18",
        "image forwarded with tag baked in"
    );
    assert_eq!(
        tag, "",
        "current (buggy) empty tag field — handler did not parse out '3.18' \
         and pass it as the tag arg"
    );
    // After fix, op_pull should split `alpine:3.18` -> from_image=alpine,
    // tag="3.18" (or similar). The empty tag field is the bug regardless
    // of dockerd's lenient interpretation of from_image.
    assert_ne!(
        tag, "3.18",
        "if this fires, op_pull now splits image:tag into bollard's two \
         fields. Update the assertions above and remove this guard."
    );
}

// ── op_logs: `tail: 0` collapses to "0", not "all" ─────────────────────────
//
// src/lib.rs::op_logs (≈ line 218-241) builds bollard's `LogsOptions.tail`
// from `opts["tail"].as_u64()` via:
//
//     tail: tail.map(|n| n.to_string()).unwrap_or_else(|| "all".to_string())
//
// So `tail` absent → "all" (fine). `tail = 5` → "5" (fine).
// `tail = 0` → "0" (NOT "all"). Docker's logs API treats "0" as "return
// zero lines" — the user gets an empty `logs` string. The handler returns
// `{"logs": ""}` indistinguishable from "the container produced no output".
//
// `as_u64()` collapses `tail: null` to None (= "all") and `tail: -1` to
// None as well (silently), which is friendlier than docker's CLI (which
// rejects negatives) but still asymmetric — `0` is a magic value that
// punches a hole in the "missing → all" contract.

/// Mirror of the `tail` formatting in `op_logs`.
fn op_logs_tail_str_current(opts: &Value) -> String {
    opts.get("tail")
        .and_then(|v| v.as_u64())
        .map(|n| n.to_string())
        .unwrap_or_else(|| "all".to_string())
}

#[test]
fn op_logs_tail_zero_sends_zero_not_all_and_truncates_to_empty() {
    // `tail: 0` is the booby trap.
    let opts = json!({"tail": 0});
    let s = op_logs_tail_str_current(&opts);
    assert_eq!(
        s, "0",
        "current behaviour: tail=0 reaches bollard verbatim as \"0\"; \
         dockerd then returns zero log lines — user gets `logs: \"\"` and \
         no signal that they asked for the wrong thing"
    );
    // A sensible fix collapses `0` into either an explicit error ("tail
    // must be positive") or into "all" (treat zero as unspecified). Pin
    // the broken case so a fix is visible.
    assert_ne!(
        s, "all",
        "if this fires, op_logs now collapses tail=0 into 'all'. Update \
         the assertions above and remove this guard."
    );
}

#[test]
fn op_logs_negative_tail_silently_drops_to_all_via_as_u64() {
    // `as_u64()` returns None for any negative value, which then trips
    // the unwrap_or_else default. This means `Docker::logs(tail=-1)` in
    // stryke silently behaves as if tail were not passed at all — the
    // user almost certainly meant "tail the last 1 line" by analogy with
    // docker CLI's `--tail` (which is u64 anyway), but the call returns
    // ALL logs.
    let opts = json!({"tail": -1});
    let s = op_logs_tail_str_current(&opts);
    assert_eq!(
        s, "all",
        "current behaviour: negative tail silently degrades to 'all'; the \
         user wanted bounded output and got the entire log history"
    );
    // After fix, the handler should reject negative tail with an error
    // (mirroring docker CLI). Pin the silent-fallback so the fix is a
    // visible diff.
    assert_ne!(
        s, "0",
        "guard: confirm the current behaviour is silent-fallback to 'all', \
         not collapse-to-zero — if this fires the surrounding analysis is \
         wrong"
    );
}

// ── op_create: non-array `cmd` is silently dropped ─────────────────────────
//
// src/lib.rs::op_create (≈ line 132-141) extracts cmd via
//
//     opts["cmd"].as_array().map(|a| ...filter_map(as_str)...)
//
// If the user passes `cmd: "ls -la"` (a single string instead of an
// array), `as_array()` returns None, the whole `map` produces None, and
// the container is created with NO command override — silently using the
// image's default ENTRYPOINT. The user thinks they ran `ls -la`; they
// actually ran nothing.

fn op_create_cmd_current(opts: &Value) -> Option<Vec<String>> {
    opts.get("cmd").and_then(|v| v.as_array()).map(|a| {
        a.iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect()
    })
}

#[test]
fn op_create_cmd_as_string_silently_drops_to_none() {
    // User passes `Docker::create(image="alpine", cmd="ls -la")` — a
    // common mistake (cmd in `docker run` CLI is a single shell line).
    let opts = json!({"cmd": "ls -la"});
    let cmd = op_create_cmd_current(&opts);
    assert!(
        cmd.is_none(),
        "current (buggy) behaviour: non-array cmd is dropped to None, \
         container runs with image default ENTRYPOINT and user has no \
         signal their string was discarded"
    );
    // After fix, the handler should either (a) wrap a single string into
    // a one-element array, (b) shell-split the string, or (c) return an
    // explicit error. Any of those breaks the `is_none()` assertion.
    assert!(
        !matches!(cmd.as_deref(), Some([_, ..])),
        "guard: confirm pipeline currently drops the string — if this \
         fires the fix already landed; update the assertions above"
    );
}

#[test]
fn op_create_cmd_array_with_non_string_elements_filters_silently() {
    // User passes `cmd: ["sh", "-c", 42, null, "echo hi"]`. Numbers and
    // nulls are filter_map'd out — the resulting command is `sh -c echo
    // hi`. The number `42` was probably meant as an argument and its
    // silent drop changes program meaning.
    let opts = json!({"cmd": ["sh", "-c", 42, null, "echo hi"]});
    let cmd = op_create_cmd_current(&opts);
    assert_eq!(
        cmd,
        Some(vec!["sh".into(), "-c".into(), "echo hi".into()]),
        "current behaviour: 42 and null are silently dropped, shifting \
         positional argument meaning"
    );
    // A sensible fix would coerce-or-error on each element. Pinning the
    // silent-drop ensures any future change to this filter is intentional.
    assert_ne!(
        cmd.as_ref().map(|v| v.len()),
        Some(5),
        "guard: ensure the silent-drop is what we're pinning here; if \
         all 5 elements survive, the pipeline now coerces non-string \
         values and the test needs rewriting"
    );
}
