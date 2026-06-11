//! Pinned-invariant tests for `op_pull`'s `image` → `(from_image, tag)`
//! splitter in `src/lib.rs` (≈ line 298-304).
//!
//! Background: `op_pipeline_invariants.rs` pinned the *pre-fix* op_pull that
//! forwarded the whole image string and left bollard's `tag` field empty
//! (which dockerd interprets as "pull ALL tags"). Commit d1f743a627
//! ("fix(docker): op_pull splits image:tag and defaults to 'latest'") replaced
//! that with a real splitter:
//!
//! ```ignore
//! let (from_image, tag) = match image.rsplit_once(':') {
//!     Some((repo, t)) if !repo.contains('@') && !t.contains('/') => {
//!         (repo.to_string(), t.to_string())
//!     }
//!     _ => (image.clone(), "latest".to_string()),
//! };
//! ```
//!
//! That splitter has THREE non-obvious guard conditions whose interactions
//! are exactly where docker-reference parsing goes wrong:
//!   1. registry-with-port + no tag (`localhost:5000/img`) — the `:` belongs
//!      to the port, not a tag. Guarded by `!t.contains('/')`.
//!   2. digest form (`img@sha256:hex`) — the `:` belongs to the digest. Guarded
//!      by `!repo.contains('@')`.
//!   3. registry-with-port + path + explicit tag (`localhost:5000/img:3.18`) —
//!      MUST split on the *last* colon only (`rsplit_once`), keeping the port
//!      colon inside from_image.
//!
//! The crate is `crate-type = ["cdylib"]` only, so an integration test cannot
//! `use stryke_docker::*` and call `op_pull` directly (and it needs a docker
//! daemon anyway). Per the existing repo pattern we mirror the splitter EXACTLY
//! and pin its observable output. If the splitter in lib.rs changes, these
//! pins must change in the same diff — that is the point.

/// Byte-for-byte mirror of the `op_pull` image splitter in src/lib.rs.
/// Returns the `(from_image, tag)` pair handed to bollard's CreateImageOptions.
fn op_pull_split(image: &str) -> (String, String) {
    match image.rsplit_once(':') {
        Some((repo, t)) if !repo.contains('@') && !t.contains('/') => {
            (repo.to_string(), t.to_string())
        }
        _ => (image.to_string(), "latest".to_string()),
    }
}

/// Bare image name (no colon at all) must default the tag to "latest", NOT
/// empty string. This is the headline fix: an empty tag made dockerd pull
/// every tag of the repo. Regression here re-opens the bandwidth/disk blowup.
#[test]
fn bare_image_defaults_to_latest() {
    let (from_image, tag) = op_pull_split("alpine");
    assert_eq!(from_image, "alpine");
    assert_eq!(
        tag, "latest",
        "bare image must default to 'latest', never empty (empty = pull ALL tags)"
    );
}

/// Simple `repo:tag` form splits on the single colon. The tag must NOT remain
/// baked into from_image, and must NOT be defaulted to 'latest' (that would
/// silently downgrade an explicit version request).
#[test]
fn simple_repo_tag_splits_cleanly() {
    let (from_image, tag) = op_pull_split("alpine:3.18");
    assert_eq!(from_image, "alpine");
    assert_eq!(tag, "3.18");
}

/// Registry-with-port, NO explicit tag: `localhost:5000/img`. The only colon
/// is the port separator. `rsplit_once(':')` yields `("localhost", "5000/img")`
/// and the `!t.contains('/')` guard MUST reject it (t = "5000/img" has a slash),
/// falling through to the default arm: from_image keeps the port, tag="latest".
/// If the `!t.contains('/')` guard is dropped, this misparses to
/// from_image="localhost", tag="5000/img" — a broken reference dockerd rejects.
#[test]
fn registry_port_no_tag_keeps_port_in_image() {
    let (from_image, tag) = op_pull_split("localhost:5000/img");
    assert_eq!(
        from_image, "localhost:5000/img",
        "the port colon must stay inside from_image; it is not a tag separator"
    );
    assert_eq!(tag, "latest");
}

/// Registry-with-port AND explicit tag: `localhost:5000/img:3.18`. There are
/// two colons. `rsplit_once` (last colon) must split only the trailing `:3.18`,
/// keeping `localhost:5000/img` intact as from_image. A naive `split_once`
/// (first colon) would wrongly produce from_image="localhost",
/// tag="5000/img:3.18". This pins that rsplit (not split) is used.
#[test]
fn registry_port_with_explicit_tag_splits_on_last_colon_only() {
    let (from_image, tag) = op_pull_split("localhost:5000/img:3.18");
    assert_eq!(
        from_image, "localhost:5000/img",
        "must split on the LAST colon, preserving the registry port"
    );
    assert_eq!(tag, "3.18");
}

/// Digest form `img@sha256:<hex>`: the colon belongs to the digest algorithm,
/// not a tag. `rsplit_once(':')` yields `("img@sha256", "<hex>")` and the
/// `!repo.contains('@')` guard MUST reject it (repo contains '@'), so the whole
/// reference is forwarded as from_image with tag defaulted. Dropping the '@'
/// guard would mangle the digest into from_image="img@sha256", tag="<hex>".
#[test]
fn digest_form_is_not_split_into_tag() {
    let digest = "alpine@sha256:abc123def456";
    let (from_image, tag) = op_pull_split(digest);
    assert_eq!(
        from_image, digest,
        "digest reference must be forwarded whole; ':' after sha256 is not a tag"
    );
    assert_eq!(tag, "latest");
}

/// Digest WITH a registry port in front: `localhost:5000/img@sha256:hex`.
/// Two colons: the port colon and the digest colon. `rsplit_once` finds the
/// digest colon; repo = "localhost:5000/img@sha256" contains '@', guard rejects,
/// default arm keeps the entire reference intact. This is the combined-edge
/// case (port + digest) that would break if either guard were checked against
/// the wrong substring.
#[test]
fn registry_port_plus_digest_stays_whole() {
    let r = "localhost:5000/img@sha256:abc123";
    let (from_image, tag) = op_pull_split(r);
    assert_eq!(from_image, r);
    assert_eq!(tag, "latest");
}

/// Trailing-colon form `alpine:` — `rsplit_once` returns `Some(("alpine", ""))`.
/// `t = ""` contains no '/', repo no '@', so the guard PASSES and the splitter
/// produces from_image="alpine", tag="". This is a latent gap: an empty tag
/// reaches bollard via the Some arm even after the 'latest' default fix, because
/// the default only fires on the `_` arm. Pinning the CURRENT behaviour so any
/// future tightening (normalize empty -> "latest") is a visible diff.
#[test]
fn trailing_colon_still_yields_empty_tag_through_some_arm() {
    let (from_image, tag) = op_pull_split("alpine:");
    assert_eq!(from_image, "alpine");
    assert_eq!(
        tag, "",
        "current behaviour: 'alpine:' produces empty tag via the Some arm, \
         bypassing the 'latest' default which only guards the no-colon path"
    );
    // Guard: when the splitter is hardened to map Some(\"\") -> \"latest\",
    // this fires and forces the assertion above to be updated in the same diff.
    assert_ne!(
        tag, "latest",
        "if this fires, op_pull now normalizes the trailing-colon empty tag to \
         'latest' — update the assertion above and remove this guard"
    );
}
