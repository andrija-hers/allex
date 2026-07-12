//! Golden test: generating the Dart client for allex's own `IdentityV1` service from the real
//! `identity/lib.rs` + `identity/types.rs` must produce the checked-in reference under
//! `tests/golden/`. This is the "ship the identity client" guarantee — consumers no longer
//! hand-write it (see stasera's former `core/allex/identity.dart`), and any change to the identity
//! DTOs that would reflow the generated client fails here until the golden is re-blessed.
//!
//! The identity trait's file leads with a `//!` header, so this also exercises the per-file
//! parsing path (a plain concatenation of the two sources would not parse).
//!
//! Re-bless after an intentional change: `UPDATE_GOLDEN=1 cargo test -p allex-dart-codegen`.

use std::fs;
use std::path::{Path, PathBuf};

use allex_dart_codegen::{
    find_dto_types, find_rpc_modules, reachable_dtos, referenced_type_names,
    render_dart_client_importing, render_dart_dtos,
};

fn identity_source_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../identity")
}

fn golden_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden")
}

/// Regenerate the identity client + DTOs the same way `main.rs` does (parse each source
/// independently, merge, resolve reachable DTOs).
fn generate() -> (String, String) {
    let dir = identity_source_dir();
    let sources = [dir.join("lib.rs"), dir.join("types.rs")];

    let mut modules = Vec::new();
    let mut all_dtos = Vec::new();
    for path in &sources {
        let src = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        modules.extend(find_rpc_modules(&src));
        all_dtos.extend(find_dto_types(&src));
    }
    assert_eq!(modules.len(), 1, "expected exactly the IdentityV1 module");

    let mut roots: Vec<String> = modules.iter().flat_map(referenced_type_names).collect();
    roots.sort();
    roots.dedup();
    let root_refs: Vec<&str> = roots.iter().map(String::as_str).collect();
    let dtos = reachable_dtos(all_dtos, &root_refs);

    let client = render_dart_client_importing(&modules[0], "dtos.dart");
    let dtos_out = render_dart_dtos("shared", &dtos, &[]);
    (client, dtos_out)
}

fn assert_golden(name: &str, actual: &str) {
    let path = golden_dir().join(name);
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        fs::write(&path, actual).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
        return;
    }
    let expected = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read golden {}: {e}", path.display()));
    assert_eq!(
        actual, expected,
        "generated {name} differs from tests/golden/{name}; \
         re-bless with UPDATE_GOLDEN=1 cargo test -p allex-dart-codegen"
    );
}

#[test]
fn identity_client_matches_golden() {
    let (client, dtos) = generate();
    assert_golden("identity_v_1_client.dart", &client);
    assert_golden("dtos.dart", &dtos);

    // Independent of byte-exactness: the whole IdentityV1 surface is present.
    for method in ["register", "login", "refresh", "validate", "logout"] {
        assert!(
            client.contains(&format!(" {method}(")),
            "generated client is missing the {method} method"
        );
    }
    // Wire keys must stay snake_case even though Dart fields are camelCase.
    assert!(dtos.contains("refresh_token"), "TokenResponse must map refresh_token");
}
