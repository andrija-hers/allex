use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
};

use allex_dart_codegen::{
    DtoInfo, RpcModuleInfo, find_dto_types, find_rpc_modules, reachable_dtos,
    referenced_type_names, render_dart_client_importing, render_dart_dtos,
};

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    let usage = "usage: cargo run -p allex-dart-codegen -- --source <rust-file> [--source <rust-file> ...] [--extra-import <dart-path> ...] --out-dir <dart-dir>";
    let (source_paths, out_dir, extra_imports) = parse_args(args, usage)?;

    // Parse each source file independently and merge the results. A plain text concatenation of
    // Rust files is not valid Rust — a non-first file's `//!` inner-doc header (or any `#![…]` crate
    // attribute) is only legal at the very start of a file, so it would fail to parse mid-stream.
    let mut modules: Vec<RpcModuleInfo> = Vec::new();
    let mut all_dtos: Vec<DtoInfo> = Vec::new();
    for source_path in &source_paths {
        let contents = fs::read_to_string(source_path)
            .map_err(|err| format!("failed to read {}: {err}", source_path.display()))?;
        modules.extend(find_rpc_modules(&contents));
        all_dtos.extend(find_dto_types(&contents));
    }
    if modules.is_empty() {
        let listed = source_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!("no #[rpc_module] traits found in {listed}"));
    }

    fs::create_dir_all(&out_dir)
        .map_err(|err| format!("failed to create {}: {err}", out_dir.display()))?;

    // All modules generated in one run share a single `dtos.dart`. DTOs referenced by more
    // than one module would otherwise be emitted into each module's file, so an app importing
    // two clients would hit duplicate-definition clashes. Collect the union of reachable DTOs
    // once and have every client import the shared file.
    let mut roots: Vec<String> = Vec::new();
    for module in &modules {
        roots.extend(referenced_type_names(module));
    }
    roots.sort();
    roots.dedup();
    let root_refs = roots.iter().map(String::as_str).collect::<Vec<_>>();
    let dtos = reachable_dtos(all_dtos, &root_refs);

    const SHARED_DTOS: &str = "dtos.dart";
    let dto_path = out_dir.join(SHARED_DTOS);
    let extra_import_refs = extra_imports.iter().map(String::as_str).collect::<Vec<_>>();
    fs::write(
        &dto_path,
        render_dart_dtos("shared", &dtos, &extra_import_refs),
    )
    .map_err(|err| format!("failed to write {}: {err}", dto_path.display()))?;

    for module in &modules {
        let client_path = out_dir.join(format!("{}_client.dart", module.module_name));
        fs::write(
            &client_path,
            render_dart_client_importing(module, SHARED_DTOS),
        )
        .map_err(|err| format!("failed to write {}: {err}", client_path.display()))?;
    }

    Ok(())
}

fn parse_args(
    args: Vec<String>,
    usage: &str,
) -> Result<(Vec<PathBuf>, PathBuf, Vec<String>), String> {
    let mut source_paths: Vec<PathBuf> = Vec::new();
    let mut out_dir: Option<PathBuf> = None;
    let mut extra_imports: Vec<String> = Vec::new();
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--source" => {
                let value = iter
                    .next()
                    .ok_or_else(|| format!("missing value for --source\n{usage}"))?;
                source_paths.push(PathBuf::from(value));
            }
            "--out-dir" => {
                let value = iter
                    .next()
                    .ok_or_else(|| format!("missing value for --out-dir\n{usage}"))?;
                out_dir = Some(PathBuf::from(value));
            }
            // A DTO can legitimately live in hand-written Dart: a type the app
            // gives real behaviour to (formatting, derived getters) is not one
            // the generator should flatten into a data class. `render_dart_dtos`
            // has always taken these; only the CLI had no way to pass them, so
            // moving to a shared dtos.dart silently dropped the per-module
            // imports consumers relied on.
            "--extra-import" => {
                let value = iter
                    .next()
                    .ok_or_else(|| format!("missing value for --extra-import\n{usage}"))?;
                extra_imports.push(value);
            }
            "--help" | "-h" => return Err(usage.to_string()),
            other => return Err(format!("unknown argument `{other}`\n{usage}")),
        }
    }

    if source_paths.is_empty() {
        return Err(format!("missing --source\n{usage}"));
    }
    let out_dir = out_dir.ok_or_else(|| format!("missing --out-dir\n{usage}"))?;

    for source_path in &source_paths {
        if !Path::new(source_path).is_file() {
            return Err(format!(
                "source file does not exist: {}",
                source_path.display()
            ));
        }
    }

    Ok((source_paths, out_dir, extra_imports))
}
