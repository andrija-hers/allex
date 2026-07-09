use std::{env, fs, path::PathBuf, process};

use allex_ts_codegen::{
    DtoInfo, RpcModuleInfo, find_dto_types, find_rpc_modules, reachable_dtos,
    referenced_type_names, render_client, render_dtos,
};

const USAGE: &str = "usage: cargo run -p allex-ts-codegen -- --source <rust-file> [--source ...] \
     --out-dir <ts-dir> [--runtime <import-specifier>]";

fn main() {
    if let Err(e) = run(env::args().skip(1).collect()) {
        eprintln!("{e}");
        process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    let cfg = parse_args(args)?;

    // Parse each source file independently and merge the results. A plain text concatenation of
    // Rust files is not valid Rust — a second file's `//!` inner-doc header (or any `#![…]` crate
    // attribute) is only legal at the very start of a file, so it would fail to parse mid-stream.
    let mut modules: Vec<RpcModuleInfo> = Vec::new();
    let mut all_dtos: Vec<DtoInfo> = Vec::new();
    for path in &cfg.sources {
        let source = fs::read_to_string(path)
            .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
        modules.extend(find_rpc_modules(&source));
        all_dtos.extend(find_dto_types(&source));
    }

    if modules.is_empty() {
        return Err("no #[rpc_module] traits found in the given source(s)".to_string());
    }

    fs::create_dir_all(&cfg.out_dir)
        .map_err(|e| format!("failed to create {}: {e}", cfg.out_dir.display()))?;

    // Shared dtos.ts: the union of every DTO reachable from any module's methods, across all files.
    let mut roots: Vec<String> = Vec::new();
    for m in &modules {
        roots.extend(referenced_type_names(m));
    }
    roots.sort();
    roots.dedup();
    let root_refs: Vec<&str> = roots.iter().map(String::as_str).collect();
    let dtos = reachable_dtos(all_dtos, &root_refs);

    const DTOS_FILE: &str = "dtos.ts";
    let dtos_import = "./dtos.ts";
    let dto_path = cfg.out_dir.join(DTOS_FILE);
    fs::write(&dto_path, render_dtos(&dtos))
        .map_err(|e| format!("failed to write {}: {e}", dto_path.display()))?;
    println!("wrote {}", dto_path.display());

    for m in &modules {
        let file = format!("{}_client.ts", m.module_name);
        let path = cfg.out_dir.join(&file);
        fs::write(&path, render_client(m, dtos_import, &cfg.runtime))
            .map_err(|e| format!("failed to write {}: {e}", path.display()))?;
        println!("wrote {}", path.display());
    }

    Ok(())
}

struct Config {
    sources: Vec<PathBuf>,
    out_dir: PathBuf,
    runtime: String,
}

fn parse_args(args: Vec<String>) -> Result<Config, String> {
    let mut sources = Vec::new();
    let mut out_dir = None;
    let mut runtime = "../allex/transport.ts".to_string();
    let mut it = args.into_iter();

    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--source" => sources
                .push(PathBuf::from(it.next().ok_or_else(|| {
                    format!("missing value for --source\n{USAGE}")
                })?)),
            "--out-dir" => {
                out_dir =
                    Some(PathBuf::from(it.next().ok_or_else(|| {
                        format!("missing value for --out-dir\n{USAGE}")
                    })?))
            }
            "--runtime" => {
                runtime = it
                    .next()
                    .ok_or_else(|| format!("missing value for --runtime\n{USAGE}"))?
            }
            "--help" | "-h" => return Err(USAGE.to_string()),
            other => return Err(format!("unknown argument `{other}`\n{USAGE}")),
        }
    }

    if sources.is_empty() {
        return Err(format!("missing --source\n{USAGE}"));
    }
    for s in &sources {
        if !s.is_file() {
            return Err(format!("source file does not exist: {}", s.display()));
        }
    }
    let out_dir = out_dir.ok_or_else(|| format!("missing --out-dir\n{USAGE}"))?;

    Ok(Config {
        sources,
        out_dir,
        runtime,
    })
}
