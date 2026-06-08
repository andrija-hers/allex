use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
};

use allex_dart_codegen::{
    find_reachable_dtos, find_rpc_modules, referenced_type_names, render_dart_client,
    render_dart_dtos,
};

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("{error}");
        process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<(), String> {
    let usage = "usage: cargo run -p allex-dart-codegen -- --source <rust-file> [--source <rust-file> ...] --out-dir <dart-dir>";
    let (source_paths, out_dir) = parse_args(args, usage)?;
    let mut source = String::new();
    for source_path in &source_paths {
        let contents = fs::read_to_string(source_path)
            .map_err(|err| format!("failed to read {}: {err}", source_path.display()))?;
        source.push_str(&contents);
        source.push('\n');
    }
    let modules = find_rpc_modules(&source);
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

    for module in modules {
        let roots = referenced_type_names(&module);
        let root_refs = roots.iter().map(String::as_str).collect::<Vec<_>>();
        let dtos = find_reachable_dtos(&source, &root_refs);
        let client_path = out_dir.join(format!("{}_client.dart", module.module_name));
        let dto_path = out_dir.join(format!("{}_dtos.dart", module.module_name));

        fs::write(&client_path, render_dart_client(&module))
            .map_err(|err| format!("failed to write {}: {err}", client_path.display()))?;
        fs::write(&dto_path, render_dart_dtos(&module.module_name, &dtos, &[]))
            .map_err(|err| format!("failed to write {}: {err}", dto_path.display()))?;
    }

    Ok(())
}

fn parse_args(args: Vec<String>, usage: &str) -> Result<(Vec<PathBuf>, PathBuf), String> {
    let mut source_paths: Vec<PathBuf> = Vec::new();
    let mut out_dir: Option<PathBuf> = None;
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

    Ok((source_paths, out_dir))
}
