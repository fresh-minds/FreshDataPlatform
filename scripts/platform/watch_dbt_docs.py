#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import time
from pathlib import Path


WATCH_EXTENSIONS = {".sql", ".yml", ".yaml", ".md", ".csv"}
WATCH_BASENAMES = {"dbt_project.yml", "packages.yml", "profiles.yml"}
WATCH_DIRECTORIES = [
    "models",
    "macros",
    "snapshots",
    "seeds",
    "tests",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Watch dbt project files and regenerate dbt docs on change.",
    )
    parser.add_argument("--project-dir", default="dbt")
    parser.add_argument("--profiles-dir", default="dbt")
    parser.add_argument("--vars", default="{use_seed_data: true}")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--once", action="store_true")
    return parser.parse_args()


def should_watch_file(file_path: Path) -> bool:
    return file_path.name in WATCH_BASENAMES or file_path.suffix.lower() in WATCH_EXTENSIONS


def get_watched_files(project_dir: Path) -> list[Path]:
    files: list[Path] = []
    for directory in WATCH_DIRECTORIES:
        root = project_dir / directory
        if not root.exists():
            continue
        files.extend(path for path in root.rglob("*") if path.is_file() and should_watch_file(path))

    for basename in WATCH_BASENAMES:
        path = project_dir / basename
        if path.exists() and path.is_file():
            files.append(path)

    return sorted({path.resolve() for path in files})


def fingerprint(files: list[Path]) -> str:
    digest = hashlib.sha256()
    for file_path in files:
        try:
            stat = file_path.stat()
        except FileNotFoundError:
            continue
        digest.update(str(file_path).encode("utf-8"))
        digest.update(str(stat.st_mtime_ns).encode("utf-8"))
        digest.update(str(stat.st_size).encode("utf-8"))
    return digest.hexdigest()


def run_dbt_docs_generate(project_dir: Path, profiles_dir: Path, vars_value: str) -> int:
    command = [
        ".venv/bin/dbt",
        "docs",
        "generate",
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--vars",
        vars_value,
    ]
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Running: {' '.join(command)}", flush=True)
    # In watch mode we intentionally continue after failures and retry on the
    # next file change; `--once` still returns this exit code to callers.
    completed = subprocess.run(command, check=False)
    if completed.returncode == 0:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dbt docs updated", flush=True)
    else:
        print(
            f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] dbt docs generation failed (exit={completed.returncode})",
            flush=True,
        )
    return completed.returncode


def main() -> int:
    args = parse_args()

    repo_root = Path(os.getcwd())
    project_dir = (repo_root / args.project_dir).resolve()
    profiles_dir = (repo_root / args.profiles_dir).resolve()

    if not project_dir.exists():
        print(f"Project dir does not exist: {project_dir}", file=sys.stderr)
        return 1
    if not profiles_dir.exists():
        print(f"Profiles dir does not exist: {profiles_dir}", file=sys.stderr)
        return 1

    first_result = run_dbt_docs_generate(project_dir, profiles_dir, args.vars)
    if args.once:
        return first_result

    if first_result != 0:
        print("Continuing in watch mode; will retry when files change.", flush=True)

    files = get_watched_files(project_dir)
    current_fingerprint = fingerprint(files)
    print(
        f"Watching {len(files)} dbt files in {project_dir} (poll interval: {args.interval}s)",
        flush=True,
    )

    while True:
        time.sleep(args.interval)
        files = get_watched_files(project_dir)
        new_fingerprint = fingerprint(files)
        if new_fingerprint == current_fingerprint:
            continue
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Change detected, regenerating docs...", flush=True)
        run_dbt_docs_generate(project_dir, profiles_dir, args.vars)
        current_fingerprint = new_fingerprint


if __name__ == "__main__":
    raise SystemExit(main())
