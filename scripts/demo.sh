#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd "$script_dir/.." && pwd)"
demo_parent="${TMPDIR:-/tmp}"
keep_demo=0

usage() {
  echo "Usage: $0 [--keep] [--parent DIRECTORY]"
  echo ""
  echo "Runs a create/modify/move/replace/delete demo in a new temporary folder."
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep)
      keep_demo=1
      shift
      ;;
    --parent)
      if [[ $# -lt 2 ]]; then
        echo "--parent requires a directory" >&2
        exit 2
      fi
      demo_parent="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "$demo_parent"
demo_parent="$(cd "$demo_parent" && pwd)"
demo_dir="$(mktemp -d "$demo_parent/fsrecon-demo.XXXXXX")"
build_dir="$(mktemp -d "${TMPDIR:-/tmp}/fsrecon-demo-bin.XXXXXX")"
watch_bin="$build_dir/fsrecon-demo-watcher"
watch_pid=""
replacement_path=""

cleanup() {
  if [[ -n "$watch_pid" ]] && kill -0 "$watch_pid" 2>/dev/null; then
    kill -TERM "$watch_pid" 2>/dev/null || true
    wait "$watch_pid" 2>/dev/null || true
  fi
  if [[ "$keep_demo" -eq 0 ]]; then
    rm -rf -- "$demo_dir"
  else
    echo "Demo folder retained at: $demo_dir"
  fi
  if [[ -n "$replacement_path" ]]; then
    rm -f -- "$replacement_path"
  fi
  rm -rf -- "$build_dir"
}
trap cleanup EXIT INT TERM

echo "Building demo watcher..."
(cd "$repo_dir" && go build -o "$watch_bin" ./examples/watcher)

echo "Demo folder: $demo_dir"
echo "Starting fsrecon native watcher (periodic reconciliation disabled)..."
"$watch_bin" "$demo_dir" 0s &
watch_pid=$!
sleep 0.6

echo
echo "[1/5] Create file"
printf 'version one\n' > "$demo_dir/report.txt"
sleep 0.6

echo
echo "[2/5] Modify file"
printf 'another line\n' >> "$demo_dir/report.txt"
sleep 0.6

echo
echo "[3/5] Rename file (same identity => MOVED)"
mv "$demo_dir/report.txt" "$demo_dir/final.txt"
sleep 0.6

echo
echo "[4/5] Atomic replace (same path, new identity => REPLACED)"
replacement_path="$(mktemp "$demo_parent/fsrecon-replacement.XXXXXX")"
printf 'replacement contents\n' > "$replacement_path"
mv "$replacement_path" "$demo_dir/final.txt"
replacement_path=""
sleep 0.6

echo
echo "[5/5] Delete file"
rm "$demo_dir/final.txt"
sleep 0.6

echo
echo "Demo complete."
