#!/usr/bin/env bash
# Compatibility differences are informational; setup and capture failures fail CI.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
recordings="$root/hack/hf-api-diff/recordings/huggingface"
tmp="${RUNNER_TEMP:-${TMPDIR:-/tmp}}"
out="${HF_API_DIFF_OUTPUT:-$tmp/hf-api-diff-output}"
cache="${HF_API_DIFF_CACHE:-$tmp/hf-api-diff-cache}"
hub="${HF_ENDPOINT:-https://huggingface.co}"
report="$out/hf-api-diff.md"
stage=setup
pid=
work=

export GIT_TERMINAL_PROMPT=0

fail() {
	echo "hf-api-diff: $stage: $*" >&2
	exit 1
}

cleanup() {
	rc=$?
	if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	fi
	[ -z "$work" ] || rm -rf "$work"
	if [ "$rc" -ne 0 ] && [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
		printf '\n**hf-api-diff failed** during %s (exit %s); see the job log.\n' "$stage" "$rc" >>"$GITHUB_STEP_SUMMARY"
	fi
	exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

sha256() {
	if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}

# seed_repo KIND REPO PREFIX: pins <data>/repositories/PREFIX/REPO.git to the recorded refs.
seed_repo() {
	local refs="$recordings/$1.refs.json" cached="$cache/git/$3$2.git" repo="$work/data/repositories/$3$2.git"
	local head ref commit
	head="$(jq -r '.response.json.sha' "$recordings/$1.info.json")"
	[ -d "$cached" ] || git init -q --bare "$cached"
	mkdir -p "$(dirname "$repo")"
	cp -R "$cached" "$repo"
	git --git-dir="$repo" for-each-ref --format='delete %(refname)' | git --git-dir="$repo" update-ref --stdin
	while IFS=$'\t' read -r ref commit; do
		if ! git --git-dir="$repo" cat-file -e "$commit^{commit}" 2>/dev/null; then
			echo "fetching $3$2 $commit"
			git --git-dir="$cached" fetch -q --no-tags "$hub/$3$2" "+$commit:$ref" || fail "fetch $3$2 $commit"
			git --git-dir="$repo" fetch -q --no-tags "$cached" "+$commit:$ref"
		fi
		git --git-dir="$repo" rev-list --objects "$commit" >/dev/null || fail "$3$2: incomplete history for $commit"
		git --git-dir="$repo" update-ref "$ref" "$commit"
		[ "$commit" != "$head" ] || git --git-dir="$repo" symbolic-ref HEAD "$ref"
	done < <(jq -r '.response.json | (.branches + .tags + .converts)[] | [.ref, .targetCommit] | @tsv' "$refs")
	[ "$(git --git-dir="$repo" rev-parse HEAD)" = "$head" ] || fail "$3$2: no recorded ref at $head"
}

command -v jq >/dev/null || fail "jq is required"
if [ -n "${HF_API_DIFF_PORT:-}" ]; then
	port="$HF_API_DIFF_PORT"
	! port_open "$port" || fail "port $port is already in use"
else
	for _ in 1 2 3 4 5 6 7 8 9 10; do
		port=$((20000 + RANDOM % 40000))
		port_open "$port" || break
	done
fi
hfd_url="http://127.0.0.1:$port"
work="$(mktemp -d "$tmp/hf-api-diff.XXXXXX")"
mkdir -p "$out" "$cache/lfs" "$work/data/repositories"
rm -rf "$out/hfd" "$report" "$out/hfd.log"

stage=build
cd "$root"
go build -o "$work/bin/" ./cmd/hfd ./hack/hf-api-diff

stage=fixtures
model="$(jq -r '.fixture.model' "$recordings/meta.json")"
seed_repo models "$model" ""
seed_repo datasets "$(jq -r '.fixture.dataset' "$recordings/meta.json")" datasets/
seed_repo spaces "$(jq -r '.fixture.space' "$recordings/meta.json")" spaces/

stage=payload
oid="$(jq -r '.final.sha256' "$recordings/models.resolve.lfs.json")"
size="$(jq -r '.final.size' "$recordings/models.resolve.lfs.json")"
path="$(jq -r '.request.path' "$recordings/models.resolve.lfs.json")"
model_head="$(jq -r '.response.json.sha' "$recordings/models.info.json")"
pinned="${path/\/resolve\/main\//\/resolve\/$model_head\/}"
[ "$pinned" != "$path" ] || fail "cannot pin $path to $model_head"
payload="$cache/lfs/$oid"
if [ -f "$payload" ] && [ "$(wc -c <"$payload" | tr -d ' ')" = "$size" ] && [ "$(sha256 "$payload")" = "$oid" ]; then
	echo "using cached payload $oid"
else
	rm -f "$payload"
	echo "downloading $pinned"
	curl -fsSL --retry 3 --retry-all-errors --max-time 900 -o "$work/payload" "$hub$pinned" || fail "download $pinned"
	[ "$(wc -c <"$work/payload" | tr -d ' ')" = "$size" ] || fail "downloaded payload is not $size bytes"
	[ "$(sha256 "$work/payload")" = "$oid" ] || fail "downloaded payload does not hash to $oid"
	mv "$work/payload" "$payload"
fi

stage=server
"$work/bin/hfd" --addr "127.0.0.1:$port" --ssh-addr= --data "$work/data" >"$out/hfd.log" 2>&1 &
pid=$!
ready=
for _ in $(seq 1 100); do
	kill -0 "$pid" 2>/dev/null || fail "hfd exited early; see $out/hfd.log"
	if [ "$(curl -s -o /dev/null -w '%{http_code}' "$hfd_url/api/agent-harnesses")" = 200 ]; then
		ready=1
		break
	fi
	sleep 0.3
done
[ -n "$ready" ] || fail "hfd did not become ready on $hfd_url"
kill -0 "$pid" 2>/dev/null || fail "hfd exited after answering; see $out/hfd.log"
echo "hfd $pid serving $hfd_url"

stage=ingest
curl -fsS -o /dev/null -T "$payload" -H 'Content-Type: application/octet-stream' "$hfd_url/objects/$oid" || fail "ingest $oid"

stage=compare
compare_rc=0
"$work/bin/hf-api-diff" compare -hfd-url "$hfd_url" -recordings "$recordings" -out "$out/hfd" -report "$report" \
	-max-body "${HF_API_MAX_BODY:-128MiB}" -timeout "${HF_API_TIMEOUT:-30m}" || compare_rc=$?
if [ -f "$report" ] && [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	awk '/^<details>/ { exit } { print }' "$report" >>"$GITHUB_STEP_SUMMARY"
fi
[ "$compare_rc" -eq 0 ] || fail "compare exited $compare_rc"

stage=verify
expected="$(find "$recordings" -name '*.json' ! -name meta.json | wc -l | tr -d ' ')"
captured="$(jq -s 'map(select(has("request"))) | length' "$out"/hfd/*.json)"
[ "$captured" = "$expected" ] || fail "captured $captured of $expected cases"
incomplete="$(jq -rs 'map(select(has("request")) | select(.error != null or .response.truncated == true or .final.truncated == true) | .request.name) | join(" ")' "$out"/hfd/*.json)"
[ -z "$incomplete" ] || fail "incomplete captures: $incomplete"
errors="$(sed -n 's/^Summary: .*, \([0-9][0-9]*\) capture error\.$/\1/p' "$report")"
[ "$errors" = 0 ] || fail "report counts ${errors:-?} capture errors"
echo "report written to $report"
