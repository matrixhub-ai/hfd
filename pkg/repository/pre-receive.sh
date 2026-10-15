#!/bin/sh
# hfd's LFS policy as git's pre-receive hook (core.hooksPath). HFD_LFS_MAX_REGULAR_SIZE and
# HFD_LFS_MAX_BINARY_SIZE are the limits (0 turns a rule off); HFD_LFS_LARGE_BANNER and
# HFD_LFS_BINARY_BANNER are lfs.PolicyError's banners, the offending files going before their last line.
set -u
# Objects are judged as stored, never as the object a refs/replace/<oid> ref puts in their place.
export GIT_NO_REPLACE_OBJECTS=1
git=${HFD_GIT:-git}
max=${HFD_LFS_MAX_REGULAR_SIZE:?}
maxbin=${HFD_LFS_MAX_BINARY_SIZE:?}
tmp=$(mktemp -d) || exit 1
trap 'rm -rf "$tmp"' EXIT
trap 'exit 1' HUP INT TERM

# Slurped first, so that nothing below can read the pushed commands.
cat >"$tmp/commands" || exit 1
# "<new> <ref>" for each command that does not delete.
awk '$2 ~ /[^0]/ { print $2, $3 }' "$tmp/commands" >"$tmp/updates" || exit 1
[ -s "$tmp/updates" ] || exit 0
# git refuses a missing tip itself, as "missing necessary objects".
cut -d ' ' -f 1 "$tmp/updates" | "$git" cat-file --batch-check='%(objectname)' >"$tmp/found" || exit 1
awk 'FILENAME == ARGV[1] { found[FNR] = (NF == 1); n = FNR; next }
	{ m = FNR; if (found[FNR]) print }
	END { if (m != n) exit 1 }' "$tmp/found" "$tmp/updates" >"$tmp/tips" || exit 1
[ -s "$tmp/tips" ] || exit 0
# What the refs reach already was judged when it was pushed; one walk lists what all the tips add.
cut -d ' ' -f 1 "$tmp/tips" | "$git" rev-list --objects --stdin --not --all >"$tmp/objects" || exit 1
[ -s "$tmp/objects" ] || exit 0
cut -d ' ' -f 1 "$tmp/objects" |
	"$git" cat-file --batch-check='%(objectname) %(objecttype) %(objectsize)' >"$tmp/types" || exit 1
# "<oid> <size>" for each blob over a limit.
awk -v max="$max" -v maxbin="$maxbin" '
	NR == FNR { type[FNR] = $0; types = FNR; next }
	{
		objects = FNR
		if (split(type[FNR], t, " ") != 3 || t[1] != $1) { bad = 1; exit }
		if (t[2] == "blob" && (max > 0 && t[3] + 0 > max + 0 || maxbin > 0 && t[3] + 0 > maxbin + 0)) print $1, t[3]
	}
	END { if (bad || objects != types) exit 1 }' "$tmp/types" "$tmp/objects" >"$tmp/blobs" || exit 1

# "<oid> <kind>" for each blob refused, L when large and B when binary.
: >"$tmp/refused"
while read -r oid size <&3; do
	if [ "$max" -gt 0 ] && [ "$size" -gt "$max" ]; then
		# Below git-lfs's 1024-byte cutoff a pointer is never refused; only tiny limits get here.
		[ "$size" -lt 1024 ] && [ "$("$git" cat-file blob "$oid" | head -n 1)" = 'version https://git-lfs.github.com/spec/v1' ] && continue
		kind=L
	else
		# libgit2's git_str_is_binary over the first 8000 bytes, as huggingface.co judges pushed blobs.
		want=$size
		[ "$want" -gt 8000 ] && want=8000
		verdict=$("$git" cat-file blob "$oid" | head -c 8000 | od -An -v -tu1 | awk -v want="$want" '
			{ for (i = 1; i <= NF; i++) c[n++] = $i }
			END {
				if (n != want) exit 1
				i = 0
				if (c[0] == 239 && c[1] == 187 && c[2] == 191) i = 3
				else if (c[0] == 254 && c[1] == 255 || c[0] == 255 && c[1] == 254 || c[0] == 0 && c[1] == 0 && c[2] == 254 && c[3] == 255) { print "binary"; exit }
				for (; i < n; i++) {
					if (c[i] > 31 && c[i] != 127 || c[i] == 8 || c[i] == 27 || c[i] == 12) printable++
					else if (c[i] == 0) { print "binary"; exit }
					else if (c[i] != 9 && c[i] != 10 && c[i] != 11 && c[i] != 13) nonprintable++
				}
				print (int(printable / 128) < nonprintable ? "binary" : "text")
			}') || exit 1
		[ "$verdict" = binary ] || continue
		kind=B
	fi
	printf '%s %s\n' "$oid" "$kind" >>"$tmp/refused"
done 3<"$tmp/blobs"
[ -s "$tmp/refused" ] || exit 0

# Only a refused push walks ref by ref: each blob goes under the first ref, in command order,
# whose own walk lists it, with the path listed there.
cp "$tmp/refused" "$tmp/pending" || exit 1
: >"$tmp/listed"
while [ -s "$tmp/pending" ] && read -r new ref <&3; do
	"$git" rev-list --objects "$new" --not --all >"$tmp/objects" || exit 1
	: >"$tmp/rest"
	rest="$tmp/rest" awk -v ref="$ref" '
		BEGIN { rest = ENVIRON["rest"] }
		NR == FNR { kind[$1] = $2; order[++n] = $1; next }
		$1 in kind { p = index($0, " "); print kind[$1], ref, (p ? substr($0, p + 1) : ""); delete kind[$1] }
		END { for (i = 1; i <= n; i++) if (order[i] in kind) print order[i], kind[order[i]] >rest }' \
		"$tmp/pending" "$tmp/objects" >>"$tmp/listed" || exit 1
	mv "$tmp/rest" "$tmp/pending" || exit 1
done 3<"$tmp/tips"

# The large rule outranks the binary one across the whole push.
if grep -q ' L$' "$tmp/refused"; then
	kind=L banner=HFD_LFS_LARGE_BANNER
else
	kind=B banner=HFD_LFS_BINARY_BANNER
fi
awk -v kind="$kind" -v banner="$banner" '
	function red(s) { printf "%c[31m%s%c[0m\n", 27, s, 27 }
	BEGIN { n = split(ENVIRON[banner], text, "\n"); for (i = 1; i < n; i++) red(text[i]) }
	$1 == kind { red("  - " substr($0, length($1 $2) + 3) " (ref: " $2 ")") }
	END { red(text[n]) }' "$tmp/listed" >"$tmp/banner" || exit 1
# One write, which git relays in 128-byte packets as it does any hook's output.
cat "$tmp/banner" >&2
exit 1
