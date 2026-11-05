#!/usr/bin/env bash
# Vendors SIP.js 0.21.2 (MIT) into the agent page: npm's tarball, checked against its registry
# integrity (sha512), unpacked into src/main/resources/static/agent/vendor/sip.js/. Not committed
# until there is git; the page loads it from 'self' (strict CSP, no third-party script).
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUT="$ROOT/src/main/resources/static/agent/vendor/sip.js"
url=https://registry.npmjs.org/sip.js/-/sip.js-0.21.2.tgz
want=$(curl -fsS https://registry.npmjs.org/sip.js/0.21.2 | python3 -I -c 'import json,sys; print(json.load(sys.stdin)["dist"]["integrity"])')
tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
curl -fsSo "$tmp/sip.tgz" "$url"
got="sha512-$(openssl dgst -sha512 -binary "$tmp/sip.tgz" | base64 -w0)"
[[ "$got" == "$want" ]] || { echo "integrity mismatch" >&2; exit 3; }
rm -rf "$OUT" && mkdir -p "$OUT" && tar -xzf "$tmp/sip.tgz" -C "$tmp" && cp -r "$tmp/package/lib" "$OUT/"
echo "SIP.js 0.21.2 vendored into $OUT"
