#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 5 ]]; then
  echo "usage: $0 GOOS GOARCH VERSION REVISION OUTPUT_DIR" >&2
  exit 2
fi

goos="$1"
goarch="$2"
version="$3"
revision="$4"
output_dir="$5"

if [[ ! "${goos}" =~ ^[A-Za-z0-9_]+$ ]] ||
  [[ ! "${goarch}" =~ ^[A-Za-z0-9_]+$ ]] ||
  [[ ! "${version}" =~ ^[A-Za-z0-9._+-]+$ ]] ||
  [[ ! "${revision}" =~ ^[A-Za-z0-9._+-]+$ ]]; then
  echo "package arguments contain unsupported characters" >&2
  exit 2
fi

binary="coral"
if [[ "${goos}" == "windows" ]]; then
  binary="coral.exe"
fi

archive_base="coral_${version}_${goos}_${goarch}"
stage="$(mktemp -d)"
trap 'rm -rf "${stage}"' EXIT
mkdir -p "${stage}/${archive_base}" "${output_dir}"

CGO_ENABLED=0 GOOS="${goos}" GOARCH="${goarch}" go build \
  -trimpath \
  -buildvcs=false \
  -ldflags "-s -w -X github.com/yaop-labs/coral/internal/buildinfo.version=${version} -X github.com/yaop-labs/coral/internal/buildinfo.revision=${revision}" \
  -o "${stage}/${archive_base}/${binary}" \
  ./cmd/coral

cp LICENSE README.md CHANGELOG.md "${stage}/${archive_base}/"

tar \
  --sort=name \
  --mtime="@0" \
  --owner=0 \
  --group=0 \
  --numeric-owner \
  -C "${stage}" \
  -cf - \
  "${archive_base}" |
  gzip -n >"${output_dir}/${archive_base}.tar.gz"
