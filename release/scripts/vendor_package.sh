#!/usr/bin/env bash
set -eu

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd "${script_dir}/.."
VENDOR_PACKAGING_CONFIGURATION="vendor-packaging.yml"
if [ -s "$VENDOR_PACKAGING_CONFIGURATION" ]; then
  VENDOR_REPO="$(bosh int --path="/packaged_release/repo" "$VENDOR_PACKAGING_CONFIGURATION")"
  VENDOR_PACKAGE="$(bosh int --path="/packaged_release/package" "$VENDOR_PACKAGING_CONFIGURATION")"
  VENDOR_TAG="$(bosh int --path="/packaged_release/tag" "$VENDOR_PACKAGING_CONFIGURATION")"

  TARGET_VENDOR_DIR=../vendor_repo
  git clone --depth 1 --branch "$VENDOR_TAG" --recurse-submodules "$VENDOR_REPO" "$TARGET_VENDOR_DIR"

  bosh vendor-package "$VENDOR_PACKAGE" "$TARGET_VENDOR_DIR"
fi
cd -
