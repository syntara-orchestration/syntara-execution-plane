#!/usr/bin/env bash
# Check out feat/ANSTRAT-1803 (or AO_REF), generate AO secrets/certs, and build AO_IMAGE.
#
# Temporary until that Syntara branch lands on devel. Override AO_REF afterwards.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

AO_REF="${AO_REF:-feat/ANSTRAT-1803}"
AO_REMOTE="${AO_REMOTE:-https://github.com/syntara-orchestration/syntara.git}"
SYNTARA_GIT="${SYNTARA_GIT:-${PROJECT_ROOT}/../syntara}"
SYNTARA_ROOT="${SYNTARA_ROOT:-${PROJECT_ROOT}/.worktrees/syntara-anstrat-1803}"
AO_IMAGE="${AO_IMAGE:-localhost/syntara:anstrat-1803}"

info() {
  printf '[INFO] %s\n' "$1"
}

error() {
  printf '[ERROR] %s\n' "$1" >&2
  exit 1
}

checkout_ao() {
  mkdir -p "$(dirname "${SYNTARA_ROOT}")"

  if [[ -e "${SYNTARA_ROOT}/.git" || -f "${SYNTARA_ROOT}/.git" ]]; then
    info "Updating AO checkout ${SYNTARA_ROOT} to origin/${AO_REF}"
    git -C "${SYNTARA_ROOT}" fetch origin "${AO_REF}"
    git -C "${SYNTARA_ROOT}" checkout --detach "origin/${AO_REF}"
    return
  fi

  if [[ -e "${SYNTARA_GIT}/.git" || -f "${SYNTARA_GIT}/.git" ]]; then
    info "Fetching origin/${AO_REF} from ${SYNTARA_GIT}"
    git -C "${SYNTARA_GIT}" fetch origin "${AO_REF}"
    info "Adding worktree ${SYNTARA_ROOT} at origin/${AO_REF}"
    git -C "${SYNTARA_GIT}" worktree add --detach "${SYNTARA_ROOT}" "origin/${AO_REF}"
    return
  fi

  info "Cloning ${AO_REMOTE} (${AO_REF}) into ${SYNTARA_ROOT}"
  git clone --branch "${AO_REF}" --single-branch "${AO_REMOTE}" "${SYNTARA_ROOT}"
}

verify_ao_ref() {
  local expected actual
  expected="$(git -C "${SYNTARA_ROOT}" rev-parse "origin/${AO_REF}" 2>/dev/null \
    || git -C "${SYNTARA_ROOT}" rev-parse "${AO_REF}")"
  actual="$(git -C "${SYNTARA_ROOT}" rev-parse HEAD)"
  if [[ "${expected}" != "${actual}" ]]; then
    error "AO checkout HEAD ${actual} does not match ${AO_REF} (${expected})"
  fi
  info "AO ${AO_REF} @ $(git -C "${SYNTARA_ROOT}" rev-parse --short HEAD)"
}

build_ao_image() {
  local ao_sha have_sha containerfile
  ao_sha="$(git -C "${SYNTARA_ROOT}" rev-parse HEAD)"
  containerfile="${SYNTARA_ROOT}/backend/containers/syntara/Containerfile"
  [[ -f "${containerfile}" ]] || error "AO Containerfile not found: ${containerfile}"

  have_sha="$(podman inspect -f '{{index .Config.Labels "ao.git.sha"}}' "${AO_IMAGE}" 2>/dev/null || true)"
  if [[ "${have_sha}" == "${ao_sha}" ]]; then
    info "AO image ${AO_IMAGE} already matches ${ao_sha:0:12}"
    return
  fi

  info "Building ${AO_IMAGE} from ${AO_REF} (${ao_sha:0:12})"
  podman build \
    -f "${containerfile}" \
    -t "${AO_IMAGE}" \
    --label "ao.git.ref=${AO_REF}" \
    --label "ao.git.sha=${ao_sha}" \
    "${SYNTARA_ROOT}/backend"
}

copy_jwt_public_key() {
  local src="${SYNTARA_ROOT}/backend/.secrets/jwt-primary.pub"
  [[ -f "${src}" ]] || error "AO JWT public key missing: ${src}"
  mkdir -p "${PROJECT_ROOT}/.secrets"
  cp "${src}" "${PROJECT_ROOT}/.secrets/ao-jwt-public.pem"
  chmod 644 "${PROJECT_ROOT}/.secrets/ao-jwt-public.pem"
}

checkout_ao
verify_ao_ref
"${SYNTARA_ROOT}/backend/tools/generate_secrets.sh"
make -C "${SYNTARA_ROOT}/backend" certs-generate
copy_jwt_public_key
build_ao_image
