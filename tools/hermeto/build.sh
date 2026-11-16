#!/bin/bash
# Isolated (network-none) podman build using Hermeto-prefetched PyPI deps.
# Run ./tools/ci/hermeto.sh first.

set -euo pipefail

SCRIPT_NAME=$(basename "$0")
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
TEKTON_PUSH="$PROJECT_ROOT/.tekton/ansible-automation-orchestrator-ep-devel-push.yaml"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1" >&2; }
log_error() { echo -e "${RED}[ERROR]${NC} $1" >&2; }
log_warning() { echo -e "${YELLOW}[WARNING]${NC} $1" >&2; }

restore_requirements_backup() {
    local backup="$PROJECT_ROOT/requirements.txt.bak"
    if [ -f "$backup" ]; then
        log_info "Restoring requirements.txt from backup"
        mv "$backup" "$PROJECT_ROOT/requirements.txt"
    fi
}

trap restore_requirements_backup EXIT

usage() {
    cat << EOF
Usage: $SCRIPT_NAME [OPTIONS]

Build the execution-plane image with no network, using Hermeto output from
./tools/ci/hermeto.sh.

OPTIONS:
    -h, --help          Show this help message
    -t, --tag TAG       Image tag (default: localhost/execution-plane:dev)
    -f, --file PATH     Containerfile (default: generated Containerfile.hermetic)
    -v, --volume SPEC   Extra volume mount (repeatable)
    --build-arg ARG     Pass a build-arg (repeatable)
    --no-cache          Build without cache
    --platform PLATFORM Target platform (e.g. linux/amd64)

ENVIRONMENT:
    HERMETIC_BUILDER_IMAGE  Builder image. Default: BUILDER_IMAGE from
                            .tekton/ansible-automation-orchestrator-ep-devel-push.yaml
                            (uv-builder, so the image does not pip-install uv).

EXAMPLES:
    ./tools/ci/hermeto.sh
    $SCRIPT_NAME
    $SCRIPT_NAME -t localhost/execution-plane:local --no-cache
EOF
}

konflux_builder_image() {
    if [ -n "${HERMETIC_BUILDER_IMAGE:-}" ]; then
        printf '%s' "$HERMETIC_BUILDER_IMAGE"
        return
    fi
    if [ -f "$TEKTON_PUSH" ]; then
        grep -oE 'BUILDER_IMAGE=[^[:space:]]+' "$TEKTON_PUSH" | head -1 | cut -d= -f2-
    fi
}

IMAGE_TAG="localhost/execution-plane:dev"
CONTAINERFILE=""
ADDITIONAL_VOLUMES=()
BUILD_ARGS=()
NO_CACHE=()
PLATFORM=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help) usage; exit 0 ;;
        -t|--tag) IMAGE_TAG="$2"; shift 2 ;;
        -f|--file) CONTAINERFILE="$2"; shift 2 ;;
        -v|--volume|--volumes) ADDITIONAL_VOLUMES+=("$2"); shift 2 ;;
        --no-cache) NO_CACHE=(--no-cache); shift ;;
        --platform) PLATFORM="$2"; shift 2 ;;
        --build-arg) BUILD_ARGS+=("$2"); shift 2 ;;
        build) shift ;;
        inspect)
            log_error "inspect/debug support was removed; this script only builds"
            exit 1
            ;;
        *) log_error "Unknown option: $1"; usage; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT"

get_target_arch() {
    local arch
    if [ -n "$PLATFORM" ]; then
        arch="${PLATFORM##*/}"
        case "$arch" in
            amd64) echo "x86_64" ;;
            arm64) echo "aarch64" ;;
            *) echo "$arch" ;;
        esac
    else
        uname -m
    fi
}

has_build_arg() {
    local name="$1"
    local arg
    for arg in "${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}"; do
        if [[ "$arg" == "$name"=* ]]; then
            return 0
        fi
    done
    return 1
}

prepare_hermetic_build() {
    log_info "Preparing isolated build"

    if [ ! -d "hermeto-output" ] || [ ! -f "hermeto.env" ]; then
        log_error "Hermeto output not found. Run: ./tools/ci/hermeto.sh"
        exit 1
    fi

    log_info "Running inject-files"
    if ! "$SCRIPT_DIR/fetch.sh" --inject-files; then
        log_error "Failed to run inject-files"
        exit 1
    fi

    log_info "Generating Containerfile.hermetic"
    if ! "$SCRIPT_DIR/fetch.sh" --generate-containerfile; then
        log_error "Failed to generate Containerfile.hermetic"
        exit 1
    fi

    if [ -z "$CONTAINERFILE" ]; then
        CONTAINERFILE="./Containerfile.hermetic"
    fi
}

prepare_hermetic_build

if ! has_build_arg BUILDER_IMAGE; then
    builder_image="$(konflux_builder_image)"
    if [ -n "$builder_image" ]; then
        BUILD_ARGS+=("BUILDER_IMAGE=$builder_image")
        log_info "BUILDER_IMAGE=$builder_image"
    fi
fi

hermeto_output_realpath="$(realpath hermeto-output)"
hermeto_env_realpath="$(realpath hermeto.env)"
target_arch="$(get_target_arch)"
repos_d_path="$hermeto_output_realpath/deps/rpm/$target_arch/repos.d"

podman_args=(
    podman build
    --layers
    --network none
    -v "$hermeto_output_realpath:/tmp/hermeto-output:Z"
    -v "$hermeto_env_realpath:/tmp/hermeto.env:Z"
    -t "$IMAGE_TAG"
    -f "$CONTAINERFILE"
)

if [ -d "$repos_d_path" ]; then
    podman_args+=(-v "$repos_d_path:/etc/yum.repos.d/:Z")
    log_info "RPM repos: $repos_d_path"
else
    log_warning "Hermeto RPM repos.d not found at $repos_d_path; microdnf will fail offline"
fi

if [ ${#NO_CACHE[@]} -gt 0 ]; then
    podman_args+=("${NO_CACHE[@]}")
fi
if [ -n "$PLATFORM" ]; then
    podman_args+=(--platform "$PLATFORM")
fi
for arg in "${BUILD_ARGS[@]+"${BUILD_ARGS[@]}"}"; do
    podman_args+=(--build-arg "$arg")
done
for vol in "${ADDITIONAL_VOLUMES[@]+"${ADDITIONAL_VOLUMES[@]}"}"; do
    podman_args+=(-v "$vol")
done
podman_args+=(.)

log_info "Containerfile: $CONTAINERFILE"
log_info "Image tag: $IMAGE_TAG"
log_info "Isolated build (--network none)"

if "${podman_args[@]}"; then
    log_info "Build completed: $IMAGE_TAG"
else
    log_error "Build failed"
    exit 1
fi
