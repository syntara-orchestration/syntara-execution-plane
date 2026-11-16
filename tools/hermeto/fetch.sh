#!/bin/bash
# Prefetch PyPI and RPM dependencies with Hermeto for a local isolated build.
# Workflow:
#   ./tools/ci/hermeto.sh
#   ./tools/ci/build-and-debug.sh
#
# Matches the Konflux prefetch-input (pip + rpm). See:
#   https://hermetoproject.github.io/hermeto/latest/pip/
#   https://hermetoproject.github.io/hermeto/latest/rpm/

set -euo pipefail

SCRIPT_DIR="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
PROJECT_ROOT="$(realpath "$SCRIPT_DIR/../..")"
OUTPUT_DIR="$PROJECT_ROOT/hermeto-output"
ENV_FILE="$PROJECT_ROOT/hermeto.env"
CONTAINER_OUTPUT_DIR="/tmp/hermeto-output"
CONTAINER_ENV_FILE="/tmp/hermeto.env"
HERMETO_IMAGE="${HERMETO_IMAGE:-quay.io/konflux-ci/hermeto:latest}"
# Same package set Konflux prefetches (pip + rpm).
# Override with HERMETO_PREFETCH_INPUT.
DEFAULT_PREFETCH_INPUT='[{"type": "pip", "path": "."}, {"type": "rpm", "path": "."}]'

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

restore_requirements_backup() {
    local backup="$PROJECT_ROOT/requirements.txt.bak"
    if [ -f "$backup" ]; then
        log_info "Restoring requirements.txt from backup"
        mv "$backup" "$PROJECT_ROOT/requirements.txt"
    fi
}

# Undo inject-files if this script fails. On success the build script restores.
trap restore_requirements_backup ERR INT TERM

run_hermeto() {
    # No -t: fetch must work without a TTY (scripts, CI).
    podman run --rm \
        -v "$PROJECT_ROOT:$PROJECT_ROOT:z" \
        -w "$PROJECT_ROOT" \
        "$HERMETO_IMAGE" \
        "$@"
}

generate_hermetic_containerfile() {
    local src="$PROJECT_ROOT/Containerfile"
    local dst="$PROJECT_ROOT/Containerfile.hermetic"

    if [ ! -f "$src" ]; then
        log_error "Containerfile not found: $src"
        return 1
    fi

    log_info "Generating Containerfile.hermetic"

    {
        echo "# GENERATED FILE - DO NOT EDIT"
        echo "# Generated from Containerfile for local hermetic builds."
        echo "# Mount hermeto-output -> ${CONTAINER_OUTPUT_DIR} and hermeto.env -> ${CONTAINER_ENV_FILE}"
        echo "# Build with: ./tools/ci/build-and-debug.sh"
        echo ""
        sed -E "s|^([[:space:]]*)RUN[[:space:]]+|\\1RUN . ${CONTAINER_ENV_FILE} \&\& |" "$src"
    } > "$dst"

    log_info "Generated: $dst"
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Prefetch this repo's PyPI and RPM dependencies with Hermeto and write
hermeto-output/, hermeto.env, and Containerfile.hermetic.

With no options, runs fetch-deps, generate-env, and generate-containerfile.

OPTIONS:
  --run                     Same as the default (fetch + env + Containerfile)
  --skip-if-exists          Skip fetch-deps when hermeto-output/deps already exists
  --fetch-deps              Fetch dependencies only
  --generate-env            Write hermeto.env
  --inject-files            Rewrite source files for hermetic install (used by the build script)
  --generate-containerfile  Write Containerfile.hermetic
  -h, --help                Show this help

ENVIRONMENT:
  HERMETO_IMAGE             Hermeto image (default: ${HERMETO_IMAGE})
  HERMETO_PREFETCH_INPUT    JSON prefetch-input (default: ${DEFAULT_PREFETCH_INPUT})

EXAMPLES:
  $0                        # Fetch PyPI sdists + RPMs and prepare the hermetic Containerfile
  $0 --run --skip-if-exists # Reuse an existing hermeto-output
  $0 --generate-env         # Regenerate hermeto.env from an existing cache
EOF
}

RUN_COMMANDS=false
FETCH_DEPS=false
GENERATE_ENV=false
INJECT_FILES=false
GENERATE_CONTAINERFILE=false
SKIP_IF_EXISTS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --run)
            RUN_COMMANDS=true
            FETCH_DEPS=true
            GENERATE_ENV=true
            GENERATE_CONTAINERFILE=true
            shift
            ;;
        --skip-if-exists)
            SKIP_IF_EXISTS=true
            shift
            ;;
        --fetch-deps)
            RUN_COMMANDS=true
            FETCH_DEPS=true
            shift
            ;;
        --generate-env)
            RUN_COMMANDS=true
            GENERATE_ENV=true
            shift
            ;;
        --inject-files)
            RUN_COMMANDS=true
            INJECT_FILES=true
            shift
            ;;
        --generate-containerfile)
            RUN_COMMANDS=true
            GENERATE_CONTAINERFILE=true
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --*)
            log_error "Unknown option: $1"
            usage
            exit 1
            ;;
        *)
            log_error "Unexpected argument: $1"
            usage
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

if [ "$RUN_COMMANDS" = false ]; then
    FETCH_DEPS=true
    GENERATE_ENV=true
    GENERATE_CONTAINERFILE=true
fi

PREFETCH_INPUT="${HERMETO_PREFETCH_INPUT:-$DEFAULT_PREFETCH_INPUT}"

if [ "$FETCH_DEPS" = true ]; then
    if ! git rev-parse --git-dir &> /dev/null; then
        log_error "Not in a git repository. Hermeto requires a valid git repository."
        exit 1
    fi
    if ! git remote get-url origin &> /dev/null; then
        log_error "No 'origin' remote found. Hermeto requires an 'origin' remote."
        exit 1
    fi
fi

if [ "$FETCH_DEPS" = true ]; then
    if [ "$SKIP_IF_EXISTS" = true ] && [ -d "$OUTPUT_DIR/deps" ]; then
        log_info "Skipping fetch-deps; $OUTPUT_DIR/deps already exists"
    else
        if [ ! -f "$PROJECT_ROOT/requirements-build.txt" ]; then
            log_error "requirements-build.txt is missing. Isolated sdist installs need PEP 517 backends (setuptools, hatchling, ...)."
            log_error "Run: make sync-requirements"
            exit 1
        fi
        if [ ! -f "$PROJECT_ROOT/rpms.lock.yaml" ]; then
            log_error "rpms.lock.yaml is missing. Isolated microdnf installs need a locked RPM set."
            log_error "Run: make sync-rpms-lock-yaml"
            exit 1
        fi
        log_info "Fetching PyPI and RPM dependencies with Hermeto"
        log_info "prefetch-input: $PREFETCH_INPUT"
        rm -rf "$OUTPUT_DIR"
        mkdir -p "$OUTPUT_DIR"
        run_hermeto \
            fetch-deps --source . --output "$OUTPUT_DIR" "$PREFETCH_INPUT"
        log_info "Fetched dependencies into $OUTPUT_DIR"
    fi
fi

if [ "$GENERATE_ENV" = true ]; then
    if [ ! -d "$OUTPUT_DIR" ]; then
        log_error "hermeto-output not found. Run $0 --fetch-deps first."
        exit 1
    fi
    log_info "Generating hermeto.env"
    run_hermeto generate-env hermeto-output -o hermeto.env --for-output-dir "$CONTAINER_OUTPUT_DIR"
    log_info "Generated: $ENV_FILE"
fi

if [ "$INJECT_FILES" = true ]; then
    if [ ! -d "$OUTPUT_DIR" ]; then
        log_error "hermeto-output not found. Run $0 --fetch-deps first."
        exit 1
    fi
    REQUIREMENTS_FILE="$PROJECT_ROOT/requirements.txt"
    REQUIREMENTS_BACKUP="$PROJECT_ROOT/requirements.txt.bak"
    if [ -f "$REQUIREMENTS_FILE" ]; then
        cp "$REQUIREMENTS_FILE" "$REQUIREMENTS_BACKUP"
        log_info "Backed up requirements.txt"
    fi
    log_info "Running inject-files"
    run_hermeto inject-files hermeto-output --for-output-dir "$CONTAINER_OUTPUT_DIR"
fi

if [ "$GENERATE_CONTAINERFILE" = true ]; then
    generate_hermetic_containerfile
fi

if [ "$FETCH_DEPS" = true ]; then
    log_info "Hermeto fetch completed"
    [ -d "$OUTPUT_DIR" ] && log_info "Dependencies: $OUTPUT_DIR"
    [ -f "$ENV_FILE" ] && log_info "Environment: $ENV_FILE"
    [ -f "$PROJECT_ROOT/Containerfile.hermetic" ] && log_info "Containerfile: $PROJECT_ROOT/Containerfile.hermetic"
    if [ -d "$OUTPUT_DIR" ] && [ -f "$ENV_FILE" ] && [ -f "$PROJECT_ROOT/Containerfile.hermetic" ]; then
        log_info "Next: ./tools/hermeto/build.sh"
    else
        log_warn "Not ready to build. Missing:"
        if [ ! -d "$OUTPUT_DIR" ]; then
            log_warn "  - hermeto-output (run --fetch-deps)"
        fi
        if [ ! -f "$ENV_FILE" ]; then
            log_warn "  - hermeto.env (run --generate-env)"
        fi
        if [ ! -f "$PROJECT_ROOT/Containerfile.hermetic" ]; then
            log_warn "  - Containerfile.hermetic (run --generate-containerfile)"
        fi
    fi
fi
