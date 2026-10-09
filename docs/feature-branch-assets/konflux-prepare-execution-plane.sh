#!/usr/bin/env bash
# Deploy the standalone Execution Plane onto the AO Kind cluster (Konflux / aap-dev).
#
# Does not change the Automation Orchestrator operator, CR, or CSV. After
# deploy-ao, this script:
#   1. Applies deploy/kubernetes/execution-target/rbac.yaml (dispatcher SA).
#   2. Creates an execution_plane database/roles on ao-postgres.
#   3. Mints EP TLS, AO JWT public-key, and encryption-key secrets.
#   4. Runs the migrate Job and deploys API + worker from the aap-dev ep image.
#   5. Registers the Kind cluster as an EP cluster-binding via the EP API.
#   6. Pre-pulls the script-node image and enables script dispatch on myao-worker.
#
# Requires the ep image already built by aap-dev (addons/ep), default
# localhost:5001/<AO_NAMESPACE>/ep:<AAP_VERSION>.
set -euo pipefail

export PATH="${HOME}/aap-dev/bin:${PATH}"

KUBECONFIG="${KUBECONFIG:-${HOME}/aap-dev/.tmp/27-next-ao-operator.kubeconfig}"
export KUBECONFIG

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
INIT_YAML="${INIT_YAML:-${REPO_ROOT}/deploy/kubernetes/execution-target/rbac.yaml}"
BASE_DIR="${BASE_DIR:-${REPO_ROOT}/deploy/kubernetes/base}"
NAMESPACE="${NAMESPACE:-execution-plane}"
AO_NAMESPACE="${AO_NAMESPACE:-aap27-next}"
AO_NAME="${AO_NAME:-myao}"
AAP_VERSION="${AAP_VERSION:-2.7-next}"
EP_IMAGE="${EP_IMAGE:-localhost:5001/${AO_NAMESPACE}/ep:${AAP_VERSION}}"
SA_NAME="${SA_NAME:-syntara-dispatcher}"
CLUSTER_NAME="${CLUSTER_NAME:-27-next-ao-operator}"
ENDPOINT="${ENDPOINT:-https://kubernetes.default.svc}"
TOKEN_FILE="${TOKEN_FILE:-/var/tmp/syntara-dispatcher.token}"
NODE_IMAGE="${NODE_IMAGE:-quay.io/ahetheri/syntara-node-script:migration-test}"
WORK_DIR=""

if [[ ! -f "${KUBECONFIG}" ]]; then
  echo "ERROR: kubeconfig not found: ${KUBECONFIG}" >&2
  exit 1
fi
if [[ ! -f "${INIT_YAML}" ]]; then
  echo "ERROR: init manifest not found: ${INIT_YAML}" >&2
  exit 1
fi
if [[ ! -d "${BASE_DIR}" ]]; then
  echo "ERROR: EP manifests not found: ${BASE_DIR}" >&2
  exit 1
fi

_cleanup() {
  rm -f "${TOKEN_FILE}"
  if [[ -n "${WORK_DIR}" ]]; then
    rm -rf "${WORK_DIR}"
  fi
}

_secret_data() {
  kubectl get secret "$1" -n "$2" -o json | python3 -c "
import base64, json, sys
key = sys.argv[1]
print(base64.b64decode(json.load(sys.stdin)['data'][key]).decode(), end='')
" "$3"
}

_quote_pg() {
  python3 -c "
import sys
from urllib.parse import quote
print(quote(sys.argv[1], safe=''), end='')
" "$1"
}

_postgres_pod() {
  local pod
  pod=$(kubectl get pods -n "${AO_NAMESPACE}" --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | awk '/^ao-postgres/{print; exit}')
  if [[ -z "${pod}" ]]; then
    echo "ERROR: no Running ao-postgres pod in ${AO_NAMESPACE}" >&2
    exit 1
  fi
  printf '%s' "${pod}"
}

_ao_jwt_issuer() {
  local issuer
  issuer=$(kubectl exec -n "${AO_NAMESPACE}" "deploy/${AO_NAME}-backend" -- \
    /opt/app-root/src/.venv/bin/python -c \
    'from syntara.core.config.base import get_settings; print(get_settings().jwt_issuer, end="")' \
    2>/dev/null || true)
  if [[ -n "${issuer}" ]]; then
    printf '%s' "${issuer}"
    return 0
  fi
  issuer=$(kubectl get deploy "${AO_NAME}-backend" -n "${AO_NAMESPACE}" \
    -o jsonpath="{.spec.template.spec.containers[0].env[?(@.name=='APP_SERVER_PUBLIC_URL')].value}" \
    2>/dev/null || true)
  if [[ -n "${issuer}" ]]; then
    printf '%s' "${issuer%/}"
    return 0
  fi
  printf '%s' "https://0.0.0.0:8000"
}

_generate_tls() {
  local dir=$1
  openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
    -keyout "${dir}/ca.key" -out "${dir}/ca.pem" \
    -subj "/CN=execution-plane-konflux-ca" >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -keyout "${dir}/tls.key" -out "${dir}/tls.csr" \
    -subj "/CN=execution-plane-api" >/dev/null 2>&1
  cat > "${dir}/tls.ext" <<EOF
subjectAltName=DNS:execution-plane-api,DNS:execution-plane-api.${NAMESPACE},DNS:execution-plane-api.${NAMESPACE}.svc,DNS:execution-plane-api.${NAMESPACE}.svc.cluster.local,DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth
EOF
  openssl x509 -req -in "${dir}/tls.csr" -CA "${dir}/ca.pem" -CAkey "${dir}/ca.key" \
    -CAcreateserial -out "${dir}/tls.crt" -days 365 -sha256 -extfile "${dir}/tls.ext" >/dev/null 2>&1

  openssl req -newkey rsa:2048 -nodes \
    -keyout "${dir}/client.key" -out "${dir}/client.csr" \
    -subj "/CN=execution-plane-callback" >/dev/null 2>&1
  cat > "${dir}/client.ext" <<EOF
extendedKeyUsage=clientAuth
EOF
  openssl x509 -req -in "${dir}/client.csr" -CA "${dir}/ca.pem" -CAkey "${dir}/ca.key" \
    -CAcreateserial -out "${dir}/client.crt" -days 365 -sha256 -extfile "${dir}/client.ext" >/dev/null 2>&1
}

_ensure_db_secret() {
  local migrator_pw runtime_pw host
  host="ao-postgres.${AO_NAMESPACE}.svc"
  if kubectl get secret execution-plane-db -n "${NAMESPACE}" >/dev/null 2>&1; then
    migrator_pw=$(_secret_data execution-plane-db "${NAMESPACE}" migrator-password)
    runtime_pw=$(_secret_data execution-plane-db "${NAMESPACE}" runtime-password)
  else
    migrator_pw=$(openssl rand -hex 24)
    runtime_pw=$(openssl rand -hex 24)
  fi
  printf '%s' "${migrator_pw}" > "${WORK_DIR}/migrator.pw"
  printf '%s' "${runtime_pw}" > "${WORK_DIR}/runtime.pw"
  chmod 600 "${WORK_DIR}/migrator.pw" "${WORK_DIR}/runtime.pw"
  kubectl create secret generic execution-plane-db \
    --namespace "${NAMESPACE}" \
    --from-literal=migrator-password="${migrator_pw}" \
    --from-literal=runtime-password="${runtime_pw}" \
    --from-literal=migrator-url="postgresql+asyncpg://execution_plane_migrator:$(_quote_pg "${migrator_pw}")@${host}:5432/execution_plane" \
    --from-literal=runtime-url="postgresql+asyncpg://execution_plane_runtime:$(_quote_pg "${runtime_pw}")@${host}:5432/execution_plane" \
    --dry-run=client -o yaml | kubectl apply -f -
}

_ensure_ep_database() {
  local pg_pod admin_pw migrator_pw runtime_pw
  pg_pod=$(_postgres_pod)
  admin_pw=$(_secret_data ao-postgres-admin-configuration "${AO_NAMESPACE}" password)
  _ensure_db_secret
  migrator_pw=$(cat "${WORK_DIR}/migrator.pw")
  runtime_pw=$(cat "${WORK_DIR}/runtime.pw")
  echo "=== Create execution_plane database on ${pg_pod} ==="
  kubectl exec -i -n "${AO_NAMESPACE}" "${pg_pod}" -- \
    env PGPASSWORD="${admin_pw}" psql -U postgres -v ON_ERROR_STOP=1 <<SQL
SELECT format('CREATE ROLE execution_plane_migrator LOGIN PASSWORD %L', '${migrator_pw}')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'execution_plane_migrator')
\\gexec
ALTER ROLE execution_plane_migrator PASSWORD '${migrator_pw}';

SELECT format('CREATE ROLE execution_plane_runtime LOGIN PASSWORD %L', '${runtime_pw}')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'execution_plane_runtime')
\\gexec
ALTER ROLE execution_plane_runtime PASSWORD '${runtime_pw}';

SELECT 'CREATE DATABASE execution_plane OWNER execution_plane_migrator'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'execution_plane')
\\gexec
ALTER DATABASE execution_plane OWNER TO execution_plane_migrator;
GRANT CONNECT ON DATABASE execution_plane TO execution_plane_runtime;
SQL
}

_ensure_keys_and_tls() {
  local jwt_secret="${AO_NAME}-jwt-primary"
  echo "=== Extract AO JWT public key from ${jwt_secret} ==="
  _secret_data "${jwt_secret}" "${AO_NAMESPACE}" jwt-primary.pem > "${WORK_DIR}/jwt-primary.pem"
  chmod 600 "${WORK_DIR}/jwt-primary.pem"
  if ! openssl pkey -in "${WORK_DIR}/jwt-primary.pem" -pubout -out "${WORK_DIR}/jwt-public.pem" 2>/dev/null; then
    openssl ec -in "${WORK_DIR}/jwt-primary.pem" -pubout -out "${WORK_DIR}/jwt-public.pem"
  fi

  if kubectl get secret execution-plane-api-tls -n "${NAMESPACE}" >/dev/null 2>&1 \
    && kubectl get secret execution-plane-ao-auth -n "${NAMESPACE}" >/dev/null 2>&1 \
    && kubectl get secret execution-plane-callback-client -n "${NAMESPACE}" >/dev/null 2>&1; then
    echo "=== Reuse existing EP TLS secrets ==="
    _secret_data execution-plane-ao-auth "${NAMESPACE}" ca.pem > "${WORK_DIR}/ca.pem"
  else
    echo "=== Generate EP API TLS ==="
    _generate_tls "${WORK_DIR}"
    kubectl create secret tls execution-plane-api-tls \
      --namespace "${NAMESPACE}" \
      --cert="${WORK_DIR}/tls.crt" --key="${WORK_DIR}/tls.key" \
      --dry-run=client -o yaml | kubectl apply -f -
    kubectl create secret tls execution-plane-callback-client \
      --namespace "${NAMESPACE}" \
      --cert="${WORK_DIR}/client.crt" --key="${WORK_DIR}/client.key" \
      --dry-run=client -o yaml | kubectl apply -f -
  fi

  kubectl create secret generic execution-plane-ao-auth \
    --namespace "${NAMESPACE}" \
    --from-file=jwt-public.pem="${WORK_DIR}/jwt-public.pem" \
    --from-file=ca.pem="${WORK_DIR}/ca.pem" \
    --dry-run=client -o yaml | kubectl apply -f -

  if kubectl get secret execution-plane-keys -n "${NAMESPACE}" >/dev/null 2>&1; then
    echo "=== Reuse existing credential encryption key ==="
    return 0
  fi
  echo "=== Mint credential encryption key ==="
  python3 -c "import os,base64; print(base64.urlsafe_b64encode(os.urandom(32)).decode(), end='')" \
    > "${WORK_DIR}/credential-encryption-key"
  kubectl create secret generic execution-plane-keys \
    --namespace "${NAMESPACE}" \
    --from-file=credential-encryption-key="${WORK_DIR}/credential-encryption-key" \
    --dry-run=client -o yaml | kubectl apply -f -
}

_apply_ep_workloads() {
  local overlay
  overlay=$(mktemp -d)
  cp "${BASE_DIR}/"*.yaml "${overlay}/"
  rm -f "${overlay}/kustomization.yaml"
  sed -i "s#image: ghcr.io/syntara-orchestration/execution-plane:latest#image: ${EP_IMAGE}#g" "${overlay}"/*.yaml
  sed -i 's/^  replicas: 2$/  replicas: 1/' "${overlay}/api.yaml"
  echo "=== Apply EP API, worker, and migrate Job (${EP_IMAGE}) ==="
  kubectl delete job execution-plane-migrate -n "${NAMESPACE}" --ignore-not-found --wait=true
  kubectl apply -n "${NAMESPACE}" -f "${overlay}"
  rm -rf "${overlay}"
}

_wait_for_migrate() {
  echo "=== Wait for execution-plane-migrate ==="
  if ! kubectl wait --for=condition=complete "job/execution-plane-migrate" \
    -n "${NAMESPACE}" --timeout=180s; then
    kubectl logs -n "${NAMESPACE}" "job/execution-plane-migrate" || true
    kubectl describe -n "${NAMESPACE}" "job/execution-plane-migrate" || true
    echo "ERROR: execution-plane-migrate did not complete" >&2
    exit 1
  fi
}

_kind_node_container() {
  printf '%s' "${CLUSTER_NAME}-control-plane"
}

_node_exec() {
  local node
  node=$(_kind_node_container)
  if command -v podman >/dev/null 2>&1 && podman inspect "${node}" >/dev/null 2>&1; then
    podman exec "${node}" "$@"
    return 0
  fi
  if command -v docker >/dev/null 2>&1 && docker inspect "${node}" >/dev/null 2>&1; then
    docker exec "${node}" "$@"
    return 0
  fi
  echo "ERROR: Kind node container ${node} not found" >&2
  return 1
}

_prepull_node_image() {
  local image=$1
  local node
  node=$(_kind_node_container)
  echo "=== Pre-pull ${image} on Kind node ${node} ==="
  _node_exec crictl pull "${image}"
  _node_exec crictl images
  echo "pre-pulled ${image} on ${node}"
}

_enable_script_dispatch() {
  local image=$1
  local images_json
  images_json=$(python3 -c 'import json, sys; print(json.dumps({"script": sys.argv[1]}))' "${image}")
  echo "=== Enable script nodes on ${AO_NAME}-worker ==="
  kubectl set env "deploy/${AO_NAME}-worker" -n "${AO_NAMESPACE}" \
    APP_SCRIPT_NODES_ENABLED=true \
    "APP_NODE_CONTAINER_IMAGES=${images_json}"
  kubectl rollout status "deploy/${AO_NAME}-worker" -n "${AO_NAMESPACE}" --timeout=180s
}

_register_cluster_binding() {
  local issuer=$1
  kubectl get configmap kube-root-ca.crt -n "${NAMESPACE}" \
    -o jsonpath='{.data.ca\.crt}' > "${WORK_DIR}/cluster-ca.pem"
  python3 -c "
import json, sys, uuid
from pathlib import Path
cluster, endpoint, namespace, issuer, token_path, ca_path = sys.argv[1:]
payload = {
    'source_integration_id': str(uuid.uuid5(uuid.NAMESPACE_URL, f'https://execution-plane.local/kind/{cluster}')),
    'issuer': issuer,
    'desired': {
        'name': cluster,
        'endpoint': endpoint,
        'namespace': namespace,
        'credential': Path(token_path).read_text(),
        'ca_certificate': Path(ca_path).read_text(),
        'enabled': True,
        'labels': {'execution-plane.syntara.io/local': 'kind'},
    },
}
json.dump(payload, sys.stdout)
" "${CLUSTER_NAME}" "${ENDPOINT}" "${NAMESPACE}" "${issuer}" "${TOKEN_FILE}" "${WORK_DIR}/cluster-ca.pem" \
    > "${WORK_DIR}/binding.json"

  kubectl exec -i -n "${NAMESPACE}" deploy/execution-plane-api -- \
    /bin/sh -c 'cat > /tmp/jwt-primary.pem' < "${WORK_DIR}/jwt-primary.pem"
  kubectl exec -i -n "${NAMESPACE}" deploy/execution-plane-api -- \
    /bin/sh -c 'cat > /tmp/binding.json' < "${WORK_DIR}/binding.json"

  echo "=== Register cluster binding via EP API ==="
  kubectl exec -i -n "${NAMESPACE}" deploy/execution-plane-api -- \
    /opt/app-root/src/.venv/bin/python - <<'PY'
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import jwt

payload = json.loads(Path("/tmp/binding.json").read_text())
key = Path("/tmp/jwt-primary.pem").read_text()
now = datetime.now(UTC)
token = jwt.encode(
    {
        "iss": payload["issuer"],
        "aud": "execution-plane",
        "client_id": "syntara-orchestration",
        "scope": "cluster-bindings:read cluster-bindings:write execution-targets:read",
        "all_projects": True,
        "iat": now,
        "exp": now + timedelta(hours=1),
    },
    key,
    algorithm="ES256",
)
sid = payload["source_integration_id"]
base = "https://127.0.0.1:8443"
headers = {"Authorization": f"Bearer {token}"}
with httpx.Client(verify=False, timeout=30.0) as client:
    got = client.get(f"{base}/v1/cluster-bindings/{sid}", headers=headers)
    revision = 1
    if got.status_code == 200:
        revision = int(got.json()["desired_revision"]) + 1
    body = dict(payload["desired"], revision=revision)
    put = client.put(f"{base}/v1/cluster-bindings/{sid}", headers=headers, json=body)
    if put.status_code >= 400:
        raise SystemExit(f"cluster-binding PUT failed ({put.status_code}): {put.text}")
    print(f"upserted cluster binding {sid} revision {revision} status={put.status_code}")
    deadline = time.time() + 90
    while time.time() < deadline:
        status, message = "unknown", ""
        got = client.get(f"{base}/v1/cluster-bindings/{sid}", headers=headers)
        if got.status_code == 200:
            doc = got.json()
            status = doc.get("status") or "unknown"
            message = doc.get("status_message") or ""
            print(f"cluster binding status: {status}")
            if status == "ready":
                break
            if status == "error":
                raise SystemExit(message or "cluster binding reconciliation failed")
        time.sleep(3)
    else:
        raise SystemExit(f"timed out waiting for cluster binding {sid} to become ready")
    targets = client.get(f"{base}/v1/execution-targets", headers=headers)
    if targets.status_code >= 400:
        raise SystemExit(f"listing execution targets failed ({targets.status_code}): {targets.text}")
    print(f"execution targets: {len(targets.json())}")
PY
  kubectl exec -n "${NAMESPACE}" deploy/execution-plane-api -- \
    rm -f /tmp/jwt-primary.pem /tmp/binding.json >/dev/null 2>&1 || true
}

echo "=== Cluster ==="
kubectl cluster-info
kubectl get namespace "${AO_NAMESPACE}" >/dev/null
echo "AO namespace ${AO_NAMESPACE} is present (operator remains there)"
echo "EP image: ${EP_IMAGE}"

WORK_DIR=$(mktemp -d)
chmod 700 "${WORK_DIR}"
trap _cleanup EXIT

echo "=== Apply ${INIT_YAML} ==="
kubectl apply -f "${INIT_YAML}"

echo "=== Verify ${NAMESPACE} ==="
kubectl get namespace "${NAMESPACE}"
kubectl get serviceaccount "${SA_NAME}" -n "${NAMESPACE}"
kubectl get role syntara-node-dispatcher -n "${NAMESPACE}"
kubectl get rolebinding syntara-node-dispatcher -n "${NAMESPACE}"

DISPATCHER_AS="system:serviceaccount:${NAMESPACE}:${SA_NAME}"

echo "=== RBAC: jobs in ${NAMESPACE} (must be yes) ==="
if ! kubectl auth can-i create jobs.batch --as="${DISPATCHER_AS}" -n "${NAMESPACE}"; then
  echo "ERROR: ${SA_NAME} cannot create jobs in ${NAMESPACE}" >&2
  exit 1
fi

echo "=== RBAC: list pods in ${NAMESPACE} (must be yes) ==="
if ! kubectl auth can-i list pods --as="${DISPATCHER_AS}" -n "${NAMESPACE}"; then
  echo "ERROR: ${SA_NAME} cannot list pods in ${NAMESPACE}" >&2
  exit 1
fi

echo "=== RBAC: create pods in ${NAMESPACE} (must be no) ==="
if kubectl auth can-i create pods --as="${DISPATCHER_AS}" -n "${NAMESPACE}"; then
  echo "ERROR: ${SA_NAME} can create pods in ${NAMESPACE}; Role is too wide" >&2
  exit 1
fi

echo "=== RBAC: jobs in ${AO_NAMESPACE} (must be no) ==="
if kubectl auth can-i create jobs.batch --as="${DISPATCHER_AS}" -n "${AO_NAMESPACE}"; then
  echo "ERROR: ${SA_NAME} can create jobs in ${AO_NAMESPACE}; Role is too wide" >&2
  exit 1
fi

echo "=== Long-lived ${SA_NAME} token ==="
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: syntara-dispatcher-token
  namespace: ${NAMESPACE}
  annotations:
    kubernetes.io/service-account.name: ${SA_NAME}
type: kubernetes.io/service-account-token
EOF
TOKEN=""
for ((i = 1; i <= 30; i++)); do
  TOKEN=$(kubectl get secret syntara-dispatcher-token -n "${NAMESPACE}" \
    -o jsonpath='{.data.token}' 2>/dev/null | base64 -d || true)
  if [[ -n "${TOKEN}" ]]; then
    break
  fi
  sleep 1
done
if [[ -z "${TOKEN}" ]]; then
  echo "ERROR: syntara-dispatcher-token was not populated" >&2
  exit 1
fi
printf '%s' "${TOKEN}" > "${TOKEN_FILE}"
chmod 600 "${TOKEN_FILE}"
echo "minted ${SA_NAME} token (${#TOKEN} chars)"
unset TOKEN

_ensure_ep_database
_ensure_keys_and_tls

AO_JWT_ISSUER=$(_ao_jwt_issuer)
echo "AO JWT issuer: ${AO_JWT_ISSUER}"
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: execution-plane-config
  namespace: ${NAMESPACE}
data:
  ao-jwt-issuer: "${AO_JWT_ISSUER}"
  ao-completion-callback-url: ""
EOF

_apply_ep_workloads
_wait_for_migrate

echo "=== Wait for execution-plane-api ==="
if ! kubectl rollout status deployment/execution-plane-api -n "${NAMESPACE}" --timeout=180s; then
  kubectl get pods -n "${NAMESPACE}" -o wide
  kubectl describe deployment/execution-plane-api -n "${NAMESPACE}" || true
  exit 1
fi
echo "=== Wait for execution-plane-worker ==="
if ! kubectl rollout status deployment/execution-plane-worker -n "${NAMESPACE}" --timeout=180s; then
  kubectl get pods -n "${NAMESPACE}" -o wide
  kubectl describe deployment/execution-plane-worker -n "${NAMESPACE}" || true
  exit 1
fi
kubectl get pods -n "${NAMESPACE}" -o wide

_register_cluster_binding "${AO_JWT_ISSUER}"
rm -f "${TOKEN_FILE}"

_prepull_node_image "${NODE_IMAGE}"
_enable_script_dispatch "${NODE_IMAGE}"

echo "execution-plane API and worker are up; script nodes dispatch ${NODE_IMAGE}"
echo "AO operator was not modified; EP is served from ${NAMESPACE}"
