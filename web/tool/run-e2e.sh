#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
project="e2e-submitqueue-web"
token="submitqueue-e2e-local-token-000000000000"
gate_dir="$(mktemp -d -t submitqueue-web-e2e.XXXXXX)"
compose=(
  docker-compose
  -f "$repo_root/service/submitqueue/docker-compose.yml"
  -f "$repo_root/service/submitqueue/docker-compose.fake.yml"
  -f "$repo_root/service/submitqueue/docker-compose.web.yml"
  -p "$project"
)

cleanup() {
  local status=$?
  "${compose[@]}" down -v --remove-orphans || true
  rm -rf "$gate_dir"
  return "$status"
}
trap cleanup EXIT

export REPO_ROOT="$repo_root"
export SQ_PROVIDER_CONFIG_DIR="$repo_root/service/submitqueue/demo/provider/fake"
export SQ_CONSUMER_GATE_DIR="$gate_dir"
export SUBMITQUEUE_WEB_TOKEN="$token"
export SQ_MYSQL_INITDB_SKIP_TZINFO=1

if docker info --format '{{json .SecurityOptions}}' | grep -q 'name=rootless'; then
  export SQ_CONTAINER_USER=0:0
else
  export SQ_CONTAINER_USER="$(id -u):$(id -g)"
fi

"${compose[@]}" up -d --build --wait

for schema in "$repo_root"/submitqueue/extension/storage/mysql/schema/*.sql; do
  docker exec -i "$project-mysql-app-1" mysql -uroot -proot submitqueue < "$schema"
done
for schema in "$repo_root"/platform/extension/counter/mysql/schema/*.sql; do
  docker exec -i "$project-mysql-app-1" mysql -uroot -proot submitqueue < "$schema"
done
for schema in "$repo_root"/platform/extension/messagequeue/mysql/schema/*.sql; do
  docker exec -i "$project-mysql-queue-1" mysql -uroot -proot submitqueue < "$schema"
done

gateway_port="$(docker port "$project-gateway-service-1" 8080/tcp | head -1 | sed 's/.*://')"
web_port="$(docker port "$project-web-service-1" 3000/tcp | head -1 | sed 's/.*://')"
export SUBMITQUEUE_E2E_GATEWAY_URL="http://127.0.0.1:$gateway_port"
export SUBMITQUEUE_E2E_WEB_URL="http://127.0.0.1:$web_port"

corepack pnpm@10.17.1 --dir "$repo_root/web" e2e
