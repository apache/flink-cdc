#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

function transpile_if_needed() {
    local current_hash
    local last_compilation_hash
    current_hash=$(md5sum ./src/main.ts)
    last_compilation_hash=$(cat ./transpilation_state/main.ts.md5sum || true)
    if [[ ${last_compilation_hash} == "${current_hash}" ]]; then
        echo "No transpiling needed"
    else
        npm run-script release --scripts-prepend-node-path
        echo "${current_hash}" > ./transpilation_state/main.ts.md5sum
    fi
}

transpile_if_needed
