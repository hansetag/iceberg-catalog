#!/bin/sh
set -eux

VAULT_ADDR=${1:-http://hvault:8200}

vault login -address "$VAULT_ADDR" myroot
vault auth enable -address "$VAULT_ADDR" userpass
echo "path \"secret/*\" { capabilities = [\"create\", \"read\", \"update\", \"delete\", \"list\"] }" > /tmp/app.hcl
vault policy write -address "$VAULT_ADDR" app /tmp/app.hcl
vault write -address "$VAULT_ADDR" auth/userpass/users/test password=test policies=app
vault status -address "$VAULT_ADDR"