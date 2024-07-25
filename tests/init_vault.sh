#!/bin/sh
set -eux

vault login -address http://hvault:8200 myroot
vault auth  enable -address http://hvault:8200  userpass
echo "path \"secret/*\" { capabilities = [\"create\", \"read\", \"update\", \"delete\", \"list\"] }" > /tmp/app.hcl
vault policy write -address http://hvault:8200 app /tmp/app.hcl
vault write -address http://hvault:8200 auth/userpass/users/test password=test policies=app
vault status -address http://hvault:8200
# grant access to the secrets kv store to test user


