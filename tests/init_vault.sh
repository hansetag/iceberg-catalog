#!/bin/sh
set -eux

vault login -address http://hvault:1234 myroot
vault auth  enable -address http://hvault:1234  userpass
echo "path \"secret/*\" { capabilities = [\"create\", \"read\", \"update\", \"delete\", \"list\"] }" > /tmp/app.hcl
vault policy write -address http://hvault:1234 app /tmp/app.hcl
vault write -address http://hvault:1234 auth/userpass/users/test password=test policies=app
# grant access to the secrets kv store to test user


