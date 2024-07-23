#!/bin/sh
set -eux

vault login -address http://hvault:1234 myroot
vault auth  enable -address http://hvault:1234  userpass
vault write -address http://hvault:1234 auth/userpass/users/test password=test policies=default
