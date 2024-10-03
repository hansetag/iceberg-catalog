import conftest
import pytest
import base64
import json
import urllib.parse
import keycloak


def get_access_token(new_keycloak_client: conftest.KeycloakClient) -> str:
    connection = keycloak.KeycloakOpenIDConnection(
        server_url=conftest.OPENID_PROVIDER_URI.rstrip("/realm/test"),
        client_id=new_keycloak_client.client_id,
        client_secret_key=new_keycloak_client.client_secret,
        realm_name="test",
        verify=True,
    )
    connection.refresh_token()
    return connection.token["access_token"]


def test_create_user(warehouse: conftest.Warehouse, new_keycloak_client: conftest.KeycloakClient):
    access_token = get_access_token(new_keycloak_client)
    user: conftest.User = warehouse.create_user(access_token)
    token = json.loads(base64.b64decode(warehouse.server.access_token.split(".")[1]).decode("utf-8"))

    assert user.user_origin == "explicit-via-register-call", f"is: {user.user_origin} should: explicit-via-register-call"
    user_id = user.id.split("/")[1]
    assert user_id == token["sub"], f"is: {user_id} should: {token['sub']}"
    encoded_issuer = user.id.split("/")[0]
    assert encoded_issuer == urllib.parse.urlencode(
        token["iss"]), f"is: {encoded_issuer} should: {urllib.parse.urlencode(token['iss'])}"
    assert user.email == token.get("email", None)
    assert user.updated_at is None

    # Check that the user is in the database
    users = warehouse.list_users(access_token, name_filter=user.name)
    assert len(users.users) == 1
    assert users.users[0].id == user.id
    assert users.users[0].name == user.name
    assert users.users[0].email == user.email
    assert users.users[0].updated_at is None
    assert users.users[0].user_origin == user.user_origin

    warehouse.delete_user(access_token, user.id)

    users = warehouse.list_users(access_token, name_filter=user.name)
    assert len(users.users) == 0
