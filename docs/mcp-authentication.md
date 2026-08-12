# MCP server authentication

The PyAirbyte MCP HTTP server supports interactive OIDC and headless bearer
authentication. Configure the deployment with the `AIRBYTE_MCP_*` environment
variables described in the HTTP entry point's module documentation.

For a headless client that cannot refresh bearer tokens, enable the opt-in
static client-credentials path:

- `AIRBYTE_MCP_AUTH_ALLOW_CLIENT_CREDENTIALS`: set to a truthy value.
- `AIRBYTE_MCP_AUTH_CLIENT_CREDENTIALS_TOKEN_URL`: set to the deployment's
  OAuth token endpoint.

The client can send `Client-Id` and `Client-Secret` headers, or use
`Authorization: Basic base64(client_id:client_secret)`. The server exchanges
the credentials for a short-lived bearer token. This path is off by default.
Use TLS and prevent intermediaries from logging the credential headers.
