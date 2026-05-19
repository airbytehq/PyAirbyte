# PyAirbyte CLI convention comparison proposal

AJ asked for a comparison between the proposed PyAirbyte CLI in PyAirbyte PR 1010 and
the separate `airbytehq/airbyte-cli` codebase before applying CLI behavior changes.
This document is intentionally a research/proposal artifact only.

## Sources reviewed

- PyAirbyte PR 1010 branch: `devin/1775171846-airbyte-cli`
- PyAirbyte CLI implementation:
  - `airbyte/cli/cloud/`
  - `airbyte/cli/local/`
  - `airbyte/cli/_input.py`
  - `airbyte/cloud/credentials.py`
- `airbytehq/airbyte-cli` reference implementation:
  - `README.md`
  - `CONTEXT.md`
  - `AGENTS.md`
  - `cmd/`
  - `internal/registry/`
  - `internal/resources/`
  - `internal/auth/`

## Executive summary

PyAirbyte PR 1010 is already aligned with `airbyte-cli` on several high-level
ergonomics: plural resource groups, noun-before-verb command shape, JSON stdout,
credential files with restrictive permissions, explicit login/logout, and dedicated
`sync`/`wait` verbs.

The most important approval decisions before implementation are:

1. Whether PyAirbyte should follow `airbyte-cli`'s resource-detail verb `describe`,
   or keep the broader API/SDK-style `get`.
2. Whether PyAirbyte should preserve its current two-entrypoint shape
   (`airbyte-cloud`, `airbyte-local`) or move closer to `airbyte agents ...`.
3. Whether agent-oriented JSON payload ergonomics should use a single `--json`
   operation payload, as in `airbyte-cli`, or keep explicit typed options plus
   `--config-json`/`--config-file`.
4. Whether Cloud credential storage should intentionally use PyAirbyte's existing
   `~/.airbyte/credentials` YAML file, or mirror `airbyte-cli`'s
   `~/.airbyte-cli/settings.json` shape for muscle memory.
5. Whether self-managed/root flags should keep PyAirbyte's explicit split between
   `--public-api-root` and `--config-api-root`, or use `airbyte-cli`'s simpler
   `AIRBYTE_API_HOST` precedent for public API-only operations.

## Decision matrix

| Topic | Observed `airbyte-cli` precedent | Current PyAirbyte PR 1010 shape | Suggested decision/options | Requires later code changes? |
| --- | --- | --- | --- | --- |
| `get` vs `describe` | `connectors describe` is the detail/introspection verb for a connector, and agent docs require `connectors describe` before `connectors execute`. There is no production `get` operation in the shipped resource surface; `get` appears only in registry tests. | Cloud resources use `get` for workspaces, sources, destinations, connections, and jobs. | Decide whether user muscle memory from `airbyte-cli` should win for detail reads. Option A: rename detail commands to `describe`. Option B: keep `get` for standard CRUD resources, and reserve `describe` for connector schema/action introspection if PyAirbyte later adds that. | Yes if renaming commands or adding aliases. |
| Command grouping | Commands live under `airbyte agents <resource> <operation>`. Resources are plural nouns (`organizations`, `workspaces`, `connectors`), and operations are verbs (`list`, `use`, `describe`, `execute`, `create`, `delete`). | Commands live under separate console scripts: `airbyte-cloud <resource> <operation>` and `airbyte-local <operation/group>`. Cloud groups are plural nouns (`workspaces`, `sources`, `destinations`, `connections`, `jobs`). | Keep plural noun groups and verb leaf commands. Decide separately whether two entrypoints are intentional product positioning or whether a future top-level namespace should converge toward `airbyte cloud ...` / `airbyte local ...`. | No for keeping current grouping; yes for entrypoint/namespace changes. |
| Noun/verb order | Noun before verb: `connectors list`, `connectors describe`, `connectors execute`, `workspaces use`, `organizations use`. | Noun before verb for Cloud resources: `sources list`, `sources get`, `connections sync`, `jobs wait`. Local has top-level `sync` and grouped debug/connectors commands. | Keep noun-before-verb for Cloud. For `airbyte-local sync`, decide whether local sync is important enough as a top-level verb or should be grouped under a noun later. | Probably no for Cloud; yes if local hierarchy changes. |
| Login behavior | `airbyte agents login` defaults to browser PKCE login, bootstraps client credentials and organization ID, writes `~/.airbyte-cli/settings.json`, preserves non-auth settings, and supports `--manual` and `--org-id`. `login show` prints obfuscated saved settings. | `airbyte-cloud login` supports non-interactive client credentials. Interactive login is explicitly unavailable. Login writes a bearer token and API roots to `~/.airbyte/credentials`. `logout` removes local credentials. | Decide whether PyAirbyte should intentionally remain non-interactive first, or eventually mirror browser-first `airbyte-cli` login. If mirroring, consider `login show` and organization/workspace selection commands. | No for documenting current behavior; yes for browser login, `login show`, or org/workspace persistence features. |
| Logout behavior | No `logout` command is visible in the current `airbyte-cli` command surface; credential refresh is handled by login/settings. | `airbyte-cloud logout` exists and removes locally stored credentials. | Keep `logout`; it is expected in many CLIs and pairs naturally with PyAirbyte's bearer-token file. If strict parity is desired, document it as a PyAirbyte-specific convenience rather than a precedent match. | No if kept. |
| Credential file expectations | Reads env vars first, then `~/.airbyte-cli/settings.json`. Requires `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, and `AIRBYTE_ORGANIZATION_ID` together. Settings JSON includes credentials, organization ID, workspace, destructive-operation preference, telemetry, and version-check flags. Writes with `0600`. | Resolves explicit args, env vars, then `~/.airbyte/credentials` YAML. Supports bearer token or client credentials; stores `bearer_token`, `airbyte_api_root`, and `config_api_root`. Writes with `0600`. Workspace and organization IDs can resolve from env/file. | Decide whether PyAirbyte should use its existing `~/.airbyte/credentials` as the PyAirbyte-native location or adopt/also read `~/.airbyte-cli/settings.json` for CLI muscle memory. | Yes if changing file path/format or adding compatibility reads. |
| Positional IDs vs named ID args | `airbyte-cli` mostly uses JSON payloads and `--id` convenience. `connectors describe`, `execute`, and `delete` accept either `name` + `workspace` or `id`; passing both is rejected. | PR 1010 accepts one positional ID or a named ID flag like `--source-id`, `--destination-id`, `--connection-id`, or `--job-id`. If both are supplied and differ, it errors. | PyAirbyte's positional-or-named ID support is a useful human ergonomic. For agent parity, consider adding a generic `--id` alias or documenting named flags clearly. | No unless adding aliases/changing validation. |
| Names vs IDs | `airbyte-cli` emphasizes connector name resolution for human use, with server-side validation hooks. Workspace can default to configured or literal `default`. | PyAirbyte Cloud operations are ID-first for sources, destinations, connections, jobs, and workspaces. | Decide whether ID-first is acceptable for Airbyte Cloud resources. If adding name resolution later, follow `airbyte-cli`'s server-side lookup pattern rather than embedding user-supplied names in API paths. | Yes if adding name resolution/default workspace behavior. |
| JSON input conventions | Every operation supports `--json` for all params or per-parameter flags, mutually exclusive. Agent docs strongly recommend `--json '{...}'`. `@filename` loads JSON from a file. Object flags are JSON objects. | PR 1010 emits JSON stdout, but command inputs are typed flags. Connector configs use `--config-json` or `--config-file`; there is no universal operation-level `--json` payload. | Decide whether PyAirbyte should optimize for human typed flags, agent replayability, or both. Option A: keep typed flags and `--config-json`/`--config-file`. Option B: add a universal `--json` payload mode for Cloud commands. | Yes for universal `--json` support. |
| JSON output conventions | stdout is JSON. List responses generally return an envelope such as `{"data": [...]}`; `--fields` can filter output. Errors are structured JSON on stderr with stable exit codes. | stdout is pretty JSON via `orjson`; many list commands return a bare array rather than a `data` envelope. Error shape follows PyAirbyte exceptions/Cyclopts rather than the Go CLI's stable JSON error contract. | Decide whether API/agent parity needs `{"data": [...]}` list envelopes, `--fields`, or structured stderr errors. For library users, bare arrays may be simpler; for agents, envelopes and filters improve consistency. | Yes for output envelope/filter/error changes. |
| API root and self-managed flags | Global API root override is `AIRBYTE_API_HOST`, defaulting to `https://api.airbyte.ai`. The Go CLI currently models a single API host, not a split public/config API surface. | CLI args expose `--public-api-root`; login also exposes `--config-api-root`. Env aliases include `AIRBYTE_API_ROOT`, `AIRBYTE_CLOUD_API_URL`, `AIRBYTE_CONFIG_API_ROOT`, and `AIRBYTE_CLOUD_CONFIG_API_URL`. Self-managed login requires both roots. | Keep the explicit split if PyAirbyte truly needs both Public API and Config API roots. Consider whether `airbyte_api_root` should be renamed in outputs to `public_api_root` to match the flag, or whether compatibility with existing constants is more important. | No if accepted; yes for output/alias renames. |
| Self-managed naming | `airbyte-cli` does not expose a dedicated `--self-managed` flag; users override API host via env and credentials/settings. | PR 1010 uses root URL flags rather than a boolean `--self-managed` mode. Self-managed is inferred when API roots are provided. | Prefer root flags over `--self-managed` boolean. A boolean would still need URLs and could add ambiguity. | No. |
| Sync and wait ergonomics | The current `airbyte-cli` resource surface is connector-action oriented and does not expose connection sync/job wait commands. It does use browser credential polling for `connectors create`, with clear timeout/polling behavior. | `connections sync` supports `--wait`/`--no-wait` and `--wait-timeout`. `jobs wait` waits on a job by ID. | Keep explicit `sync` and `wait` commands. Decide whether default `connections sync` should wait or return immediately; current default is non-blocking, which is script-friendly. | No unless changing wait defaults or option names. |
| Destructive operation safety | `connectors delete` prompts interactively unless `allow_destructive` is set in settings or env (`AIRBYTE_ALLOW_DESTRUCTIVE`). Non-interactive default refuses rather than hanging. | `connections delete` has a `--force` flag that skips safe mode; `sources delete` and `destinations delete` use safe mode without an explicit force flag in the current command shape. | Decide whether destructive commands should consistently expose `--force`, `--allow-destructive`, or rely on safe mode. `airbyte-cli` precedent favors persistent safety settings plus non-interactive refusal. | Yes for flag/settings consistency. |
| Help text and flag style | Cobra/pflag uses kebab-case flags generated from snake_case schema params. Persistent flags include `--output, -o`, `--verbose, -v`, and `--fields`. Help text emphasizes JSON payloads, schemas, and agent safety. | Cyclopts uses typed Python annotations and kebab-case flags such as `--workspace-id`, `--public-api-root`, `--config-json`, `--wait-timeout`, and negative bools such as `--no-wait`. No `--fields`, `--output`, or schema command. | Keep kebab-case flags. Consider adding `--fields` later if agent/context-window ergonomics are important. `--output` is lower priority because shell redirection is sufficient for most PyAirbyte workflows. | No for current style; yes for `--fields`, `--output`, or schema/help additions. |
| Schema/introspection | `airbyte agents schema <resource> <operation>` prints merged CLI/OpenAPI parameter schema. `connectors describe` is mandatory before `execute` in agent guidance. | PR 1010 does not expose a schema/introspection command for CLI operations. Generated docs are produced from Cyclopts for documentation, not runtime discovery. | Decide whether runtime schema discovery matters for agent use. If yes, a lightweight command that emits Cyclopts-derived command metadata could provide parity without changing API behavior. | Yes. |
| Generated docs | `airbyte-cli` keeps agent docs and skills alongside code and has a usage guide in `CONTEXT.md`. | PR 1010 generates Markdown CLI docs under `docs/generated/` and includes them in pdoc output. | Keep generated docs out of this proposal PR. If CLI behavior changes later, regenerate docs as part of the implementation PR. | No for this proposal; yes in implementation PRs that change CLI surface. |

## Recommended approval checklist

Before implementing CLI behavior changes, ask AJ to approve these decisions:

- Detail verb: `get`, `describe`, or `get` with a future `describe` reserved for
  schema/action introspection.
- Entrypoint strategy: keep `airbyte-cloud`/`airbyte-local`, or converge toward a
  shared root namespace later.
- Input mode: typed flags only, or typed flags plus universal `--json` payloads.
- Credential compatibility: PyAirbyte-native `~/.airbyte/credentials`, Go CLI-style
  `~/.airbyte-cli/settings.json`, or read-compatible support for both.
- Output shape: bare arrays vs `{"data": [...]}` envelopes, and whether `--fields`
  should be added for agent/context-window ergonomics.
- Self-managed flag names: keep `--public-api-root`/`--config-api-root` and no
  `--self-managed` boolean.
- Sync default: keep non-blocking `connections sync` by default, with `--wait` opt-in.
